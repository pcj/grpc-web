//Copyright 2017 Improbable. All Rights Reserved.
// See LICENSE for licensing terms.

package grpcweb_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/websocket"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	testproto "github.com/improbable-eng/grpc-web/integration_test/go/_proto/improbable/grpcweb/test"
	tpb "github.com/improbable-eng/grpc-web/integration_test/go/_proto/improbable/grpcweb/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
)

var (
	pingEmptyEndpoint = "/improbable.grpcweb.test.TestService/PingEmpty"
	pingEndpoint      = "/improbable.grpcweb.test.TestService/Ping"
)

// headers := http.Header{}
// headers.Add("Access-Control-Request-Method", "POST")
// headers.Add("Access-Control-Request-Headers", "origin, x-something-custom, x-grpc-web, accept")

func TestWebSockets(t *testing.T) {
	testCases := []*websocketTestCase{
		// This case checks that if we try and dial a websocket server without
		// the correct subprotocol, server responds with bad connection.
		websocketTest(
			"connection refused without subprotocol 'grpc-websockets",
			withTimeout(1*time.Second),
			withSetup(
				withGrpcLogging(),
				withGrpcListener(),
				withTestServiceGrpcServer(),
				withWrappedServer(
					grpcweb.WithWebsockets(true),
					grpcweb.WithWebsocketPingInterval(time.Second*10),
					grpcweb.WithWebsocketOriginFunc(func(req *http.Request) bool { return true }),
				),
				withHttpListener(),
				withHttpServer(),
				withWebSocketConnectionError(pingEmptyEndpoint, func(t *testing.T, w *http.Response, err error) {
					if err == nil {
						t.Fatalf("expected websocket connection error")
					}
					// FIXME: is internal server error actually what we want?
					assert.Equal(t, http.StatusInternalServerError, w.StatusCode,
						"expected internal server error when websocket subprotocol missing")
				}),
			),
			withTeardown(
				withGrpcServerTeardown(),
				withHttpServerTeardown(),
			),
		),
		// this case demonstrates what happens when the connection headers are
		// not provided.  These are typically provided as the first payload.
		websocketTest(
			"response error when connection headers are not provided",
			withTimeout(1*time.Second),
			withSetup(
				withGrpcLogging(),
				withGrpcListener(),
				withTestServiceGrpcServer(),
				withWrappedServer(
					grpcweb.WithWebsockets(true),
					grpcweb.WithWebsocketPingInterval(time.Second*10),
					grpcweb.WithWebsocketOriginFunc(func(req *http.Request) bool { return true }),
				),
				withHttpListener(),
				withHttpServer(),
				withWebSocketConnection(pingEndpoint),
			),
			withTeardown(
				withWebsocketTeardown(),
				withGrpcServerTeardown(),
				withHttpServerTeardown(),
			),
			withExecution(
				withWebsocketSendProto(&tpb.PingRequest{}),
				withWebsocketRecvProto(&tpb.PingResponse{}),
			),
		),
		// this case demonstrates that when we use the correct subprotocol, we
		// can send and recv a proto from the endpoint
		websocketTest(
			"connects with correct subprotocol and headers",
			withOnly(),
			withTimeout(1*time.Second),
			withSetup(
				withGrpcLogging(),
				withGrpcListener(),
				withTestServiceGrpcServer(),
				withWrappedServer(
					grpcweb.WithWebsockets(true),
					grpcweb.WithWebsocketPingInterval(time.Second*10),
					grpcweb.WithWebsocketOriginFunc(func(req *http.Request) bool { return true }),
				),
				withHttpListener(),
				withHttpServer(),
				withWebSocketConnection(pingEndpoint),
			),
			withTeardown(
				withWebsocketTeardown(),
				withGrpcServerTeardown(),
				withHttpServerTeardown(),
			),
			withExecution(
				withWebsocketSendHeaders(map[string]string{
					"content-type": "application/grpc",
				}),
				withWebsocketSendProto(&tpb.PingRequest{}),
				// withWebsocketFinishSendFrame(),
				withWebsocketRecvProto(&tpb.PingResponse{}),
			),
		),
	}

	var only *websocketTestCase
	for _, tc := range testCases {
		if tc.only {
			only = tc
		}
	}
	if only != nil {
		testCases = []*websocketTestCase{only}
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: %s", i, tc.d), func(t *testing.T) {
			tc.Run(t)
		})
	}
}

// testCaseInitializer initially modifies the test case
type testCaseInitializer func(*websocketTestCase) *websocketTestCase

// testCaseSetup performs setup actions
type testCaseSetup func(*websocketTestCase, *testing.T) error

// testCaseSetup performs teardown actions
type testCaseTeardown func(*websocketTestCase, *testing.T)

// testCaseExecution are functions that are run after setup.  All executions are
// run in parallel.  Execution functions should signal completed via wg.Done().
// Perfectly reasonable to perform assertions in these functions as well.
type testCaseExecution func(*websocketTestCase, *testing.T, *sync.WaitGroup)

// testCaseAssertion makes additional assertions after all the exections have
// occurred
type testCaseAssertion func(*websocketTestCase, *testing.T)

// websocketTestCase represents a single test case of the websocket subsystem.
// Each case sets up a full client and server and performs rpc calls between
// them.
type websocketTestCase struct {
	d          string              // test description
	only       bool                // only run this test case
	setup      []testCaseSetup     // setup functions
	teardown   []testCaseTeardown  // teardown functions
	execution  []testCaseExecution // execution functions called in parallel
	assertions []testCaseAssertion // post test assertions
	timeout    time.Duration

	httpListener  net.Listener
	grpcListener  net.Listener
	grpcServer    *grpc.Server
	httpServer    *http.Server
	wrappedServer *grpcweb.WrappedGrpcServer
	testClient    tpb.TestServiceClient
	websocketConn *websocket.Conn
	writeMux      sync.Mutex // protects websocketConn writes
	readMux       sync.Mutex // protects websocketConn reads
}

func (tc *websocketTestCase) Run(t *testing.T) {
	t.Log("setup")
	for i, setup := range tc.setup {
		if err := setup(tc, t); err != nil {
			t.Fatalf("%d setup error: %v", i, err)
		}
	}

	defer func() {
		t.Log("teardown")
		for _, teardown := range tc.teardown {
			teardown(tc, t)
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(len(tc.execution))

	t.Log("perform executions")
	for i, execute := range tc.execution {
		t.Logf("execute #%d", i)
		execute(tc, t, wg)
	}

	if waitTimeout(wg, tc.timeout) {
		fmt.Println("Timed out waiting for execution wait group")
	} else {
		fmt.Println("Wait group finished")
	}

	t.Log("perform assertions")
	for _, assert := range tc.assertions {
		assert(tc, t)
	}

	t.Fatal("DONE?")
}

func websocketTest(d string, initializers ...testCaseInitializer) *websocketTestCase {
	test := &websocketTestCase{
		d: d,
	}
	for _, f := range initializers {
		test = f(test)
	}
	return test
}

func withOnly() testCaseInitializer {
	return func(tc *websocketTestCase) *websocketTestCase {
		tc.only = true
		return tc
	}
}

func withTimeout(d time.Duration) testCaseInitializer {
	return func(tc *websocketTestCase) *websocketTestCase {
		tc.timeout = d
		return tc
	}
}

func withSetup(setup ...testCaseSetup) testCaseInitializer {
	return func(tc *websocketTestCase) *websocketTestCase {
		tc.setup = setup
		return tc
	}
}

func withTeardown(teardown ...testCaseTeardown) testCaseInitializer {
	return func(tc *websocketTestCase) *websocketTestCase {
		tc.teardown = teardown
		return tc
	}
}

func withAssertions(assertions ...testCaseAssertion) testCaseInitializer {
	return func(tc *websocketTestCase) *websocketTestCase {
		tc.assertions = tc.assertions
		return tc
	}
}

func withExecution(execution ...testCaseExecution) testCaseInitializer {
	return func(tc *websocketTestCase) *websocketTestCase {
		tc.execution = execution
		return tc
	}
}

func withTestServiceGrpcServer() testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		tc.grpcServer = grpc.NewServer()
		tpb.RegisterTestServiceServer(tc.grpcServer, &recordingTestServiceImpl{})
		go func() {
			t.Logf("starting grpcServer %v", tc.grpcListener.Addr())
			tc.grpcServer.Serve(tc.grpcListener)
		}()
		return nil
	}
}

func withHttpServer() testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		tc.httpServer = &http.Server{
			Handler: http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
				tc.wrappedServer.ServeHTTP(resp, req)
			}),
		}
		go func() {
			t.Logf("starting httpServer %v", tc.httpListener.Addr())
			tc.httpServer.Serve(tc.httpListener)
		}()
		return nil
	}
}

func withHttpListener() testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		tc.httpListener = listener
		return nil
	}
}

func withGrpcListener() testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		tc.grpcListener = listener
		return nil
	}
}

func withWebSocketConnection(path string) testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		u := url.URL{Scheme: "ws", Host: tc.httpListener.Addr().String(), Path: path}
		t.Logf("connecting to %s", u.String())
		// ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*3)
		// defer cancelFunc()
		dialer := &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 3 * time.Second,
			Subprotocols:     []string{grpcweb.GrpcWebsocketsSubprotocolName},
		}
		c, _, err := dialer.Dial(u.String(), nil)
		if err != nil {
			return fmt.Errorf("could not dial websocket: %v", err)
		}
		tc.websocketConn = c
		return nil
	}
}

// withWebSocketConnectionError attempts to connect to the given websocket path.
// Caller is presented with the http response and error from the websocket
// Dialer in anticipation that the error will be validated.
func withWebSocketConnectionError(path string, callback func(t *testing.T, resp *http.Response, err error)) testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		u := url.URL{
			Scheme: "ws",
			Host:   tc.httpListener.Addr().String(),
			Path:   path,
		}
		t.Logf("connecting to %s (expecting this to fail...)", u.String())
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*3)
		defer cancelFunc()
		_, resp, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
		callback(t, resp, err)
		return nil
	}
}

func withGrpcLogging() testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		grpclog.SetLogger(log.New(os.Stderr, "websocket_test: ", log.LstdFlags))
		return nil
	}
}

func withWrappedServer(options ...grpcweb.Option) testCaseSetup {
	return func(tc *websocketTestCase, t *testing.T) error {
		tc.wrappedServer = grpcweb.WrapServer(tc.grpcServer, options...)
		t.Logf("wrapped grpc server")
		return nil
	}
}

func withGrpcServerTeardown() testCaseTeardown {
	return func(tc *websocketTestCase, t *testing.T) {
		tc.grpcServer.Stop()
		tc.grpcListener.Close()
	}
}

func withHttpServerTeardown() testCaseTeardown {
	return func(tc *websocketTestCase, t *testing.T) {
		tc.httpServer.Shutdown(context.Background())
		tc.httpListener.Close()
	}
}

func withWebsocketCloseMessage() testCaseTeardown {
	return func(tc *websocketTestCase, t *testing.T) {
		if err := tc.websocketConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
			t.Errorf("websocket close error: %v", err)
		}
	}
}

func withWebsocketTeardown() testCaseTeardown {
	return func(tc *websocketTestCase, t *testing.T) {
		tc.websocketConn.Close()
	}
}

func withWebsocketExecution(fn func(t *testing.T, conn *websocket.Conn, wg *sync.WaitGroup)) testCaseExecution {
	return func(tc *websocketTestCase, t *testing.T, wg *sync.WaitGroup) {
		fn(t, tc.websocketConn, wg)
	}
}

// withWebsocketSendBinaryMessage provides the caller with a writer that can be
// used to send a single message.
func withWebsocketSendBinaryMessage(callback func(t *testing.T, out io.Writer)) testCaseExecution {
	return func(tc *websocketTestCase, t *testing.T, wg *sync.WaitGroup) {
		defer wg.Done()
		tc.writeMux.Lock()
		defer tc.writeMux.Unlock()
		out, err := tc.websocketConn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			t.Errorf("could not get next websocket writer: %v", err)
			return
		}
		callback(t, out)
		out.Close()
		t.Log("sent binary message")
	}
}

// withWebsocketSendHeaders prepares a function that sends a set of headers over
// the websocket.
func withWebsocketSendHeaders(h map[string]string) testCaseExecution {
	headers := http.Header{}
	for k, v := range h {
		headers.Add(k, v)
	}
	return withWebsocketSendBinaryMessage(func(t *testing.T, out io.Writer) {
		out.Write(grpcweb.EncodeHeaderFrame(headers))
		t.Logf("sent headers %v", h)
	})
}

// withWebsocketFinishSendFrame prepares a function that sends a finish send
// frame to the websocket.
func withWebsocketFinishSendFrame() testCaseExecution {
	return withWebsocketSendBinaryMessage(func(t *testing.T, out io.Writer) {
		out.Write([]byte{1})
		t.Logf("sent finish send control frame")
	})
}

// withWebsocketSendProto prepares a function that sends a single protobuf over
// the websocket.
func withWebsocketSendProto(msg proto.Message) testCaseExecution {
	return withWebsocketSendBinaryMessage(func(t *testing.T, out io.Writer) {
		data, err := proto.Marshal(msg)
		if err != nil {
			t.Errorf("could not marshal: %v", err)
			return
		}
		out.Write(encodeMessage(data))
		t.Logf("sent proto message %v", msg)
	})
}

// withWebsocketNextBinaryMessage provides the caller with a writer that can be
// used to send a single message.
func withWebsocketRecvBinaryMessage(callback func(t *testing.T, data []byte)) testCaseExecution {
	return func(tc *websocketTestCase, t *testing.T, wg *sync.WaitGroup) {
		t.Log("expecting to recv binary message")
		defer wg.Done()
		tc.readMux.Lock()
		defer tc.readMux.Unlock()
		messageType, r, err := tc.websocketConn.NextReader()
		if err != nil {
			t.Fatalf("could not read next websocket message: %v", err)
		}
		if messageType != websocket.BinaryMessage {
			t.Fatalf("expected binary message, not %v", messageType)
		}
		data, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatalf("could not read websocket message: %v", err)
		}
		t.Logf("recv'd binary message %q with len %d: %v", string(data), len(data), data)

		callback(t, data)
	}
}

// withWebsocketRecvProto prepares a function that expects to recv a proto
// message that matches the given value.
func withWebsocketRecvProto(msg proto.Message) testCaseExecution {
	return withWebsocketRecvBinaryMessage(func(t *testing.T, data []byte) {
		reader := bytes.NewReader(data)
		responseMessages := make([][]byte, 0)
		var trailers grpcweb.Trailer
		for {
			grpcPreamble := []byte{0, 0, 0, 0, 0}
			readCount, err := reader.Read(grpcPreamble)
			if err == io.EOF {
				break
			}
			if readCount != 5 || err != nil {
				t.Fatalf("Unexpected end of body in preamble: %v", err)
			}
			payloadLength := binary.BigEndian.Uint32(grpcPreamble[1:])
			payloadBytes := make([]byte, payloadLength)

			readCount, err = reader.Read(payloadBytes)
			if uint32(readCount) != payloadLength || err != nil {
				t.Fatalf("Unexpected end of msg: %v", err)
			}
			if grpcPreamble[0]&(1<<7) == (1 << 7) { // MSB signifies the trailer parser
				trailers = readTrailersFromBytes(t, payloadBytes)
			} else {
				responseMessages = append(responseMessages, payloadBytes)
			}
		}

		if len(responseMessages) != 1 {
			t.Fatalf("expected a single payload for proto: %v", responseMessages)
		}

		grpclog.Infof("trailer: %+v", trailers)

		payload := responseMessages[0]

		// expecting that a data payload starts with '0'
		if len(payload) == 0 {
			t.Fatalf("expected non-zero length payload: %v", payload)
		}
		if payload[0] != 0 {
			t.Fatalf("expected payload '0' header byte: %v", payload)
		}
		clone := proto.Clone(msg)
		clone.Reset()
		if err := proto.Unmarshal(payload[1:], msg); err != nil {
			t.Errorf("could not unmarshal: %v into %+v", err, clone)
		}
		if !proto.Equal(msg, clone) {
			t.Errorf("websocket recv proto error: want %+v, got %+v", msg, clone)
		}
	})
}

// withWebsocketRecvProto prepares a function that expects to recv a set of
// headers matching the given value
func withWebsocketRecvHeader(expected map[string]string) testCaseExecution {
	return withWebsocketRecvBinaryMessage(func(t *testing.T, data []byte) {
		actual := make(map[string]string)

		assert.EqualValues(t, expected, actual)
	})
}

type recordingTestServiceImpl struct {
}

func (s *recordingTestServiceImpl) PingEmpty(ctx context.Context, _ *google_protobuf.Empty) (*testproto.PingResponse, error) {
	grpc.SendHeader(ctx, expectedHeaders)
	grpclog.Printf("Handling PingEmpty")
	grpc.SetTrailer(ctx, expectedTrailers)
	return &testproto.PingResponse{Value: "foobar"}, nil
}

func (s *recordingTestServiceImpl) Ping(ctx context.Context, ping *testproto.PingRequest) (*testproto.PingResponse, error) {
	grpc.SendHeader(ctx, expectedHeaders)
	grpclog.Printf("Handling Ping")
	grpc.SetTrailer(ctx, expectedTrailers)
	return &testproto.PingResponse{Value: ping.Value}, nil
}

func (s *recordingTestServiceImpl) PingError(ctx context.Context, ping *testproto.PingRequest) (*google_protobuf.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if _, exists := md[useFlushForHeaders]; exists {
		grpc.SendHeader(ctx, expectedHeaders)
		grpclog.Printf("Handling PingError with flushed headers")

	} else {
		grpc.SetHeader(ctx, expectedHeaders)
		grpclog.Printf("Handling PingError without flushing")
	}
	grpc.SetTrailer(ctx, expectedTrailers)
	return nil, grpc.Errorf(codes.Unimplemented, "Not implemented PingError")
}

func (s *recordingTestServiceImpl) PingList(ping *testproto.PingRequest, stream testproto.TestService_PingListServer) error {
	stream.SendHeader(expectedHeaders)
	stream.SetTrailer(expectedTrailers)
	grpclog.Printf("Handling PingList")
	for i := int32(0); i < int32(expectedListResponses); i++ {
		stream.Send(&testproto.PingResponse{Value: fmt.Sprintf("%s %d", ping.Value, i), Counter: i})
	}
	return nil
}

func (s *recordingTestServiceImpl) PingStream(stream testproto.TestService_PingStreamServer) error {
	stream.SendHeader(expectedHeaders)
	stream.SetTrailer(expectedTrailers)
	grpclog.Printf("Handling PingStream")
	allValues := ""
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			stream.SendAndClose(&testproto.PingResponse{
				Value: allValues,
			})
			return nil
		}
		if err != nil {
			return err
		}
		if allValues == "" {
			allValues = in.GetValue()
		} else {
			allValues = allValues + "," + in.GetValue()
		}
		if in.FailureType == testproto.PingRequest_CODE {
			if in.ErrorCodeReturned == 0 {
				stream.SendAndClose(&testproto.PingResponse{
					Value: allValues,
				})
				return nil
			} else {
				return grpc.Errorf(codes.Code(in.ErrorCodeReturned), "Intentionally returning status code: %d", in.ErrorCodeReturned)
			}
		}
	}
}

func (s *recordingTestServiceImpl) Echo(ctx context.Context, text *testproto.TextMessage) (*testproto.TextMessage, error) {
	grpc.SendHeader(ctx, expectedHeaders)
	grpclog.Printf("Handling Echo")
	grpc.SetTrailer(ctx, expectedTrailers)
	return text, nil
}

func (s *recordingTestServiceImpl) PingPongBidi(stream testproto.TestService_PingPongBidiServer) error {
	stream.SendHeader(expectedHeaders)
	stream.SetTrailer(expectedTrailers)
	grpclog.Printf("Handling PingPongBidi")
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if in.FailureType == testproto.PingRequest_CODE {
			if in.ErrorCodeReturned == 0 {
				stream.Send(&testproto.PingResponse{
					Value: in.Value,
				})
				return nil
			} else {
				return grpc.Errorf(codes.Code(in.ErrorCodeReturned), "Intentionally returning status code: %d", in.ErrorCodeReturned)
			}
		}
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

// encodes a set of messages as a preamble-delimited list of bytes
func encodeMessage(msgs ...[]byte) []byte {
	buf := new(bytes.Buffer)
	for _, msgBytes := range msgs {
		grpcPreamble := []byte{0, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(grpcPreamble[1:], uint32(len(msgBytes)))
		buf.Write(grpcPreamble)
		buf.Write(msgBytes)
	}
	return buf.Bytes()
}

// // encodes a set of messages as a preamble-delimited list of bytes
// func encodeHeader(h http.Header) []byte {
// 	var s string
// 	for k, vals := range h {
// 		s += fmt.Sprintf("%s: %s\r\n", k, strings.Join(vals, ","))
// 	}
// 	return []byte(s)
// }
