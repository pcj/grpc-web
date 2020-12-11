package grpcweb

import (
	"bufio"
	"context"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/grpclog"
)

var (
	// GrpcWebsocketsSubprotocolName is the subprotocol that websocket clients
	// must provide when connecting
	GrpcWebsocketsSubprotocolName = "grpc-websockets"
	websocketUpgrader             = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
		Subprotocols:    []string{GrpcWebsocketsSubprotocolName},
	}
)

// wrappedGrpcWebsocketHandler encapsulates the websocket handler for incoming
// websocket requests. It is similar to wrappedGrpcServer
type wrappedGrpcWebsocketHandler struct {
	opts           *options
	handler        http.Handler
	allowedHeaders []string
	endpointFunc   func(req *http.Request) string
	hub            *websocketClientHub
}

// newWrappedWebsocketServer constructs a new wrappedGrpcWebsocketHandler.  The
// client hub is initialized and it's run() function is immediately started in a
// separate goroutine.
func newWrappedWebsocketServer(
	opts *options,
	handler http.Handler,
	endpointFunc func(req *http.Request) string,
) *wrappedGrpcWebsocketHandler {

	hub := newWebsocketClientHub()
	go hub.run()

	return &wrappedGrpcWebsocketHandler{
		opts:         opts,
		handler:      handler,
		endpointFunc: endpointFunc,
		hub:          hub,
	}
}

// ServeHTTP takes a HTTP request that is assumed to be a gRPC-Websocket request
// and wraps it with a compatibility layer to transform it to a standard gRPC
// request for the wrapped gRPC server and transforms the response to comply
// with the gRPC-Web protocol.
func (w *wrappedGrpcWebsocketHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	wsConn, err := websocketUpgrader.Upgrade(resp, req, nil)
	if err != nil {
		grpclog.Errorf("Unable to upgrade websocket request: %v", err)
		return
	}

	messageType, readBytes, err := wsConn.ReadMessage()
	if err != nil {
		grpclog.Errorf("Unable to read first websocket message: %v", err)
		return
	}

	if messageType != websocket.BinaryMessage {
		grpclog.Errorf("First websocket message is non-binary")
		return
	}

	headers := make(http.Header)
	for _, name := range w.allowedHeaders {
		if values, exist := req.Header[name]; exist {
			headers[name] = values
		}
	}
	wsHeaders, err := parseHeaders(string(readBytes))
	if err != nil {
		grpclog.Errorf("Unable to parse websocket headers: %v", err)
		return
	}
	for name, values := range wsHeaders {
		headers[name] = values
	}

	client := &websocketClient{
		hub:  w.hub,
		conn: wsConn,
		send: make(chan []byte, 256),
	}
	client.hub.register <- client

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	go client.writePump()
	// go client.readPump()

	respWriter := newWebSocketResponseWriter(client)
	if w.opts.websocketPingInterval >= time.Second {
		// TODO(pcj): what is the use case for no ping?
		// respWriter.enablePing(w.opts.websocketPingInterval)
	}

	ctx, cancelFunc := context.WithCancel(req.Context())
	defer cancelFunc()

	wrappedReader := newWebsocketWrappedReader(wsConn, respWriter, cancelFunc)

	req.Body = wrappedReader
	req.Method = http.MethodPost
	req.Header = headers

	interceptedRequest, isTextFormat := hackIntoNormalGrpcRequest(req.WithContext(ctx))
	if isTextFormat {
		grpclog.Errorf("web socket text format requests not yet supported")
	}

	req.URL.Path = w.endpointFunc(req)
	w.handler.ServeHTTP(respWriter, interceptedRequest)
}

func defaultWebsocketOriginFunc(req *http.Request) bool {
	origin, err := WebsocketRequestOrigin(req)
	if err != nil {
		grpclog.Warning(err)
		return false
	}
	return origin == req.Host
}

// isGrpcWebSocketRequest determines if a request is a gRPC-Web request by
// checking that the "Sec-Websocket-Protocol" header value is "grpc-websockets"
func isGrpcWebSocketRequest(req *http.Request) bool {
	return strings.ToLower(req.Header.Get("Upgrade")) == "websocket" &&
		strings.ToLower(req.Header.Get("Sec-Websocket-Protocol")) == GrpcWebsocketsSubprotocolName
}

func parseHeaders(headerString string) (http.Header, error) {
	reader := bufio.NewReader(strings.NewReader(headerString + "\r\n"))
	tp := textproto.NewReader(reader)

	mimeHeader, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, err
	}

	// http.Header and textproto.MIMEHeader are both just a map[string][]string
	return http.Header(mimeHeader), nil
}
