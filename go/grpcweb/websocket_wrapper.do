package grpcweb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/grpclog"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var (
	grpcWebsocketsSubprotocolName = "grpc-websockets"
	websocketUpgrader             = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
		Subprotocols:    []string{grpcWebsocketsSubprotocolName},
	}
)

// wrappedGrpcWebsocketServer encapsulates the websocket handler for incoming
// websocket requests. It is similar to wrappedGrpcServer
type wrappedGrpcWebsocketServer struct {
	opts           *options
	handler        http.Handler
	allowedHeaders []string
	endpointFunc   func(req *http.Request) string
	hub            *websocketClientHub
}

// newWrappedWebsocketServer constructs a new wrappedGrpcWebsocketServer.  The
// client hub is initialized and it's run() function is immediately started in a
// separate goroutine.
func newWrappedWebsocketServer(
	opts *options,
	handler http.Handler,
	endpointFunc func(req *http.Request) string,
) *wrappedGrpcWebsocketServer {

	hub := newWebsocketClientHub()
	go hub.run()

	return &wrappedGrpcWebsocketServer{
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
func (w *wrappedGrpcWebsocketServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
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
	go client.readPump()

	respWriter := newWebSocketResponseWriter(wsConn, client)
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

// webSocketResponseWriter acts as a http.ResponseWriter.  It accepts bytes
// written to the response and propagates them to the websocket.  If ping is
// enabled, this also sends pings according to the timoutOutInterval when.
type webSocketResponseWriter struct {
	writtenHeaders bool
	wsConn         *websocket.Conn
	headers        http.Header
	flushedHeaders http.Header
	client         *websocketClient
}

func newWebSocketResponseWriter(wsConn *websocket.Conn, client *websocketClient) *webSocketResponseWriter {
	return &webSocketResponseWriter{
		writtenHeaders: false,
		headers:        make(http.Header),
		flushedHeaders: make(http.Header),
		wsConn:         wsConn,
		client:         client,
	}
}

// Header implements part of the https://golang.org/pkg/net/http/#ResponseWriter
// interface.
func (w *webSocketResponseWriter) Header() http.Header {
	return w.headers
}

// Write implements part of the https://golang.org/pkg/net/http/#ResponseWriter
// interface.
func (w *webSocketResponseWriter) Write(b []byte) (int, error) {
	if !w.writtenHeaders {
		w.WriteHeader(http.StatusOK)
	}
	return len(b), w.writeMessage(b)
}

// WriteHeader implements part of the https://golang.org/pkg/net/http/#ResponseWriter
// interface.
func (w *webSocketResponseWriter) WriteHeader(code int) {
	w.copyFlushedHeaders()
	w.writtenHeaders = true
	w.writeHeaderFrame(w.headers)
	return
}

func (w *webSocketResponseWriter) writeHeaderFrame(headers http.Header) {
	headerBuffer := new(bytes.Buffer)
	headers.Write(headerBuffer)
	headerGrpcDataHeader := []byte{1 << 7, 0, 0, 0, 0} // MSB=1 indicates this is a header data frame.
	binary.BigEndian.PutUint32(headerGrpcDataHeader[1:5], uint32(headerBuffer.Len()))
	w.writeMessage(headerGrpcDataHeader)
	w.writeMessage(headerBuffer.Bytes())
}

func (w *webSocketResponseWriter) copyFlushedHeaders() {
	copyHeader(
		w.flushedHeaders, w.headers,
		skipKeys("trailer"),
		keyCase(http.CanonicalHeaderKey),
	)
}

func (w *webSocketResponseWriter) extractTrailerHeaders() http.Header {
	th := make(http.Header)
	copyHeader(
		th, w.headers,
		skipKeys(append([]string{"trailer"}, headerKeys(w.flushedHeaders)...)...),
		replaceInKeys(http2.TrailerPrefix, ""),
		// gRPC-Web spec says that must use lower-case header/trailer names.
		// See "HTTP wire protocols" section in
		// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md#protocol-differences-vs-grpc-over-http2
		keyCase(strings.ToLower),
	)
	return th
}

func (w *webSocketResponseWriter) writeMessage(b []byte) error {
	w.client.send <- b
	return nil // TODO(pcj): is there a need to propagate write errors?
}

func (w *webSocketResponseWriter) flushTrailers() {
	w.writeHeaderFrame(w.extractTrailerHeaders())
}

// Flush implements the https://golang.org/pkg/net/http/#Flusher interface.
func (w *webSocketResponseWriter) Flush() {
	// no-op
}

type webSocketWrappedReader struct {
	wsConn          *websocket.Conn
	respWriter      *webSocketResponseWriter
	remainingBuffer []byte
	remainingError  error
	cancel          context.CancelFunc
}

func newWebsocketWrappedReader(wsConn *websocket.Conn, respWriter *webSocketResponseWriter, cancel context.CancelFunc) *webSocketWrappedReader {
	return &webSocketWrappedReader{
		wsConn:          wsConn,
		respWriter:      respWriter,
		remainingBuffer: nil,
		remainingError:  nil,
		cancel:          cancel,
	}
}

// Close implements the https://golang.org/pkg/io/#Closer interface.  It flushes
// any remaining trailers and signals to close the websocket connection.
func (w *webSocketWrappedReader) Close() error {
	w.respWriter.flushTrailers()
	return w.wsConn.Close()
}

// Read implements the https://golang.org/pkg/io/#Reader interface. The first
// byte of a binary WebSocket frame is used for control flow: 0 = Data, 1 = End
// of client send
func (w *webSocketWrappedReader) Read(p []byte) (int, error) {
	// If a buffer remains from a previous WebSocket frame read then continue reading it
	if w.remainingBuffer != nil {

		// If the remaining buffer fits completely inside the argument slice then read all of it and return any error
		// that was retained from the original call
		if len(w.remainingBuffer) <= len(p) {
			copy(p, w.remainingBuffer)

			remainingLength := len(w.remainingBuffer)
			err := w.remainingError

			// Clear the remaining buffer and error so that the next read will be a read from the websocket frame,
			// unless the error terminates the stream
			w.remainingBuffer = nil
			w.remainingError = nil
			return remainingLength, err
		}

		// The remaining buffer doesn't fit inside the argument slice, so copy the bytes that will fit and retain the
		// bytes that don't fit - don't return the remainingError as there are still bytes to be read from the frame
		copy(p, w.remainingBuffer[:len(p)])
		w.remainingBuffer = w.remainingBuffer[len(p):]

		// Return the length of the argument slice as that was the length of the written bytes
		return len(p), nil
	}

	// Read a whole frame from the WebSocket connection
	messageType, framePayload, err := w.wsConn.ReadMessage()
	if err == io.EOF || messageType == -1 {
		// The client has closed the connection. Indicate to the response writer that it should close
		w.cancel()
		return 0, io.EOF
	}

	// Only Binary frames are valid
	if messageType != websocket.BinaryMessage {
		return 0, errors.New("websocket frame was not a binary frame")
	}

	// If the frame consists of only a single byte of value 1 then this indicates the client has finished sending
	if len(framePayload) == 1 && framePayload[0] == 1 {
		return 0, io.EOF
	}

	// If the frame is somehow empty then just return the error
	if len(framePayload) == 0 {
		return 0, err
	}

	// The first byte is used for control flow, so the data starts from the second byte
	dataPayload := framePayload[1:]

	// If the remaining buffer fits completely inside the argument slice then read all of it and return the error
	if len(dataPayload) <= len(p) {
		copy(p, dataPayload)
		return len(dataPayload), err
	}

	// The data read from the frame doesn't fit inside the argument slice, so copy the bytes that fit into the argument
	// slice
	copy(p, dataPayload[:len(p)])

	// Retain the bytes that do not fit in the argument slice
	w.remainingBuffer = dataPayload[len(p):]
	// Retain the error instead of returning it so that the retained bytes will be read
	w.remainingError = err

	// Return the length of the argument slice as that is the length of the written bytes
	return len(p), nil
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
		strings.ToLower(req.Header.Get("Sec-Websocket-Protocol")) == grpcWebsocketsSubprotocolName
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

// websocketClientHub maintains the set of active clients and broadcasts
// messages to the clients.  It is based on
// https://github.com/gorilla/websocket/blob/master/examples/chat/hub.go
type websocketClientHub struct {
	// Registered clients.
	clients map[*websocketClient]bool

	// Inbound messages from the clients.
	broadcast chan []byte

	// Register requests from the clients.
	register chan *websocketClient

	// Unregister requests from clients.
	unregister chan *websocketClient
}

func newWebsocketClientHub() *websocketClientHub {
	return &websocketClientHub{
		broadcast:  make(chan []byte),
		register:   make(chan *websocketClient),
		unregister: make(chan *websocketClient),
		clients:    make(map[*websocketClient]bool),
	}
}

func (h *websocketClientHub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

// websocketClient is a middleman between the websocket connection and the websocketClientHub.
type websocketClient struct {
	hub *websocketClientHub

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *websocketClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		c.hub.broadcast <- message
	}
}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *websocketClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
