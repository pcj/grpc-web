package grpcweb

import (
	"bytes"
	"encoding/binary"
	"net/http"
	"strings"

	"golang.org/x/net/http2"
	"google.golang.org/grpc/grpclog"
)

// webSocketResponseWriter acts as an http.ResponseWriter.  It accepts bytes
// written to the response and propagates them to the websocket via the
// websocket client.
type webSocketResponseWriter struct {
	client         *websocketClient
	writtenHeaders bool
	headers        http.Header
	flushedHeaders http.Header
}

func newWebSocketResponseWriter(client *websocketClient) *webSocketResponseWriter {
	return &webSocketResponseWriter{
		client:         client,
		writtenHeaders: false,
		headers:        make(http.Header),
		flushedHeaders: make(http.Header),
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
	grpclog.Infof("writeHeaderFrame")

	w.writeMessage(EncodeHeaderFrame(headers))
}

func (w *webSocketResponseWriter) writeHeaderFrameOld(headers http.Header) {
	grpclog.Infof("writeHeaderFrame")

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

func (w *webSocketResponseWriter) writeMessage(data []byte) error {
	grpclog.Infof("writeMessage %q: %v", string(data), data)
	w.client.send <- data
	return nil // TODO(pcj): is there a need to propagate write errors?  Only the client knows about them.
}

func (w *webSocketResponseWriter) flushTrailers() {
	w.writeHeaderFrame(w.extractTrailerHeaders())
}

// Flush implements the https://golang.org/pkg/net/http/#Flusher interface.
func (w *webSocketResponseWriter) Flush() {
	// no-op
}

// EncodeHeaderFrame prepares a byte array consisting of the "data header" and
// the header itself as ascii bytes.
func EncodeHeaderFrame(headers http.Header) []byte {
	buf := new(bytes.Buffer)
	headers.Write(buf)
	headerGrpcDataHeader := []byte{1 << 7, 0, 0, 0, 0} // MSB=1 indicates this is a header data frame.
	binary.BigEndian.PutUint32(headerGrpcDataHeader[1:5], uint32(buf.Len()))
	buf.Grow(len(headerGrpcDataHeader))
	buf.Reset()
	buf.Write(headerGrpcDataHeader)
	headers.Write(buf)
	return buf.Bytes()
}
