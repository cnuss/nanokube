package docker

import (
	"bytes"
	"io"
	"net/http"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// loggingTransport wraps an http.RoundTripper and logs Docker API calls.
type loggingTransport struct {
	inner http.RoundTripper
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	method := req.Method
	url := req.URL.String()

	// Tee request body at trace level — captures body as it's read by the
	// inner transport without consuming it ahead of time.
	var reqBuf bytes.Buffer
	if req.Body != nil && log.Logger.GetLevel() <= zerolog.TraceLevel {
		req.Body = io.NopCloser(io.TeeReader(req.Body, &reqBuf))
	}

	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		evt := log.Debug().Str("method", method).Str("url", url).Err(err)
		if reqBuf.Len() > 0 {
			evt.Str("reqBody", reqBuf.String())
		}
		evt.Msg("dockerapi")
		return resp, err
	}

	// Wiretap: wrap response body to log it as it's consumed, without
	// reading/replacing the body ourselves.
	if resp.Body != nil && log.Logger.GetLevel() <= zerolog.TraceLevel {
		resp.Body = &wiretapReader{
			reader:  resp.Body,
			method:  method,
			url:     url,
			status:  resp.StatusCode,
			reqBody: reqBuf.String(),
		}
	} else {
		// Debug level: just log method/url/status immediately
		evt := log.Debug().Str("method", method).Str("url", url).Int("status", resp.StatusCode)
		if reqBuf.Len() > 0 {
			evt.Str("reqBody", reqBuf.String())
		}
		evt.Msg("dockerapi")
	}

	return resp, nil
}

// wiretapReader wraps an io.ReadCloser and logs the full body on Close.
type wiretapReader struct {
	reader  io.ReadCloser
	buf     bytes.Buffer
	method  string
	url     string
	status  int
	reqBody string
}

func (w *wiretapReader) Read(p []byte) (int, error) {
	n, err := w.reader.Read(p)
	if n > 0 {
		w.buf.Write(p[:n])
	}
	return n, err
}

func (w *wiretapReader) Close() error {
	err := w.reader.Close()
	evt := log.Trace().Str("method", w.method).Str("url", w.url).Int("status", w.status)
	if w.reqBody != "" {
		evt.Str("reqBody", w.reqBody)
	}
	evt.Str("respBody", w.buf.String())
	evt.Msg("dockerapi")
	return err
}
