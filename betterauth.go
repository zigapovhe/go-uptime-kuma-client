package kuma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	engineio "github.com/maldikhan/go.socket.io/engine.io/v4/client"
	polling "github.com/maldikhan/go.socket.io/engine.io/v4/client/transport/polling"
	websocket "github.com/maldikhan/go.socket.io/engine.io/v4/client/transport/websocket"
	socketio "github.com/maldikhan/go.socket.io/socket.io/v5/client"
)

// signInBetterAuth logs in through the HTTP API that Uptime Kuma 3 uses
// instead of the socket "login" event, and returns the session cookie to send
// on the socket handshake. An empty cookie with a nil error means the server
// has no such endpoint (Uptime Kuma 2), so the socket login should be used.
func signInBetterAuth(ctx context.Context, baseURL string, username string, password string) (string, error) {
	body, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		return "", fmt.Errorf("marshal sign-in request: %w", err)
	}

	signInURL := strings.TrimSuffix(baseURL, "/") + "/api/auth/sign-in/username"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, signInURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create sign-in request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sign in: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("sign in returned status %d: %s", resp.StatusCode, msg)
	}

	cookies := make([]string, 0, len(resp.Cookies()))
	for _, cookie := range resp.Cookies() {
		cookies = append(cookies, cookie.Name+"="+cookie.Value)
	}

	if len(cookies) == 0 {
		return "", fmt.Errorf("sign in returned no session cookie")
	}

	return strings.Join(cookies, "; "), nil
}

type cookieHTTPClient struct {
	cookie string
}

func (c cookieHTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Cookie", c.cookie)
	return http.DefaultClient.Do(req)
}

// newCookieSocketClient builds the socket.io client like socketio.WithRawURL
// does, except that the polling transport sends the session cookie. The server
// reads the session from the handshake request only.
func newCookieSocketClient(baseURL string, cookie string, logger socketio.Logger) (*socketio.Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	if u.Path == "" {
		u.Path = "/socket.io/"
	}

	pollingTransport, err := polling.NewTransport(
		polling.WithHTTPClient(cookieHTTPClient{cookie: cookie}),
		polling.WithDefaultPinger(time.NewTicker(10*time.Second)),
		polling.WithLogger(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("create polling transport: %w", err)
	}

	wsTransport, err := websocket.NewTransport(websocket.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("create websocket transport: %w", err)
	}

	engineioClient, err := engineio.NewClient(
		engineio.WithURL(u),
		engineio.WithLogger(logger),
		engineio.WithSupportedTransports([]engineio.Transport{wsTransport, pollingTransport}),
	)
	if err != nil {
		return nil, fmt.Errorf("create engine.io client: %w", err)
	}

	return socketio.NewClient(
		socketio.WithEngineIOClient(engineioClient),
		socketio.WithLogger(logger),
	)
}
