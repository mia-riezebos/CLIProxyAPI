package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type weeklyResetExecutor struct {
	*unauthorizedRefreshExecutor
	stateMu       sync.Mutex
	used          int
	credits       int
	seconds       int
	errorType     string
	consumeIDs    []string
	lostReply     bool
	staleUsage    bool
	bootstrap     bool
	late          bool
	primaryWindow bool
}

func (e *weeklyResetExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if auth.ID == "aa-primary" && e.used >= 100 {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: 429, Message: fmt.Sprintf(`{"error":{"type":%q}}`, e.errorType)}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func (e *weeklyResetExecutor) ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	response, err := e.Execute(ctx, auth, req, opts)
	if err != nil && !e.bootstrap && !e.late {
		return nil, err
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	if err != nil {
		if e.late {
			chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("accepted output")}
		}
		chunks <- cliproxyexecutor.StreamChunk{Err: err}
	} else {
		chunks <- cliproxyexecutor.StreamChunk{Payload: response.Payload}
	}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Headers: http.Header{}, Chunks: chunks}, nil
}

func (e *weeklyResetExecutor) HttpRequest(_ context.Context, _ *Auth, req *http.Request) (*http.Response, error) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if req.URL.Host != "chatgpt.com" || req.Header.Get("Chatgpt-Account-Id") != "account-primary" {
		return nil, fmt.Errorf("wrong quota destination or account")
	}
	var body string
	switch req.URL.Path {
	case "/backend-api/wham/usage":
		window := "secondary_window"
		if e.primaryWindow {
			window = "primary_window"
		}
		body = fmt.Sprintf(`{"rate_limit":{"allowed":%t,"limit_reached":%t,%q:{"used_percent":%d,"limit_window_seconds":%d,"reset_at":%d}},"rate_limit_reset_credits":{"available_count":%d,"applicable_available_count":%d}}`, e.used < 100, e.used >= 100, window, e.used, e.seconds, time.Now().Add(24*time.Hour).Unix(), e.credits, e.credits)
	case "/backend-api/wham/rate-limit-reset-credits/consume":
		var payload struct {
			ID string `json:"redeem_request_id"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			return nil, err
		}
		e.consumeIDs = append(e.consumeIDs, payload.ID)
		if len(e.consumeIDs) == 1 {
			e.credits--
			if !e.staleUsage {
				e.used = 0
			}
			if e.lostReply {
				return nil, io.ErrUnexpectedEOF
			}
		} else if payload.ID != e.consumeIDs[0] {
			return nil, fmt.Errorf("a second ticket would be spent")
		} else {
			e.used = 0
		}
		body = `{"code":"reset","windows_reset":2}`
	default:
		return nil, fmt.Errorf("unexpected quota request: %s", req.URL.Path)
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

func weeklyResetFixture(t *testing.T, dir string, executor *weeklyResetExecutor) (*Manager, string) {
	t.Helper()
	m, existing, primary, backup, model := newUnauthorizedRefreshFixture(t, false)
	backup.Disabled = true
	if _, err := m.Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	primary.Metadata["account_id"] = "account-primary"
	if _, err := m.Update(context.Background(), primary); err != nil {
		t.Fatal(err)
	}
	executor.unauthorizedRefreshExecutor = existing
	m.RegisterExecutor(executor)
	cfg, err := config.ParseConfigBytes([]byte(fmt.Sprintf("config-version: 8\noauth:\n  auth-dir: %q\nupstream:\n  codex:\n    auto-reset-weekly-limit: true\n", dir)))
	if err != nil {
		t.Fatal(err)
	}
	m.SetConfig(cfg)
	return m, model
}

// Proves weekly rejection recovers the same account for normal and streaming requests.
func TestCodexWeeklyResetRecoversRequest(t *testing.T) {
	for _, mode := range []string{"execute", "primary", "stream", "bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			executor := &weeklyResetExecutor{used: 100, credits: 2, seconds: 604800, errorType: "usage_limit_reached", bootstrap: mode == "bootstrap", primaryWindow: mode == "primary"}
			m, model := weeklyResetFixture(t, t.TempDir(), executor)
			request := cliproxyexecutor.Request{Model: model}
			if mode == "execute" || mode == "primary" {
				response, err := m.Execute(context.Background(), []string{"codex"}, request, cliproxyexecutor.Options{})
				if err != nil || string(response.Payload) != "aa-primary" {
					t.Fatalf("response = %s, error = %v", response.Payload, err)
				}
			} else {
				response, err := m.ExecuteStream(context.Background(), []string{"codex"}, request, cliproxyexecutor.Options{})
				if err != nil {
					t.Fatal(err)
				}
				var output strings.Builder
				for chunk := range response.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					output.Write(chunk.Payload)
				}
				if output.String() != "aa-primary" {
					t.Fatalf("output = %s", output.String())
				}
			}
			if len(executor.consumeIDs) != 1 || executor.credits != 1 {
				t.Fatalf("consumes = %v, credits = %d", executor.consumeIDs, executor.credits)
			}
		})
	}
}

// Proves short windows, capacity errors, missing tickets and opt-out preserve tickets.
func TestCodexWeeklyResetPreservesTickets(t *testing.T) {
	for _, test := range []struct {
		name             string
		seconds, credits int
		errorType        string
		enabled          bool
	}{
		{"five-hour", 18000, 2, "usage_limit_reached", true},
		{"capacity", 604800, 2, "turn_rate_limit_exceeded", true},
		{"no tickets", 604800, 0, "usage_limit_reached", true},
		{"disabled", 604800, 2, "usage_limit_reached", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			executor := &weeklyResetExecutor{used: 100, credits: test.credits, seconds: test.seconds, errorType: test.errorType}
			m, model := weeklyResetFixture(t, dir, executor)
			m.SetConfig(&config.Config{AuthDir: dir, Codex: config.CodexConfig{AutoResetWeeklyLimit: test.enabled}})
			_, _ = m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
			if len(executor.consumeIDs) != 0 || executor.credits != test.credits {
				t.Fatalf("spent a ticket for %s", test.name)
			}
		})
	}
}

// Proves concurrent weekly rejections spend one ticket for the shared account.
func TestCodexWeeklyResetConcurrentRequests(t *testing.T) {
	executor := &weeklyResetExecutor{used: 100, credits: 2, seconds: 604800, errorType: "usage_limit_reached"}
	m, model := weeklyResetFixture(t, t.TempDir(), executor)
	var requests sync.WaitGroup
	for range 10 {
		requests.Go(func() {
			response, err := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
			if err != nil || string(response.Payload) != "aa-primary" {
				t.Errorf("response = %s, error = %v", response.Payload, err)
			}
		})
	}
	requests.Wait()
	if len(executor.consumeIDs) != 1 {
		t.Fatalf("consumes = %v", executor.consumeIDs)
	}
}

// Proves a lost consume response reuses its persisted UUID after a restart, even with zero tickets left.
func TestCodexWeeklyResetLostReplyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	executor := &weeklyResetExecutor{used: 100, credits: 1, seconds: 604800, errorType: "usage_limit_reached", lostReply: true, staleUsage: true}
	m, model := weeklyResetFixture(t, dir, executor)
	_, _ = m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	m, model = weeklyResetFixture(t, dir, executor)
	response, err := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil || string(response.Payload) != "aa-primary" {
		t.Fatalf("response = %s, error = %v", response.Payload, err)
	}
	if len(executor.consumeIDs) != 2 || executor.consumeIDs[0] != executor.consumeIDs[1] || executor.credits != 0 {
		t.Fatalf("consumes = %v, credits = %d", executor.consumeIDs, executor.credits)
	}
}

// Proves an error after generated output restores quota without replaying that generation.
func TestCodexWeeklyResetAfterOutput(t *testing.T) {
	executor := &weeklyResetExecutor{used: 100, credits: 2, seconds: 604800, errorType: "usage_limit_reached", late: true}
	m, model := weeklyResetFixture(t, t.TempDir(), executor)
	response, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	var finalErr error
	for chunk := range response.Chunks {
		output.Write(chunk.Payload)
		if chunk.Err != nil {
			finalErr = chunk.Err
		}
	}
	if output.String() != "accepted output" || finalErr == nil || len(executor.consumeIDs) != 1 {
		t.Fatalf("output = %s, error = %v, consumes = %v", output.String(), finalErr, executor.consumeIDs)
	}
	result, err := m.Execute(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil || string(result.Payload) != "aa-primary" {
		t.Fatalf("next request = %s, error = %v", result.Payload, err)
	}
}
