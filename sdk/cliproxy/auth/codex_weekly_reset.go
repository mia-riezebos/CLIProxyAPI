package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	codexauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const codexQuotaBaseURL = "https://chatgpt.com/backend-api/wham"
const codexWeeklyWindowSeconds = 7 * 24 * 60 * 60
const codexResetPropagationSeconds = 60

// The journal keeps the provider's idempotency key across timeouts and restarts.
// It also prevents stale usage snapshots from spending another ticket.
type codexResetAttempt struct {
	RequestID   string `json:"request_id"`
	ResetAt     int64  `json:"reset_at"`
	Credits     int64  `json:"credits"`
	Completed   bool   `json:"completed"`
	AttemptedAt int64  `json:"attempted_at"`
}

// tryRecoverAfterError restores quota or credentials before the existing one-shot retry.
func (m *Manager) tryRecoverAfterError(ctx context.Context, auth *Auth, execErr error, alreadyTried bool, clientHeaders ...http.Header) (*Auth, bool) {
	if m != nil && auth != nil && execErr != nil && !alreadyTried && m.tryResetCodexWeeklyQuota(ctx, auth, execErr, clientHeaders...) {
		return auth, true
	}
	return m.tryRefreshAfterUnauthorized(ctx, auth, execErr, alreadyTried, clientHeaders...)
}

func (m *Manager) tryResetCodexWeeklyQuota(ctx context.Context, auth *Auth, execErr error, clientHeaders ...http.Header) bool {
	cfg := m.runtimeConfigSnapshot()
	if cfg == nil || !cfg.Codex.AutoResetWeeklyLimit || cfg.AuthDir == "" || auth.Provider != "codex" || auth.Disabled {
		return false
	}
	// Only first-party ChatGPT OAuth accounts have banked reset entitlements.
	token, _ := auth.Metadata["access_token"].(string)
	accountID, _ := auth.Metadata["account_id"].(string)
	if token == "" || accountID == "" || authAccessTokenForRequest(ctx, auth, clientHeaders...) != token || auth.Attributes["api_key"] != "" || auth.Attributes["base_url"] != "" || isRequestScopedError(execErr) {
		return false
	}
	if _, matched := matchRequestScopedErrorAction(auth, execErr, cfg); matched {
		return false
	}
	var status interface{ StatusCode() int }
	if !errors.As(execErr, &status) || status.StatusCode() != http.StatusTooManyRequests {
		return false
	}
	body := gjson.Parse(execErr.Error())
	if body.Get("error.type").String() != "usage_limit_reached" && body.Get("type").String() != "usage_limit_reached" {
		return false
	}
	executor := m.executorFor("codex")
	if executor == nil {
		return false
	}
	// All aliases of the same ChatGPT account share the same process/file lock.
	journal := filepath.Join(cfg.AuthDir, ".codex-weekly-resets", fmt.Sprintf("%x.json", sha256.Sum256([]byte(accountID))))
	recovered := false
	err := codexauth.WithFileLock(journal, func() error {
		call := func(method, path string, payload []byte) (gjson.Result, error) {
			req, err := http.NewRequestWithContext(ctx, method, codexQuotaBaseURL+path, bytes.NewReader(payload))
			if err != nil {
				return gjson.Result{}, err
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Chatgpt-Account-Id", accountID)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json")
			req.Header.Set("OpenAI-Beta", "codex-1")
			req.Header.Set("Originator", "Codex Desktop")
			resp, err := executor.HttpRequest(ctx, auth, req)
			if err != nil {
				return gjson.Result{}, err
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if err != nil {
				return gjson.Result{}, err
			}
			if resp.StatusCode != http.StatusOK || !gjson.ValidBytes(raw) {
				return gjson.Result{}, fmt.Errorf("Codex quota %s %s returned HTTP %d or invalid JSON", method, path, resp.StatusCode)
			}
			return gjson.ParseBytes(raw), nil
		}
		usage, err := call(http.MethodGet, "/usage", nil)
		if err != nil {
			return err
		}
		var weekly gjson.Result
		for _, name := range []string{"primary_window", "secondary_window"} {
			window := usage.Get("rate_limit." + name)
			if window.Get("limit_window_seconds").Int() == codexWeeklyWindowSeconds {
				weekly = window
			}
		}
		if !weekly.Exists() || weekly.Get("used_percent").Type != gjson.Number || weekly.Get("reset_at").Type != gjson.Number {
			return nil
		}
		if weekly.Get("used_percent").Float() < 100 || weekly.Get("reset_at").Int() <= time.Now().Unix() {
			// Another request or the natural reset already restored quota.
			recovered = usage.Get("rate_limit.allowed").Bool() && !usage.Get("rate_limit.limit_reached").Bool()
			return nil
		}
		credits := usage.Get("rate_limit_reset_credits.available_count")
		if !credits.Exists() {
			details, err := call(http.MethodGet, "/rate-limit-reset-credits", nil)
			if err != nil {
				return err
			}
			credits = details.Get("available_count")
		}
		var attempt codexResetAttempt
		stored, err := os.ReadFile(journal)
		if err == nil {
			if err := json.Unmarshal(stored, &attempt); err != nil {
				return err
			}
			if _, err := uuid.Parse(attempt.RequestID); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// A pending POST may already have spent the last ticket. Replay its UUID
		// even if available_count is now zero; never create a replacement UUID.
		if attempt.RequestID == "" || attempt.Completed {
			if applicable := usage.Get("rate_limit_reset_credits.applicable_available_count"); applicable.Exists() && (applicable.Type != gjson.Number || applicable.Int() <= 0) {
				return nil
			}
			if credits.Type != gjson.Number || credits.Int() <= 0 {
				return nil
			}
			if attempt.Completed && attempt.ResetAt == weekly.Get("reset_at").Int() && attempt.Credits == credits.Int() {
				return nil
			}
			if attempt.Completed && time.Now().Unix()-attempt.AttemptedAt < codexResetPropagationSeconds {
				recovered = true
				return nil
			}
			attempt = codexResetAttempt{RequestID: uuid.NewString(), ResetAt: weekly.Get("reset_at").Int(), Credits: credits.Int(), AttemptedAt: time.Now().Unix()}
			if err := writeCodexResetAttempt(journal, attempt); err != nil {
				return err
			}
		}
		payload, _ := json.Marshal(map[string]string{"redeem_request_id": attempt.RequestID})
		result, err := call(http.MethodPost, "/rate-limit-reset-credits/consume", payload)
		if err != nil {
			return err
		}
		if result.Get("code").String() != "reset" || result.Get("windows_reset").Int() <= 0 {
			return fmt.Errorf("Codex reset returned no confirmed window reset")
		}
		attempt.Completed = true
		if err := writeCodexResetAttempt(journal, attempt); err != nil {
			return err
		}
		recovered = true
		log.WithField("auth_id", auth.ID).Info("Codex weekly quota exhausted; consumed one banked reset")
		return nil
	})
	if err != nil {
		log.WithError(err).WithField("auth_id", auth.ID).Warn("Codex automatic weekly reset failed; retaining normal quota handling")
		return false
	}
	if recovered {
		if _, _, err := m.ResetQuota(ctx, auth.ID); err != nil {
			log.WithError(err).WithField("auth_id", auth.ID).Warn("Codex reset recovery could not persist cooldown clearance")
		}
	}
	return recovered
}

func writeCodexResetAttempt(path string, attempt codexResetAttempt) error {
	return codexauth.WriteJSONFileAtomic(path, map[string]any{
		"request_id": attempt.RequestID, "reset_at": attempt.ResetAt,
		"credits": attempt.Credits, "completed": attempt.Completed, "attempted_at": attempt.AttemptedAt,
	})
}
