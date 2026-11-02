# Automatic Codex weekly resets

Enable this per deployment for ChatGPT OAuth accounts with banked Codex reset
tickets:

```yaml
upstream:
  codex:
    auto-reset-weekly-limit: true
```

The default is false. A `usage_limit_reached` rejection triggers a fresh lookup
of that account's Codex quota. CPA consumes a ticket only when the main quota
contains an exhausted 604800-second window with a future reset time and an
available ticket. Five-hour limits, code review limits, capacity errors, API
keys, custom upstreams and SIWC credentials do not consume tickets.

After a confirmed reset, CPA clears the account's routing cooldown and retries
the request once. This includes HTTP errors and initial streaming errors.
An error after generated output restores quota for the next request and keeps
the current stream's error; CPA never replays an accepted generation.

Redemptions use Codex's `rate-limit-reset-credits/consume` endpoint through
the existing authenticated provider transport. A journal under
`<auth-dir>/.codex-weekly-resets` retains each redemption UUID before sending
the request. Account-scoped file locks serialize concurrent failures, including
aliases. An uncertain redemption reuses its UUID after a restart, even when
the account now reports zero tickets. Successful redemptions retain their
quota snapshot and a one-minute propagation guard to prevent duplicate spends.

Keep the journal on persistent storage. If quota lookup, journal storage or
redemption fails, CPA logs the failure and keeps its normal cooldown and
account fallback behavior. The endpoint is a ChatGPT backend contract, so
provider changes can require a fork update.
