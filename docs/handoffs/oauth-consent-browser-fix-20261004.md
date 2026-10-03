# OAuth consent browser redirect correction

Date: 2026-10-04 (Europe/Kaliningrad)

## Observed failure

The real ChatGPT connection accepted the owner login and the first consent POST
returned HTTP 303. Chrome did not navigate to the ChatGPT callback. A second
submission of the already consumed request returned `Consent rejected`.

Live evidence: owner login 303 at 2026-10-03 22:22:27 UTC, initial consent 303
at 22:22:28, repeat consent 400 at 22:22:38, and no subsequent token exchange.
The encrypted OAuth state contained an unconsumed authorization code for the
registered ChatGPT callback. No credentials or private state are retained here.

A fresh headless Chrome flow against the public hostname reproduced the
`form-action` CSP violation after native form submission and never reached the
callback. Protocol-only HTTP acceptance does not enforce browser CSP and had
therefore missed this failure.

## Correction

The authenticated consent page and its redirect response now permit the origin
of the already validated pending OAuth callback in `form-action`. The owner
password form remains restricted to `self`. Callback paths and query parameters
are never concatenated into CSP. Redirect validation, exact registered callback
matching, CSRF, origin checks, PKCE and single-use codes remain enforced.

Concise OAuth login/consent outcome logs distinguish rejection reasons without
logging credentials, cookies, form bodies, callback queries or authorization
codes. Regression coverage exercises both approved and denied consent for the
two supported ChatGPT callback patterns, rejecting bad CSRF, foreign origins and
unapproved redirect destinations.

## Verification

Run the Python test suite, then repeat the real Chrome form flow against the
deployed public hostname and verify callback navigation plus token exchange.
The browser acceptance intercepts only the final ChatGPT callback to inspect
the return and exchange its code, without impersonating the user's ChatGPT
account. Actual connection of that account remains a user browser action.

Retained incident evidence and browser acceptance helper:
`/home/dev/artifacts/regional-knowledge-base/20261003T222444Z-oauth-consent-20261004/`.

No DNS, edge, provider, Supabase, storage, owner credential or OAuth state
migration is part of this correction.
