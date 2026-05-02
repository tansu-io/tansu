# Phase Logs

Phase logs are the activity record for a phase. The manifest in `phase-logs/index.json` is the source of truth for which phase docs have logs and which ones are still historical.

## Manifest Format

Each manifest entry tracks one `tips/phases/*.md` file with these fields:

- `phase`: two-digit phase number as a string.
- `phase_file`: phase document path.
- `status`: one of `legacy-unlogged`, `legacy-complete`, or `in-progress`.
- `log_file`: `null` for `legacy-unlogged`, otherwise the phase-log path.
- `ledger_api_keys`: ledger API keys owned by that phase.
- `primary_paths`: the main files touched or depended on while doing the phase work.
- `verification_commands`: commands used to verify the phase.
- `residual_risks`: the remaining work or gaps left after the phase.

`legacy-complete` is for older phases that already have logs, but those logs are still in the historical freeform format.
`legacy-unlogged` is for older phase docs that do not yet have a log file.
`in-progress` is for a phase that is being actively tracked with the current structured log format.

## Structured Log Format

Structured logs use this heading order:

1. `Date`
2. `Phase File`
3. `Intent`
4. `Files Touched`
5. `Ledger Rows Touched`
6. `Tests Added`
7. `Verification Commands`
8. `Outcomes`
9. `Residual Risks`

Format rules:

- `Files Touched` lists one repo-relative path per bullet.
- `Ledger Rows Touched` lists one `api_key` per bullet.
- `Tests Added` lists fully qualified test names.
- `Verification Commands` lists one shell command per bullet.
- `Outcomes` and `Residual Risks` are short bullet summaries.

Current legacy logs for phases 03, 05, and 06 remain readable as historical records. New work should use the structured headings above.
