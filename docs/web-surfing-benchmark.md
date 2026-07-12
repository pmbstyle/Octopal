# Web surfing benchmark

The backend benchmark compares web extraction paths without starting an Octopal runtime or
using a live Octo instance.

## Backends

- `basic`: the direct `web_fetch` HTTP path with Firecrawl disabled for the run.
- `markdown_new`: `markdown.new` without its fallback.
- `webclaw`: the local WebClaw binary with cloud credentials removed.
- `browser`: the configured browser tool path (Playwright by default, or PinchTab when explicitly enabled).

## Run

Use a pinned local WebClaw binary and write results outside the repository or under ignored
runtime storage:

```bash
uv run python scripts/benchmark_web_backends.py \
  --backends basic,markdown_new,webclaw,browser \
  --webclaw-binary /absolute/path/to/webclaw \
  --repeat 3 \
  --output data/web-backend-benchmark.json
```

The default corpus lives at `benchmarks/web/corpus.json`. Every entry contains a small set of
expected visible terms. The report records success, latency, returned character count, matched
terms, fidelity, and typed failure reasons.

WebClaw cloud fallback is intentionally disabled even if `WEBCLAW_API_KEY` exists in the parent
environment. The benchmark does not require or restart an Octopal instance.
