# Contributing to XferDB

Thank you for your interest in contributing to XferDB!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://gitea.homelab.local/nextdevops/XferDB.git`
3. Create a branch: `git checkout -b feature/your-feature-name`

## Development Setup

```bash
# Install dependencies
go mod download

# Run tests
go test ./...

# Build
go build ./cmd/xferdb
```

## Making Changes

1. Make your changes
2. Add tests if applicable
3. Ensure all tests pass: `go test ./...`
4. Commit with a signed-off-by line (see DCO below)

## DCO — Developer Certificate of Origin

By contributing to XferDB you agree to the Developer Certificate of Origin (DCO). This certifies that you have the right to contribute the code you're submitting.

All commits must include a signed-off-by line:

```
Signed-off-by: Your Name <your@email.com>
```

You can auto-add this with:

```bash
git commit -s
```

## Code Style

- Run `go fmt ./...` before committing
- Follow standard Go conventions
- Add comments for exported functions

## Submitting Changes

1. Push your branch to your fork
2. Open a pull request against `main`
3. Ensure CI passes

## Continuous Integration

PRs (and pushes) touching `**.go` files run two Gitea Actions workflows:

- **Build** (`.gitea/workflows/go-build.yaml`) — `go build`/`go test` inside the runner
- **XferDB Matrix Gate** (`.gitea/workflows/xferdb-matrix-gate.yaml`) — triggers the Jenkins
  `xferdb-matrix` job, which seeds real PostgreSQL/MySQL/MongoDB sources in Kubernetes and runs
  every compatible migration case against them (`SIZE=S`: ~5k rows/table)

PRs touching `cmd/xferdb-web/**` (the web UI) also run:

- **UI Smoke Test** (`.gitea/workflows/ui-smoke-test.yaml`) — triggers the Jenkins `xferdb-ui`
  job, which deploys the XferDB API + web server in Kubernetes and runs Playwright E2E tests
  (project CRUD, basic smoke tests). Can also be triggered manually via `workflow_dispatch`.

All three workflows post a review on the PR when they finish — **APPROVE** on success,
**REQUEST_REVIEW** on failure — from a bot identity, not a human reviewer, so a PR can show as
"approved" before anyone has actually looked at it. Maintainers: these need repo secrets
`JENKINS_TOKEN` (and `GH_TOKEN` for PR reviews) plus variables `JENKINS_URL`/`JENKINS_USER`
configured under Settings → Actions to run at all; without them the jobs fail immediately.

## Reporting Issues

Please report bugs via the issue tracker with:
- XferDB version (`xferdb version`)
- Database versions involved
- Steps to reproduce
- Expected vs actual behavior
