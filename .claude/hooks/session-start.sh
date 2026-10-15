#!/bin/bash
# Sync the project's .venv from uv.lock at the start of each Claude Code session.
set -euo pipefail

cd "${CLAUDE_PROJECT_DIR:-$(dirname "$0")/../..}"

# The uv installer puts uv in ~/.local/bin, which may not be on PATH yet.
export PATH="$HOME/.local/bin:$PATH"

if ! command -v uv >/dev/null 2>&1; then
  echo "session-start: uv not found on PATH, skipping uv sync" >&2
  exit 0
fi

uv sync --locked
