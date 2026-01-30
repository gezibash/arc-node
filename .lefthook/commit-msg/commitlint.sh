#!/usr/bin/env bash
set -euo pipefail

commit_msg_file="${1:?missing commit message file}"
subject=$(head -n1 "$commit_msg_file")

# Skip merge commits
if [[ "$subject" =~ ^Merge" " ]]; then
  exit 0
fi

types="feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert"

if ! [[ "$subject" =~ ^($types)(\(.+\))?:" ".+ ]]; then
  echo "ERROR: commit message does not follow Conventional Commits format." >&2
  echo "" >&2
  echo "  Expected: type(scope): description" >&2
  echo "       or:  type: description" >&2
  echo "" >&2
  echo "  Allowed types: $types" >&2
  echo "" >&2
  echo "  Got: $subject" >&2
  exit 1
fi

if (( ${#subject} > 72 )); then
  echo "ERROR: subject line is ${#subject} chars (max 72)." >&2
  exit 1
fi
