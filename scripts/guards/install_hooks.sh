#!/bin/bash
# Install the schema-drift pre-commit hook.
#
# Idempotent: safe to run repeatedly. Writes to
# .git/hooks/pre-commit; if a hook already exists, appends the
# schema-drift check unless the check is already present.
set -e
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$REPO_ROOT" ]; then
    echo "[install_hooks] not inside a git repo; skipping"
    exit 0
fi
HOOK="$REPO_ROOT/.git/hooks/pre-commit"
MARKER="# BEGIN schema-drift-guard"

if [ -f "$HOOK" ] && grep -qF "$MARKER" "$HOOK"; then
    echo "[install_hooks] schema-drift hook already installed"
    exit 0
fi

mkdir -p "$(dirname "$HOOK")"

if [ ! -f "$HOOK" ]; then
    cat > "$HOOK" <<'SHBANG'
#!/bin/bash
SHBANG
fi

cat >> "$HOOK" <<'EOF'

# BEGIN schema-drift-guard
python3 -m scripts.guards.check_schema_drift
if [ $? -ne 0 ]; then
    echo ""
    echo "Pre-commit aborted: add bootstrap migrations for the columns above."
    echo "To bypass (NOT recommended), commit with --no-verify."
    exit 1
fi
# END schema-drift-guard
EOF

chmod +x "$HOOK"
echo "[install_hooks] schema-drift pre-commit installed at $HOOK"
