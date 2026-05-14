#!/usr/bin/env bash
# Usage: ./scripts/new-migration.sh <description_with_underscores>
# Example: ./scripts/new-migration.sh add_expiry_to_positions
#
# Creates the next numbered migration file in clickhouse/migrations/ and
# opens it in $EDITOR (falls back to printing the path).

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <description>"
    echo "  Example: $0 add_expiry_to_positions"
    exit 1
fi

DESCRIPTION="${1// /_}"   # replace spaces with underscores if any
MIGRATIONS_DIR="$(git rev-parse --show-toplevel)/clickhouse/migrations"

LAST=$(ls "$MIGRATIONS_DIR"/*.sql 2>/dev/null | sort | tail -1)
if [[ -z "$LAST" ]]; then
    NEXT="001"
else
    NUM=$(basename "$LAST" | grep -oP '^\d+')
    NEXT=$(printf '%03d' $((10#$NUM + 1)))
fi

FILENAME="${MIGRATIONS_DIR}/${NEXT}_${DESCRIPTION}.sql"

cat > "$FILENAME" <<EOF
-- ${DESCRIPTION//_/ }
-- Migration ${NEXT}: written $(date +%Y-%m-%d)

EOF

echo "Created: $FILENAME"

if [[ -n "${EDITOR:-}" ]]; then
    "$EDITOR" "$FILENAME"
fi
