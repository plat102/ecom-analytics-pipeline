#!/bin/bash
# Copy common module into Cloud Function deployment folder

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Copying common/ module to Cloud Function..."

# Clean old common if exists
rm -rf "$SCRIPT_DIR/common"

# Copy common module
cp -r "$PROJECT_ROOT/common" "$SCRIPT_DIR/common"

echo "Done. Cloud Function can now import from common/"
