#!/bin/bash
# Deploy Cloud Function with common/ module

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Preparing Cloud Function deployment ==="

# Step 1: Copy common module
echo "1. Copying common/ module..."
"$SCRIPT_DIR/copy_common.sh"

# Step 2: Deploy function
echo ""
echo "2. Deploying to Google Cloud..."
gcloud functions deploy gcs-to-bq-loader \
  --gen2 \
  --runtime=python312 \
  --region=asia-southeast1 \
  --source="$SCRIPT_DIR" \
  --entry-point=gcs_to_bigquery \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=raw_glamira" \
  --trigger-location=asia-southeast1 \
  --memory=256MB \
  --timeout=540s

echo ""
echo "=== Deployment complete ==="
