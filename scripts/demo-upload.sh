#!/bin/sh
# Uses a valid tiny PNG so the whole demo works without finding an image first.
set -eu
cd "$(dirname "$0")/.."
mkdir -p .tools
cp testdata/demo.png .tools/demo.png

# New fixture version needs a new default key; old keys may refer to the broken PNG.
key=${1:-demo-upload-v2-1}
caption=${2:-My first image}
base_url=${SIS2_BASE_URL:-http://localhost:8080}
curl --fail-with-body "$base_url/v1/images/upload" \
  -H "Idempotency-Key: $key" \
  -F 'owner_id=demo-owner' -F "caption=$caption" \
  -F 'image=@.tools/demo.png'
echo
