# N-ovation-Data-and-Analytics-Masters
# Nedbank DE Challenge - Stage 1 Pipeline

## Run Locally

```bash
# Pull base image
docker pull nedbank-de-challenge/base:1.0

# Build
docker build -t nedbank-stage1 .

# Run with test data
docker run --rm \
  --network=none \
  --memory=2g --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /path/to/your/test/data:/data \
  nedbank-stage1