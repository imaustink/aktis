# syntax=docker/dockerfile:1
#
# Build from the REPO ROOT (skaffold.yaml, .github/release-images.json and
# release.yml all use context "."):
#
#   docker build -f orchestrator/apps/mcp-broker/Dockerfile -t mcp-broker:latest .
#
# This is a SEPARATE image from the engines on purpose (ADR 0045 §3). The
# broker is the only process that links an MCP client and holds MCP sessions;
# the engines hold the deterministic agent loop and cluster RBAC. Keeping them
# in one pod would let a malformed or hostile MCP server reach the loop's
# process, secrets or identity — the containment this split exists to provide.

############################
# Build stage
############################
FROM golang:1.26-alpine AS build
WORKDIR /src

COPY orchestrator/apps/mcp-broker/go.mod orchestrator/apps/mcp-broker/go.sum ./
RUN go mod download

COPY orchestrator/apps/mcp-broker/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mcp-broker .

############################
# Runtime stage
############################
# Static and shell-less. The chart pins runAsUser 10001 with a read-only root
# filesystem; the binary writes nothing to disk, so either uid works.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mcp-broker /mcp-broker
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/mcp-broker"]
