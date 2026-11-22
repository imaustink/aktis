# syntax=docker/dockerfile:1
#
# Build from the REPO ROOT (this tool depends on the shared
# @aktis/messaging workspace package):
#
#   docker build -f catalog/tools/recipe-scraper/Dockerfile -t recipe-scraper:latest .

############################
# Build stage
############################
FROM node:20-bookworm-slim AS build
WORKDIR /repo

# Browsers already ship in the runtime image; don't download them here.
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Install across the whole workspace so the local @aktis/messaging
# package resolves without hitting the registry.
COPY package.json package-lock.json* ./
COPY framework/messaging/package.json framework/messaging/package.json
COPY catalog/tools/recipe-scraper/package.json catalog/tools/recipe-scraper/package.json
RUN npm ci

COPY framework/messaging framework/messaging
COPY catalog/tools/recipe-scraper/tsconfig.json catalog/tools/recipe-scraper/tsconfig.json
COPY catalog/tools/recipe-scraper/src catalog/tools/recipe-scraper/src

RUN npm run build --workspace=@aktis/messaging \
  && npm run build --workspace=recipe-scraper \
  && npm prune --omit=dev \
  # Materialize the workspace package into a real directory (not the npm-created
  # symlink) so it survives being copied into the runtime stage below.
  && rm -rf node_modules/@aktis/messaging \
  && mkdir -p node_modules/@aktis/messaging \
  && cp -r framework/messaging/dist node_modules/@aktis/messaging/dist \
  && cp framework/messaging/package.json node_modules/@aktis/messaging/package.json

############################
# Runtime stage
############################
# Pinned Playwright image bundles Chromium + its OS dependencies.
# Keep this version in lockstep with the "playwright" dependency in package.json.
FROM mcr.microsoft.com/playwright:v1.61.1-jammy AS runtime

# yt-dlp + ffmpeg for audio extraction/transcription.
# yt-dlp must stay current: YouTube/TikTok et al. change constantly, so an old
# build breaks (e.g. "Sign in to confirm you're not a bot", TikTok's
# "Unexpected response from webpage request"). Defaults to the latest release;
# set YT_DLP_VERSION to a tag (e.g. 2026.10.26) to pin for reproducibility.
#
# We install the self-contained PyInstaller build, NOT the zipapp `yt-dlp`:
#   - the zipapp runs on the system Python, and this jammy base only has 3.10,
#     which current yt-dlp refuses to start on (3.11+ required);
#   - the standalone build bundles curl_cffi, which TikTok's extractor needs
#     for browser impersonation -- without it TikTok serves a challenge page.
# It's the *onedir* (.zip) variant on purpose: the onefile binary unpacks its
# shared libraries into /tmp at startup, which fails on the hardened runtime's
# noexec /tmp ("failed to map segment from shared object").
ARG YT_DLP_VERSION=latest
ARG TARGETARCH
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl unzip \
  && case "$TARGETARCH" in \
       amd64) YT_DLP_ASSET="yt-dlp_linux" ;; \
       arm64) YT_DLP_ASSET="yt-dlp_linux_aarch64" ;; \
       *) echo "unsupported TARGETARCH '$TARGETARCH'" >&2; exit 1 ;; \
     esac \
  && if [ "$YT_DLP_VERSION" = "latest" ]; then \
       YT_DLP_URL="https://github.com/yt-dlp/yt-dlp/releases/latest/download/${YT_DLP_ASSET}.zip"; \
     else \
       YT_DLP_URL="https://github.com/yt-dlp/yt-dlp/releases/download/${YT_DLP_VERSION}/${YT_DLP_ASSET}.zip"; \
     fi \
  && curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 "$YT_DLP_URL" -o /tmp/yt-dlp.zip \
  && mkdir -p /opt/yt-dlp \
  && unzip -q /tmp/yt-dlp.zip -d /opt/yt-dlp \
  && rm /tmp/yt-dlp.zip \
  && chmod -R a+rX /opt/yt-dlp \
  && chmod a+rx "/opt/yt-dlp/${YT_DLP_ASSET}" \
  && ln -s "/opt/yt-dlp/${YT_DLP_ASSET}" /usr/local/bin/yt-dlp \
  # Fail the build (not a user's job) if the binary can't run or lost impersonation.
  && yt-dlp --version \
  # (targets are listed even without curl_cffi, suffixed "(unavailable)").
  && yt-dlp --list-impersonate-targets | grep -i chrome | grep -qv unavailable \
  && apt-get purge -y curl unzip \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /repo/node_modules ./node_modules
COPY --from=build /repo/catalog/tools/recipe-scraper/dist ./dist
COPY --from=build /repo/catalog/tools/recipe-scraper/package.json ./package.json

# The Playwright base image provides an unprivileged 'pwuser'.
USER pwuser

ENTRYPOINT ["node", "dist/index.js"]
