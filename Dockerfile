# The web studio (server/server.ts) on a small server of your own: no GPU here, the runs go to COMFY_BASE_URL
# (a RunPod endpoint, Modal). Dockerfile.dockerignore keeps the build context to what the server reads.
#
#   docker build -t freefield-web . && docker run -p 3000:3000 -v freefield-out:/data \
#     -e COMFY_BASE_URL=... -e FREEFIELD_TOKEN=... -e FREEFIELD_PASSCODE=... freefield-web
#
# Public on the internet, set FREEFIELD_PASSCODE (and the run caps, server/guard.ts): whoever opens the page spends
# your GPU money. History and outputs live in /data/out; mount a volume there.
FROM node:26-slim
WORKDIR /app
ENV NODE_ENV=production PORT=3000 FREEFIELD_HOST=0.0.0.0 FREEFIELD_OUT=/data/out
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY schema ./schema
COPY apps ./apps
COPY server ./server
COPY docs/vram-map.json ./docs/vram-map.json
COPY lab/flags.json ./lab/flags.json
COPY web ./web
EXPOSE 3000
CMD ["node", "--experimental-strip-types", "--disable-warning=ExperimentalWarning", "server/server.ts"]
