# Frontend Builder
################################################################################
FROM node:18 as frontend_builder
WORKDIR /app

# Copy code for frontend UI.
COPY ./waste_web/frontend_ui/package.json ./waste_web/frontend_ui/pnpm-lock.yaml ./
RUN yarn global add pnpm && pnpm install

COPY ./waste_web/frontend_ui/ ./
RUN pnpm run build

# Backend Builder
################################################################################
FROM rust:1.72 as backend_builder
WORKDIR /usr/src/waste_land

COPY . .
RUN cd waste_web && cargo install --path .

# Runner
################################################################################
FROM debian:12 as runner
WORKDIR /app

COPY --from=frontend_builder /app/dist/ /app/frontend_ui/dist/
COPY --from=backend_builder /usr/local/cargo/bin/waste_web /app

ENTRYPOINT [ "sh", "-c", "cd /app && ./waste_web" ]

