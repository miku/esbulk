################################
# STEP 1 build executable binary
################################
FROM golang:1.12.5-alpine3.9 AS builder

# https://github.com/moby/moby/issues/34513#issuecomment-389250632, we hope
# this label is not used for something critical in your setup.
LABEL stage=intermediate

# Install git, required for fetching the dependencies.
RUN apk update && apk add --no-cache git make

WORKDIR /app
COPY . .

# Fetch dependencies, using go get, download only, verbose.
RUN go get -d -v

# Build the binary.
RUN make esbulk

############################
# STEP 2 build a small image
############################
FROM scratch

# Copy our static executable.
COPY --from=builder /app/esbulk /app/esbulk

# https://stackoverflow.com/questions/52969195/docker-container-running-golang-http-client-getting-error-certificate-signed-by
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Default command.
ENTRYPOINT ["/app/esbulk"]

