FROM --platform=$BUILDPLATFORM golang:1.26.6 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG GIT_COMMIT='not set'
ARG GIT_TAG=development
ENV GIT_COMMIT=$GIT_COMMIT
ENV GIT_TAG=$GIT_TAG

WORKDIR /build

COPY ./go.mod go.mod
COPY ./go.sum go.sum

RUN go mod download

COPY . .

# worker|proxy
ARG SERVICE=worker

RUN CGO_ENABLED=0 GO111MODULE=on GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-X 'main.date=$BUILD_DATE' -X 'main.version=$GIT_TAG' -X 'main.commit=$GIT_COMMIT'" -o chorus ./cmd/${SERVICE}

# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
USER nonroot:nonroot
WORKDIR /bin

COPY --chown=nonroot:nonroot --from=builder /build/chorus chorus

CMD ["chorus"]
