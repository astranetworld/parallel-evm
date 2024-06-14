FROM amd64/golang:latest

WORKDIR /erigon-evm

COPY go.mod go.sum ./
RUN apt update && apt install libext2fs-dev
RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go mod download && go mod verify
