VERSION := 1.0.0

.PHONY: run build install

run: atCleanup.go go.mod
	chmod +x atCleanup.go
	cd "${GOPATH}" && go install github.com/erning/gorun@latest

# https://golang.org/cmd/link/
build: atCleanup.go go.mod
	go build ./atCleanup.go

install: atCleanup.go go.mod
	go install ./atCleanup.go