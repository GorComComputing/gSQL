.PHONY: main
main: *.go deps
	#CGO_ENABLED=0 GOOS=linux GOARCH=arm go build -o gSQL .
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o gSQL .


.PHONY:deps
deps:
	#go get github.com/stretchr/testify/assert
	#go get github.com/marianogappa/sqlparser



