.PHONY: main
main: *.go deps
	GOOS=linux GOARCH=arm go build -o gSQL .


.PHONY:deps
deps:
#	go get github.com/gorilla/sessions



