FROM golang:1.25
WORKDIR /usr/src/app

COPY . .
EXPOSE 8080

CMD ["go", "run", "server/server.go"]