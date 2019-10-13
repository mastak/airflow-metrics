FROM golang:alpine AS build-env

RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/src/github.com/mastak/airflow-metric

COPY ./server.go ./

RUN go get -d -v ./...

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /airflow-metric

FROM scratch
COPY --from=build-env /airflow-metric /airflow-metric
ENTRYPOINT ["/airflow-metric"]
