FROM golang:1.10 AS build-env
WORKDIR /go/src/app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -o /coffee-demo-reporting .

FROM alpine:3.6
EXPOSE 5000
COPY --from=build-env /coffee-demo-reporting /coffee-demo-reporting
# RUN apk update && apk add --no-cache ca-certificates
CMD [ "/coffee-demo-reporting" ]
