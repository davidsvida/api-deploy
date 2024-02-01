FROM alpine:latest

COPY api .

EXPOSE 8080

CMD ["./api"]
