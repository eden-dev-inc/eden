FROM peaceiris/mdbook:latest AS builder
WORKDIR /book
COPY . .
RUN mdbook build

FROM nginx:alpine
COPY --from=builder /book/book /usr/share/nginx/html
EXPOSE 80
