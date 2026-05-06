FROM python:3.12-alpine

# Hello world page
RUN echo "Hello world" > /index.html

# Run HTTP server
CMD ["python3", "-m", "http.server", "8080"]