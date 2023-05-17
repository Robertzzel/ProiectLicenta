FROM golang:1.20
WORKDIR /app
COPY . .
RUN apt update
RUN apt install -y ffmpeg
RUN go mod download && go mod tidy && go mod verify
CMD ["go", "run", "DatabaseMicroservice.go", "DatabaseManager.go", "FileOperations.go", "models.go", "PasswordOperations.go"]