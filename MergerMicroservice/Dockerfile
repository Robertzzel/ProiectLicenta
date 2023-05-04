FROM python:3.8-slim
WORKDIR /app
COPY . .
RUN apt update
RUN apt install -y ffmpeg
RUN python -m pip install -r requirements.txt
CMD ["python", "MergerMicroservice.py"]