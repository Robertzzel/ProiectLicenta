FROM python:3.8-slim
WORKDIR /app
COPY . .
RUN apt update
RUN apt install -y ffmpeg
RUN /usr/local/bin/python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt
CMD ["python", "MergerMicroservice.py"]