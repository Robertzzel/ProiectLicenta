import queue
import socket
import time
import numpy as np
import soundfile as sf
from recorder import Recorder

SYNC_SOCKET_NAME = "/tmp/sync.sock"
COMPOSER_SOCKET_NAME = "/tmp/composer.sock"
VIDE_SIZE = 1
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10


def synchronise():
    sync_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sync_socket.connect(SYNC_SOCKET_NAME)

    timestamp = receive_message(sync_socket)
    return int(timestamp.decode())


def receive_message(connection: socket.socket) -> bytes:
    message_size = b''
    while True:
        message_size += connection.recv(MESSAGE_SIZE_LENGTH - len(message_size))
        if len(message_size) >= MESSAGE_SIZE_LENGTH:
            break

    message_size = int(message_size.decode())
    message = b''
    while True:
        message += connection.recv(message_size - len(message))
        if len(message) >= message_size:
            break

    return message


def send_message(connection: socket.socket, message: bytes):
    connection.sendall(str(len(message)).rjust(10, '0').encode())
    connection.sendall(message)


def create_audio_file(path, audio_buffer, samplerate):
    open(path, "w").close()  # create file
    sf.write(path, np.array(audio_buffer, dtype='float32'), samplerate=samplerate)


if __name__ == "__main__":
    print("Starting...")

    recorder_queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(recorder_queue)

    composer_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    composer_socket.connect(COMPOSER_SOCKET_NAME)

    current_time = synchronise()
    print("SYNC: ", current_time)

    audio_recorder.start(current_time, VIDE_SIZE)
    while True:
        audio_file: str = recorder_queue.get(block=True)
        send_message(composer_socket, audio_file.encode())
        print(f"message {audio_file} sent at {time.time()}")

