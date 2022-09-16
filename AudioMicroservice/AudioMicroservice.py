import socket
import time
import numpy as np
import soundfile as sf
from recorder import Recorder

SYNC_SOCKET_NAME = "/tmp/sync.sock"
COMPOSER_SOCKET_NAME = "/tmp/composer.sock"
VIDE_SIZE = 1
SAMPLERATE = 44100


def synchronise():
    sync_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sync_socket.connect(SYNC_SOCKET_NAME)
    timestamp = sync_socket.recv(10)
    return int(timestamp.decode())


def send_message(connection, message):
    connection.sendall(str(len(message)).rjust(10, '0').encode())
    connection.sendall(message.encode())
    print(f"message {message} sent at {time.time()}")


def create_audio_file(file_path, audio_buffer, samplerate):
    open(file_path, "w").close()  # create file
    sf.write(file_path, np.array(audio_buffer, dtype='float32'), samplerate=samplerate)


if __name__ == "__main__":
    print("Starting...")

    audio_recorder: Recorder = Recorder()
    audio_recorder.start()

    composer_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    composer_socket.connect(COMPOSER_SOCKET_NAME)

    current_time = synchronise()
    print("SYNC: ", current_time)

    iteration = 0
    while True:
        part_start_time = current_time + iteration * VIDE_SIZE
        buffer = audio_recorder.get(part_start_time, VIDE_SIZE)
        file_path = f"audio/" + str(part_start_time) + ".wav"

        create_audio_file(file_path, buffer, SAMPLERATE)
        send_message(composer_socket, file_path)

        iteration += 1
