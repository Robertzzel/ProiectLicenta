import threading
from pynput import mouse
from pynput import keyboard
import socket

MESSAGE_SIZE_LENGTH = 10
SOCKET_NAME = "/tmp/inputListener.sock"

MOVE = 1
CLICK = 2
SCROLL = 3
PRESS = 4
RELEASE = 5


def get_screen_sizes():
    import subprocess

    p = subprocess.Popen(['xrandr'], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['grep', '*'], stdin=p.stdout, stdout=subprocess.PIPE)

    p.stdout.close()
    resolution_string, junk = p2.communicate()
    resolution = resolution_string.split()[0]
    width, height = resolution.decode().split('x')

    p.wait()
    p2.wait()
    return int(width), int(height)


screen_width, screen_height = get_screen_sizes()


def get_ui_connection() -> socket.socket:
    import os

    if os.path.exists(SOCKET_NAME):
        os.remove(SOCKET_NAME)

    ui_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ui_connection.bind(("localhost", 5001))

    return ui_connection

# def get_ui_connection() -> socket.socket:
#     import os
#
#     if os.path.exists(SOCKET_NAME):
#         os.remove(SOCKET_NAME)
#
#     ui_connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
#     ui_connection.bind(SOCKET_NAME)
#
#     return ui_connection


def receive_message(connection: socket.socket) -> bytes:
    message_size = b''
    while True:
        message_size += connection.recv(MESSAGE_SIZE_LENGTH)
        if len(message_size) >= MESSAGE_SIZE_LENGTH:
            break

    message_size = int(message_size.decode())
    message = b''
    while True:
        message += connection.recv(message_size)
        if len(message) >= message_size:
            break

    return message


def send_message(connection: socket.socket, message: bytes):
    connection.sendall(str(len(message)).rjust(10, '0').encode())
    connection.sendall(message)


def on_move(x, y, ui):
    send_message(ui, f"{MOVE},{x/screen_width},{y/screen_height}".encode())


def on_click(x, y, button, pressed, ui):
    send_message(ui, f"{CLICK},{button.name},{int(pressed)}".encode())


def on_scroll(x, y, dx, dy, ui):
    send_message(ui, f"{SCROLL},{dx},{dy}".encode())


def on_press(key, ui):
    if type(key) == keyboard.KeyCode:
        send_message(ui, f"{PRESS},{key}".encode())
        return
    send_message(ui, f"{PRESS},{key.name}".encode())


def on_release(key, ui):
    if type(key) == keyboard.KeyCode:
        send_message(ui, f"{RELEASE},{key}".encode())
        return

    send_message(ui, f"{RELEASE},{key.name}".encode())


def main():
    ui_connection = get_ui_connection()

    mouse_listener: threading.Thread = mouse.Listener(
        on_move=lambda x, y: on_move(x, y, ui_connection),
        on_click=lambda x, y, button, pressed: on_click(x, y, button, pressed, ui_connection),
        on_scroll=lambda x, y, dx, dy: on_scroll(x, y, dx, dy, ui_connection)
    )

    keyboard_listener: threading.Thread = keyboard.Listener(
        on_press=lambda key: on_press(key, ui_connection),
        on_release=lambda key: on_release(key, ui_connection),
    )

    keyboard_listener.start()
    mouse_listener.start()

    mouse_listener.join()
    keyboard_listener.join()


if __name__ == "__main__":
    main()
