import socket
import pynput

INPUT_EXECUTOR_SOCKET_NAME = "/tmp/inputExecutor.sock"
MESSAGE_SIZE_LENGTH = 10
MOVE = 1
CLICK = 2
SCROLL = 3
PRESS = 4
RELEASE = 5


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


def get_router_connection() -> socket.socket:
    import os

    if os.path.exists(INPUT_EXECUTOR_SOCKET_NAME):
        os.remove(INPUT_EXECUTOR_SOCKET_NAME)

    router_connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    router_connection.bind(INPUT_EXECUTOR_SOCKET_NAME)

    return router_connection


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


def main():
    router = get_router_connection()
    width, height = get_screen_sizes()
    keyboard_controller: pynput.keyboard.Controller = pynput.keyboard.Controller()
    mouse_controller: pynput.mouse.Controller = pynput.mouse.Controller()

    while True:
        command = receive_message(router).decode()
        components = command.split(",")
        action = int(components[0])

        if action == MOVE:
            x, y = float(components[1]), float(components[2])
            mouse_controller.move(int(x * width), int(y * height))
        elif action == CLICK:
            button, pressed = components[1], int(components[2])
            if pressed:
                mouse_controller.press(button)
            else:
                mouse_controller.release(button)
        elif action == PRESS:
            keyboard_controller.press(components[1])
        elif action == RELEASE:
            keyboard_controller.release(components[1])
        elif action == SCROLL:
            dx, dy = int(components[1]), int(components[1])
            mouse_controller.scroll(dx, -dy)


if __name__ == "__main__":
    main()
