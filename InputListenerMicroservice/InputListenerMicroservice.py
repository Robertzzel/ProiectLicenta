import threading
import queue
import sys
import time
from pynput import mouse
from pynput import keyboard
import kafka

INPUTS_TOPIC = "inputs"
BROKER_ADDRESS = "localhost:9092"

MOVE = 1
CLICK = 2
SCROLL = 3
PRESS = 4
RELEASE = 5


class SafeBytes:
    def __init__(self, txt: bytes = b""):
        self.text = txt
        self.lock = threading.Lock()
        self.last_update: time.time = None

    def add(self, text: bytes):
        with self.lock:
            current_time = time.time()
            self.text += text + b"," + str(round(current_time - self.last_update, 4) if self.last_update is not None else 0).encode() + b";"
            self.last_update = current_time

    def get(self) -> bytes:
        with self.lock:
            returned = self.text[:-1]
            self.text = b""
            self.last_update = None

        return returned


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
producer = kafka.KafkaProducer(bootstrap_servers=BROKER_ADDRESS, acks=1)
inputs_queue = queue.Queue(100)
inputs_message = SafeBytes()


def send_inputs():
    while True:
        time.sleep(0.1)
        producer.send(INPUTS_TOPIC, inputs_message.get())


def on_move(x, y):
    inputs_message.add(f"{MOVE},{round(x/screen_width, 2)},{round(y/screen_height, 2)}".encode())


def on_click(x, y, button, pressed):
    inputs_message.add(f"{CLICK},{button.name},{int(pressed)}".encode())


def on_scroll(x, y, dx, dy):
    inputs_message.add(f"{SCROLL},{dx},{dy}".encode())


def on_press(key):
    if type(key) == keyboard.KeyCode:
        inputs_message.add(f"{PRESS},{ord(key.char)}".encode())
        return
    inputs_message.add(f"{PRESS},{key.name}".encode())


def on_release(key):
    if type(key) == keyboard.KeyCode:
        inputs_message.add(f"{RELEASE},{ord(key.char)}".encode())
        return

    inputs_message.add(f"{RELEASE},{key.name}".encode())


def main():
    try:
        mouse_listener: threading.Thread = mouse.Listener(
            on_move=on_move,
            on_click=on_click,
            on_scroll=on_scroll,
        )

        keyboard_listener: threading.Thread = keyboard.Listener(
            on_press=on_press,
            on_release=on_release,
        )

        sending_thread = threading.Thread(target=send_inputs)

        keyboard_listener.start()
        mouse_listener.start()
        sending_thread.start()

        mouse_listener.join()
        keyboard_listener.join()
        sending_thread.join()
    except KeyboardInterrupt:
        pass
    except Exception:
        pass

    kafka.KafkaAdminClient(bootstrap_servers=BROKER_ADDRESS).delete_topics([INPUTS_TOPIC])
    print("Cleanup done.")
    sys.exit(0)


if __name__ == "__main__":
    main()
