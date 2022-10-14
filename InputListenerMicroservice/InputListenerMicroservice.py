import threading
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


def on_move(x, y):
    producer.send(INPUTS_TOPIC, f"{MOVE},{x/screen_width},{y/screen_height}".encode())


def on_click(x, y, button, pressed):
    producer.send(INPUTS_TOPIC, f"{CLICK},{button.name},{int(pressed)}".encode())


def on_scroll(x, y, dx, dy):
    producer.send(INPUTS_TOPIC, f"{SCROLL},{dx},{dy}".encode())


def on_press(key):
    if type(key) == keyboard.KeyCode:
        producer.send(INPUTS_TOPIC, f"{PRESS},{key.char}".encode())
        return
    producer.send(INPUTS_TOPIC, f"{PRESS},{key.name}".encode())


def on_release(key):
    if type(key) == keyboard.KeyCode:
        producer.send(INPUTS_TOPIC, f"{RELEASE},{key.char}".encode())
        return

    producer.send(INPUTS_TOPIC, f"{RELEASE},{key.name}".encode())


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

        keyboard_listener.start()
        mouse_listener.start()

        mouse_listener.join()
        keyboard_listener.join()
    except KeyboardInterrupt:
        pass
    except Exception:
        pass

    kafka.KafkaAdminClient(bootstrap_servers=BROKER_ADDRESS).delete_topics([INPUTS_TOPIC])
    print("Cleanup done.")


if __name__ == "__main__":
    main()
