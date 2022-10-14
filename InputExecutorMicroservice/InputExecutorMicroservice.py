import kafka
import pynput
import time

INPUTS_TOPIC = "inputs"
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


def main():
    width, height = get_screen_sizes()
    keyboard_controller: pynput.keyboard.Controller = pynput.keyboard.Controller()
    mouse_controller: pynput.mouse.Controller = pynput.mouse.Controller()
    consumer = kafka.KafkaConsumer(INPUTS_TOPIC)

    for msg in consumer:
        if msg.value == b"":
            continue

        for command in msg.value.decode().split(";"):
            components = command.split(",")
            action = int(components[0])

            if action == MOVE:
                x, y = int(float(components[1]) * width), int(float(components[2]) * height)
                time.sleep(float(components[-1]))
                mouse_controller.position = (x, y)
            elif action == CLICK:
                button, pressed = components[1], int(components[2])
                time.sleep(float(components[-1]))
                if pressed:
                    mouse_controller.press(pynput.mouse.Button[button])
                else:
                    mouse_controller.release(pynput.mouse.Button[button])
            elif action == PRESS:
                time.sleep(float(components[-1]))
                if components[1].isnumeric():
                    keyboard_controller.press(chr(int(components[1])))
                else:
                    keyboard_controller.press(pynput.keyboard.Key[components[1]])
            elif action == RELEASE:
                time.sleep(float(components[-1]))
                if components[1].isnumeric():
                    keyboard_controller.release(chr(int(components[1])))
                else:
                    keyboard_controller.release(pynput.keyboard.Key[components[1]])
            elif action == SCROLL:
                dx, dy = int(components[1]), int(components[1])
                time.sleep(float(components[-1]))
                mouse_controller.scroll(dx, -dy)


if __name__ == "__main__":
    main()