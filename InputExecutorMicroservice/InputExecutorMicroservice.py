import kafka
import pynput
import time
from TkPynputKeyCodes import KeyTranslator
from pyautogui import size
import sys

MOVE = 1
CLICK = 2
SCROLL = 3
PRESS = 4
RELEASE = 5
LEFT = 1
MIDDLE = 2
RIGHT = 3
SCROLL_UP = 4
SCROLL_DOWN = 5


def main():
    if len(sys.argv) < 3:
        raise Exception("No broker address and topic given")

    brokerAddress = sys.argv[1]
    topic = sys.argv[2]

    width, height = size()
    keyboard_controller: pynput.keyboard.Controller = pynput.keyboard.Controller()
    mouse_controller: pynput.mouse.Controller = pynput.mouse.Controller()
    consumer = kafka.KafkaConsumer(topic, bootstrap_servers=brokerAddress)

    for msg in consumer:
        for command in msg.value.decode().split(";"):
            components = command.split(",")
            action = int(components[0])

            if action == MOVE:
                time.sleep(float(components[-1]))
                mouse_controller.position = (float(components[1]) * width, float(components[2]) * height)
            elif action == CLICK:
                button = int(components[1])
                time.sleep(float(components[-1]))

                if button == SCROLL_UP:
                    mouse_controller.scroll(0, 2)
                elif button == SCROLL_DOWN:
                    mouse_controller.scroll(0, -2)

                if int(components[2]):
                    mouse_controller.press(pynput.mouse.Button(button))
                else:
                    mouse_controller.release(pynput.mouse.Button(button))
            elif action == PRESS:
                time.sleep(float(components[-1]))
                keyboard_controller.press(KeyTranslator.translate(int(components[1])))
            elif action == RELEASE:
                time.sleep(float(components[-1]))
                keyboard_controller.release(KeyTranslator.translate(int(components[1])))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as ex:
        print(ex)
    except Exception as ex:
        print(ex)