import kafka
import pynput

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
        components = msg.value.decode().split(",")
        action = int(components[0])

        if action == MOVE:
            x, y = int(float(components[1]) * width), int(float(components[2]) * height)
            mouse_controller.position = (x, y)
        elif action == CLICK:
            button, pressed = components[1], int(components[2])
            if pressed:
                mouse_controller.press(pynput.mouse.Button[button])
            else:
                mouse_controller.release(pynput.mouse.Button[button])
        elif action == PRESS:
            if len(components[1]) == 1:
                keyboard_controller.press(components[1])
            elif len(components[1]) > 1:
                keyboard_controller.press(pynput.keyboard.Key[components[1]])
            else:
                keyboard_controller.press(",")
        elif action == RELEASE:
            if len(components[1]) == 1:
                keyboard_controller.release(components[1])
            elif len(components[1]) > 1:
                keyboard_controller.release(pynput.keyboard.Key[components[1]])
            else:
                keyboard_controller.press(",")
        elif action == SCROLL:
            dx, dy = int(components[1]), int(components[1])
            mouse_controller.scroll(dx, -dy)


if __name__ == "__main__":
    main()
