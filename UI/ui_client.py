import pickle
import queue
import av
import time
import threading
import logging
from queue import Queue
import tkinter as tk
from PIL import ImageTk, Image, ImageOps
import sounddevice as sd
import kafka
from io import BytesIO
import numpy as np

TOPIC = "aggregator"
KAFKA_ADDRESS = "localhost:9092"
logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used
MOVE = 1
CLICK = 2
PRESS = 4
RELEASE = 5
INPUTS_TOPIC = "inputs"

class SafeString:
    def __init__(self, txt: str = ""):
        self.text = txt
        self.lock = threading.Lock()
        self.last_update: time.time = None

    def add(self, text: str):
        with self.lock:
            current_time = time.time()
            self.text += text + "," + str(
                round(current_time - self.last_update, 4) if self.last_update is not None else 0) + ";"
            self.last_update = current_time

    def get(self) -> str:
        with self.lock:
            returned = self.text[:-1]
            self.text = ""
            self.last_update = None

        return returned


class TkinterVideo(tk.Label):
    def __init__(self, master, scaled: bool = True, keep_aspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self._stream_initialized = False
        self._display_video_thread = threading.Thread(target=self._display_video, daemon=True)
        self._receive_thread = threading.Thread(target=self._receive_videos, daemon=True)
        self._play_audio_thread = threading.Thread(target=self._play_audio, daemon=True)
        self._send_inputs_thread = threading.Thread(target=self._send_inputs, daemon=True)
        self._inputs_listener_thread = threading.Thread
        self._frame_queue = Queue(10)
        self._audio_queue = Queue(10)
        self._current_img = None
        self._output_audio_stream = None
        self._current_frame_size = (0, 0)
        self._video_fps = 0
        self._video_sizes = (0, 0)
        self._audio_samplerate = 0
        self._audio_buffer_per_video_frame = 0
        self._scaled = scaled
        self._keep_aspect_ratio = keep_aspect
        self._running = True
        self._focused = False
        self._inputs = SafeString()

        self.set_scaled()
        self._keep_aspect_ratio = keep_aspect
        self._resampling_method: int = Image.NEAREST

        master.bind("<FocusIn>", self._focused_in)
        master.bind("<FocusOut>", self._focused_out)

        self.bind('<Motion>', self._motion_event_handler)
        self.bind("<Button>", self._mouse_button_press_handler)
        self.bind("<ButtonRelease>", self._mouse_button_release_handler)

        master.bind('<KeyPress>', self._key_press_handler)
        master.bind('<KeyRelease>', self._key_release_handler)

        self.bind("<<FrameGenerated>>", self._display_frame)

    def _motion_event_handler(self, event: tk.Event):
        self._inputs.add(f"{MOVE},{round(event.x/self._current_frame_size[0], 3)},{round(event.y/self._current_frame_size[1], 3)}")

    def _mouse_button_press_handler(self, event: tk.Event):
        self._inputs.add(f"{CLICK},{event.num},1")

    def _mouse_button_release_handler(self, event: tk.Event):
        self._inputs.add(f"{CLICK},{event.num},0")

    def _key_press_handler(self, event: tk.Event):
        self._inputs.add(f"{PRESS},{event.keysym_num}")

    def _key_release_handler(self, event: tk.Event):
        self._inputs.add(f"{RELEASE},{event.keysym_num}")

    def _send_inputs(self):
        producer = kafka.KafkaProducer(bootstrap_servers=KAFKA_ADDRESS, acks=1)
        while self._running:
            time.sleep(0.1)
            inputs = self._inputs.get()
            if inputs != "":
                producer.send(INPUTS_TOPIC, inputs.encode())

        kafka.KafkaAdminClient(bootstrap_servers=KAFKA_ADDRESS).delete_topics([INPUTS_TOPIC])

    def _focused_in(self, event):
        self._focused = True

    def _focused_out(self, event):
        self._focused = False

    def play(self):
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._display_video_thread is not None:
            self._display_video_thread.start()

        if self._play_audio_thread is not None:
            self._play_audio_thread.start()

        if self._send_inputs_thread is not None:
            self._send_inputs_thread.start()

    def _resize_event(self, event):
        self._current_frame_size = event.width, event.height

        if self._current_img:
            if self._keep_aspect_ratio:
                proxy_img = ImageOps.contain(self._current_img.copy(), self._current_frame_size)
            else:
                proxy_img = self._current_img.copy().resize(self._current_frame_size)

            self._current_imgtk = ImageTk.PhotoImage(proxy_img)
            self.config(image=self._current_imgtk)

    def set_scaled(self):
        if self._scaled:
            self.bind("<Configure>", self._resize_event)
        else:
            self.unbind("<Configure>")
            self._current_frame_size = self._video_sizes

    def _init_video_stream(self, stream, audio_sample_rate):
        self._video_fps = stream.guessed_rate
        self._audio_samplerate = audio_sample_rate
        self._video_sizes = (stream.width, stream.height)
        self._audio_buffer_per_video_frame = self._audio_samplerate // self._video_fps

        self.current_imgtk = ImageTk.PhotoImage(Image.new("RGB", self._video_sizes, (255, 255, 255)))
        self.config(width=800, height=600, image=self.current_imgtk)

        self._output_audio_stream = sd.OutputStream(channels=1, samplerate=self._audio_samplerate, dtype="int16",
                                                    callback=self.audio_callback,
                                                    blocksize=self._audio_buffer_per_video_frame)

        self._stream_initialized = True

    def _display_video(self):
        self._current_img = self._frame_queue.get(block=True)
        rate = 1 / self._video_fps
        try:
            while self._running:
                start = time.time()

                self._current_img = self._frame_queue.get(timeout=3).to_image()
                self.event_generate("<<FrameGenerated>>")

                time.sleep(max(rate - (time.time() - start) % rate, 0))
        except queue.Empty as ex:
            print("Queue empty", ex)
        except Exception as ex:
            print(ex)

        self._display_video_thread = None

        try:
            self.event_generate("<<Ended>>")
        except tk.TclError:
            pass

        print("Done")

    def _play_audio(self):
        self._audio_queue.get()
        time.sleep(0.1)
        self._output_audio_stream.start()

    def audio_callback(self, outdata: np.ndarray, frames: int, timet, status):
        try:
            data: np.ndarray = self._audio_queue.get_nowait()
            data.shape = (self._audio_buffer_per_video_frame, 1)
            outdata[:] = data
        except queue.Empty as ex:
            print("Queue empty", ex)
        except Exception as ex:
            print("Eroare", ex)

    def _receive_videos(self):
        consumer = kafka.KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_ADDRESS)
        while self._running:
            video, audio, audio_sample_rate = self.get_audio_video_from_message(next(consumer))

            with av.open(BytesIO(video)) as container:
                if not self._stream_initialized:
                    self._init_video_stream(container.streams.video[0], audio_sample_rate)

                container.fast_seek, container.discard_corrupt, empty_queue = True, True, True

                for frame in container.decode(video=0):
                    self._frame_queue.put(item=frame, block=True)
                    self._audio_queue.put(item=audio[frame.index * self._audio_buffer_per_video_frame: (frame.index + 1) * self._audio_buffer_per_video_frame], block=True)

                    if empty_queue:
                        while self._frame_queue.qsize() > 1:
                            self._frame_queue.get_nowait()
                        while self._audio_queue.qsize() > 1:
                            self._audio_queue.get_nowait()

                        empty_queue = False

    def get_audio_video_from_message(self, kafka_message):
        audio_start_index = None
        video_start_index = None
        audio_sample_rate = None

        for header in kafka_message.headers:
            if header[0] == "audio-start-index":
                audio_start_index = int(header[1].decode())
            elif header[0] == "video-start-index":
                video_start_index = int(header[1].decode())
            elif header[0] == "audio-sample-rate":
                audio_sample_rate = int(header[1].decode())

        if audio_start_index > video_start_index:
            return kafka_message.value[video_start_index:audio_start_index], pickle.loads(
                kafka_message.value[audio_start_index:]), audio_sample_rate
        return kafka_message.value[video_start_index:], pickle.loads(
            kafka_message.value[audio_start_index:video_start_index]), audio_sample_rate

    def _display_frame(self, event):
        if self._keep_aspect_ratio:
            self._current_img = ImageOps.contain(self._current_img, self._current_frame_size, self._resampling_method)
        else:
            self._current_img = self._current_img.resize(self._current_frame_size, self._resampling_method)

        self.current_imgtk.paste(self._current_img)
        self.config(image=self.current_imgtk)

    def stop(self):
        self._running = False

        if self._receive_thread:
            self._receive_thread.join()

        if self._display_video_thread:
            self._display_video_thread.join()

        if self._play_audio_thread:
            self._play_audio_thread.join()

        if self._send_inputs_thread:
            self._send_inputs_thread.join()

        if self._output_audio_stream:
            self._output_audio_stream.stop()


if __name__ == "__main__":
    root = tk.Tk()
    videoplayer = TkinterVideo(master=root, scaled=True)
    videoplayer.pack(expand=True, fill="both")

    videoplayer.play()
    root.mainloop()

    print("Oprim")
    videoplayer.stop()
    print("Gata")
