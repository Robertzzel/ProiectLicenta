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
from typing import Optional
from InputsBuffer import InputsBuffer

TOPIC = "aggregator"
KAFKA_ADDRESS = "localhost:9092"
logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used
MOVE = 1
CLICK = 2
PRESS = 4
RELEASE = 5
INPUTS_TOPIC = "inputs"


class TkinterVideo(tk.Label):
    def __init__(self, master, keep_aspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self._display_video_thread: threading.Thread = threading.Thread(target=self._display_video, daemon=True)
        self._receive_thread: threading.Thread = threading.Thread(target=self._receive_videos, daemon=True)
        self._send_inputs_thread: threading.Thread = threading.Thread(target=self._send_inputs, daemon=True)
        self._frame_queue: Queue = Queue(10)
        self._audio_queue: Queue = Queue(10)
        self._current_img = None
        self._output_audio_stream: Optional[sd.OutputStream] = None
        self._current_frame_size = (0, 0)
        self._video_fps = 0
        self._audio_samplerate = 0
        self._audio_buffer_per_video_frame = 0
        self._keep_aspect_ratio = keep_aspect
        self._running = True
        self._inputs: InputsBuffer = InputsBuffer()

        self.bind("<Configure>", self._resize_event)
        self._keep_aspect_ratio = keep_aspect
        self._resampling_method: int = Image.NEAREST

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

    def play(self):
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._display_video_thread is not None:
            self._display_video_thread.start()

        if self._send_inputs_thread is not None:
            self._send_inputs_thread.start()

    def _resize_event(self, event):
        self._current_frame_size = event.width, event.height

    def _init_video_stream(self, stream, audio_sample_rate):
        self._video_fps = stream.guessed_rate
        self._audio_samplerate = audio_sample_rate
        self._audio_buffer_per_video_frame = self._audio_samplerate // self._video_fps

        self.current_imgtk = ImageTk.PhotoImage(Image.new("RGB", (stream.width, stream.height), (255, 255, 255)))
        self.config(width=10, height=10, image=self.current_imgtk)

        self._output_audio_stream = sd.OutputStream(channels=1, samplerate=self._audio_samplerate, dtype="int16",
                                                    callback=self.audio_callback,
                                                    blocksize=self._audio_buffer_per_video_frame)

    def _display_video(self):
        self._current_img = self._frame_queue.get(block=True)
        self._output_audio_stream.start()
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

    def audio_callback(self, outdata: np.ndarray, frames: int, timet, status):
        try:
            data: np.ndarray = self._audio_queue.get_nowait()

            if len(data) < self._audio_buffer_per_video_frame:
                np.append(data, [0] * (self._audio_buffer_per_video_frame - len(data)))

        except queue.Empty:
            data = np.zeros(shape=(self._audio_buffer_per_video_frame, 1), dtype=np.int16)
        except Exception as ex:
            print("Error:", ex)
            return

        outdata[:] = data

    def _receive_videos(self):
        consumer = kafka.KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_ADDRESS)

        # init stream
        video, audio, audioSamplerate = self.get_audio_video_from_message(next(consumer))
        with av.open(BytesIO(video)) as container:
            self._init_video_stream(container.streams.video[0], audioSamplerate)

        # start stream
        while self._running:
            video, audio, audio_sample_rate = self.get_audio_video_from_message(next(consumer))
            audio.shape = (len(audio), 1)

            with av.open(BytesIO(video)) as container:
                container.fast_seek, container.discard_corrupt = True, True

                with self._frame_queue.mutex:
                    self._frame_queue.queue.clear()
                with self._audio_queue.mutex:
                    self._audio_queue.queue.clear()

                for frame in container.decode(video=0):
                    self._frame_queue.put(item=frame, block=True)
                    self._audio_queue.put(item=audio[frame.index * self._audio_buffer_per_video_frame: (frame.index + 1) * self._audio_buffer_per_video_frame], block=True)

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

        if self._send_inputs_thread:
            self._send_inputs_thread.join()

        if self._output_audio_stream:
            self._output_audio_stream.stop()


if __name__ == "__main__":
    root = tk.Tk()
    videoplayer = TkinterVideo(master=root)
    videoplayer.pack(expand=True, fill="both")
    videoplayer.play()

    root.mainloop()

    print("Oprim")
    videoplayer.stop()
    print("Gata")
