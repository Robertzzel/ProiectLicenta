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


class TkinterVideo(tk.Label):
    def __init__(self, master, scaled: bool = True, keep_aspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self._stream_initialized = False
        self._display_video_thread = threading.Thread(target=self._display_video, daemon=True)
        self._receive_thread = threading.Thread(target=self._receive_videos, daemon=True)
        self._play_audio_thread = threading.Thread(target=self._play_audio, daemon=True)
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

        self.set_scaled(scaled)
        self._keep_aspect_ratio = keep_aspect
        self._resampling_method: int = Image.BOX

        self.bind("<<FrameGenerated>>", self._display_frame)

    def play(self):
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._display_video_thread is not None:
            self._display_video_thread.start()

        if self._play_audio_thread is not None:
            self._play_audio_thread.start()

    def _resize_event(self, event):
        self._current_frame_size = event.width, event.height

        if self._current_img and self._scaled:
            if self._keep_aspect_ratio:
                proxy_img = ImageOps.contain(self._current_img.copy(), self._current_frame_size)

            else:
                proxy_img = self._current_img.copy().resize(self._current_frame_size)

            self._current_imgtk = ImageTk.PhotoImage(proxy_img)
            self.config(image=self._current_imgtk)

    def set_scaled(self, scaled: bool, keep_aspect: bool = False):
        if self._scaled:
            self.bind("<Configure>", self._resize_event)
        else:
            self.unbind("<Configure>")
            self._current_frame_size = self._video_sizes

    def _init_video_stream(self, stream, audio_sample_rate):
        self._video_fps = stream.guessed_rate
        self._audio_samplerate = audio_sample_rate
        self._video_sizes = (stream.width, stream.height)
        self._audio_buffer_per_video_frame = self._audio_samplerate//self._video_fps

        self.current_imgtk = ImageTk.PhotoImage(Image.new("RGB", self._video_sizes, (255, 255, 255)))
        self.config(width=1280, height=720, image=self.current_imgtk)

        self._output_audio_stream = sd.OutputStream(channels=1, samplerate=self._audio_samplerate, dtype="int16",
                                                    callback=self.audio_callback,
                                                    blocksize=self._audio_buffer_per_video_frame)

        self._stream_initialized = True

    def _display_video(self):
        self._current_img = self._frame_queue.get(block=True)
        rate = 1 / self._video_fps
        try:
            while True:
                start = time.time()

                self._current_img = self._frame_queue.get(timeout=3).to_image()
                self.event_generate("<<FrameGenerated>>")

                t = max(rate - (time.time() - start) % rate, 0)
                print(t)
                time.sleep(t)
        except queue.Empty as ex:
            print("Queue empty", ex)
        except Exception as ex:
            print(ex)

        self._display_video_thread = None

        try:
            self.event_generate("<<Ended>>")
        except tk.TclError:
            pass

        self._receive_thread.join()
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
        for message in consumer:
            video, audio, audio_sample_rate = self.get_audio_video_from_message(message)

            with av.open(BytesIO(video)) as container:
                if not self._stream_initialized:
                    self._init_video_stream(container.streams.video[0], audio_sample_rate)

                container.fast_seek, container.discard_corrupt, empty_queue = True, True, True

                for frame in container.decode(video=0):
                    self._frame_queue.put(item=frame, block=True)
                    self._audio_queue.put(item=audio[frame.index*self._audio_buffer_per_video_frame: (frame.index+1) * self._audio_buffer_per_video_frame], block=True)

                    if empty_queue:
                        for i in range(self._frame_queue.qsize()-1):
                            self._frame_queue.get_nowait()
                        for i in range(self._audio_queue.qsize()-1):
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
            return kafka_message.value[video_start_index:audio_start_index], pickle.loads(kafka_message.value[audio_start_index:]), audio_sample_rate
        return kafka_message.value[video_start_index:], pickle.loads(kafka_message.value[audio_start_index:video_start_index]), audio_sample_rate

    def _display_frame(self, event):
        if self._scaled or (len(self._current_frame_size) == 2 and all(self._current_frame_size)):

            if self._keep_aspect_ratio:
                self._current_img = ImageOps.contain(self._current_img, self._current_frame_size,
                                                     self._resampling_method)

            else:
                self._current_img = self._current_img.resize(self._current_frame_size, self._resampling_method, reducing_gap=3.0)

        else:
            if self._keep_aspect_ratio:
                self._current_img = ImageOps.contain(self._current_img, self._current_frame_size,
                                                     self._resampling_method)

            else:
                self._current_img = self._current_img.resize(self._current_frame_size, self._resampling_method)

        self.current_imgtk.paste(self._current_img)
        self.config(image=self.current_imgtk)


if __name__ == "__main__":
    root = tk.Tk()
    videoplayer = TkinterVideo(master=root, scaled=True)
    videoplayer.pack(expand=True, fill="both")

    videoplayer.play()

    root.mainloop()
