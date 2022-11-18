import pickle

import av
import time
import threading
import logging
from queue import Queue
import tkinter as tk
from PIL import ImageTk, Image, ImageOps
from typing import Tuple, Dict
import sounddevice as sd
import kafka
from io import BytesIO
import numpy as np

TOPIC = "aggregator"
logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used


class TkinterVideo(tk.Label):
    def __init__(self, master, scaled: bool = True, keep_aspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self._stream_initialized = False
        self._display_video_thread = threading.Thread(target=self._display_video, daemon=True)
        self._receive_thread = threading.Thread(target=self._receive_videos, daemon=True)
        self._play_audio_thread = threading.Thread(target=self._play_audio, daemon=True)
        self._frame_queue = Queue()
        self._audio_queue = Queue()
        self._current_img = None
        self._output_audio_stream = None
        self._current_frame_size = (0, 0)

        self._video_info = {
            "framerate": 0,  # frame rate of the video
            "framesize": (0, 0),  # tuple containing frame height and width of the video
            "audio_samplerate": 0,
        }

        self.set_scaled(scaled)
        self._keep_aspect_ratio = keep_aspect
        self._resampling_method: int = Image.NEAREST

        self.bind("<<FrameGenerated>>", self._display_frame)

    def keep_aspect(self, keep_aspect: bool):
        """ keeps the aspect ratio when resizing the image """
        self._keep_aspect_ratio = keep_aspect

    def set_resampling_method(self, method: int):
        """ sets the resampling method when resizing """
        self._resampling_method = method

    def set_size(self, size: Tuple[int, int], keep_aspect: bool = False):
        """ sets the size of the video """
        self.set_scaled(False, self._keep_aspect_ratio)
        self._current_frame_size = size
        self._keep_aspect_ratio = keep_aspect

    def _resize_event(self, event):
        self._current_frame_size = event.width, event.height

        if self._current_img and self.scaled:
            if self._keep_aspect_ratio:
                proxy_img = ImageOps.contain(self._current_img.copy(), self._current_frame_size)

            else:
                proxy_img = self._current_img.copy().resize(self._current_frame_size)

            self._current_imgtk = ImageTk.PhotoImage(proxy_img)
            self.config(image=self._current_imgtk)

    def set_scaled(self, scaled: bool, keep_aspect: bool = False):
        self.scaled = scaled
        self._keep_aspect_ratio = keep_aspect

        if scaled:
            self.bind("<Configure>", self._resize_event)

        else:
            self.unbind("<Configure>")
            self._current_frame_size = self.video_info()["framesize"]

    def _init_video_stream(self, stream, audio_sample_rate):
        # frame rate
        self._video_info["framerate"] = stream.guessed_rate
        self._video_info["audio_samplerate"] = audio_sample_rate

        # frame size
        self._video_info["framesize"] = (stream.width, stream.height)

        self.current_imgtk = ImageTk.PhotoImage(Image.new("RGB", self._video_info["framesize"], (255, 0, 0)))
        self.config(width=1280, height=720, image=self.current_imgtk)

        self._output_audio_stream = sd.OutputStream(channels=1, samplerate=self._video_info["audio_samplerate"],
                                                    dtype="float32", callback=self.audio_callback, blocksize=self._video_info["audio_samplerate"]//self._video_info["framerate"])

        self._stream_initialized = True

    def _display_video(self):
        while not self._stream_initialized:
            time.sleep(0.1)

        while self._frame_queue.empty():
            time.sleep(0.01)

        rate = 1 / self._video_info["framerate"]
        while True:
            start = time.time()

            self._current_img = self._frame_queue.get().to_image()
            self.event_generate("<<FrameGenerated>>")

            time.sleep(max(rate - (time.time() - start) % rate, 0))

        self._display_video_thread = None

        try:
            self.event_generate("<<Ended>>")
        except tk.TclError:
            pass

        self._receive_thread.join()
        print("Done")

    def _play_audio(self):
        while not self._stream_initialized:
            time.sleep(0.1)

        self._output_audio_stream.start()

    def audio_callback(self, outdata: np.ndarray, frames: int, timet, status):
        try:
            data: np.ndarray = self._audio_queue.get_nowait()
            data.shape = (self._video_info["audio_samplerate"]//self._video_info["framerate"], 1)
            outdata[:] = data
        except Exception as ex:
            print(ex)

    def play(self):
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._display_video_thread is not None:
            self._display_video_thread.start()

        if self._play_audio_thread is not None:
            self._play_audio_thread.start()

    def _receive_videos(self):
        consumer = kafka.KafkaConsumer(TOPIC)
        for message in consumer:
            print(self._frame_queue.qsize(), self._audio_queue.qsize())

            video, audio, audio_sample_rate = self.get_audio_video_from_message(message)
            audio = pickle.loads(audio)

            with av.open(BytesIO(video)) as container:
                if not self._stream_initialized:
                    self._init_video_stream(container.streams.video[0], audio_sample_rate)

                container.fast_seek = True
                container.discard_corrupt = True
                audio_frames_per_video_frame = audio_sample_rate // container.streams.video[0].guessed_rate
                frame_number = 0
                for frame in container.decode(video=0):
                    self._frame_queue.put(item=frame, block=True)
                    self._audio_queue.put(item=audio[frame_number*audio_frames_per_video_frame: (frame_number+1) * audio_frames_per_video_frame], block=True)
                    frame_number += 1

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
            return kafka_message.value[video_start_index:audio_start_index], kafka_message.value[audio_start_index:], audio_sample_rate
        return kafka_message.value[video_start_index:], kafka_message.value[audio_start_index:video_start_index], audio_sample_rate

    def video_info(self) -> Dict:
        """ returns dict containing frame_rate, file"""
        return self._video_info

    def current_img(self) -> Image:
        """ returns current frame image """
        return self._current_img

    def _display_frame(self, event):
        """ displays the frame on the label """

        if self.scaled or (len(self._current_frame_size) == 2 and all(self._current_frame_size)):

            if self._keep_aspect_ratio:
                self._current_img = ImageOps.contain(self._current_img, self._current_frame_size,
                                                     self._resampling_method)

            else:
                self._current_img = self._current_img.resize(self._current_frame_size, self._resampling_method)

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
