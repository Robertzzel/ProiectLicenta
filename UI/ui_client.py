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
            "audio_frames": 0,
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

    def _init_stream(self, video_stream, audio_stream):
        # frame rate
        self._video_info["framerate"] = video_stream.guessed_rate
        print(self._video_info["framerate"])

        self._video_info["audio_rate"] = audio_stream.sample_rate
        print(self._video_info["audio_rate"])

        # frame size
        self._video_info["framesize"] = (video_stream.width, video_stream.height)

        self.current_imgtk = ImageTk.PhotoImage(Image.new("RGB", self._video_info["framesize"], (255, 0, 0)))
        self.config(width=1280, height=720, image=self.current_imgtk)

        self._output_audio_stream = sd.OutputStream(channels=1, samplerate=self._video_info["audio_rate"],
                                                    dtype="float32", callback=self.audio_callback, blocksize=1024)
        self._stream_initialized = True

    def _display_video(self):
        while not self._stream_initialized:
            time.sleep(0.1)

        while self._frame_queue.empty():
            time.sleep(0.01)

        rate = 1 / self._video_info["framerate"]
        while True:
            start = time.time()

            self._current_img = self._frame_queue.get(timeout=2).to_image()
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
            data.shape = (1024, 1)
            outdata[:] = data
        except Exception as ex:
            pass

    def play(self):
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._display_video_thread is not None:
            self._display_video_thread.start()

        if self._play_audio_thread is not None:
            self._play_audio_thread.start()

    def _receive_videos(self):
        consumer = kafka.KafkaConsumer(TOPIC)
        last_received = None
        total_ping = 0

        for video_file in consumer:
            # if last_received is None:
            #     last_received = time.time()
            # else:
            #     now = time.time()
            #     diff = now - last_received - 1
            #     last_received = now
            #     print("diff = ", diff)
            #     total_ping += diff if diff > 0 else 0
            #
            # print("Ping: ", total_ping)
            print(self._frame_queue.qsize(), self._audio_queue.qsize())

            with av.open(BytesIO(video_file.value)) as container:
                if not self._stream_initialized:
                    self._init_stream(container.streams.video[0], container.streams.audio[0])

                for packet in container.demux():
                    for frame in packet.decode():
                        if isinstance(frame, av.AudioFrame):
                            self._audio_queue.put(item=frame.to_ndarray()[0], block=True)
                        else:
                            self._frame_queue.put(item=frame, block=True)

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
