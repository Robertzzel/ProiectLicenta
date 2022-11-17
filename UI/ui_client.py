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
        self._output_audio_stream = sd.OutputStream(channels=1, samplerate=44100, dtype="float32")

        self._current_frame_size = (0, 0)

        self._video_info = {
            "duration": 0,  # duration of the video
            "framerate": 0,  # frame rate of the video
            "framesize": (0, 0)  # tuple containing frame height and width of the video
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

    def _init_stream(self, stream):
        # frame rate
        self._video_info["framerate"] = stream.average_rate

        # duration
        self._video_info["duration"] = float(stream.duration * stream.time_base)
        self.event_generate("<<Duration>>")

        # frame size
        self._video_info["framesize"] = (stream.width, stream.height)

        self.current_imgtk = ImageTk.PhotoImage(Image.new("RGBA", self._video_info["framesize"], (255, 0, 0, 0)))
        self.config(width=1280, height=720, image=self.current_imgtk)

        self._stream_initialized = True

    def _display_video(self):
        while not self._stream_initialized:
            time.sleep(0.1)

        while not self._frame_queue.empty():
            start = time.time()

            self._current_img = self._frame_queue.get(timeout=1).to_image()
            self.event_generate("<<FrameGenerated>>")

            time.sleep(max(1/self._video_info["framerate"] - time.time() + start, 0))

        self._display_video_thread = None

        try:
            self.event_generate("<<Ended>>")  # this is generated when the video ends
        except tk.TclError:
            pass

        self._receive_thread.join()
        print("Done")

    def _play_audio(self):
        while not self._stream_initialized:
            time.sleep(0.1)

        self._output_audio_stream.start()

        while self._audio_queue.empty():
            time.sleep(0.1)

        while not self._audio_queue.empty():
            self._output_audio_stream.write(self._audio_queue.get(block=True))

        self._output_audio_stream.stop()

    def play(self):
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._display_video_thread is not None:
            self._display_video_thread.start()

        if self._play_audio_thread is not None:
            self._play_audio_thread.start()

        self._output_audio_stream.start()

    def _receive_videos(self):
        consumer = kafka.KafkaConsumer(TOPIC)
        for video_file in consumer:
            with av.open(BytesIO(video_file.value)) as container:
                if not self._stream_initialized:
                    self._init_stream(container.streams.video[0])

                container.streams.video[0].thread_type = "AUTO"
                container.discard_corrupt = True

                for packet in container.demux():
                    for frame in packet.decode():
                        if isinstance(frame, av.AudioFrame):
                            self._audio_queue.put(item=frame.to_ndarray()[0], block=True)
                        else:
                            self._frame_queue.put(item=frame, block=True)

    def video_info(self) -> Dict:
        """ returns dict containing duration, frame_rate, file"""
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
            self._current_frame_size = self.video_info()["framesize"] if all(self.video_info()["framesize"]) else (1, 1)

            if self._keep_aspect_ratio:
                self._current_img = ImageOps.contain(self._current_img, self._current_frame_size,
                                                     self._resampling_method)

            else:
                self._current_img = self._current_img.resize(self._current_frame_size, self._resampling_method)

        self.current_imgtk = ImageTk.PhotoImage(self._current_img)
        self.config(image=self.current_imgtk)
        self._current_img = None


if __name__ == "__main__":
    root = tk.Tk()
    videoplayer = TkinterVideo(master=root, scaled=True)
    videoplayer.pack(expand=True, fill="both")

    videoplayer.play()

    root.mainloop()





