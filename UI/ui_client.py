import av
import time
import threading
import logging
from queue import Queue
import tkinter as tk
from PIL import ImageTk, Image, ImageOps
from typing import Tuple, Dict

logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used


class TkinterVideo(tk.Label):
    def __init__(self, master, scaled: bool = True, keep_aspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self._stream_initialized = False
        self._load_thread = threading.Thread(target=self._display, daemon=True)
        self._receive_thread = threading.Thread(target=self._receive_and_decode)
        self._frame_queue = Queue(5)
        self._audio_queue = Queue(5)
        self._current_img = None

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

    def _display(self):
        """ load's file from a thread """

        while not self._stream_initialized:
            time.sleep(0.1)

        while not self._frame_queue.empty():
            start = time.time()

            self._current_img = self._frame_queue.get(timeout=1)
            self.event_generate("<<FrameGenerated>>")

            time.sleep(max(1/self._video_info["framerate"] - time.time() + start, 0))

        self._load_thread = None

        try:
            self.event_generate("<<Ended>>")  # this is generated when the video ends
        except tk.TclError:
            pass

        self._receive_thread.join()
        print("Done")

    def play(self):
        """ plays the video file """
        if self._receive_thread is not None:
            self._receive_thread.start()

        if self._load_thread is not None:
            self._load_thread.start()

    def _receive_and_decode(self):
        videos = ["/home/robert/Desktop/Licenta/audioVideos/1.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/2.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/3.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/4.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/5.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/6.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/7.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/8.mp4",
                  "/home/robert/Desktop/Licenta/audioVideos/9.mp4"]

        for video in videos:
            with av.open(video) as container:
                if not self._stream_initialized:
                    self._init_stream(container.streams.video[0])

                container.streams.video[0].thread_type = "AUTO"
                container.discard_corrupt = True
                for frame in container.decode(video=0):
                    self._frame_queue.put(item=frame.to_image(), block=True)

                for audio_frame in container.decode(audio=0):
                    self._audio_queue.put(item=audio_frame, block=True)

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
    # import tkinter as tk

    root = tk.Tk()
    #
    videoplayer = TkinterVideo(master=root, scaled=True)
    videoplayer.pack(expand=True, fill="both")

    videoplayer.play() # play the video

    root.mainloop()
    # s = time.time()
    # videoplayer.receive_and_decode()
    # end = time.time()
    # print(end-s)




# import av
# import sounddevice as sd
# import pyaudio
# import time
# import numpy as np
#
# def main():
#     import av
#
#     input_container = av.open('/home/robert/Desktop/Licenta/audioVideos/1.mp4')
#     input_stream = input_container.streams.get(audio=0)[0]
#
#     output_container = av.open('live_stream.mp3', 'w')
#     output_stream = output_container.add_stream('mp3')
#
#     for frame in input_container.decode(input_stream):
#         #frame.pts = None
#         for packet in output_stream.encode(frame):
#             output_container.mux(packet)
#
#     for packet in output_stream.encode(None):
#         output_container.mux(packet)
#
#     output_container.close()
#
# if __name__ == "__main__":
#     videos = ["/home/robert/Desktop/Licenta/audioVideos/1.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/2.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/3.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/4.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/5.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/6.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/7.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/8.mp4",
#               "/home/robert/Desktop/Licenta/audioVideos/9.mp4"]
#
#     stream = sd.OutputStream(channels=1, samplerate=44100, dtype="float32")
#
#     all_stream = np.array([], dtype=np.float32)
#     for video in videos:
#         with av.open(video) as container:
#             container.streams.video[0].thread_type = "AUTO"
#             container.discard_corrupt = True
#             for frame in container.decode(container.streams.audio[0]):
#                 all_stream = np.append(all_stream, frame.to_ndarray()[0])
#                 #sd.play(data=frame.to_ndarray()[0], samplerate=44100, blocking=True)
#
#     c = 0
#     stream.start()
#     s = time.time()
#     while c < len(all_stream):
#         #sd.play(data=all_stream.tolist()[c:c+1470 * 2], samplerate=44100, blocking=True)
#         stream.write(all_stream[c:c+1024])
#         c += 1024
#
#     print(time.time() - s)
