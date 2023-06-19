import queue
import time
import wave

import av
import numpy as np
import sounddevice as sd
import pyaudio

print(__file__)

def get_device(device_name: str):
    for index, dev in enumerate(sd.query_devices()):
        if device_name in dev['name']:
            return index
    return None

duration_in_seconds = 3
sample_rate = 44100
print(sd.query_devices())
# Set up PyAudio
audio = pyaudio.PyAudio()

# Find the index of the PulseAudio device
pulse_device_index = get_device("pulse")
# Define the audio callback function
def process_audio(indata, frame_count, time_info, status):
    recording.extend(indata)
    return None, pyaudio.paContinue

# Start recording the audio
recording = bytearray()
stream = audio.open(format=pyaudio.paInt32,
                    channels=1,
                    rate=sample_rate,
                    input=True,
                    #input_device_index=pulse_device_index,
                    output=False,
                    stream_callback=process_audio)

# Start the audio stream
print("start")
stream.start_stream()

# Wait for the duration of the recording
time.sleep(duration_in_seconds)

# Stop the audio stream
stream.stop_stream()
stream.close()
audio.terminate()
print("finish")

# Save the recorded audio to a file
if len(recording) > 0:
    filename = "output.wav"
    wf = wave.open(filename, 'wb')
    wf.setnchannels(1)
    wf.setsampwidth(audio.get_sample_size(pyaudio.paInt32))
    wf.setframerate(sample_rate)
    wf.writeframes(recording)
    wf.close()

# Set chunk size of 1024 samples per data frame
chunk = 1024

# Open the sound file
wf = wave.open("output.wav", 'rb')

# Create an interface to PortAudio
q = queue.Queue()
with open('output.wav', "rb") as f:
    with av.open(f) as container:
        container.fast_seek, container.discard_corrupt = True, True
        for packet in container.demux():
            for frame in packet.decode():
                q.put(frame.to_ndarray())

def process_audio_data(data):
    data = np.where(np.abs(data) < np.mean(np.abs(data))/2, 0, data)
    return data

def audioCallback(outdata: np.ndarray, frames: int, timet, status):
    try:
        data: np.ndarray = q.get_nowait()
        print(data)
        data = np.append(data, np.array([0] * (1024 - data.size), dtype=data.dtype))
        data.shape = (1024, 1)

        #data = process_audio_data(data)
    except queue.Empty:
        data = np.zeros(shape=(1024, 1), dtype=np.int32)
    except Exception as ex:
        print("Error:", ex)
        return

    outdata[:] = data

process_audio_data(q.get())
audioStream = sd.OutputStream(channels=1, samplerate=sample_rate, dtype="int32", callback=audioCallback, blocksize=1024)
audioStream.start()
time.sleep(duration_in_seconds + 0.5)
print(q.queue)