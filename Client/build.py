import platform
import subprocess


def buildForLinux():
    p = subprocess.Popen("go build -o VideoMicroservice.exe ./VideoMicroservice/VideoMicroservice.go ./VideoMicroservice/recorder.go ./VideoMicroservice/resizer.go ./VideoMicroservice/screenshot.go ./VideoMicroservice/screen.go ./VideoMicroservice/byte_image.go ./VideoMicroservice/Messager.go".split(), stderr=subprocess.PIPE)
    p.wait()
    err = p.stderr.read()
    if err != b"":
        return err

    p = subprocess.Popen("go build -o AggregatorMicroservice.exe AggregatorMicroservice/AggregatorMicroservice.go AggregatorMicroservice/AudioVideoPair.go AggregatorMicroservice/Messager.go".split(), stderr=subprocess.PIPE)
    p.wait()
    err = p.stderr.read()
    if err != b"":
        return err

    p = subprocess.Popen("chmod +x VideoMicroservice.exe".split(), stderr=subprocess.PIPE)
    p.wait()
    err = p.stderr.read()
    if err != b"":
        return err

    p = subprocess.Popen("chmod +x AggregatorMicroservice.exe".split(), stderr=subprocess.PIPE)
    p.wait()
    err = p.stderr.read()
    if err != b"":
        return err


def buildForWindows():
    p = subprocess.Popen("go build -o VideoMicroservice.exe ./VideoMicroservice/VideoMicroservice.go ./VideoMicroservice/recorder.go ./VideoMicroservice/resizer.go ./VideoMicroservice/screenshot.go ./VideoMicroservice/screen.go ./VideoMicroservice/byte_image.go ./VideoMicroservice/Messager.go".split(), stderr=subprocess.PIPE)
    p.wait()
    err = p.stderr.read()
    if err != b"":
        return err

    p = subprocess.Popen("go build -o AggregatorMicroservice.exe AggregatorMicroservice/AggregatorMicroservice.go AggregatorMicroservice/AudioVideoPair.go AggregatorMicroservice/Messager.go".split(), stderr=subprocess.PIPE)
    p.wait()
    err = p.stderr.read()
    if err != b"":
        return err

if __name__ == "__main__":
    PLATFORM = platform.system().lower()
    if PLATFORM == "linux":
        buildForLinux()
    elif PLATFORM == "windows":
        buildForWindows()
