import subprocess, sys


class VideoAggregator:
    @staticmethod
    def aggregateVideos(files: str, resultFile: str):
        process = subprocess.Popen(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", files, "-c", "copy", resultFile], stdout=subprocess.PIPE, stderr=sys.stderr)
        out, err = process.communicate()
        if err is not None and err != b"":
            raise Exception("CONCAT ERROR:\n" + err.decode())