import tempfile


class TempFile:

    def __init__(self, delete=True):
        self.file = tempfile.NamedTemporaryFile(mode="w+", suffix=".mp4", delete=delete)
        self.name = self.file.name

    def readStringLines(self):
        with open(self.name, 'r') as f:
            return f.readlines()

    def readString(self):
        with open(self.name, 'r') as f:
            return f.read()

    def readBytes(self):
        with open(self.name, "rb") as f:
            return f.read()

    def write(self, text):
        with open(self.name, "a+") as f:
            f.write(text)

    def writeBytes(self, text):
        with open(self.name, "ab") as f:
            f.write(text)

    def close(self):
        self.file.close()