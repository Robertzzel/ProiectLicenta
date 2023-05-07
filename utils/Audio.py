import speech_recognition as sr
import pyttsx3
from PySide6.QtCore import QThread, Signal


class AudioTool(QThread):
    audioClickSignal = Signal(str)

    def __init__(self):
        super().__init__()
        self.recognizer = sr.Recognizer()
        self.microphone = sr.Microphone()
        self.quitFlag = True

    def speechToText(self):
        with self.microphone as source:
            self.recognizer.adjust_for_ambient_noise(source)
            audio = self.recognizer.listen(source)

        try:
            return self.recognizer.recognize_google(audio)
        except sr.RequestError:
            return Exception("API unavailable")
        except sr.UnknownValueError:
            return Exception("Unable to recognize speech")

    def textToSpeech(self, myText):
        engine.say(myText)
        engine.runAndWait()

    def stop(self):
        self.quitFlag = False

    def run(self):
        while self.quitFlag:
            text = self.speechToText()
            if type(text) is Exception:
                print(str(text))
            else:
                self.audioClickSignal.emit(text.lower())


if __name__ == '__main__':
    audio = AudioTool()
    engine = pyttsx3.init()
    action = 'Listening'
    print(action)
    audio.textToSpeech(action)
    quitFlag = True

