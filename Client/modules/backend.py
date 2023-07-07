import json
import pathlib

from Client.Kafka.Kafka import KafkaConsumerWrapper
from Client.utils.KafkaContainer import KafkaContainer
from Client.utils.Start import Recorder, VideoMerger
from Client.utils.User import User


class Backend:
    def __init__(self):
        self.kafkaContainer: KafkaContainer = None
        self.user: User = None
        self.sender: Recorder = None
        self.truststorePath = str(pathlib.Path(__file__).parent.parent / "truststore.pem")

    def getMyTopic(self):
        return self.kafkaContainer.topic

    def setKafkaConnection(self, address):
        try:
            self.kafkaContainer = KafkaContainer(address, {
                'auto.offset.reset': 'latest',
                'allow.auto.create.topics': "true",
            })
            return True
        except Exception as ex:
            return ex

    def disconnectFromKafka(self):
        self.kafkaContainer = None

    def isConnectedToKafka(self):
        return self.kafkaContainer is not None

    def login(self, username: str, password: str):
        if not self.isConnectedToKafka():
            return Exception("Not Connected To Kafka")
        if username == "" or password == "":
            return Exception("Need both name and pass")

        loggInDetails = {"Name": username, "Password": password, "Topic": self.kafkaContainer.topic}
        message = self.kafkaContainer.databaseCall("LOGIN", json.dumps(loggInDetails).encode(), timeoutSeconds=100)
        if message is None:
            return Exception("Cannot talk to the database")

        if KafkaContainer.getStatusFromMessage(message).lower() != "ok":
            return Exception("username or password wrong")

        message = json.loads(message.value())
        self.user = User(message.get("Id", None), username, password, message.get("CallKey", None),
                         message.get("CallPassword", None), message.get("SessionId", None))
        return None

    def disconnect(self):
        self.user = None

    def registerNewAccount(self, username: str, password: str):
        if not self.isConnectedToKafka():
            return Exception("Not Connected To Kafka")

        if username == "" or password == "":
            return Exception("All fields must be filled")

        message = self.kafkaContainer.databaseCall("REGISTER",
                                                   json.dumps({"Name": username, "Password": password}).encode(),
                                                   timeoutSeconds=1)
        if message is None:
            return Exception("Cannot talk with the database")

        if KafkaContainer.getStatusFromMessage(message).lower() != "ok":
            return Exception("Cannot create account")

        return None

    def getPartnerTopic(self, callKey, callPassword):
        responseMessage = self.kafkaContainer.databaseCall("GET_CALL_BY_KEY", json.dumps(
            {"Key": callKey, "Password": callPassword, "CallerId": str(self.user.id)}).encode(),
                                                           username=self.user.name, password=self.user.password, timeoutSeconds=2)
        if responseMessage is None:
            return Exception("Cannot start call, database not responding")

        status = self.kafkaContainer.getStatusFromMessage(responseMessage)
        if status.lower() != "ok":
            return Exception(f"Cannot start call, user did not start a session")

        responseValue = json.loads(responseMessage.value())
        return responseValue

    def createSession(self):
        msg = self.kafkaContainer.databaseCall(operation="CREATE_SESSION", message=json.dumps({
            "Topic": self.kafkaContainer.topic,
            "UserID": str(self.user.id),
        }).encode(), timeoutSeconds=5, username=self.user.name, password=self.user.password)

        if msg is None:
            return Exception("Cannot talk to the database")

        status = KafkaContainer.getStatusFromMessage(msg)
        if status is None or status.lower() != "ok":
            return Exception("Cannot start sharing")

        self.user.sessionId = int(msg.value().decode())

    def startRecorder(self):
        if self.sender is None:
            self.sender = Recorder(self.kafkaContainer.address)
        if self.sender is not None:
            self.sender.start(self.kafkaContainer.topic)

    def startMerger(self):
        VideoMerger(self.kafkaContainer.address, self.kafkaContainer.topic, self.user.sessionId)

    def stopRecorder(self):
        if self.sender is not None:
            self.sender.stop()

    def changePassword(self, oldPassword, newPassword):
        msg = self.kafkaContainer.databaseCall("CHANGE_PASSWORD", json.dumps({
            "username": self.user.name,
            "password": oldPassword,
            "newPassword": newPassword,
        }).encode(),timeoutSeconds=3, username=self.user.name, password=self.user.password)

        if msg is None:
            return Exception("Cannot talk to the database")

        status = KafkaContainer.getStatusFromMessage(msg)
        if status is None or status.lower() != "ok":
            return Exception("Cannot change password")

        return None

    def deleteVideo(self, videoId):
        msg = self.kafkaContainer.databaseCall("DELETE_VIDEO", json.dumps({
            "userId": str(self.user.id),
            "videoId": str(videoId),
        }).encode(), username=self.user.name, password=self.user.password, timeoutSeconds=10) # TODO VEZI CUM FACI AICI
        if msg is None:
            return Exception("Cannot delete video, database not responding")

        status = self.kafkaContainer.getStatusFromMessage(msg)
        if status.lower() != "ok":
            return Exception(f"Video deleted")

        return None

    def getUserVideos(self):
        msg = self.kafkaContainer.databaseCall("GET_VIDEOS_BY_USER", str(self.user.id).encode(),
                                                      timeoutSeconds=2, username=self.user.name,
                                                      password=self.user.password)
        if msg is None:
            return Exception("Cannot talk to the database")

        status = KafkaContainer.getStatusFromMessage(msg)
        if status is None or status.lower() != "ok":
            return Exception("Cannot get videos")

        data = json.loads(msg.value())

        return ((video.get('Duration'), video.get('Size'),
                 video.get("CreatedAt"), video.get("ID")) for video in data)

    def downloadVideo(self, videoId):
        msg = self.kafkaContainer.databaseCall("DOWNLOAD_VIDEO_BY_ID", str(videoId).encode(), username=self.user.name,
                                               password=self.user.password, timeoutSeconds=100) # TODO VEZI CUM FACI AICI
        if msg is None:
            return Exception("Cannot download video, database not responding")

        status = self.kafkaContainer.getStatusFromMessage(msg)
        if status.lower() != "ok":
            return Exception(f"Download video, {status}")

        return msg.value()

    def getTopicsFromCurrentSession(self):
        if self.user.sessionId is None:
            return Exception("no session active")
        msg = self.kafkaContainer.databaseCall(operation="TOPICS", message=b"msg",
                                               username=self.user.name, sessionId=self.user.sessionId,
                                               password=self.user.password, timeoutSeconds=3)

        return json.loads(msg.value())

    def getNewKafkaConsumer(self, topic, partition=0):
        return KafkaConsumerWrapper(
            brokerAddress=self.kafkaContainer.address,
            topics=[(topic, partition)], certificatePath=self.truststorePath)
