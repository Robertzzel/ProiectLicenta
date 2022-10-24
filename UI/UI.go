package main

import (
	"Licenta/Kafka"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

const (
	HtmlFileContents = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"  content="connect-src * 'unsafe-inline';">
</head>
<body>
  <div class="col align-self-center">
    <video playsinline muted controls preload="none" width="100%" style="pointer-events: none;"></video>
  </div>
  
  <script>

let queue = []
let video = document.querySelector('video');
video.onpause = () => { video.play(); }
video.defaultPlaybackRate = 1.03;
let webSocket = null;
let sourceBuffer = null;
let ms = new MediaSource();
video.src = window.URL.createObjectURL(ms);
const VIDEO_TYPE = 'video/mp4;codecs="avc1.64001e, mp4a.40.2"'
const SOCKET_URL = "ws://localhost:8081"

function initMediaSource() {
    video.onerror = () => {
        console.log("Media element error");
    }
    video.loop = false;
    video.addEventListener('canplay', (event) => {
        console.log('Video can start, but not sure it will play through.');
        video.play();
    });
    video.addEventListener('paused', (event) => {
        console.log('Video paused for buffering...');
        setTimeout(function() {
            video.play();
        }, 1);
    });

    ms.addEventListener('sourceopen', onMediaSourceOpen);

    function onMediaSourceOpen() {
        sourceBuffer = ms.addSourceBuffer(VIDEO_TYPE);
        sourceBuffer.mode = 'sequence';
        sourceBuffer.addEventListener("onerror", () => {
            console.log("Media source error");
        });
    }
}
function openWSConnection() {
    console.log("openWSConnection::Connecting to: " + SOCKET_URL);

    webSocket = new WebSocket(SOCKET_URL);
    webSocket.debug = false;
    webSocket.timeoutInterval = 3000;
    webSocket.onopen = function(openEvent) {
        console.log("WebSocket open");
    };
    webSocket.onclose = function(closeEvent) {
        console.log("WebSocket closed");
    };
    webSocket.onerror = function(errorEvent) {
        console.log("WebSocket ERROR: " + errorEvent);
    };
    webSocket.onmessage = async function(messageEvent) {
        sourceBuffer.appendBuffer(await messageEvent.data.arrayBuffer());
    }
}

if (!window.MediaSource) {
    console.error("No Media Source API available");
}

if (!MediaSource.isTypeSupported(VIDEO_TYPE)) {
    console.error("Unsupported MIME type or codec: " + VIDEO_TYPE);
}

initMediaSource();
openWSConnection();
</script>
</body>
</html>`
	HtmlFileName    = "UI.html"
	AggregatorTopic = "aggregator"
	ReceiverTopic   = "ReceiverPing"
)

func checkErr(err error, msg string) {
	if err != nil {
		log.Println(msg)
		panic(err)
	}
}

func openUiInBrowser() error {
	err := os.WriteFile(HtmlFileName, []byte(HtmlFileContents), 0777)
	if err != nil {
		return err
	}

	return exec.Command("xdg-open", HtmlFileName).Run()
}

func main() {
	aggregatorConsumer, err := Kafka.NewConsumer(AggregatorTopic)
	checkErr(err, "Error creating aggregator consumer")
	producer, err := Kafka.NewProducer()
	checkErr(err, "Error while creating producer")

	checkErr(openUiInBrowser(), "Error while opening web browser")

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		checkErr(err, "Error while upgrading connection")

		for {
			checkErr(ws.WriteMessage(websocket.BinaryMessage, aggregatorConsumer.Consume()), "Error while sending message to web")

			producer.Publish([]byte(fmt.Sprint(time.Now().UnixMilli())), ReceiverTopic)
			fmt.Println("Message sent ", time.Now().UnixMilli())
		}
	})

	quit := make(chan os.Signal, 2)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("Cleanup...")
		aggregatorConsumer.Close()
		producer.Close()
		fmt.Println("Cleanup Done")
	}()

	log.Fatal(http.ListenAndServe("localhost:8081", nil))
}
