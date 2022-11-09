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
	HtmlFileContents = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"  content="connect-src * 'unsafe-inline';">
  <style>
    video {
      background-color: black;
    }
  </style>
</head>
<body style="height: 100vh; width: 100vw">
  <video playsinline autoplay width="100%" height="95%"></video>
  <button id="fullscreen-button">Fullscreen</button>
  <button id="play-button">PLAY</button>

<script>
  const SOCKET_URL = "ws://localhost:8081"
  const VIDEO_TYPE = 'video/mp4; codecs="avc1.4D0033, mp4a.40.2'
  //const mimeCodec = 'video/mp4; codecs="avc1.4D0033, mp4a.40.2"';
  //const mimeCodec = 'video/mp4; codecs=avc1.42E01E,mp4a.40.2'; baseline
  //const mimeCodec = 'video/mp4; codecs=avc1.4d002a,mp4a.40.2'; main
  //const mimeCodec = 'video/mp4; codecs="avc1.64001E, mp4a.40.2"'; high

  let isContentDisplayed = false;
  let videoBuffer = [];
  let videoElement = document.querySelector('video');
  let webSocket = null;
  let videoSourceBuffer = null;
  let mediaSource = new MediaSource();

  videoElement.src = window.URL.createObjectURL(mediaSource);
  let getBufferedLength = () => videoSourceBuffer.buffered.end(0) - videoElement.currentTime;

  function initMediaSource() {
    videoElement.loop = false;

    videoElement.onerror = (err) => {
      console.log("Media element error", err);
    }
    videoElement.addEventListener('canplay', (event) => {
      console.log('Video can start, but not sure it will play through.');
      videoElement.play();
    });
    videoElement.addEventListener('paused', (event) => {
      console.log('Video paused for buffering...');
      setTimeout(() => {videoElement.play()}, 1000);
    });

    mediaSource.addEventListener('sourceopen', () => {
      videoSourceBuffer = mediaSource.addSourceBuffer(VIDEO_TYPE);
      videoSourceBuffer.mode = 'sequence';
      videoSourceBuffer.addEventListener("onerror", () => {console.log("Media source error");});
      videoSourceBuffer.addEventListener("updateend", () => {
        if (videoSourceBuffer.updating) {
          return;
        }

        if (videoBuffer.length>0) {
          videoSourceBuffer.appendBuffer(videoBuffer.shift())
        } else {
          isContentDisplayed = false;
        }
      });
    });
  }
  function openWSConnection() {
    console.log("openWSConnection::Connecting to: " + SOCKET_URL);

    webSocket = new WebSocket(SOCKET_URL);
    webSocket.debug = true;
    webSocket.onopen = () => { console.log("WebSocket open") };
    webSocket.onclose = () => { console.log("WebSocket closed") };
    webSocket.onerror = (errorEvent) => { console.log("WebSocket ERROR: " + errorEvent) };
    webSocket.onmessage = async (messageEvent) => {
      if(!isContentDisplayed) {
        videoSourceBuffer.appendBuffer(await messageEvent.data.arrayBuffer());
        isContentDisplayed = true;
      } else {
        videoBuffer.push(await messageEvent.data.arrayBuffer())
      }

      if(getBufferedLength() > 1) {
        videoElement.playbackRate += 0.11111
        setTimeout(()=> {videoElement.playbackRate -= 0.11111}, 10_000)
      }

      if(!videoElement.muted) {
        try {
          videoElement.muted = false
        } catch (err) {
          console.log("Unmute error, ", err)
        }
      }
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
  document.getElementById("fullscreen-button").addEventListener("click", () => {
    videoElement.requestFullscreen()
  })
  document.getElementById("play-button").addEventListener("click", () => {
    videoElement.play()
  })
</script>
</body>
</html>`
	HtmlFileName    = "UI.html"
	AggregatorTopic = "aggregator"
	ReceiverTopic   = "ReceiverPing"
)

var quit = make(chan os.Signal, 2)

var webSocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func openUiInBrowser() error {
	err := os.WriteFile(HtmlFileName, []byte(HtmlFileContents), 0777)
	if err != nil {
		return err
	}

	return exec.Command("xdg-open", HtmlFileName).Run()
}

func CollectAndSendNextVideoFile(producer *Kafka.Producer, consumer *Kafka.Consumer, webSocket *websocket.Conn) error {
	if err := consumer.SetOffsetToNow(); err != nil {
		return err
	}

	for {
		aggregatorMessage, err := consumer.Consume()
		if err != nil {
			return err
		}

		if err := webSocket.WriteMessage(websocket.BinaryMessage, aggregatorMessage.Message); err != nil {
			return err
		}

		if err := producer.Publish(&Kafka.ProducerMessage{Message: []byte(fmt.Sprint(time.Now().UnixMilli())), Topic: ReceiverTopic}); err != nil {
			return err
		}
		fmt.Println("Message sent ", time.Now().UnixMilli())

		return nil
	}
}

func main() {
	aggregatorConsumer := Kafka.NewConsumer(AggregatorTopic)
	defer aggregatorConsumer.Close()

	producer := Kafka.NewProducer()
	defer producer.Close()

	if err := openUiInBrowser(); err != nil {
		log.Println("Error while opening web browser", err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := webSocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			quit <- syscall.SIGINT
			return
		}

		for {
			if err := CollectAndSendNextVideoFile(producer, aggregatorConsumer, ws); err != nil {
				log.Println(err)
				quit <- syscall.SIGINT
				return
			}
		}
	})

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		fmt.Println("Cleanup...")
		aggregatorConsumer.Close()
		producer.Close()
		fmt.Println("Cleanup Done")
		os.Exit(1)
	}()

	if err := http.ListenAndServe("localhost:8081", nil); err != nil {
		log.Println(err)
	}
}
