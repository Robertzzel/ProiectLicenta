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
</head>
<body>
<div class="col align-self-center">
  <video playsinline muted controls preload="none" width="100%" style="pointer-events: none;"></video>
</div>
<button id="fullscreen-button">Fullscreen</button>

<script>
  let streamingStarted = false;
  let queue = [];
  let video = document.querySelector('video');
  let webSocket = null;
  let sourceBuffer = null;
  let ms = new MediaSource();
  video.src = window.URL.createObjectURL(ms);
  const VIDEO_TYPE = 'video/mp4; codecs=avc1.42E01E,mp4a.40.2'
  //const mimeCodec = 'video/mp4; codecs="avc1.4D0033, mp4a.40.2"';
  //const mimeCodec = 'video/mp4; codecs=avc1.42E01E,mp4a.40.2'; baseline
  //const mimeCodec = 'video/mp4; codecs=avc1.4d002a,mp4a.40.2'; main
  //const mimeCodec = 'video/mp4; codecs="avc1.64001E, mp4a.40.2"'; high
  const SOCKET_URL = "ws://localhost:8081"
  let getBufferedLength = () => {
    return sourceBuffer.buffered.end(0) - video.currentTime;
  }

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
      sourceBuffer.addEventListener("onerror", () => {console.log("Media source error");});
      sourceBuffer.addEventListener("updateend", () => {
        if (sourceBuffer.updating) {
          return;
        }

        if (queue.length>0) {
          sourceBuffer.appendBuffer(queue.shift())
        } else {
          streamingStarted = false;
        }
      });
    }
  }
  function openWSConnection() {
    console.log("openWSConnection::Connecting to: " + SOCKET_URL);

    webSocket = new WebSocket(SOCKET_URL);
    webSocket.debug = true;
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
      if(!streamingStarted) {
        sourceBuffer.appendBuffer(await messageEvent.data.arrayBuffer());
        streamingStarted = true;
      } else {
        queue.push(await messageEvent.data.arrayBuffer())
      }
      
      if(getBufferedLength() > 0.100) {
        video.playbackRate += 0.11111
        setTimeout(()=> {video.playbackRate -= 0.11111}, 1000)
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
    video.requestFullscreen()
    video.muted = false
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
