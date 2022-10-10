package main

import (
	. "Licenta/SocketFunctions"
	"fmt"
	"github.com/gorilla/websocket"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
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
    <video playsinline muted controls preload="none" width="100%"></video>
  </div>
  
  <script>
    let queue = []
    let video = document.querySelector('video');
    let webSocket   = null;
    let sourceBuffer = null;
    let streamingStarted = false;
    let ms = new MediaSource();
    video.src = window.URL.createObjectURL(ms);
    const VIDEO_TYPE = 'video/mp4;codecs="avc1.64001e, mp4a.40.2"'
    const SOCKET_URL = "ws://localhost:8081"

	function initMediaSource() {
	video.onerror = () => {console.log("Media element error");}
	video.loop = false;
	video.addEventListener('canplay', (event) => {
	console.log('Video can start, but not sure it will play through.');
	video.play();
	});
	video.addEventListener('paused', (event) => {
	console.log('Video paused for buffering...');
	setTimeout(function() { video.play(); }, 2000);
	});
	
	ms.addEventListener('sourceopen', onMediaSourceOpen);
	
	function onMediaSourceOpen() {
	sourceBuffer = ms.addSourceBuffer(VIDEO_TYPE);
	sourceBuffer.mode = 'sequence';
	sourceBuffer.addEventListener("updateend",loadPacket);
	sourceBuffer.addEventListener("onerror", () => {console.log("Media source error");});
	}
	
	function loadPacket() {
	if (sourceBuffer.updating) {
	return
	}
	if (queue.length>0) {
	appendToBuffer(queue.shift());
	} else {
	streamingStarted = false;
	}
	}
	}
	
	function appendToBuffer(videoChunk) {
	if (videoChunk) {
	sourceBuffer.appendBuffer(videoChunk);
	}
	}
	
	function openWSConnection() {
	console.log("openWSConnection::Connecting to: " + SOCKET_URL);
	
	try {
	webSocket = new WebSocket(SOCKET_URL);
	webSocket.debug = true;
	webSocket.timeoutInterval = 3000;
	webSocket.onopen = function(openEvent) {
	console.log("WebSocket open");
	};
	webSocket.onclose = function (closeEvent) {
	console.log("WebSocket closed");
	};
	webSocket.onerror = function (errorEvent) {
	console.log("WebSocket ERROR: " + error);
	};
	webSocket.onmessage = async function (messageEvent) {
	let wsMsg = messageEvent.data.arrayBuffer();
	console.log(new Date())
	
	if (!streamingStarted) {
	appendToBuffer(await wsMsg);
	streamingStarted=true;
	return;
	}
	queue.push(await wsMsg);
	};
	} catch (exception) {
	console.error(exception);
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
	HtmlFileName = "UI.html"
	MergerSocket = "/tmp/merger.sock"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal("ERROR!! ", err)
	}
}

func openUIInBrowser() error {
	err := os.WriteFile(HtmlFileName, []byte(HtmlFileContents), 0777)
	if err != nil {
		return err
	}

	return exec.Command("xdg-open", HtmlFileName).Run()
}

func handleInputs(inputsReader net.Conn, inputsWriter net.Conn) {
	inputCommand, err := ReceiveMessage(inputsReader)
	checkErr(err)

	checkErr(SendMessage(inputsWriter, inputCommand))
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Need host and port to connect")
	}

	host := os.Args[1]
	port, err := strconv.Atoi(os.Args[2])
	checkErr(err)

	inputListener, err := net.Dial("tcp", "localhost:5001")
	checkErr(err)
	merger, err := net.Dial("unix", MergerSocket)
	checkErr(err)
	router, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	checkErr(err)

	checkErr(openUIInBrowser())

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		checkErr(err)

		go handleInputs(inputListener, router)
		for {
			routerMessage, err := ReceiveMessage(router)
			checkErr(err)

			checkErr(ws.WriteMessage(websocket.BinaryMessage, routerMessage))
			checkErr(SendMessage(merger, routerMessage))
		}
	})

	log.Fatal(http.ListenAndServe("localhost:8081", nil))
}
