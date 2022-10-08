import os.path
import sys
import asyncio
import time

import websockets
import webbrowser

FRONTEND_PAGE = """
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
    const SOCKET_URL = `ws://localhost:8081`

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
</html>
"""
MERGER_SOCKET = "/tmp/merger.sock"
DATABASE_SOCKET_NAME = "/tmp/database.sock"
INPUTS_SOCKET_NAME = "/tmp/inputListener.sock"
MESSAGE_SIZE_LENGTH = 10


async def receive_message(reader: asyncio.StreamReader) -> bytes:
    buffer = b''
    while True:
        buffer += await reader.read(MESSAGE_SIZE_LENGTH)
        if len(buffer) >= MESSAGE_SIZE_LENGTH:
            break

    message_size = int(buffer.decode())
    buffer = b''
    while True:
        buffer += await reader.read(message_size)
        if len(buffer) >= message_size:
            break

    return buffer


def send_message(writer: asyncio.StreamWriter, message: bytes):
    writer.write(str(len(message)).rjust(10, '0').encode())
    writer.write(message)


async def start_app(ip: str, port: int):
    inputs_reader, _ = await asyncio.open_unix_connection(path=INPUTS_SOCKET_NAME)
    _, merger_writer = await asyncio.open_unix_connection(path=MERGER_SOCKET)
    router_reader, router_writer = await asyncio.open_connection(host=ip, port=port)

    if not os.path.isfile("UI.html"):
        with open("UI.html", "w") as f:
            f.write(FRONTEND_PAGE)

    webbrowser.open("file://" + os.path.realpath("UI.html"))

    async with websockets.serve(lambda ws: handle(ws, router_reader, merger_writer, inputs_reader, router_writer), "localhost", 8081):
        await asyncio.Future()


async def handle(websocket, reader, merger, inputs_reader, videos_writer):
    inputs_task = asyncio.create_task(send_inputs(videos_writer, inputs_reader))

    try:
        while True:
            buffer = await receive_message(reader)
            print("Message received, ", time.time())
            await websocket.send(buffer)
            send_message(merger, buffer)
    except Exception as ex:
        print(ex)
        await inputs_task
        sys.exit(1)


async def send_inputs(router_writer, inputs_reader):
    while True:
        send_message(router_writer, await receive_message(inputs_reader))


async def print_all_videos():
    reader, writer = await asyncio.open_unix_connection(path=DATABASE_SOCKET_NAME)
    send_message(writer, b"query;all")
    message = await receive_message(reader)
    print(message)


def main():
    command = sys.argv[1]

    if command == "start":
        ip, port = sys.argv[2], int(sys.argv[3])
        asyncio.run(start_app(ip, port))
    if command == "videos":
        asyncio.run(print_all_videos())
    else:
        print("Command not found")


if __name__ == "__main__":
    main()
