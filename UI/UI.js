const mediaSource = new MediaSource();
var buffer;
var queue = [];
const videoElement = document.getElementById("video")
videoElement.src = window.URL.createObjectURL(mediaSource);
const mimeType = 'video/mp4;codecs="avc1.64001e, mp4a.40.2"'


mediaSource.addEventListener('sourceopen', onSourceOpen, false)


function onSourceOpen() {
    buffer = mediaSource.addSourceBuffer(mimeType);

    buffer.addEventListener('update', function() { // Note: Have tried 'updateend'
        if (queue.length > 0 && !buffer.updating) {
            console.log("updating")
            buffer.appendBuffer(queue.shift());
            videoElement.play()
            //mediaSource.endOfStream()
        }
    });

    var socket = new WebSocket("ws://localhost:8080")
    socket.binaryType = "arraybuffer";

    socket.onopen = () => {
        console.log("Sal")
        socket.send("hello")
    }
    socket.onclose = () => {
        console.log("Pa")
    }
    socket.addEventListener('message', function(e) {
        if (typeof e.data !== 'string') {
            if (buffer.updating || queue.length > 0) {
                queue.push(e.data);
            } else {
                console.log("adaugat")
                buffer.appendBuffer(e.data);
                videoElement.play()
                //mediaSource.endOfStream()
            }
        }
    }, false);
}

