const mediaSource = new MediaSource();
var buffer;
var queue = [];
const videoElement = document.getElementById("video")
videoElement.src = window.URL.createObjectURL(mediaSource);
const mimeType = 'video/mp4;codecs="avc1.64001e, mp4a.40.2"'


mediaSource.addEventListener('sourceopen', onSourceOpen, false)
//mediaSource.addEventListener('sourceended', function(e) { console.log('sourceended: ' + mediaSource.readyState); });
//mediaSource.addEventListener('sourceclose', function(e) { console.log('sourceclose: ' + mediaSource.readyState); });
//mediaSource.addEventListener('error', function(e) { console.log('error: ' + mediaSource.readyState); });


function onSourceOpen() {
    if(MediaSource.isTypeSupported(mimeType)){
        console.log("suported")
    }
    buffer = mediaSource.addSourceBuffer(mimeType);

    //buffer.addEventListener('updatestart', function(e) { console.log('updatestart: ' + mediaSource.readyState); });
    //buffer.addEventListener('update', function(e) { console.log('update: ' + mediaSource.readyState); });
    //buffer.addEventListener('updateend', function(e) { console.log('updateend: ' + mediaSource.readyState); });
    //buffer.addEventListener('error', function(e) { console.log('error: ' + mediaSource.readyState); });
    //buffer.addEventListener('abort', function(e) { console.log('abort: ' + mediaSource.readyState); });
    // buffer.addEventListener('update', function() { // Note: Have tried 'updateend'
    //     if (queue.length > 0 && !buffer.updating) {
    //         console.log("updating")
    //         buffer.appendBuffer(queue.shift());
    //         videoElement.play()
    //         mediaSource.endOfStream()
    //     }
    // });

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
            // if (buffer.updating || queue.length > 0) {
            //     queue.push(e.data);
            // } else {
            console.log("adaugat")
            buffer.appendBuffer(e.data);
            videoElement.currentTime = 0
            videoElement.play()

            // }
        }
    }, false);
}