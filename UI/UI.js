let socket = new WebSocket("ws://localhost:8080")

socket.onopen = () => {
    console.log("Sal")
}

socket.onclose = () => {
    console.log("Pa")
}

socket.onmessage = () => {
    console.log("message")
}