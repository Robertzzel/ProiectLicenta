package main

import "C"
import (
	"errors"
	"unsafe"
)

/*
#cgo pkg-config: xcb
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <xcb/xcb.h>

int captureScreen(xcb_connection_t* connection, xcb_screen_t* screen, uint8_t* data, int length) {
	xcb_get_image_reply_t *reply = xcb_get_image_reply(
		connection,
		xcb_get_image(connection, XCB_IMAGE_FORMAT_Z_PIXMAP, screen->root, 0, 0, screen->width_in_pixels, screen->height_in_pixels, 0xffffffff),
		NULL);
	if (reply == NULL) {
		return 1;
	}
	memcpy(data, xcb_get_image_data(reply), length);
	free(reply);
	return 0;
}
*/
import "C"

type Screen struct {
	connection *C.xcb_connection_t
	screen     *C.xcb_screen_t
	Width      int // Lățimea imaginii capturate
	Height     int // Înălțimea imaginii capturate
	Image      []byte
}

func NewScreen() (Screen, error) {
	connection := C.xcb_connect(nil, nil)
	if C.xcb_connection_has_error(connection) != 0 {
		return Screen{}, errors.New("Eroare la conectarea la serverul X")
	}
	screen := C.xcb_setup_roots_iterator(C.xcb_get_setup(connection)).data
	width := int(screen.width_in_pixels)
	height := int(screen.height_in_pixels)

	return Screen{
		connection,
		screen,
		width,
		height,
		make([]byte, width*height*4)}, nil
}

func (sc *Screen) Capture() bool {
	return C.captureScreen(sc.connection, sc.screen, (*C.uint8_t)(unsafe.Pointer(&sc.Image[0])), (C.int)(cap(sc.Image))) == 1
}

func (sc *Screen) Close() {
	C.xcb_disconnect(sc.connection)
}
