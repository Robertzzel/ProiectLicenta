package main

import (
	kafka "Licenta/kafka"
	"fmt"
	"github.com/mattn/go-gtk/gdkpixbuf"
	"github.com/mattn/go-gtk/glib"
	"github.com/mattn/go-gtk/gtk"
	"os"
	"time"
)

func initWindow() *gtk.Window {
	gtk.Init(nil)
	window := gtk.NewWindow(gtk.WINDOW_TOPLEVEL)
	window.SetPosition(gtk.WIN_POS_CENTER)
	window.SetTitle("GTK Go!")
	window.SetDefaultSize(800, 600)
	window.SetIconName("gtk-dialog-info")
	window.Connect("destroy", func(ctx *glib.CallbackContext) {
		println("got destroy!", ctx.Data().(string))
		gtk.MainQuit()
	}, "foo")
	return window
}

func startApp(window *gtk.Window) {
	window.ShowAll()
	gtk.Main()
}

func main() {
	kp := kafka.NewKafkaConsumer("test1")
	imageFile, err := os.Create("image.png")
	msg, err := kp.Consume()
	if err != nil {
		fmt.Println(err)
		return
	}

	image, _ := gdkpixbuf.NewPixbufFromBytes(msg.Value)
	pixBuff := gtk.NewImageFromPixbuf(image)

	window := initWindow()

	label1 := gtk.NewLabel("sal1")
	label2 := gtk.NewLabel("sal2")
	label3 := gtk.NewLabel("sal3")
	button := gtk.NewButtonWithLabel("Click")

	button.Clicked(func() {
		go func() {
			for {
				msg, _ := kp.Consume()
				imageFile.Seek(0, 0)
				imageFile.Write(msg.Value)
				pixBuff.SetFromFile("image.png")
				time.Sleep(time.Second / 120)
			}
		}()
	})

	verticalLayout := gtk.NewVBox(false, 0)
	horizontalLayout := gtk.NewHBox(false, 0)

	horizontalLayout.Add(label1)
	horizontalLayout.Add(label2)
	verticalLayout.Add(horizontalLayout)
	verticalLayout.Add(label3)
	verticalLayout.Add(button)
	verticalLayout.Add(pixBuff)

	window.Add(verticalLayout)

	startApp(window)
}
