package main

import (
	"image"
	"math"
	"runtime"
	"sync"
)

// values <1 will sharpen the image
var blur = 1.0

func Thumbnail(maxWidth, maxHeight uint, img *ByteImage) image.Image {
	origWidth := img.Width
	origHeight := img.Height
	newWidth, newHeight := origWidth, origHeight

	// Preserve aspect ratio
	if origWidth > maxWidth {
		newHeight = origHeight * maxWidth / origWidth
		if newHeight < 1 {
			newHeight = 1
		}
		newWidth = maxWidth
	}

	if newHeight > maxHeight {
		newWidth = newWidth * maxHeight / newHeight
		if newWidth < 1 {
			newWidth = 1
		}
		newHeight = maxHeight
	}
	return Resize(newWidth, newHeight, img)
}

func Resize(width, height uint, img *ByteImage) image.Image {
	scaleX, scaleY := calcFactors(width, height, float64(img.Width), float64(img.Height))
	if width == 0 {
		width = uint(0.7 + float64(img.Width)/scaleX)
	}
	if height == 0 {
		height = uint(0.7 + float64(img.Height)/scaleY)
	}

	return resizeNearest(width, height, scaleX, scaleY, img)
}

func resizeNearest(width, height uint, scaleX, scaleY float64, img *ByteImage) image.Image {
	taps := 2
	cpus := runtime.GOMAXPROCS(0)
	wg := sync.WaitGroup{}

	// 8-bit precision
	temp := image.NewRGBA(image.Rect(0, 0, int(img.Height), int(width)))
	result := image.NewRGBA(image.Rect(0, 0, int(width), int(height)))

	// horizontal filter, results in transposed temporary image
	coeffs, offset, filterLength := createWeightsNearest(temp.Bounds().Dy(), taps, blur, scaleX)
	wg.Add(cpus)
	for i := 0; i < cpus; i++ {
		slice := makeSlice(temp, i, cpus).(*image.RGBA)
		go func() {
			defer wg.Done()
			nearestRGBA(img, slice, coeffs, offset, filterLength)
		}()
	}
	wg.Wait()

	// horizontal filter on transposed image, result is not transposed
	coeffs, offset, filterLength = createWeightsNearest(result.Bounds().Dy(), taps, blur, scaleY)
	wg.Add(cpus)
	for i := 0; i < cpus; i++ {
		slice := makeSlice(result, i, cpus).(*image.RGBA)
		go func() {
			defer wg.Done()
			nearestRGBAOnImage(temp, slice, coeffs, offset, filterLength)
		}()
	}
	wg.Wait()
	return result
}

func nearestRGBA(in *ByteImage, out *image.RGBA, coeffs []bool, offset []int, filterLength int) {
	newBounds := out.Bounds()
	maxX := int(in.Width - 1)

	for x := newBounds.Min.X; x < newBounds.Max.X; x++ {
		row := in.Data[uint(x)*in.Stride:]
		for y := newBounds.Min.Y; y < newBounds.Max.Y; y++ {
			var rgba [4]float32
			var sum float32
			start := offset[y]
			ci := y * filterLength
			for i := 0; i < filterLength; i++ {
				if coeffs[ci+i] {
					xi := start + i
					switch {
					case uint(xi) < uint(maxX):
						xi *= 4
					case xi >= maxX:
						xi = 4 * maxX
					default:
						xi = 0
					}
					rgba[0] += float32(row[xi+2]) // r
					rgba[1] += float32(row[xi+1]) // g
					rgba[2] += float32(row[xi])   // b
					sum++
				}
			}

			xo := (y-newBounds.Min.Y)*out.Stride + (x-newBounds.Min.X)*4
			out.Pix[xo+0] = floatToUint8(rgba[0] / sum)
			out.Pix[xo+1] = floatToUint8(rgba[1] / sum)
			out.Pix[xo+2] = floatToUint8(rgba[2] / sum)
			out.Pix[xo+3] = 255
		}
	}
}

func nearestRGBAOnImage(in *image.RGBA, out *image.RGBA, coeffs []bool, offset []int, filterLength int) {
	newBounds := out.Bounds()
	maxX := in.Bounds().Dx() - 1

	for x := newBounds.Min.X; x < newBounds.Max.X; x++ {
		row := in.Pix[x*in.Stride:]
		for y := newBounds.Min.Y; y < newBounds.Max.Y; y++ {
			var rgba [4]float32
			var sum float32
			start := offset[y]
			ci := y * filterLength
			for i := 0; i < filterLength; i++ {
				if coeffs[ci+i] {
					xi := start + i
					switch {
					case uint(xi) < uint(maxX):
						xi *= 4
					case xi >= maxX:
						xi = 4 * maxX
					default:
						xi = 0
					}
					rgba[0] += float32(row[xi+0])
					rgba[1] += float32(row[xi+1])
					rgba[2] += float32(row[xi+2])
					sum++
				}
			}

			xo := (y-newBounds.Min.Y)*out.Stride + (x-newBounds.Min.X)*4
			out.Pix[xo+0] = floatToUint8(rgba[0] / sum)
			out.Pix[xo+1] = floatToUint8(rgba[1] / sum)
			out.Pix[xo+2] = floatToUint8(rgba[2] / sum)
			out.Pix[xo+3] = 255
		}
	}
}

// Calculates scaling factors using old and new image dimensions.
func calcFactors(width, height uint, oldWidth, oldHeight float64) (scaleX, scaleY float64) {
	if width == 0 {
		if height == 0 {
			scaleX = 1.0
			scaleY = 1.0
		} else {
			scaleY = oldHeight / float64(height)
			scaleX = scaleY
		}
	} else {
		scaleX = oldWidth / float64(width)
		if height == 0 {
			scaleY = scaleX
		} else {
			scaleY = oldHeight / float64(height)
		}
	}
	return
}

type imageWithSubImage interface {
	image.Image
	SubImage(image.Rectangle) image.Image
}

func makeSlice(img imageWithSubImage, i, n int) image.Image {
	return img.SubImage(image.Rect(img.Bounds().Min.X, img.Bounds().Min.Y+i*img.Bounds().Dy()/n, img.Bounds().Max.X, img.Bounds().Min.Y+(i+1)*img.Bounds().Dy()/n))
}

func createWeightsNearest(dy, filterLength int, blur, scale float64) ([]bool, []int, int) {
	filterLength = filterLength * int(math.Max(math.Ceil(blur*scale), 1))
	filterFactor := math.Min(1./(blur*scale), 1)

	coeffs := make([]bool, dy*filterLength)
	start := make([]int, dy)
	for y := 0; y < dy; y++ {
		interpX := scale*(float64(y)+0.5) - 0.5
		start[y] = int(interpX) - filterLength/2 + 1
		interpX -= float64(start[y])
		for i := 0; i < filterLength; i++ {
			in := (interpX - float64(i)) * filterFactor
			if in >= -0.5 && in < 0.5 {
				coeffs[y*filterLength+i] = true
			} else {
				coeffs[y*filterLength+i] = false
			}
		}
	}

	return coeffs, start, filterLength
}

func floatToUint8(x float32) uint8 {
	if x > 0xfe {
		return 0xff
	}
	return uint8(x)
}
