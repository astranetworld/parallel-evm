package main

/*
#cgo LDFLAGS: -L./lib -lrustdemo
#include <stdlib.h>
#include "./lib/rustdemo.h"
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"
)

func main() {
	var a sync.WaitGroup
	for i := 0; i < 100; i++ {
		a.Add(1)
		ii := i
		go func(ii int) {
			defer a.Done()
			s := fmt.Sprintf("Go say: Hello Rust:____%d", ii)
			input := C.CString(s)
			defer C.free(unsafe.Pointer(input))
			o := C.rustdemo(input)
			output := C.GoString(o)
			fmt.Printf("%s\n", output)
		}(ii)
	}
	a.Wait()
}
