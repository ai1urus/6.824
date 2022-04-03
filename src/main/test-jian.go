package main

import (
	"fmt"

	"6.824/mr"
)

func main() {
	files := []string{"pg-grimm.txt", "pg-being_ernest.txt"}
	c := mr.MakeCoordinator(files, 10)
	fmt.Println(c.Done())
}
