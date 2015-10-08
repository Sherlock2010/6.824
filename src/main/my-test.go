package main

import "os"
import "fmt"
// import "strings"

//go run wc.go x.txt
func main() {
  file, err := os.Open(os.Args[1])

  fi, err := file.Stat();

  //each split file size
  size := fi.Size()
  b := make([]byte, size);
  _, err = file.Read(b);
  file.Close()

  fmt.Println(string(b))
}