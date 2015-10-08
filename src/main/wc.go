package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "strings"
import "strconv"
import "unicode"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
// map function return a list of KeyValue, represents a words total occurance in each split file  
func Map(value string) *list.List {
  f := func(c rune) bool {
    return !unicode.IsLetter(c) && !unicode.IsNumber(c)
  }
  words := strings.FieldsFunc(value , f)

  kvs := make(map[string]int)

  for i := 0; i < len(words); i++ {
    word := words[i]

    _, ok := kvs[word]

    if !ok {
      kvs[word] = 1
    } else {
      kvs[word] += 1
    }

  }

  var r list.List

  for key, value := range kvs {
    r.PushBack(mapreduce.KeyValue{key, strconv.Itoa(value)})
  }

  return &r
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
  var res int

  for e := values.Front(); e != nil; e = e.Next() {
    tmp, _ := strconv.Atoi(e.Value.(string))
    res += tmp
  }

  return strconv.Itoa(res)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])    
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
