package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/dolthub/swiss"
)

var (
	// others: "heap", "threadcreate", "block", "mutex"
	profileTypes = []string{"goroutine", "allocs"}

	shouldProfile = flag.Bool("p", false, "if true, enables profiling")
)

const maxNames = 10000
const maxNameLen = 100
const maxLineLenBytes = 128
const maxChunkSize = 64 * 1024 * 1024 // 64MB

type Stats struct {
	Min   float64
	Sum   float64
	Max   float64
	Count int
}

type Line struct {
	Name string
	Temp float64
}

type BRC struct {
	inputFile       string
	numParsers      int
	output          *swiss.Map[string, *Stats]
	wg              *sync.WaitGroup
	offsetChan      chan int64
	mergeOutputChan chan *swiss.Map[string, *Stats]
}

func newBRC(inputFile string, numParsers int) *BRC {
	return &BRC{
		inputFile:       inputFile,
		numParsers:      numParsers,
		output:          swiss.NewMap[string, *Stats](maxNames),
		wg:              &sync.WaitGroup{},
		mergeOutputChan: make(chan *swiss.Map[string, *Stats], numParsers),
		offsetChan:      make(chan int64, numParsers),
	}
}

func init() {
	flag.Parse()
}

func main() {
	if *shouldProfile {
		nowUnix := time.Now().Unix()
		os.MkdirAll(fmt.Sprintf("profiles/%d", nowUnix), 0755)
		for _, profileType := range profileTypes {
			file, _ := os.Create(fmt.Sprintf("profiles/%d/%s.pprof",
				nowUnix, profileType))
			defer file.Close()
			defer pprof.Lookup(profileType).WriteTo(file, 0)
		}

		file, _ := os.Create(fmt.Sprintf("profiles/%d/cpu.pprof",
			nowUnix))
		defer file.Close()
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

	numParsers := runtime.NumCPU()
	brc := newBRC("./data/measurements.txt", numParsers)

	f, err := os.Open(brc.inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	go func() {
		stat, err := f.Stat()
		if err != nil {
			return
		}

		for i := int64(0); i < stat.Size(); i += maxChunkSize {
			brc.offsetChan <- i
		}

		close(brc.offsetChan)
	}()

	for i := 0; i < brc.numParsers; i++ {
		brc.wg.Add(1)

		go func() {
			defer brc.wg.Done()
			buf := make([]byte, maxChunkSize+maxLineLenBytes)

			for offset := range brc.offsetChan {
				agg := brc.parseFile(f, buf, offset, maxChunkSize)
				brc.mergeOutputChan <- agg
			}
		}()
	}

	go func() {
		brc.wg.Wait()
		close(brc.mergeOutputChan)
	}()

	brc.mergeOutputs()
	brc.printOutput()
}

func (b *BRC) parseFile(f *os.File, buf []byte, offset int64, chunkSize int) *swiss.Map[string, *Stats] {
	agg := swiss.NewMap[string, *Stats](maxNames)

	bytesRead, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	isNamePart := true
	currIdx := 0

	if offset != 0 {
		// move to the character after the first newline from the offset
		for currIdx < bytesRead {
			if buf[currIdx] == '\n' {
				currIdx++
				break
			}
			currIdx++
		}
	}

	start := currIdx
	name := make([]byte, maxNameLen)
	nameLen := 0

	// a line has 2 parts: <name>;<value>
	for {
		if isNamePart {
			// read until semi-colon
			for currIdx < bytesRead {
				if buf[currIdx] == ';' {
					nameLen = copy(name, buf[start:currIdx])
					currIdx++
					start = currIdx
					isNamePart = false

					break
				}

				currIdx++
			}
		} else {
			// read until newline
			for currIdx < bytesRead {
				if buf[currIdx] == '\n' {
					value := parseFloat(buf[start:currIdx])
					nameUnsafeStr := unsafe.String(&name[0], nameLen)

					if a, exists := agg.Get(nameUnsafeStr); !exists {
						name := string(name[:nameLen])
						agg.Put(name, &Stats{
							Min:   value,
							Sum:   value,
							Max:   value,
							Count: 1,
						})
					} else {
						a.Count++
						a.Sum += value
						if value < a.Min {
							a.Min = value
						}
						if value > a.Max {
							a.Max = value
						}
					}

					currIdx++
					start = currIdx
					isNamePart = true
					break
				}

				currIdx++
			}
		}

		// we want to break out if we encounter a new line after the chunkSize
		if currIdx >= bytesRead || (isNamePart && currIdx >= chunkSize) {
			break
		}
	}

	return agg
}

func parseFloat(bs []byte) float64 {
	var intStartIdx int // is negative?
	if bs[0] == '-' {
		intStartIdx = 1
	}

	v := float64(bs[len(bs)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(bs) - 3; i >= intStartIdx; i-- { // integer part
		v += float64(bs[i]-'0') * place
		place *= 10
	}

	if intStartIdx == 1 {
		v *= -1
	}
	return v
}

func (b *BRC) mergeOutputs() {
	output := swiss.NewMap[string, *Stats](maxNames)

	for agg := range b.mergeOutputChan {
		agg.Iter(func(name string, value *Stats) bool {
			if a, exists := output.Get(name); !exists {
				output.Put(name, value)
			} else {
				a.Count += value.Count
				a.Sum += value.Sum
				if value.Min < a.Min {
					a.Min = value.Min
				}
				if value.Max > a.Max {
					a.Max = value.Max
				}
			}
			return false
		})
	}

	b.output = output
}

func (b *BRC) printOutput() {
	names := make([]string, 0, b.output.Capacity())

	b.output.Iter(func(name string, _ *Stats) bool {
		names = append(names, name)
		return false
	})

	slices.Sort(names)

	str := "{"
	for i, name := range names {
		val, _ := b.output.Get(name)
		avg := round(round(val.Sum) / float64(val.Count))
		str += fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, val.Min, avg, val.Max)

		if i < len(names)-1 {
			str += ", "
		}
	}
	str += "}"

	fmt.Println(str)
}

func round(f float64) float64 {
	return math.Floor((f+0.05)*10) / 10
}
