package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Output struct {
	Name string
	Min  float64
	Avg  float64
	Max  float64
}

type Line struct {
	Name string
	Temp float64
}

type BRC struct {
	input           string
	max_go_routines int
	output          []Output
	wg              *sync.WaitGroup
	linesChan       chan []Line
	// stores name: min, max, sum, count
	aggregateChan chan map[string][4]float64
}

func newBRC(input string, max_go_routines int) *BRC {
	return &BRC{
		input:           input,
		max_go_routines: max_go_routines,
		aggregateChan:   make(chan map[string][4]float64, max_go_routines),
		output:          make([]Output, 0),
		linesChan:       make(chan []Line, max_go_routines),
		wg:              &sync.WaitGroup{},
	}
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var executionprofile = flag.String("execprofile", "", "write tarce execution to `file`")
var input = flag.String("input", "", "path to the input file to evaluate")

func init() {
	flag.Parse()
}

func main() {
	if *input == "" {
		log.Fatalln("Input file is required")
	}

	if *executionprofile != "" {
		f, err := os.Create("./profiles/" + *executionprofile)
		if err != nil {
			log.Fatal("could not create trace execution profile: ", err)
		}
		defer f.Close()
		if err := trace.Start(f); err != nil {
			panic(err)
		}
		defer trace.Stop()
	}

	if *cpuprofile != "" {
		f, err := os.Create("./profiles/" + *cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	max_go_routines := 10
	brc := newBRC(*input, max_go_routines)

	brc.wg.Add(1)
	go brc.readAndParseFile()

	brc.wg.Add(1)
	go brc.processAggregates()

	for i := 0; i < brc.max_go_routines; i++ {
		brc.wg.Add(1)
		go brc.processLines()
	}
	brc.wg.Wait()

	brc.printOutput()

	if *memprofile != "" {
		f, err := os.Create("./profiles/" + *memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func (b *BRC) readAndParseFile() {
	defer b.wg.Done()

	f, err := os.Open(b.input)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	lines := []Line{}

	for scanner.Scan() {
		line := scanner.Text()
		parsedLine := parseLine(line)
		lines = append(lines, parsedLine)

		if len(lines) == 100 {
			b.linesChan <- lines
			lines = []Line{}
		}
	}

	if len(lines) > 0 {
		b.linesChan <- lines
	}

	close(b.linesChan)
}

func (b *BRC) processLines() {
	defer b.wg.Done()

	aggregate := make(map[string][4]float64)

	for lines := range b.linesChan {
		for _, line := range lines {
			if parts, ok := aggregate[line.Name]; ok {
				min := math.Min(parts[0], line.Temp)
				max := math.Max(parts[1], line.Temp)
				sum := parts[2] + line.Temp
				count := parts[3] + 1

				aggregate[line.Name] = [4]float64{min, max, sum, count}
			} else {
				aggregate[line.Name] = [4]float64{line.Temp, line.Temp, line.Temp, 1}
			}
		}
	}

	b.aggregateChan <- aggregate
}

func parseLine(line string) Line {
	parts := strings.Split(line, ";")

	if len(parts) != 2 {
		panic("Invalid line: " + line)
	}

	temp, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		panic("Invalid temperature: " + parts[1])
	}

	return Line{
		Name: parts[0],
		Temp: math.Ceil(temp),
	}
}

func (b *BRC) processAggregates() {
	defer b.wg.Done()

	num_aggs_received := 0
	// min, max, sum, count
	combinedAggs := make(map[string][4]float64)

	// combine aggregate maps
	for agg := range b.aggregateChan {
		num_aggs_received++

		for name, parts := range agg {
			entry, ok := combinedAggs[name]
			if ok {
				min := math.Min(entry[0], parts[0])
				max := math.Max(entry[1], parts[1])
				sum := entry[2] + parts[2]
				count := entry[3] + parts[3]

				combinedAggs[name] = [4]float64{min, max, sum, count}
			} else {
				min := parts[0]
				max := parts[1]
				sum := parts[2]
				count := parts[3]

				combinedAggs[name] = [4]float64{min, max, sum, count}
			}
		}

		if num_aggs_received == b.max_go_routines {
			close(b.aggregateChan)
		}
	}

	// min, avg, max
	output := make(map[string][3]float64)

	for name, parts := range combinedAggs {
		min := parts[0]
		max := parts[1]
		sum := parts[2]
		count := parts[3]

		avg := sum / count

		output[name] = [3]float64{min, avg, max}
	}

	records := make([]Output, 0, len(output))

	for name, parts := range output {
		records = append(records, Output{
			Name: name,
			Min:  parts[0],
			Avg:  parts[1],
			Max:  parts[2],
		})
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Name < records[j].Name
	})

	b.output = records
}

func (b *BRC) printOutput() {
	fmt.Print("{")
	for i, record := range b.output {
		fmt.Printf("%s:%.1f/%.1f/%.1f", record.Name, record.Min, record.Avg, record.Max)
		if i < len(b.output)-1 {
			fmt.Print(";")
		}
	}
	fmt.Print("}")
}
