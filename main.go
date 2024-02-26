package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
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

func main() {
	// f, err := os.Create("cpu.prof")
	// if err != nil {
	// 	panic(err)
	// }

	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	entries := readAndParseFile()

	output := calculateOutput(entries)

	printOutput(output)
}

func readAndParseFile() map[string][4]float64 {
	// min, max, sum, count
	output := make(map[string][4]float64)

	f, err := os.Open("./data/measurements.txt")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()
		entry := parseLine(line)

		if parts, ok := output[entry.Name]; ok {
			min := math.Min(parts[0], entry.Temp)
			max := math.Max(parts[1], entry.Temp)
			sum := parts[2] + entry.Temp
			count := parts[3] + 1

			output[entry.Name] = [4]float64{min, max, sum, count}
		} else {
			output[entry.Name] = [4]float64{entry.Temp, entry.Temp, entry.Temp, 1}
		}
	}

	return output
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

func calculateOutput(entries map[string][4]float64) []Output {
	// min, avg, max
	output := make(map[string][3]float64)

	for name, parts := range entries {
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

	return records
}

func printOutput(output []Output) {
	fmt.Print("{")
	for i, record := range output {
		fmt.Printf("%s:%.1f/%.1f/%.1f", record.Name, record.Min, record.Avg, record.Max)
		if i < len(output)-1 {
			fmt.Print(";")
		}
	}
	fmt.Print("}")
}
