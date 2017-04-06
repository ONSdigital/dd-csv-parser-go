package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/daiLlew/csvFilterTest/model"
	"io"
	"log"
	"os"
	"time"
)

const OUT_PATH = "results.txt"

func main() {
	in := flag.String("in", "", "The V3 csv file to parse")
	flag.Parse()

	if len(*in) == 0 {
		log.Fatal("Please specify an input file.")
	} else {
		fmt.Printf("Parsing %s\n", *in)
	}

	seconds, rowsProcessed, distinctDimensions := findDistinctDimensions(*in)
	writeOutput(rowsProcessed, seconds, distinctDimensions)
}

func findDistinctDimensions(inputFile string) (seconds float64, rowsProcessed int, distinctDimensions map[string]map[string]struct{}) {
	f, _ := os.Open(inputFile)
	defer f.Close()

	start := time.Now().UnixNano()
	reader := csv.NewReader(f)
	header, _ := reader.Read()
	dimensionIndices := getDimensionIndices(header)
	distinctDimensions = make(map[string]map[string]struct{}, 0)
	rowsProcessed = 0

	var row []string
	var err error
	var key string
	var name string
	var hierarchy string
	var value string
	var index model.Indices

	fmt.Println("Looking for unique Dimensions...")
	for {
		if row, err = reader.Read(); err == io.EOF {
			fmt.Println("File parse complete.")
			break
		}

		for _, index = range dimensionIndices {
			hierarchy = row[index.Hierarchy()]
			name = row[index.Name()]
			value = row[index.Value()]

			key = "\thierarchy=" + hierarchy + ", value=" + value + "\n"

			if innerMap, ok := distinctDimensions[name]; !ok {
				distinctDimensions[name] = map[string]struct{}{
					key: struct{}{},
				}
			} else {
				innerMap[key] = struct{}{}
			}
		}
		rowsProcessed++
	}

	timeElapsed := time.Now().UnixNano() - start
	seconds = float64(timeElapsed) / float64(1000000000)
	return seconds, rowsProcessed, distinctDimensions
}

func writeOutput(rowsProcessed int, seconds float64, results map[string]map[string]struct{}) {
	var f *os.File
	var err error
	defer f.Close()

	if _, err = os.Stat(OUT_PATH); os.IsNotExist(err) {
		f, err = os.Create(OUT_PATH)
		if err != nil {
			panic(err)
		}
	} else {
		if err = os.Remove(OUT_PATH); err != nil {
			panic(err)
		}
		f, err = os.Create(OUT_PATH)
		if err != nil {
			panic(err)
		}
	}

	if f, err = os.OpenFile(OUT_PATH, os.O_APPEND|os.O_WRONLY, 0600); err != nil {
		panic(err)
	}

	f.WriteString(fmt.Sprintf("Total rows processed: %d, Time taken: %f seconds\n", rowsProcessed, seconds))
	f.WriteString(fmt.Sprintf("Distinct Dimensions found: %d\n\n", len(results)))

	for key, value := range results {
		f.WriteString(fmt.Sprintf("Dimension=%s * %d\n", key, len(value)))
		for k, _ := range value {
			f.WriteString(k)
		}
		f.WriteString("\n")
	}
}

func getDimensionIndices(header []string) []model.Indices {
	dimensionIndices := make([]model.Indices, 0)
	for i := 3; i < len(header); i += 3 {
		dimensionIndices = append(dimensionIndices, model.Indices{Start: i})
	}
	return dimensionIndices
}
