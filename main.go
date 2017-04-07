package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/daiLlew/csvFilterTest/model"
	"github.com/daiLlew/csvFilterTest/s3service"
	"io"
	"os"
	"time"
)

const (
	OUT_PATH  = "results.txt"
	STATS_FMT = "Stats:\n\tFile size: %f (MB)\n\tTime: %f seconds\n\tRows processed: %d\n\tDimensions Types: %d\n"
)

func main() {
	bucket := flag.String("bucket", "", "AWS bucket to get the file from.")
	filePath := flag.String("file", "", "AWS file to get.")
	flag.Parse()

	awsResponse := s3service.GetFileReader(*bucket, *filePath)
	defer awsResponse.Close()

	stats, distinctDimensions := findDistinctDimensions(awsResponse)
	fmt.Println(stats)
	writeOutput(stats, distinctDimensions)
}

func findDistinctDimensions(awsResponse *s3service.AWSResponse) (stats string, distinctDimensions map[string]map[string]struct{}) {
	start := time.Now().UnixNano()
	csvReader := csv.NewReader(awsResponse.Reader)
	header, _ := csvReader.Read()
	dimensionIndices := getDimensionIndices(header)
	distinctDimensions = make(map[string]map[string]struct{}, 0)
	rowsProcessed := 1

	var row []string
	var err error
	var key string
	var name string
	var hierarchy string
	var value string
	var index model.Indices

	fmt.Println("Looking for unique Dimensions...")
	for {
		if row, err = csvReader.Read(); err == io.EOF {
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
	return calculateStats(start, rowsProcessed, len(distinctDimensions), awsResponse.ByteCount), distinctDimensions
}

func calculateStats(start int64, rowsProcessed int, distinctCount int, fileSize int64) string {
	timeElapsed := time.Now().UnixNano() - start
	seconds := float64(timeElapsed) / float64(1000000000)
	sizeInMB := float64(fileSize) / 1000000.0

	return fmt.Sprintf(STATS_FMT, sizeInMB, seconds, rowsProcessed, distinctCount)
}

func writeOutput(stats string, results map[string]map[string]struct{}) {
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

	f.WriteString(stats)
	for key, value := range results {
		f.WriteString(fmt.Sprintf("Dimension=%s, %d entries\n", key, len(value)))
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
