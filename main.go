package main

import (
	"bufio"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed world-cities.txt
var input string

type ChunkData struct {
	id   int
	data []byte
}

func main() {
	startTime := time.Now()

	input = strings.TrimSpace(input)
	cities := strings.Split(input, "\n")

	var (
		outFile   string
		lineCount uint
		seed      int64
	)
	flag.StringVar(&outFile, "o", "out.txt", "Output file (out.txt by default)")
	flag.UintVar(&lineCount, "lc", 1_000_000, "Lines to output (1,000,000 by default)")
	flag.Int64Var(&seed, "s", 2002, "Seed for RNG (2,002 by default)")
	flag.Parse()

	fmt.Printf("Starting generation of file: %s with %d lines. Seed: %d\n", outFile, lineCount, seed)

	f, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("error creating file: %s\n%v", outFile, err)
	}
	out := bufio.NewWriterSize(f, 8192*1024)
	defer f.Close()

	cores := runtime.NumCPU()
	if cores > int(lineCount) {
		cores = int(lineCount)
	}
	lpw := int(lineCount) / cores
	if lpw == 0 {
		lpw = 1
	}

	cityBytes := make([][]byte, len(cities))
	for i, city := range cities {
		cityBytes[i] = []byte(city)
	}

	// Temp range: [-99.9, 99.9]; STEP: 0.1
	const tempPrecision = 1999
	tempStrings := make([][]byte, tempPrecision)
	for i := range tempPrecision {
		temp := (float64(i) / 10.0) - 99.9
		tempStrings[i] = []byte(strconv.FormatFloat(temp, 'f', 1, 64))
	}

	dataC := make(chan ChunkData, cores*2)

	var writerWG sync.WaitGroup
	writerWG.Add(1)

	go func() {
		defer writerWG.Done()

		nextId := 0
		chunks := make(map[int][]byte)

		for chunk := range dataC {
			if chunk.id == nextId {
				out.Write(chunk.data)
				nextId++

				for {
					if data, ok := chunks[nextId]; ok {
						out.Write(data)
						delete(chunks, nextId)
						nextId++
					} else {
						break
					}
				}
			} else {
				chunks[chunk.id] = chunk.data
			}
		}
		out.Flush()
	}()

	var wg sync.WaitGroup
	wg.Add(cores)

	for i := range cores {
		go func(workerId int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(seed + int64(workerId)))

			startLine := workerId * lpw
			endLine := startLine + lpw

			if workerId == cores-1 {
				endLine = int(lineCount)
			}

			chunkSize := 100_000
			if chunkSize > (endLine - startLine) {
				chunkSize = endLine - startLine
			}

			if startLine >= endLine {
				return
			}

			for chunkStart := startLine; chunkStart < endLine; chunkStart += chunkSize {
				chunkEnd := chunkStart + chunkSize
				if chunkEnd > endLine {
					chunkEnd = endLine
				}

				buf := make([]byte, 0, chunkSize*30)
				for j := chunkStart; j < chunkEnd; j++ {
					cityIdx := r.Intn(len(cityBytes))
					tempIdx := r.Intn(tempPrecision)
					tempBytes := tempStrings[tempIdx]

					buf = append(buf, cityBytes[cityIdx]...)
					buf = append(buf, ';')
					buf = append(buf, tempBytes...)
					buf = append(buf, '\n')
				}
				dataC <- ChunkData{
					data: buf,
					id:   chunkStart / chunkSize,
				}
			}
		}(i)
	}
	wg.Wait()
	close(dataC)
	writerWG.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("Generated %d lines in %v (%.2f million lines/sec)\n", lineCount, elapsed, float64(lineCount)/elapsed.Seconds()/1_000_000)
}
