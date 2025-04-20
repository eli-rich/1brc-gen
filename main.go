package main

import (
	"bufio"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"math"
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

type CityProfile struct {
	baseTemp float64
	variance float64
	seasonal float64
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

	cores := min(runtime.NumCPU(), int(lineCount))
	lpw := int(lineCount) / cores
	lpw = max(lpw, 1)

	cityBytes := make([][]byte, len(cities))
	for i, city := range cities {
		cityBytes[i] = []byte(city)
	}

	cityProfiles := make([]CityProfile, len(cities))
	cityRand := rand.New(rand.NewSource(seed))

	const (
		percentHot       = 0.2
		percentCold      = 0.15
		percentTemperate = 0.3
	)

	for i := range len(cities) {
		r := cityRand.Float64()

		if r < percentHot {
			// Hot cities (deserts, tropical)
			cityProfiles[i] = CityProfile{
				baseTemp: cityRand.Float64()*15 + 25,   // 25-40°C base
				variance: cityRand.Float64()*15 + 5,    // 5-20°C variance
				seasonal: cityRand.Float64()*0.3 + 0.2, // 0.2-0.5 seasonal effect
			}
		} else if r < percentHot+percentCold {
			// Cold cities (arctic, subarctic)
			cityProfiles[i] = CityProfile{
				baseTemp: cityRand.Float64()*15 - 25,   // -25 to -10°C base
				variance: cityRand.Float64()*20 + 10,   // 10-30°C variance
				seasonal: cityRand.Float64()*0.4 + 0.5, // 0.5-0.9 seasonal effect
			}
		} else if r < percentHot+percentCold+percentTemperate {
			// Temperate climate cities
			cityProfiles[i] = CityProfile{
				baseTemp: cityRand.Float64()*15 + 5,    // 5-20°C base
				variance: cityRand.Float64()*10 + 10,   // 10-20°C variance
				seasonal: cityRand.Float64()*0.3 + 0.6, // 0.6-0.9 seasonal effect
			}
		} else {
			// Random other cities with varied climates
			cityProfiles[i] = CityProfile{
				baseTemp: cityRand.Float64()*70 - 35,   // -35 to 35°C base
				variance: cityRand.Float64()*25 + 5,    // 5-30°C variance
				seasonal: cityRand.Float64()*0.8 + 0.2, // 0.2-1.0 seasonal effect
			}
		}
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
			chunkSize = min(chunkSize, (endLine - startLine))

			if startLine >= endLine {
				return
			}

			for chunkStart := startLine; chunkStart < endLine; chunkStart += chunkSize {
				chunkEnd := min(chunkStart+chunkSize, endLine)

				buf := make([]byte, 0, chunkSize*30)
				for j := chunkStart; j < chunkEnd; j++ {
					cityIdx := r.Intn(len(cityBytes))
					profile := cityProfiles[cityIdx]
					timeEffect := math.Sin(float64(j%365)/58) * profile.seasonal

					rawTemp := profile.baseTemp +
						(timeEffect * profile.variance) +
						(r.Float64()*2-1)*profile.variance*(1-profile.seasonal)

					// Clamp temperature to our range and round to 1 decimal place
					rawTemp = math.Max(-99.9, math.Min(99.9, rawTemp))
					temp := math.Round(rawTemp*10) / 10

					// Convert to string with exactly one decimal place
					tempBytes := []byte(strconv.FormatFloat(temp, 'f', 1, 64))

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
