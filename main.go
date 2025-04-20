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
	start int
	count int
	data  []byte
}

type CityProfile struct {
	baseTemp float64
	variance float64
	seasonal float64
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func main() {
	startTime := time.Now()

	input = strings.TrimSpace(input)
	cities := strings.Split(input, "\n")

	var (
		outFile   string
		lineCount uint
		seed      int64
		chunkSize int
		bufPW     int
	)
	flag.StringVar(&outFile, "o", "out.txt", "Output file")
	flag.UintVar(&lineCount, "lc", 1_000_000_000, "Lines to output")
	flag.Int64Var(&seed, "s", 2002, "Seed for RNG")
	flag.IntVar(&chunkSize, "c", 100_000, "How many lines to batch in each worker->writer chunk")
	flag.IntVar(&bufPW, "b", 2, "How many chunks to buffer PER WORKER (total = cores*buf)")
	flag.Parse()

	fmt.Printf("Generating %d lines into %q using chunk=%d buf=%d seed=%d\n", lineCount, outFile, chunkSize, bufPW, seed)

	f, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("error creating file: %s\n%v", outFile, err)
	}
	out := bufio.NewWriterSize(f, 8<<20)
	defer f.Close()

	cores := min(runtime.NumCPU(), int(lineCount))
	lpw := max(int(lineCount)/cores, 1)

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

	dataC := make(chan ChunkData, cores*bufPW)

	var writerWG sync.WaitGroup
	writerWG.Add(1)

	go func() {
		defer writerWG.Done()

		nextStart := 0
		buf := make(map[int]ChunkData)

		for chunk := range dataC {
			buf[chunk.start] = chunk
			for {
				if next, ok := buf[nextStart]; !ok {
					break
				} else {
					out.Write(next.data)
					delete(buf, nextStart)
					nextStart += next.count
				}
			}
		}
		out.Flush()
	}()

	var wg sync.WaitGroup
	wg.Add(cores)

	for i := range cores {
		go func(workerId int, lc int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(seed + int64(workerId)))

			start := workerId * lpw
			end := start + lpw

			if workerId == cores-1 {
				end = int(lineCount)
			}

			sz := min(chunkSize, (end - start))

			if start >= end {
				return
			}

			for base := start; base < end; base += sz {
				top := min(base+sz, end)
				buf := make([]byte, 0, sz*20)

				for l := base; l < top; l++ {
					cityIdx := r.Intn(len(cityBytes))
					profile := cityProfiles[cityIdx]
					timeEffect := math.Sin(float64(l%365)/58) * profile.seasonal

					rawTemp := profile.baseTemp +
						(timeEffect * profile.variance) +
						(r.Float64()*2-1)*profile.variance*(1-profile.seasonal)

					// clamp to [-99.9, 99.9] with 1 fractional digit
					rawTemp = max(-99.9, min(99.9, rawTemp))

					x := rawTemp * 10
					var xi int
					if x >= 0 {
						xi = int(x + 0.5)
					} else {
						xi = int(x - 0.5)
					}

					xi = max(xi, -999)
					xi = min(xi, 999)

					ip := xi / 10
					fp := absInt(xi % 10)

					buf = append(buf, cityBytes[cityIdx]...)
					buf = append(buf, ';')
					buf = append(buf, strconv.Itoa(ip)...)
					buf = append(buf, '.')
					buf = append(buf, byte('0'+fp))
					if l != lc-1 {
						buf = append(buf, '\n')
					}
				}
				dataC <- ChunkData{
					start: base,
					count: top - base,
					data:  buf,
				}
			}
		}(i, int(lineCount))
	}
	wg.Wait()
	close(dataC)
	writerWG.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("Generated %d lines in %v (%.2f million lines/sec)\n", lineCount, elapsed, float64(lineCount)/elapsed.Seconds()/1_000_000)
}
