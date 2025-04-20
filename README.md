# 1BRC - Generator

## The Challenge

### Constraints

- All lines are of format `<city name>: string;<temperature>: float64`
- There are **at most** `20,102` unique cities
- All city names are between `[4, 13]` characters inclusive
- All temperatures range between `[-99.9, 99.9]` inclusive
- All temperatures have exactly 1 digit after the decimal (ex: `22.4`, `0.0`, `-77.2`)

### Instructions

- Any programming language is allowed.
- No external libraries are allowed except std.

### Usage

- The data file is approximately 13-16 GB
- The purpose of this repo is to provide you with a program to generate this file.
- `go run github.com/eli-rich/1brc-gen@v1.0.1 -lc 1000000000`
  - `-lc`: linecount, defaults to 1 billion
  - `-o`: outfile, defaults to `out.txt`
  - `-s`: seed, defaults to `2002`

### Output

- All output can be verified using the `results.txt` file and the default seed: `2002`.
- Output should be of format: `<name>=<min>/<mean>/<max>` separated by newlines.
- All values should be computed, then ceiled to 1 digit after the decimal.
  - Example: `[-14.2, 1.0, 43.9]`
  - Min: `-14.2`
  - Mean: `30.7 / 3` = `10.2333...` = `10.3`
    - If the mean was _negative_ (`-10.2333...`) you would get `-10.2`
  - Max: `43.9`
- Example output line: `Mobile=8.3/19.5/30.7`
