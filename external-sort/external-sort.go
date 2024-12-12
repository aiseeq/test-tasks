package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

type QueryCount struct {
	Query string
	Count int
}

// fatal ленивый способ обработать ошибки
func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// dumpChunk сохраняет чанки в файлы
func dumpChunk(counts map[string]int, index int) string {
	var qc []QueryCount
	for q, c := range counts {
		qc = append(qc, QueryCount{q, c})
	}

	sort.Slice(qc, func(i, j int) bool {
		return qc[i].Query < qc[j].Query
	})

	filename := fmt.Sprintf("chunk_%04d.tsv", index)
	f, err := os.Create(filename)
	fatal(err)
	defer func() { fatal(f.Close()) }()

	w := bufio.NewWriter(f)
	for _, entry := range qc {
		_, err = fmt.Fprintf(w, "%s\t%d\n", entry.Query, entry.Count)
		fatal(err)
	}
	fatal(w.Flush())
	return filename
}

func readNextLine(sc *bufio.Scanner) (qc QueryCount) {
	if !sc.Scan() {
		return
	}
	_, err := fmt.Sscanf(sc.Text(), "%s\t%d", &qc.Query, &qc.Count)
	fatal(err)
	return qc
}

// mergeFiles складывает несколько отсортированных файлов
// в один выходной файл, суммируя частоту одинаковых запросов
func mergeFiles(files []string, output string) error {
	// Открываем все файлы
	var scanners []*bufio.Scanner
	var fs []*os.File
	for _, fn := range files {
		f, err := os.Open(fn)
		if err != nil {
			return err
		}
		fs = append(fs, f)
		scanners = append(scanners, bufio.NewScanner(f))
	}

	current := make([]QueryCount, len(scanners))
	for i, sc := range scanners {
		current[i] = readNextLine(sc)
	}

	out, err := os.Create(output)
	if err != nil {
		return err
	}
	defer func() { fatal(out.Close()) }()
	w := bufio.NewWriter(out)

	for {
		// Найдём минимальный по запросу элемент среди текущих, пропуская невалидные
		minIndex := -1
		for i, line := range current {
			if current[i].Count == 0 {
				continue
			}
			if minIndex == -1 || line.Query < current[minIndex].Query {
				minIndex = i
			}
		}

		if minIndex == -1 {
			// Все файлы закончились
			break
		}

		// Собираем все такие же запросы из разных файлов
		curQuery := current[minIndex].Query
		curSum := 0
		for i := range current {
			if current[i].Count > 0 && current[i].Query == curQuery {
				curSum += current[i].Count
				// Читаем следующую строку из этого файла
				current[i] = readNextLine(scanners[i])
			}
		}

		// Записываем результат
		// Каждую группу уникальных запросов сразу пишем в итоговый файл
		_, err = fmt.Fprintf(w, "%s\t%d\n", curQuery, curSum)
		fatal(err)
	}
	fatal(w.Flush())

	for _, f := range fs {
		fatal(f.Close())
	}

	return nil
}

func main() {
	/*N := 5
	inputFile := "input.txt"
	outputFile := "output.tsv"*/
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s N input.txt output.tsv\n", os.Args[0])
		os.Exit(1)
	}
	N, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to parse N: %v", err)
	}
	inputFile := os.Args[2]
	outputFile := os.Args[3]

	infile, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer func() { fatal(infile.Close()) }()

	counts := make(map[string]int)
	var chunkFiles []string
	chunkIndex := 0
	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		query := scanner.Text()
		log.Println(query)
		if _, ok := counts[query]; !ok && len(counts) == N {
			// В памяти уже N уникальных запросов, пора сбрасывать
			chunkFile := dumpChunk(counts, chunkIndex)
			chunkFiles = append(chunkFiles, chunkFile)
			chunkIndex++
			counts = make(map[string]int)
		}
		counts[query]++
	}

	if err = scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}

	// Если после окончания чтения остались данные в counts, сбросим их в файл
	if len(counts) > 0 {
		chunkFile := dumpChunk(counts, chunkIndex)
		chunkFiles = append(chunkFiles, chunkFile)
	}

	// Если у нас получился только один промежуточный файл, его можно просто переименовать в итоговый
	// Но если их несколько, выполним слияние
	if len(chunkFiles) == 1 {
		fatal(os.Rename(chunkFiles[0], outputFile))
	} else {
		fatal(mergeFiles(chunkFiles, outputFile))
	}

	// Очистить временные файлы
	for _, cf := range chunkFiles {
		if cf != outputFile {
			fatal(os.Remove(cf))
		}
	}
}
