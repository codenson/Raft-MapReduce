package lab0

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`

	tot := 0
	for vale := range nums {
		tot += vale
	}
	out <- tot

}

func push_to_in_channel(numValues []int, in_chan chan<- int) {
	//fmt.Println("Hello")
	for _, value := range numValues {
		in_chan <- value
	}
	//fmt.Println("Hello there")
	close(in_chan)
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {

	numbersFile, err := os.Open(fileName)
	checkError(err)
	numbers, err := readInts(numbersFile)
	checkError(err)
	defer numbersFile.Close()

	SumsChan := make(chan int)
	minNumGoRoutines := len(numbers) / num

	for i := 1; i <= num; i++ {
		chanel := make(chan int, 1)
		go sumWorker(chanel, SumsChan)

		start := -1
		end := len(numbers) - 1
		if i != 1 {
			start = (i - 1) * minNumGoRoutines
		}

		if i < num {
			end = i * minNumGoRoutines
		}

		go func(numbers []int, startpos, endPos, ith int, ch chan int) {
			push_to_in_channel(numbers[startpos+1:endPos+1], ch)

		}(numbers, start, end, i, chanel)
	}

	return HelperCalculateTotal(num, SumsChan)

}
func HelperCalculateTotal(num int, ch chan int) int {
	total := 0
	for i := 0; i < num; i++ {
		value := <-ch
		total += value

	}
	close(ch)
	return total

}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
