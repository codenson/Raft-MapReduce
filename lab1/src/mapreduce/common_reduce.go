
package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
//////////////////////////////////////////////////////////////////////////////////////
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	const targetWord = "he"
	fmt.Println("master: Starting Map/Reduce task test")

	filesRecords := decodeFiles(jobName, reduceTaskNumber, nMap)
	writeResults(jobName, reduceTaskNumber, filesRecords, reduceF, targetWord)

	fmt.Println("master: Map/Reduce task completed")
}

func decodeFiles(jobName string, reduceTaskNumber int, nMap int) map[string][]string {
	filesRecords := make(map[string][]string)

	for mapTaskIndex := 0; mapTaskIndex < nMap; mapTaskIndex++ {
		fileName := reduceName(jobName, mapTaskIndex, reduceTaskNumber)

		file, err := os.Open(fileName)
		checkError(err)
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var keyValue KeyValue
			err := decoder.Decode(&keyValue)
			if err == io.EOF {
				break
			}
			checkError(err)

			filesRecords[keyValue.Key] = append(filesRecords[keyValue.Key], keyValue.Value)
		}

		fmt.Println("Merge: read ", fileName)
	}

	return filesRecords
}

/////////////////////////////////////////////////////////////////////////////////////////

func writeResults(jobName string, reduceTaskNumber int, keyValueRecords map[string][]string, reduceF func(key string, values []string) string, targetWord string) {
	fileName := mergeName(jobName, reduceTaskNumber)
	tempFile, err := os.Create(fileName)
	checkError(err)
	defer tempFile.Close()

	encoder := json.NewEncoder(tempFile)
	for key, values := range keyValueRecords {
		reducedValue := reduceF(key, values)
		if key == targetWord {
			fmt.Println("word: ", reducedValue)
		}
		encoder.Encode(KeyValue{key, reducedValue})
	}
}
