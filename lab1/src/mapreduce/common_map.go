
package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	fileData := getContent(inFile) // file handler.
	mapVals := mapF(inFile, string(fileData))
	outFilesslices := createOutputFiles(nReduce, jobName, mapTaskNumber)
	defer helperCloseFiles(outFilesslices)
	writeMapValsToFiles(mapVals, outFilesslices, nReduce)

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func helperCloseFiles(files []*os.File) {
	for _, file := range files {
		if file != nil {
			file.Close()
		}
	}
}
func createOutputFiles(nReduce int, jobName string, mapTaskNumber int) []*os.File {
	files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		temp, err := os.Create(reduceName(jobName, mapTaskNumber, i))
		if err != nil {
			log.Fatal(fmt.Sprintf("Operation cannot be done: can't create reduce file %s", reduceName(jobName, mapTaskNumber, i)))
		}
		files[i] = temp
	}
	return files
}

func writeMapValsToFiles(mapVals []KeyValue, outFilesslices []*os.File, nReduce int) {
	for _, kv := range mapVals {
		idx := int(ihash(kv.Key)) % nReduce
		enc := json.NewEncoder(outFilesslices[idx])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to encode key-value %v", kv))
		}
	}
}

func getContent(filePath string) string {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Err cannot find and read from file %s", filePath)
	}
	return string(data)
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
