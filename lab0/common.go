package lab0

import "log"

// Propagate error if it exists
func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
