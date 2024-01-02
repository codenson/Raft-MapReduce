//Maroaune Guerouji CS475

package lab0

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"
	//path = "C:\\Users\\guero\\OneDrive\\Desktop\\go\\lab0\\simple.txt"

	fileText, err := ioutil.ReadFile(path)
	checkError(err)
	
	


	text := strings.ToLower(string(fileText))
	
	text = strings.ReplaceAll(text, "'", "")
	
	
	//regex := regexp.MustCompile("[^0-9a-z]+")
	regex := regexp.MustCompile("[^0-9a-zA-Z']+")

	text = regex.ReplaceAllString(text, " ")
	
	
	words := strings.Fields(text)
	
	
	
	///fmt.Print("String raw :::::::::\n\n% s: ", fileText)

	wordsMap := make(map[string]int)
	for _, word := range words {
		if len(word) >= charThreshold {
			wordsMap[word]++
		}
	}
	//fmt.Println("map : " ,wordsMap)
	//size := len(wordsMap)
	 ///fmt.Printf("The size of the map is: %d\n", size)

	//fmt.Println("Map size: ")
	
	///for key, value := range wordsMap {
        //fmt.Printf("Key: %s, Value: %d\n", key, value)
       // fmt.Println()
  //  }
	//fmt.Println("")

	var finalCount []WordCount
	for word, count := range wordsMap {
		finalCount = append(finalCount, WordCount{Word: word, Count: count})
	}

	sortWordCounts(finalCount)
	
	
	
	/// prints the sorted Array of structs for testing purposes. 
	//for _, WordC := range finalCount {
		///fmt.Printf("Word: %s   Count: %d\n", WordC.Word, WordC.Count)
	//}
	
	
	
	
	
	if numWords < len(finalCount) {
		return finalCount[:numWords]
	}
	
	//fmt.Println("")
	return finalCount
	// var finalCount []WordCount

	// return finalCount



}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
