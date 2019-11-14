package rpcInterface

import (
	"bufio"
	"log"
	"regexp"
	"strings"
)

//Result of RPC call (method's second argument)
type Result map[string]int

//Map input (method's first argument)
type Chunk []string

type Occurences []int

//Error for RPC
type Worker int

// Every method that we want to export must satisfy the conditions:
// (1) the method has two arguments, both exported (or builtin) types
// (2) the method's second argument is a pointer
// (3) the method has return type error

func (w *Worker) Map(input Chunk, res *Result) error {

	freq := make(Result) // stores word occurrences
	reg, error := regexp.Compile("[^A-zÀ-ú]+")
	if error != nil {
		log.Fatal(error)
	}
	for i := 0; i < len(input); i++ {
		//si rimpiazzano i caratteri speciali con uno spazio
		cleanedLines := reg.ReplaceAllString(input[i], " ")
		line := strings.NewReader(cleanedLines)
		scanCh := bufio.NewScanner(line)
		scanCh.Split(bufio.ScanWords)
		for scanCh.Scan() {
			w := scanCh.Text()
			w = strings.ToLower(w)
			//	fmt.Println(w) ////////////////////////////////////////////////////////////
			//ci si ricava il numero di occorrenze di una parola finora
			v := freq[w]
			//lo si aumenta di uno
			v++
			freq[w] = v
			//		fmt.Println(freq)
		}
	}

	// Copy on pointers to the Result
	for key, value := range freq {
		(*res)[key] = value
	}
	return nil

}

func (w *Worker) Reduce(input Occurences, res *int) error {
	reduceResult := 0
	//calcolo della somma delle varie occorrenze della parola
	for num := range input {
		reduceResult = reduceResult + input[num]
	}
	//scrittura del risultato

	(*res) = reduceResult

	return nil

}
