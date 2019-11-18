package main

import (
	"Progetto/Master"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

/*---------------------------------------------------*/

func main() {

	if len(os.Args) != 2 {
		log.Fatal("usage: PATH_TO_DIR")
	}
	// Il programma prende come parametro il percorso assoluto della cartella con i file su cui eseguire la wordcount

	//si modifica il percorso seguendo il formato di GO
	str := strings.Replace(os.Args[1], "\\", "/", -1)
	//si controlla i file nella cartella specificata nell'input
	files, err := ioutil.ReadDir(str)
	if err != nil {
		log.Fatalf("Failed to open directory %s", (str))
	}

	listFile := make([]string, len(files))
	for i, file := range files {
		listFile[i] = str + "/" + file.Name()
		//println(file.Name())
	}

	//creiamo un canale tra Master, masterController e i futuri threads dedicati alla ricezione dei risulati dei workers.Il canale verrà scritto dal masterController in caso di timeout
	failed := make(chan bool)
	//inizializziamo canale per l'output che ci invierà il master alla fine del worcount
	chanOutput := make(chan Master.Result)
	//inizializziamo un canale usato dal mastercontroller per fare heartbeat del master
	tickerChan := make(chan int, 1)
	//invocazione e passaggio di parametri al master
	go Master.StartMaster(listFile, failed, tickerChan, &chanOutput, 0)
	//invocazione e passaggio di parametri al master controller che si occuperà del recovery del master in caso di crash
	go Master.MasterController(listFile, failed, tickerChan, &chanOutput)

	//...

	record, chanIsOpen := <-chanOutput
	if chanIsOpen != true {
		println("Il canale di output è stato chiuso dal master")
		return
	} else {
		fmt.Println("Risultati: ")
		for word, count := range record {
			fmt.Printf("%s: %d \n", word, count)
		}
		return
	}
}
