package Master

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/build"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//Result of RPC call
type Result map[string]int

//chosen at runtime
var bucketName string

//---------------------------------------------
const timeoutMaster = 20               // non avendo ricevuto risposta, inserire qui il numero di secondi da aspettare prima di dichiarare il master come guasto
const chunksLines = 20                 //numero di righe del file da far elaborare ad un worker
const timeoutWorker = 20               // non avendo ricevuto risposta, inserire qui il numero di secondi da aspettare prima di dichiarare il worker come guasto
const faultProbability = 0             //probabilità che il master abbia un fault durante l'esecuzione (intesa come percentuale)
const workerAddress = "localhost:1234" //inserire qui l'indirizzo del server RPC (inserire 18.213.54.248:1234 per connettrsi a istanza EC2)
const tmpFile = "tmpFile.json"         //nome file che il master creerà alla fine del map task per conservare i risultati

///////////////////////////////////////////////////////////////////////////////////////////////////
var mapperResults []*Result //array utilizzato per sapere le risposte dei Map workers
//////////////////////////////////////////////////////////////////////////////////////////////////

//---------------------------------------------
/*StartMaster prende come parametri:
1. un array di stringhe contenente i percorsi (path) dei file di input su cui eseguire worcount
2. un channel su cui si monitora l'attività del master (nel canale viene  chiuso se il master smette di rispondere all'heartbeating)
3. un canale dove si consuma un tick periodico del mastercontroller
3. un channel di output dove il master passerà il risultato del worcount al client
4. un int che indica da quale punto il master deve iniziare a lavorare (0=dall'inzio,1=prima dello shuffle,2=dopo lo shuffle)
*/
func StartMaster(fileList []string, failed chan bool, tickerChan chan int, output *chan Result, step int) {
	var file *os.File

	//si prende come path per il file temporaneo quello della variabile d'ambiente GOPATH
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		//Go 1.8 has default GOPATH exported via go/build:
		gopath = build.Default.GOPATH
	}

	println("Un master inizia a lavorare")

	ses, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	svcs3 := s3.New(ses)

	if step == 0 {
		//si alloca un array di 100 stringhe come il numero di bucket che è possibile creare
		var bucketList [100]string
		//si listano i nomi dei bucket
		resultList, err := svcs3.ListBuckets(nil)

		if err != nil {
			fmt.Printf("Unable to list buckets, %s", err)
			os.Exit(-1)
		}

		for i, b := range resultList.Buckets {
			bucketList[i] = aws.StringValue(b.Name)
		}

		//come nome del bucket si utilizza il prefisso simesi-bucket + un numero incrementato ogni volta in caso esista già un bucket con quel nome
		for n := 0; n < len(bucketList); n++ {
			bucketName = "simesi-bucket" + strconv.Itoa(n)
			//la variaible nameIsDuplicated è utilizzata per sapere se il nome è già stato utilizzato
			nameIsDuplicated := false
			//si itera su ogni nome in bucketList
			for i := 0; i < len(bucketList); i++ {
				if bucketList[i] == bucketName {
					nameIsDuplicated = true
				}
			}
			if nameIsDuplicated == false {
				break
			}
		}
		//si crea un bucket necessario per le operazioni di recovery del master in caso di crash
		input := &s3.CreateBucketInput{
			Bucket: aws.String(bucketName)}

		result, err1 := svcs3.CreateBucket(input)
		if err1 != nil {
			if aerr, ok := err1.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeBucketAlreadyExists:
					fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
				case s3.ErrCodeBucketAlreadyOwnedByYou:
					fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				fmt.Println(err1.Error())
			}
		} else {
			fmt.Println("Si lavora su bucket ", *result.Location)
		}
		//si consuma il primo tick del MasterController
		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}

		var readedLines int
		numWorkers := 0
		for _, fileName := range fileList {

			//si consuma il tick del MasterController perchè altrimenti, avendo magari un timeoutMaster piccolo,
			//durante questa operazione di lettura dei file, che può essere lunga in base al numero e alla grandezza dei file,
			// il MasterController potrebbe dichiarare il master crashato non consumando quest'ultimo i tick
			if len(tickerChan) != 0 {
				_ = <-tickerChan
			}
			file, err := os.Open(fileName)
			if err != nil {
				fmt.Println(err)
				os.Exit(-1)
			}

			scanner := bufio.NewScanner(file)

			//continua ficnhè non finisce il file
			for {
				scanner.Split(bufio.ScanLines)
				var txtlines []string
				reg, erro := regexp.Compile("[^A-zÀ-ú]+")
				if erro != nil {
					log.Fatal(erro)
				}
				for scanner.Scan() {
					readedLines++
					//si legge linea di testo e le si tolgono i caratteri speciali e i numeri
					txtlines = append(txtlines, reg.ReplaceAllString(scanner.Text(), " "))
					if readedLines == chunksLines {
						numWorkers++
						//avvio thread dedicato alla chiamata RPC
						go rpcMapRequest(txtlines, failed)

						//	fmt.Println("Chiamata RPC con input:", txtlines)
						//reset del buffer contente le chunksLines
						txtlines = nil
						readedLines = 0
					}
				}
				if scanner.Err(); err != nil {
					//se err è uguale a nil allora abbiamo raggiunto EOF
					fmt.Println("reading input error:", err)
					os.Exit(-1)
				}
				file.Close()
				if len(txtlines) == 0 {
					//abbiamo letto spazio vuoto, quindi lo scartiamo
					readedLines = 0
					txtlines = nil
					break
				}
				numWorkers++
				rpcMapRequest(txtlines, failed)
				//fmt.Println("Chiamata RPC con input troncato:", txtlines)
				txtlines = nil
				readedLines = 0
				break

			}
		}
		//fmt.Println("Letti tutti i file e mandate le richieste RPC di map ")

		//-------------------------------------------------------------
		//attivazione monitoraggio master
	wait:
		for {
			select {
			//caso canale chiuso: vuol dire che il mastercontroller ritiene il master crashato
			case <-failed:
				//il master simula il crash ritornando
				return
			case <-tickerChan: //impedisce il timeout dallo scadere

			default:
				rand.Seed(time.Now().UnixNano())
				if rand.Intn(100) >= faultProbability {
					//	fmt.Println("magic number is", rand.Intn(100))
					//se la condizione è verificata allora tutti i map worker hanno spedito il loro risultato che è stato scritto dai thread
					if len(mapperResults) == numWorkers {
						/*scrivi su bucket s3*/
						break wait //uscita dal select e dal for
					}
				} else {
					fmt.Println("simulazione crash master durante i map works")
					//si blocca il master per simularne un crash (in questo modo non si consumerà più il ticker periodico invato dal mastercontroller e si attiverà il RecoverMaster)
					time.Sleep(time.Second * timeoutMaster * 2)
				}
			}
		}

		// check if file exists
		var _, errPath = os.Stat(gopath + "/" + tmpFile)
		// create file if not exists or truncate
		if os.IsNotExist(errPath) {
			file, err = os.Create(gopath + "/" + tmpFile)
			if err != nil {
				fmt.Println("Error in create file")
				os.Exit(-1)
			}
		}
		file.Close()

		//si apre il file in mdoalità APPEND
		file, err = os.OpenFile(gopath+"/"+tmpFile,
			os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			log.Println(err)
			os.Exit(-1)
		}

		//scrittura su file di risulato per risulato (es. {"casa":1,"esame":3}{"a":2}
		for ind := range mapperResults {
			//come prima, qui si consuma il tick se c'è perchè l'operazione di marshal su file potrebbe essere lunga e sforare il timeoutMaster
			if len(tickerChan) != 0 {
				_ = <-tickerChan
			}
			jsonData, errorJ := json.Marshal(mapperResults[ind])
			if errorJ != nil {
				fmt.Println(errorJ.Error())
				os.Exit(-1)
			}
			jsonStr := string(jsonData)

			//scrittura del file temporaneo in modalità APPEND
			_, err = file.WriteString(jsonStr)
			if err != nil {
				log.Println(err.Error())
				os.Exit(-1)
			}

			// Save file changes.
			err = file.Sync()
			if err != nil {
				fmt.Println("Error on file sync")
				os.Exit(-1)
			}
		}
		file.Close()

		//LETTURA FILE PER UPLOAD SU S3
		// Open the file for use
		file, err = os.Open(gopath + "/" + tmpFile)
		if err != nil {
			log.Println(err.Error())
			os.Exit(-1)
		}
		fileInfo, _ := file.Stat()
		var size int64 = fileInfo.Size()
		buffer := make([]byte, size)
		file.Read(buffer)
		//la scrittura del file su S3 potrebbe richiedere del tempo quindi si consuma il tick
		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}
		/**********************************************************/
		//upoad file temporaneo su bucket con tagging specifico "beforeShuffle"
		inp := &s3.PutObjectInput{
			Body:          bytes.NewReader(buffer),
			ContentLength: aws.Int64(size),
			Bucket:        aws.String(bucketName),
			Key:           aws.String(tmpFile),
			Tagging:       aws.String("state=beforeShuffle"),
		}
		_, err = svcs3.PutObject(inp)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return
		}

		//fmt.Println("file uploadato su s3")
		//si chiude il file locale perchè "the behavior of Seek on a file opened with O_APPEND is not specified."
		file.Close()

		file, err = os.OpenFile(gopath+"/"+tmpFile, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(err)
			os.Exit(-1)
		}
		//--------------------------------------------
		//******************************
		//simulazione crash master
		rand.Seed(time.Now().UnixNano())
		if rand.Intn(100) < faultProbability {
			fmt.Println("IL master fallisce dopo aver caricato file su S3")
			return
		}
		//}
		//**********************************

	}

	var objectBytes []byte
	if step == 1 {
		//qui il file su S3 è già stato scritto precedentemente da un master crashato
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(tmpFile),
		}
		fileDownloaded, erro := svcs3.GetObject(input)
		if erro != nil {
			log.Println(erro)
			os.Exit(-1)
		}
		//si consuma il tick perchè il file dei risultati dei map worker potrebbe essere grande
		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}
		objectBytes, err = ioutil.ReadAll(fileDownloaded.Body)
		if err != nil {
			panic(err)
		}

		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}

	} else if step == 0 {
		//sei il master non è fallito si legge dal file locale i risultati del Map task invece di vedere S3
		objectBytes, err = ioutil.ReadAll(file)
		if err != nil {
			panic(err)
		}
	}

	if len(tickerChan) != 0 {
		_ = <-tickerChan
	}

	arrayForReducer := make(map[string][]int)

	if step == 1 || step == 0 {
		//UNMARSHAL dei valori nel file
		res := strings.Split(string(objectBytes), "}")
		mapArray := make([]Result, len(res)-1)
		// Display all elements.
		for i := 0; i < (len(res) - 1); i++ {
			errorJ := json.Unmarshal([]byte(res[i]+"}"), &mapArray[i])
			if errorJ != nil {
				fmt.Println(errorJ.Error())
				os.Exit(-1)
			}
		}
		file.Close()

		//-------------------------------------------------------
		//SHUFFLE operation
		//From map[string]int to map[string][]int
		for i := range mapArray {
			for k, v := range mapArray[i] {
				arrayForReducer[k] = append(arrayForReducer[k], v)
			}
		}
		//------------------------------------------------------
		//si apre il file in modalità APPEND
		file, err = os.OpenFile(gopath+"/"+tmpFile,
			os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			log.Println(err)
			os.Exit(-1)
		}

		jsonData, errorJ := json.Marshal(arrayForReducer)
		if errorJ != nil {
			fmt.Println(errorJ.Error())
			os.Exit(-1)
		}
		jsonStr := string(jsonData)

		//scrittura del file temporaneo con i risultati dei map worker "ordinati" (shuffled)
		_, err = file.WriteString(jsonStr)
		if err != nil {
			log.Println(err.Error())
			os.Exit(-1)
		}

		// Save file changes.
		err = file.Sync()
		if err != nil {
			fmt.Println("Error on file sync")
			os.Exit(-1)
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			log.Fatal(err)
		}
		fileInfo, _ := file.Stat()
		var size int64 = fileInfo.Size()
		buffer := make([]byte, size)
		file.Read(buffer)

		//inserimento file con risultati shuffled su S3
		inp := &s3.PutObjectInput{
			Body:          bytes.NewReader(buffer),
			ContentLength: aws.Int64(size),
			Bucket:        aws.String(bucketName),
			Key:           aws.String(tmpFile),
			Tagging:       aws.String("state=afterShuffle"),
		}
		_, err = svcs3.PutObject(inp)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return
		}

		//	fmt.Println("file shuffled uploadato su s3")
		//si chiude il file locale perchè "the behavior of Seek on a file opened with O_APPEND is not specified."
		file.Close()

		//--------------------------------------------
		//******************************
		//simulazione crash master
		rand.Seed(time.Now().UnixNano())
		if rand.Intn(100) < faultProbability {
			fmt.Println("IL master fallisce dopo aver caricato file shuffled su S3")
			return
		}
		//}
		//**********************************

	}
	//----------------------------------------------

	if step == 2 {
		//se si entra qui allora il file con i risultati shuffled è già stato caricato su S3 precedentemente e quindi occorre solo fare UNMARSHAL
		input := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(tmpFile),
		}
		//qui il file su S3 è già stato scritto precedentemente da un master crashato
		fileDownloaded, erro := svcs3.GetObject(input)
		if erro != nil {
			log.Println(erro)
			os.Exit(-1)
		}
		//si consuma il tick perchè il file dei risultati dei map worker shuffled potrebbe essere grande
		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}
		objectBytes, err = ioutil.ReadAll(fileDownloaded.Body)
		if err != nil {
			panic(err)
		}

		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}
		//UNMARSHAL dei valori nel file
		errorJ := json.Unmarshal(objectBytes, &arrayForReducer)
		if errorJ != nil {
			fmt.Println(errorJ.Error())
			os.Exit(-1)
		}
	}

	resultRed := make(Result)
	numWorkers := 0
	//starts thread for call rcp reduce task
	for k, _ := range arrayForReducer {
		numWorkers++
		//si consuma il tick perchè il file dei risultati dei map worker shuffled potrebbe essere grande
		if len(tickerChan) != 0 {
			_ = <-tickerChan
		}
		go rpcReduceRequest(arrayForReducer[k], k, &resultRed, failed)

	}
	//fmt.Println("Mandate le richieste Reduce RPC")

	//-------------------------------------------------------------
	//attivazione monitoraggio master
wait2:
	for {
		select {
		//caso canale chiuso: vuol dire che il mastercontroller ritiene il master crashato
		case <-failed:
			//il master simula il crash ritornando
			return
		case <-tickerChan: //impedisce il timeout dallo scadere

		default:
			rand.Seed(time.Now().UnixNano())
			if rand.Intn(100) >= faultProbability {
				//	fmt.Println("magic number is", rand.Intn(100))
				//se la condizione è verificata allora tutti i reduce worker hanno spedito il loro risultato che è stato scritto dai thread
				if len(resultRed) == numWorkers {
					//fmt.Println("Tutti i reduce worker hanno risposto")
					break wait2 //uscita dal select e dal for
				}
			} else {
				fmt.Println("simulazione crash master durante l'attesa per le risposte dei Reduce workers")
				//si blocca il master per simularne un crash (in questo modo non si consumerà più il ticker periodico invato dal mastercontroller e si attiverà il RecoverMaster)
				time.Sleep(time.Second * timeoutMaster)
			}
		}
	}
	//stop del MasterController
	tickerChan <- 2
	////////////////////////////////////////
	//si elimina file locale
	// delete file
	err = os.Remove(gopath + "/" + tmpFile)
	if err != nil {
		fmt.Println("Error on delete file")
		os.Exit(-1)
	}
	//fmt.Println("File tempornaeo cancellato")

	//time.Sleep(time.Second * 1000) //**********************************************************************************
	//cancellazione di tutti i file nel bucket
	iter := s3manager.NewDeleteListIterator(svcs3, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	if err = s3manager.NewBatchDeleteWithClient(svcs3).Delete(aws.BackgroundContext(), iter); err != nil {
		exitErrorf("Unable to delete objects from bucket %q, %v", bucketName, err)
	}
	//	fmt.Println("File nel bucket eliminato")
	//eliminazione bucket
	svcs3.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		exitErrorf("Unable to delete bucket %q, %v", bucketName, err)
	}
	/*
		// Wait until bucket is deleted before finishing
		err = svcs3.WaitUntilBucketNotExists(&s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			fmt.Println(err.(awserr.Error))
			exitErrorf("Error occurred while waiting for bucket to be deleted, %v", bucketName)
		}
		fmt.Println("Bucket deleted")*/
	//---------------------------------------------------
	//gestione e invio output al client
	*output <- resultRed
	//close(*output)
	//fmt.Println("IL master termina")
	return

}

//---------------------------------------------------------------------------------
//Il RecoverMaster viene invocato dal mastercontroller allo scadere del timeout del master. Egli si occupa di capire dove il master è fallito e poi di avviarne un altro
func RecoverMaster(fileList []string, output *chan Result) {
	ses, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	//retrieve an object from s3
	svc := s3.New(ses)
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(tmpFile),
	}
	_, err = svc.GetObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {

			//il file json con stato=beforeShuffle non è stato messo nel bucket quindi occorre ricostruirlo
			case s3.ErrCodeNoSuchKey:
				/*************************************************/
				//	fmt.Println("Non c'è il file su S3 quindi si riparte da capo")
				//creiamo un canale tra master e function handler timeout che verrà scritto dalla funzione handler dell'heartbeating in caso di timeout
				failed := make(chan bool)
				//inizializziamo un canale usato dal mastercontroller per fare heartbeat del master
				tickerChan := make(chan int, 1)

				go StartMaster(fileList, failed, tickerChan, output, 0)
				//invocazione e passaggio di parametri al master controller che si occuperà del recovery del master in caso di crash
				go MasterController(fileList, failed, tickerChan, output)
				return
				/*******************************************************/
			default:
				fmt.Println(aerr.Error())
				os.Exit(-1)
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
			os.Exit(-1)
		}
	}

	// To retrieve tag set of object
	inputTag := &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(tmpFile),
	}
	resultTag, err := svc.GetObjectTagging(inputTag)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
	var state string
	for _, t := range resultTag.TagSet {
		state = aws.StringValue(t.Value)
	}

	switch state {

	case "beforeShuffle":
		failed := make(chan bool)
		//inizializziamo un canale usato dal mastercontroller per fare heartbeat del master
		tickerChan := make(chan int, 1)

		go StartMaster(fileList, failed, tickerChan, output, 1)
		//invocazione e passaggio di parametri al master controller che si occuperà del recovery del master in caso di crash
		go MasterController(fileList, failed, tickerChan, output)
		return

	case "afterShuffle":
		failed := make(chan bool)
		//inizializziamo un canale usato dal mastercontroller per fare heartbeat del master
		tickerChan := make(chan int, 1)

		go StartMaster(fileList, failed, tickerChan, output, 2)
		//invocazione e passaggio di parametri al master controller che si occuperà del recovery del master in caso di crash
		go MasterController(fileList, failed, tickerChan, output)
		return
	}
}

//**********************************************************************************/
//MasterController si occupa di mandare periodicamente dei tick al master, se un tick non viene consumato entro il timeoutMaster allora il master viene dichiarato crashato e si passa il controllo al RecoverMaster
func MasterController(fileList []string, failed chan bool, tickerChan chan int, output *chan Result) {
	const tick = 1
	tickerChan <- tick
	defer close(tickerChan)
	for {
		time.Sleep(time.Second * timeoutMaster)
		select {
		case t := <-tickerChan:
			if t != 2 {
				//il mastercontroller consuma il tick che egli stesso aveva prodotto quindi
				//timeout master raggiunto
				fmt.Println("Il masterController ha dedotto che il master ha avuto un fault")
				//si chiude il canale in modo da triggerare i thread dedicati alla ricezione dei map result
				close(failed)
				//reinizializzazione array dei risultati perchè qualche thread potrebbe averci già scritto sopra
				mapperResults = nil
				//il master non ha consumato il canale e quindi è stato dichiarato guasto
				//RecoverMaster prende come parametri la lista di file da processare e il canale di output con il client
				go RecoverMaster(fileList, output)
				return
			}
			//fmt.Println("IL mastercontroller ha saputo che il master ha finito quindi termina")
			return
		default:
			//il channel è stato svuotato dal master quindi lo si riempie
			tickerChan <- tick
			//e si aspetterà un altro intervallo di timeout
		}
	}
}

//thread implementa la chiamata RPC e fa partire un timeout, restituisce un puntatore per sapere quando la risposta è pronta e la risposta
func rpcMapRequest(txtLines []string, masterFail chan bool) {
	//connect to RPC worker using HTTP protocol
	client, err := rpc.DialHTTP("tcp", workerAddress)
	if err != nil {
		log.Fatal("Error in dialing: ", err)
	}

	args := &txtLines
	// reply will store the RPC result
	reply := new(Result)
	// Asynchronous call
	divCall := client.Go("Worker.Map", args, reply, nil)
	//avvio timer per verifica di eventuali guasti del worker
	timer := time.NewTimer(time.Second * timeoutWorker)
	defer timer.Stop()
	select {
	//caso timeout scaduto
	case <-timer.C:
		fmt.Println("Timeout per chiamata RPC Map scaduto")
		timer.Stop()
		client, err := rpc.DialHTTP("tcp", workerAddress)
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}
		//nuova RPC call
		divCall = client.Go("Worker.Map", args, reply, nil)
		timer = time.NewTimer(time.Second * timeoutWorker)

		//caso risposta ricevuta
	case replyCall := <-divCall.Done:
		if replyCall.Error != nil {
			log.Fatal("Error in Worker.Map: ", replyCall.Error.Error())
		}
		rand.Seed(time.Now().UnixNano())
		//se la condizione non è verificata si simula crash del map worker (quindi non si processa la risposta pervenuta)
		if rand.Intn(100) >= faultProbability {
			//spegnimento del timer di timeout
			timer.Stop()
			//viene aggiunto il risulato del map worker
			mapperResults = append(mapperResults, reply)
			return
		} else {
			fmt.Println("Simulazione crash di un map worker ")
		}
	case <-masterFail:
		//il master è fallito quindi questo thread deve terminare
		timer.Stop()
		return
	}
}

//thread implementa la chiamata RPC e fa partire un timeout, restituisce un puntatore per sapere quando la risposta è pronta e la risposta
func rpcReduceRequest(occurence []int, word string, reduceResult *Result, masterFail chan bool) {
	//connect to RPC worker using HTTP protocol
	client, err := rpc.DialHTTP("tcp", workerAddress)
	if err != nil {
		log.Fatal("Error in dialing: ", err)
	}

	args := &occurence
	// reply will store the RPC result
	reply := new(int)
	// Asynchronous call
	divCall := client.Go("Worker.Reduce", args, reply, nil)
	//avvio timer per verifica di eventuali guasti del worker
	timer := time.NewTimer(time.Second * timeoutWorker)
	defer timer.Stop()
	select {

	case <-masterFail:
		//il master è fallito quindi questo thread deve terminare
		timer.Stop()
		return
	//caso timeout scaduto
	case <-timer.C:
		fmt.Println("Timeout Reduce work scaduto")
		timer.Stop()
		client, err := rpc.DialHTTP("tcp", workerAddress)
		if err != nil {
			log.Fatal("Error in dialing: ", err)
		}
		//nuova RPC call
		divCall = client.Go("Worker.Reduce", args, reply, nil)
		timer = time.NewTimer(time.Second * timeoutWorker)

		//caso risposta ricevuta
	case replyCall := <-divCall.Done:
		if replyCall.Error != nil {
			log.Fatal("Error in Worker.Reduce: ", replyCall.Error.Error())
		}
		rand.Seed(time.Now().UnixNano())
		//se la condizione non è verificata si simula crash del reduce worker (quindi non si processa la risposta pervenuta)
		if rand.Intn(100) >= faultProbability {
			//spegnimento del timer di timeout
			timer.Stop()
			//viene aggiunto il risulato del reduce worker
			//fmt.Println(word, (*reply))
			(*reduceResult)[word] = (*reply)
			return
		} else {
			fmt.Println("Simulazione crash di un reduce worker ")
		}

	}
}

func exitErrorf(msg string, args ...interface{}) {

	fmt.Fprintf(os.Stderr, msg+"\n", args...)

	os.Exit(1)
}
