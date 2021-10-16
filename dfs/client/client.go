package main

import (
	"bufio"
	"bytes"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func HandleArgs() (string, string) {
	controller := os.Args[1]
	port := os.Args[2]
	//file := os.Args[3]
	//chunkSize, err := strconv.Atoi(os.Args[4])
	//return host, port, file, chunkSize, err
	return controller, port
}

func InitializeLogger() {
	file, err := os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/client_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Client start up complete")
}

func GetMetadata(fileName string) (int, string) {
	f, _ := os.Open(fileName)
	defer f.Close()
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	checkSum := messages.GetCheckSum(*f)
	return int(fileSize), checkSum
}

func LogMetadata(metadata *messages.Metadata) {
	fileName := metadata.GetFileName()
	fileSize := int(metadata.GetFileSize())
	numChunks := int(metadata.GetNumChunks())
	chunkSize := int(metadata.GetChunkSize())
	checkSum := metadata.GetCheckSum()
	log.Printf("Metadata: \nName: %s \nSize: %d \nChunks: %d \nChunk Size: %d \n", fileName, fileSize, numChunks, chunkSize)
	log.Printf("Checksum: %s \n", checkSum)
}

func UnpackPutResponse(msg *messages.Wrapper_PutResponseMessage) (bool, []string, *messages.Metadata) {
	available := msg.PutResponseMessage.GetAvailable()
	nodes := msg.PutResponseMessage.GetNodes()
	metadata := msg.PutResponseMessage.GetMetadata()
	log.Println("Received put response status: " + strconv.FormatBool(available))
	return available, nodes, metadata
}

func PackagePutRequest(fileName string) *messages.Wrapper {
	fileSize, checkSum := GetMetadata(fileName)
	metadata := &messages.Metadata{FileName: fileName, FileSize: int32(fileSize), CheckSum: checkSum}
	msg := messages.PutRequest{Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	return wrapper
}

func PackagePutRequestChunk(currentChunk string, chunkSize int32) *messages.Wrapper {
	metadata := &messages.Metadata{FileName: currentChunk, ChunkSize: chunkSize}
	msg := messages.PutRequest{Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	log.Println("Packaging chunk: " + currentChunk)
	log.Println("Metadata: " + msg.Metadata.String())
	log.Println("Wrapper: " + wrapper.String())
	return wrapper
}

func HandleInput(scanner *bufio.Scanner, controllerConn net.Conn) {
	message := scanner.Text()
	if len(message) != 0 {

		var wrapper *messages.Wrapper
		controllerMessageHandler := messages.NewMessageHandler(controllerConn)

		if strings.HasPrefix(message, "put"){
			var trimmed = strings.TrimPrefix(message, "/")
			var words = strings.Split(trimmed, " ")
			var fileName = words[1]
			wrapper = PackagePutRequest(fileName)
			log.Println("Sending metadata.")
			controllerMessageHandler.Send(wrapper)
			go HandleConnection(controllerMessageHandler)

			/*
				f, _ := os.Open(file)
				log.Println("Opened file.")
				buffer := make([]byte, chunkSize)
				reader := bytes.NewReader(buffer)
				conn := messageHandler.GetConn()
				writer := bufio.NewWriter(conn)
				log.Println("Preparing to write file to connection.")
				for {
					log.Println("writing..")
					numBytes, err := f.Read(buffer)
					reader = bytes.NewReader(buffer)
					if err != nil {
						break
					}
					_, err = io.CopyN(writer, reader, int64(numBytes))
					if err != nil {
						fmt.Print(err.Error())
						break
					}
				}
				f.Close()

			*/
			log.Println("Done writing to connection.")
		} else if strings.HasPrefix(message, "get") {
			fmt.Println("get received")
			//send get request
			//go handleConnection()
		} else if strings.HasPrefix(message, "delete") {
			fmt.Println("delete received")
			//send dlete request
			//go handleconnection
		} else if strings.HasPrefix(message, "ls") {
			fmt.Println("ls received")
			//send ls request
			//go handleconnection
		} else {
			fmt.Println("error ")
		}
	}
}

func GetChunkIndex(metadata *messages.Metadata, destinationNodes []string) map[string]string {
	LogMetadata(metadata)

	chunkIndex := make(map[string]string)
	for i := 0; i < int(metadata.NumChunks); i++ {
		moddedIndex := i % len(destinationNodes)
		node := destinationNodes[moddedIndex]
		currentChunkName := strconv.Itoa(i) + "_" + metadata.FileName
		chunkIndex[currentChunkName] = node
	}
	for chunk, node:= range chunkIndex {
		log.Println("-> " + chunk + " " + node)
	}
	return chunkIndex
}

func SendChunks(metadata *messages.Metadata, destinationNodes []string) {
	chunkIndex := GetChunkIndex(metadata, destinationNodes)

	f, _ := os.Open(metadata.FileName)
	log.Println("Opened file.")
	buffer := make([]byte, metadata.ChunkSize)
	log.Println("Preparing to write file to connections.")
	counter := 0
	for {
		log.Println("writing..")
		numBytes, err := f.Read(buffer)
		reader := bytes.NewReader(buffer)
		if err != nil {
			break
		}
		currentChunk := strconv.Itoa(counter) + "_" + metadata.FileName
		wrapper := PackagePutRequestChunk(currentChunk, metadata.ChunkSize)
		node := chunkIndex[currentChunk]
		connection, err := net.Dial("tcp", node)
		messageHandler := messages.NewMessageHandler(connection)
		messageHandler.Send(wrapper)
		log.Println("Wrapper sent")
		writer := bufio.NewWriter(connection)
		log.Println("writer created")
		_, err = io.CopyN(writer, reader, int64(numBytes))
		log.Println("copyN called")
		if err != nil {
			fmt.Print(err.Error())
			break
		}
		log.Printf("%d bytes sent\n", numBytes)
		counter++
		messageHandler.Close()
	}
	f.Close()
}

func LogFileTransferStatus(status bool) {
	if status {
		fmt.Println("File transfer successful")
		log.Println("File transfer successful")
	} else {
		fmt.Println("File transfer unsuccessful: checksums don't match")
		log.Println("File transfer unsuccessful: checksums don't match")
	}
}

func LogDestinationNodes(destinationNodes []string) {
	log.Println("Sending chunks to the following destinations: ")
	for node := range destinationNodes {
		log.Println(destinationNodes[node])
	}
}

func LogFileAlreadyExists() {
	fmt.Println("File with this name already exists, must delete first")
	log.Println("File already exists")
}

func HandleConnection(messageHandler *messages.MessageHandler) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_AcknowledgeMessage:
			status := msg.AcknowledgeMessage.GetCheckSumMatched()
			LogFileTransferStatus(status)
		case *messages.Wrapper_PutResponseMessage:
			available, destinationNodes, metadata := UnpackPutResponse(msg)
			if available {
				LogDestinationNodes(destinationNodes)
				SendChunks(metadata, destinationNodes)
			} else {
				LogFileAlreadyExists()
			}
		case *messages.Wrapper_GetResponseMessage:
			log.Println("Get Response message received")
		case *messages.Wrapper_DeleteResponseMessage:
			log.Println("Delete Response message received")
		default:
			continue
		}
	}
}

func main() {
	controller, port := HandleArgs()
	InitializeLogger()
	controllerConn, err := net.Dial("tcp", controller + ":" + port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer controllerConn.Close()
	fmt.Println("Connected to controller at " + controller + ":" + port)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if result := scanner.Scan(); result != false {
			HandleInput(scanner, controllerConn)
		}
	}
}

