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
	file, err := os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/client_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	//file, err := os.OpenFile("logs/client_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
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

func UnpackDeleteResponse(msg *messages.Wrapper_DeleteResponseMessage) (bool, []*messages.KeyValuePair){
	fileExists := msg.DeleteResponseMessage.Available
	chunkLocations := msg.DeleteResponseMessage.ChunkNodePairs
	log.Println("Delete Response message received")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for entry := range chunkLocations {
		log.Println(chunkLocations[entry].Key + " @ " + chunkLocations[entry].Value)
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunkLocations
}

func UnpackGetResponse(msg *messages.Wrapper_GetResponseMessage) (bool, []*messages.KeyValuePair) {
	fileExists := msg.GetResponseMessage.Exists
	chunkLocations := msg.GetResponseMessage.ChunkNodePairs
	log.Println("Get Response message received")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for entry := range chunkLocations {
		log.Println(chunkLocations[entry].Key + " @ " + chunkLocations[entry].Value)
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunkLocations
}

func PackagePutRequest(fileName string) *messages.Wrapper {
	log.Println("Put input received")
	fileSize, checkSum := GetMetadata(fileName)
	metadata := &messages.Metadata{FileName: fileName, FileSize: int32(fileSize), CheckSum: checkSum}
	msg := messages.PutRequest{Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	log.Println("Sending put request")
	return wrapper
}

func PackageDeleteRequest(fileName string) *messages.Wrapper {
	msg := messages.DeleteRequest{FileName: fileName}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteRequestMessage{DeleteRequestMessage: &msg},
	}
	log.Println("Sending delete request")
	return wrapper
}

func PackagePutRequestChunk(currentChunk string, metadata *messages.Metadata, chunkCheckSum string) *messages.Wrapper {
	fileMetadata := metadata
	chunkMetadata := &messages.ChunkMetadata{ChunkName: currentChunk, ChunkSize: metadata.ChunkSize, ChunkCheckSum: chunkCheckSum}

	msg := messages.PutRequest{Metadata: fileMetadata, ChunkMetadata: chunkMetadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	log.Println("Packaging chunk: " + currentChunk)
	log.Println("Metadata: " + msg.Metadata.String())
	log.Println("ChunkMetadata: " + msg.ChunkMetadata.String())
	log.Println("Wrapper: " + wrapper.String())
	return wrapper
}

func PackageGetRequest(fileName string) *messages.Wrapper {
	msg := messages.GetRequest{FileName: fileName}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetRequestMessage{GetRequestMessage: &msg},
	}
	log.Println("Sending get request")
	return wrapper
}

func GetFileName(message string) string {
	trimmed := strings.TrimPrefix(message, "/")
	words := strings.Split(trimmed, " ")
	fileName := words[1]
	return fileName
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

func LogFileTransferStatus(status bool) {
	if status {
		//fmt.Println("Chunk transfer successful")
		log.Println("Chunk transfer successful")
	} else {
		//fmt.Println("Chunk transfer unsuccessful: checksums don't match")
		log.Println("Chunk transfer unsuccessful: checksums don't match")
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

func DeleteChunks(locations []*messages.KeyValuePair) {
	for entry := range locations {
		chunk := locations[entry].Key
		node := locations[entry].Value
		wrapper := PackageDeleteRequest(chunk)
		conn, err := net.Dial("tcp", node)
		if err != nil {
			log.Fatalln(err.Error())
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		log.Println("Delete chunk request sent")
		messageHandler.Close()
	}
}

func SendChunks(metadata *messages.Metadata, destinationNodes []string) {
	chunkIndex := GetChunkIndex(metadata, destinationNodes)

	f, err := os.Open(metadata.FileName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer f.Close()
	log.Println("Opened file.")
	buffer := make([]byte, metadata.ChunkSize)
	log.Println("Preparing to write file to connections.")
	counter := 0
	for {
		log.Println("writing..")
		numBytes, err := f.Read(buffer)
		checkSum := messages.GetChunkCheckSum(buffer)
		reader := bytes.NewReader(buffer)
		if err != nil {
			break
		}
		currentChunk := strconv.Itoa(counter) + "_" + metadata.FileName
		wrapper := PackagePutRequestChunk(currentChunk, metadata, checkSum)
		node := chunkIndex[currentChunk]
		conn, err := net.Dial("tcp", node)
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		log.Println("Wrapper sent")
		writer := bufio.NewWriter(conn)
		log.Println("writer created")
		_, err = io.CopyN(writer, reader, int64(numBytes))
		log.Println("copyN called")
		if err != nil {
			fmt.Print(err.Error())
			break
		}
		log.Printf("%d bytes sent\n", numBytes)
		counter++
		//instead of waiting for ack we'll simplify this for now and close the connection
		messageHandler.Close()
	}
	fmt.Println("File saved")
}

func GetChunks(locations []*messages.KeyValuePair) {
	fmt.Println("Going to get chunks")
	for entry := range locations {
		chunk := locations[entry].Key
		node := locations[entry].Value
		wrapper := PackageGetRequest(chunk)
		conn, err := net.Dial("tcp", node)
		if err != nil {
			log.Fatalln(err.Error())
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		log.Println("Get chunk request sent to node")
		go HandleConnection(messageHandler)
	}
}

func GetIndex(chunkName string) (string, string) {
	splitIndex := strings.Index(chunkName, "_")
	index := chunkName[0:splitIndex]
	fileName := chunkName[splitIndex + 1:]
	return index, fileName
}

func WriteChunk(chunkName string, messageHandler *messages.MessageHandler) {
	log.Println("Chunk message received")
	index, fileName := GetIndex(chunkName)
	i, err := strconv.Atoi(index)
	if err != nil {
		log.Fatalln(err.Error())
	}
	file, err := os.OpenFile("copy_" + fileName, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln(err.Error())
	}
	conn := messageHandler.GetConn()
	buffer := make([]byte, 5)
	writer := bufio.NewWriter(os.Stdout)
	numBytes, err := conn.Read(buffer)
	if err != nil {
		log.Fatalln(err.Error())
	}
	fmt.Print(chunkName + " -> ")
	reader := bytes.NewReader(buffer[:numBytes])
	_, err = io.CopyN(writer, reader, int64(numBytes))
	file.WriteAt(buffer[:numBytes], int64(i * 5))

	fmt.Print("\n")
	if err != nil {
		log.Fatalln(err.Error())
	}
}

func HandleConnection(messageHandler *messages.MessageHandler) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_AcknowledgeMessage:
			status := msg.AcknowledgeMessage.GetCheckSumMatched()
			LogFileTransferStatus(status)
			messageHandler.Close()
			return
		case *messages.Wrapper_PutResponseMessage:
			available, destinationNodes, metadata := UnpackPutResponse(msg)
			if available {
				LogDestinationNodes(destinationNodes)
				SendChunks(metadata, destinationNodes)
			} else {
				LogFileAlreadyExists()
			}
			return
		case *messages.Wrapper_GetResponseMessage:
			fileExists, locations := UnpackGetResponse(msg)
			if fileExists {
				GetChunks(locations)
			}
		case *messages.Wrapper_GetResponseChunkMessage:
			chunkName := msg.GetResponseChunkMessage.ChunkName
			WriteChunk(chunkName, messageHandler)
			//verify checksums-how to verify checksum after last chunk is received?
			messageHandler.Close()
		case *messages.Wrapper_DeleteResponseMessage:
			fileExists, locations := UnpackDeleteResponse(msg)
			if fileExists {
				DeleteChunks(locations)
				fmt.Println("File deleted")
			}
			return
		default:
			continue
		}
	}
}

func HandleInput(scanner *bufio.Scanner, controllerConn net.Conn) {
	message := scanner.Text()
	if len(message) != 0 {
		var wrapper *messages.Wrapper
		controllerMessageHandler := messages.NewMessageHandler(controllerConn)

		if strings.HasPrefix(message, "put"){
			fileName := GetFileName(message)
			wrapper = PackagePutRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "get") {
			fmt.Println("get received")
			fileName := GetFileName(message)
			wrapper = PackageGetRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "delete") {
			fileName := GetFileName(message)
			wrapper = PackageDeleteRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "ls") {
			fmt.Println("ls received")
			//send ls request
			//handleconnection
		} else {
			fmt.Println("error ")
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

