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
	"sync"
	"time"
)

func HandleArgs() (string, string) {
	controller := os.Args[1]
	port := os.Args[2]
	//file := os.Args[3]
	//chunkSize, err := strconv.Atoi(os.Args[4])
	//return host, port, file, chunkSize, err
	return controller, port
}

func IsHostOrion() (string, bool) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}
	shortHostName:= strings.Split(hostname, ".")[0]
	var isOrion bool
	if strings.HasPrefix(shortHostName, "orion") {
		isOrion = true
	} else {
		isOrion = false
	}
	return shortHostName, isOrion
}

func InitializeLogger() {
	var file *os.File
	var err error
	if _, isOrion := IsHostOrion(); isOrion {
		file, err = os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/client_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	} else {
		file, err = os.OpenFile("logs/client_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	}
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

func UnpackDeleteResponse(msg *messages.Wrapper_DeleteResponseMessage) (bool, []string, []string){
	fileExists := msg.DeleteResponseMessage.Available
	//chunkLocations := msg.DeleteResponseMessage.ChunkNodePairs
	chunks := msg.DeleteResponseMessage.Chunks
	nodes := msg.DeleteResponseMessage.Nodes
	log.Println("Delete Response message received")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for i := range chunks {
		log.Println(chunks[i] + " @ " + nodes[i])
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunks, nodes
}

func UnpackGetResponse(msg *messages.Wrapper_GetResponseMessage) (bool, []string, []string) {
	fileExists := msg.GetResponseMessage.Exists
	//chunkLocations := msg.GetResponseMessage.ChunkNodePairs
	chunks := msg.GetResponseMessage.Chunks
	nodes := msg.GetResponseMessage.Nodes
	log.Println("Get Response message received")
	log.Println("File exists: " + strconv.FormatBool(fileExists))
	for i := range chunks {
		log.Println(chunks[i] + " @ " + nodes[i])
	}
	if !fileExists {
		fmt.Println("File doesn't exist")
	}
	return fileExists, chunks, nodes
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

func PackagePutRequestChunk(currentChunk string, metadata *messages.Metadata, chunkCheckSum string, numBytes int) *messages.Wrapper {
	fileMetadata := metadata
	chunkMetadata := &messages.ChunkMetadata{ChunkName: currentChunk, ChunkSize: int32(numBytes), ChunkCheckSum: chunkCheckSum}

	msg := messages.PutRequest{Metadata: fileMetadata, ChunkMetadata: chunkMetadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	log.Println("Packaging chunk: " + currentChunk)
	log.Println("Metadata: " + strconv.Itoa(int(msg.Metadata.ChunkSize)))
	log.Println("ChunkMetadata: " + strconv.Itoa(int(msg.ChunkMetadata.ChunkSize)))
	return wrapper
}

func PackageGetRequest(fileName string) *messages.Wrapper {
	msg := messages.GetRequest{FileName: fileName}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetRequestMessage{GetRequestMessage: &msg},
	}
	return wrapper
}

func PrintInfoResponse(response *messages.InfoResponse) {
	fmt.Println("Active Nodes")
	for node := range response.Nodes {
		fmt.Println(">" + response.Nodes[node])
	}
	fmt.Println("\nDisk Space: " + response.AvailableDiskSpace + "\n")
	fmt.Println("Requests Per Node")
	for _, pair := range response.RequestsPerNode {
		fmt.Println(">" + pair.Key + ": " + pair.Value)
	}
}

func GetParam(message string) string {
	words := strings.Split(message, " ")
	if len(words) < 2 {
		return ""
	} else {
		return words[1]
	}
}

func GetChunkIndex(metadata *messages.Metadata, destinationNodes []string) map[string]string {
	//LogMetadata(metadata)

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

func DeleteChunks(chunks []string, nodes []string) {
	for i := range chunks {
		chunk := chunks[i]
		node := nodes[i]
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
	buffer := make([]byte, metadata.ChunkSize)
	counter := 0
	for {
		numBytes, err := f.Read(buffer)
		//newBuffer := make([]byte, numBytes)
		newBuffer := buffer[:numBytes]
		checkSum := messages.GetChunkCheckSum(newBuffer)
		reader := bytes.NewReader(newBuffer)
		if err != nil {
			break
		}
		currentChunk := strconv.Itoa(counter) + "_" + metadata.FileName
		wrapper := PackagePutRequestChunk(currentChunk, metadata, checkSum, numBytes)
		node := chunkIndex[currentChunk]

		var conn net.Conn
		for {
			if conn, err = net.Dial("tcp", node); err != nil {
				log.Println("trying conn again" + node)
				time.Sleep(1000 * time.Millisecond)
			} else {
				break
			}
		}

		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		writer := bufio.NewWriter(conn)
		_, err = io.CopyN(writer, reader, int64(numBytes))
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

func GetChunks(chunks []string, nodes []string) {
	log.Println("Going to get chunks")
	var wg sync.WaitGroup

	for i := range chunks {
		chunk := chunks[i]
		node := nodes[i]
		wrapper := PackageGetRequest(chunk)
		conn, err := net.Dial("tcp", node)
		if err != nil {
			log.Fatalln(err.Error())
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		//wg.Add(1)
		HandleConnections(messageHandler, &wg)
	}
	//wg.Wait()
	fmt.Println("File downloaded")
}

func GetIndex(chunkName string) (string, string) {
	splitIndex := strings.Index(chunkName, "_")
	index := chunkName[0:splitIndex]
	fileName := chunkName[splitIndex + 1:]
	return index, fileName
}

func WriteChunk(chunkMetadata *messages.ChunkMetadata, fileMetadata *messages.Metadata, messageHandler *messages.MessageHandler) {
	log.Println(chunkMetadata.ChunkName + " incoming")

	index, fileName := GetIndex(chunkMetadata.ChunkName)
	i, err := strconv.Atoi(index)
	if err != nil {
		log.Fatalln(err.Error())
	}

	file, err := os.OpenFile("copy_" + fileName, os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		log.Fatalln(err.Error())
	}

	conn := messageHandler.GetConn()
	buffer := make([]byte, chunkMetadata.ChunkSize)
	numBytes, err := io.ReadFull(conn, buffer)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + " read " + strconv.Itoa(numBytes) + " bytes")

	checkSum := messages.GetChunkCheckSum(buffer)
	oldCheckSum := chunkMetadata.ChunkCheckSum
	log.Println(chunkMetadata.ChunkName + "New Checksum: " + checkSum)
	log.Println(chunkMetadata.ChunkName + "Old Checksum: " + oldCheckSum)

	n, err := file.WriteAt(buffer, int64(i * int(fileMetadata.ChunkSize)))
	log.Println("wrote to offset: " + strconv.Itoa(i * int(fileMetadata.ChunkSize)))
	log.Println("Index: " + strconv.Itoa(i) + " Chunksize: " + strconv.Itoa(int(fileMetadata.ChunkSize)))
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + "wrote " + strconv.Itoa(n) + " bytes to file")
	if err != nil {
		log.Fatalln(err.Error())
	}
	f, err := file.Stat()
	if err != nil {
		log.Println(err.Error())
	}
	log.Println("FileSize: " + strconv.Itoa(int(f.Size())))
}

func HandleConnections(messageHandler *messages.MessageHandler, waitGroup *sync.WaitGroup) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_GetResponseChunkMessage:
			//defer waitGroup.Done()
			chunkMetadata := msg.GetResponseChunkMessage.ChunkMetadata
			fileMetadata := msg.GetResponseChunkMessage.Metadata
			WriteChunk(chunkMetadata, fileMetadata, messageHandler)
			messageHandler.Close()
			return
		default:
			continue
		}
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
			fileExists, chunks, nodes := UnpackGetResponse(msg)
			if fileExists {
				GetChunks(chunks, nodes)
			}
			return
		case *messages.Wrapper_DeleteResponseMessage:
			fileExists, chunks, nodes := UnpackDeleteResponse(msg)
			if fileExists {
				DeleteChunks(chunks, nodes)
				fmt.Println("File deleted")
			}
			return
		case *messages.Wrapper_LsResponse:
			fmt.Print(msg.LsResponse.Listing)
			return
		case *messages.Wrapper_InfoResponse:
			PrintInfoResponse(msg.InfoResponse)
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
			fileName := GetParam(message)
			wrapper = PackagePutRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "get") {
			fileName := GetParam(message)
			wrapper = PackageGetRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "delete") {
			fileName := GetParam(message)
			wrapper = PackageDeleteRequest(fileName)
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "ls") {
			directory := GetParam(message)
			lsRequest := &messages.LSRequest{Directory: directory}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_LsRequest{LsRequest: lsRequest},
			}
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "info"){
			infoRequest := &messages.InfoRequest{}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_InfoRequest{InfoRequest: infoRequest},
			}
			controllerMessageHandler.Send(wrapper)
			HandleConnection(controllerMessageHandler)
		} else if strings.HasPrefix(message, "help"){
			fmt.Println("Available commands:\nput <file_name>\nget <file_name>\ndelete <file_name>\nls <directory>\ninfo")
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
		fmt.Print(">")
		if result := scanner.Scan(); result != false {
			HandleInput(scanner, controllerConn)
		}
	}
}

