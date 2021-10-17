package main

import (
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

func InitializeLogger() {
	file, err := os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/controller_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	//file, err := os.OpenFile("logs/controller_logs.txt", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Controller start up complete")
}

func UnpackMetadata(metadata *messages.Metadata) (string, int32, string){
	log.Println("Put request received")
	log.Println("Unpacking file metadata")
	return metadata.GetFileName(), metadata.GetFileSize(), metadata.GetCheckSum()
}

func FileExists(fileName string, context *context) (map[string]string, bool) {
	chunkToNodeMap, exists := context.betterFileIndex[fileName]
	if exists {
		log.Println("Result: File exists")
	} else {
		log.Println("Result: File doesn't exist")
	}
	return chunkToNodeMap, exists
}

func GetChunkIndex(metadata *messages.Metadata, context *context, chunkIndex map[string]string) []string {
	//chunkIndex key -> chunkName ... value -> node
	metadata.ChunkSize = int32(context.chunkSize)
	metadata.NumChunks = (metadata.FileSize + metadata.ChunkSize - 1) / metadata.ChunkSize
	destinationNodes := make([]string, 0)

	counter := 1
	log.Println("NumChunks: " + strconv.Itoa(int(metadata.NumChunks)))
	log.Println("NumNodes: " + strconv.Itoa(len(context.activeNodes)))
	for node, _ := range context.activeNodes {
		log.Println("Adding " + node + " to destination nodes")
		destinationNodes = append(destinationNodes, node)
		if counter == int(metadata.NumChunks){
			break
		}
		counter++
	}

	numNodes := len(destinationNodes)
	for i := 0; i < int(metadata.NumChunks); i++ {
		moddedIndex := i % numNodes
		node := destinationNodes[moddedIndex]
		currentChunk := strconv.Itoa(i) + "_" + metadata.FileName
		chunkIndex[currentChunk] = node
	}
	return destinationNodes
}

func ValidatePutRequest(metadata *messages.Metadata, context *context) validationResult {
	log.Println("Checking file index for file")
	fileName := metadata.GetFileName()
	_, exists := FileExists(fileName, context)
	log.Println("Exists = " + strconv.FormatBool(exists))
	chunkIndex := make(map[string]string)
	var nodeList []string
	if !exists {
		//add to bloom filter
		nodeList = GetChunkIndex(metadata, context, chunkIndex)
		log.Println("Adding destination nodes to file index")
		context.betterFileIndex[fileName] = chunkIndex
		log.Println("Added the following destination nodes for filename " + fileName)
		for chunkName, node := range chunkIndex {
			log.Println("-> " + chunkName + " " + node)
		}
	} else {
		nodeList = make([]string, 0)
	}
	return validationResult{fileExists: exists, nodeList: nodeList}
}

func LocateFile(fileName string, context *context) locationResult {
	log.Println("Checking file index for file")
	chunkToNodeMap, exists := FileExists(fileName, context)
	log.Println("Exists = " + strconv.FormatBool(exists))

	if !exists {
		log.Println("File [" + fileName + "] doesn't exist")
	} else {
		log.Println("File [" + fileName + "] located at the following locations:")
		for chunk, node := range chunkToNodeMap {
			log.Println(chunk + " @ " + node)
		}
	}
	return locationResult{fileExists: exists, chunkLocation: chunkToNodeMap}
}

func PackagePutResponse(validationResult validationResult, metadata *messages.Metadata, context *context) *messages.Wrapper {
	putResponse := &messages.PutResponse{Available: !validationResult.fileExists, Metadata: metadata, Nodes: validationResult.nodeList}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutResponseMessage{PutResponseMessage: putResponse},
	}
	log.Println("Sending put response to client")
	return wrapper
}

func PackageDeleteResponse(result locationResult, fileName string, context context) *messages.Wrapper {
	//chunk node
	pairs := make([]*messages.KeyValuePair, 0)

	if result.fileExists {
		for chunk, node := range result.chunkLocation {
			pair := messages.KeyValuePair{Key: chunk, Value: node}
			pairs = append(pairs, &pair)
		}
		log.Println("Sending response with the following node locations: ")
		for entry := range pairs {
			log.Println(pairs[entry].Key + " @ " + pairs[entry].Value)
		}
	}

	deleteResponse := &messages.DeleteResponse{Available: result.fileExists, Pairs: pairs}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteResponseMessage{DeleteResponseMessage: deleteResponse},
	}
	log.Println("Sending delete response to client")
	return wrapper
}

func RegisterNode(msg *messages.Wrapper_RegistrationMessage, context *context) {
	node := msg.RegistrationMessage.GetNode()
	port := msg.RegistrationMessage.GetPort()
	context.activeNodes[node + ":" + port] = struct{}{}
	log.Println(node + " registered with controller using port " + port)
	log.Print("Active nodes: ")
	for node, port := range context.activeNodes {
		log.Printf("-> %s:%s \n", node, port)
	}
}

func DeleteFileFromIndex(fileName string, context context) {
	delete(context.betterFileIndex, fileName)
	log.Println("Deleted " + fileName + " from file index")
}

func HandleConnection(conn net.Conn, context context) {
	log.Println("Accepted a connection from " + conn.RemoteAddr().String())
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_RegistrationMessage:
			RegisterNode(msg, &context)
			messageHandler.Close()
			return
		case *messages.Wrapper_PutRequestMessage:
			metadata := msg.PutRequestMessage.GetMetadata()
			validationResult := ValidatePutRequest(metadata, &context)
			wrapper := PackagePutResponse(validationResult, metadata, &context)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_GetRequestMessage:
			log.Println("Get request message received")
			//check if files exists
			//find out where its at
			//send list to client
		case *messages.Wrapper_DeleteRequestMessage: //from client
			log.Println("Delete request message received")
			fileName := msg.DeleteRequestMessage.FileName
			results := LocateFile(fileName, &context)
			if results.fileExists {
				DeleteFileFromIndex(fileName, context)
			}
			wrapper := PackageDeleteResponse(results, fileName, context)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_HeartbeatMessage: //from nodes
			log.Println("Heartbeat received")
		case nil:
			fmt.Println("Received an empty message, termination connection.")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func InitializeContext() (context, error) {
	chunkSize, err := strconv.Atoi(os.Args[2])
	return context{activeNodes: make(map[string]struct{}), betterFileIndex: make(map[string]map[string]string), bloomFilter: make(map[string]int), chunkSize: chunkSize}, err
}

type context struct {
	activeNodes map[string]struct{}
	//                   file      chunk    node//
	betterFileIndex map[string]map[string]string
	bloomFilter map[string]int
	chunkSize int
}

type validationResult struct {
	fileExists bool
	nodeList []string
}

type locationResult struct {
	fileExists bool
	//                chunk   node
	chunkLocation map[string]string
}

func main() {
	context, err := InitializeContext()
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	InitializeLogger()
	listener, err := net.Listen("tcp", ":" + os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			go HandleConnection(conn, context)
		}
	}
}
