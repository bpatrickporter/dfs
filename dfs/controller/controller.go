package main

import (
	"bufio"
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

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
		file, err = os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/controller_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	} else {
		file, err = os.OpenFile("logs/controller_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Controller start up complete")
}

func UnpackMetadata(metadata *messages.Metadata) (string, int, int, int, string){
	log.Println("Put request received")
	log.Println("Unpacking file metadata")
	log.Println(metadata)
	return metadata.GetFileName(), int(metadata.GetFileSize()), int(metadata.GetNumChunks()), int(metadata.GetChunkSize()), metadata.GetCheckSum()
}

func FindFile(fileName string, context context) ([]*messages.KeyValuePair, bool) {
	chunkToNodeMap, exists := context.fileToChunkToNodesIndex[fileName]
	if exists {
		log.Println("Result: File exists")
	} else {
		log.Println("Result: File doesn't exist")
	}
	return chunkToNodeMap, exists
}

func CalculateNumChunks(metadata *messages.Metadata, context context) {
	metadata.ChunkSize = int32(context.chunkSize)
	metadata.NumChunks = (metadata.FileSize + metadata.ChunkSize - 1) / metadata.ChunkSize
}

func GetChunkToNodeIndex(metadata *messages.Metadata, context context) []*messages.KeyValuePair {
	destinationNodes := make([]string, 0)
	chunkToNodeIndex := make([]*messages.KeyValuePair, 0)

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
		//chunkIndex[currentChunk] = node
		chunkToNodeIndex = append(chunkToNodeIndex, &messages.KeyValuePair{Key: currentChunk, Value: node})
	}
	return chunkToNodeIndex
}

func GetListing(directory string, context context) string {
	localDirectory := context.lsDirectory + directory
	dirEntries, err := os.ReadDir(localDirectory)
	var result string
	if err != nil {
		log.Println("Dir " + localDirectory + ": not a directory")
		result = "ls: " + directory + ": not a directory\n"
 	} else {
		 result = ""
		for i := range dirEntries {
			if dirEntries[i].IsDir() {
				result = result + "/" + dirEntries[i].Name() + "\n"
			} else {
				result = result + dirEntries[i].Name() + "\n"
			}
		}
	}
	return result
}

func GetInfo(context context) ([]string, string, []*messages.KeyValuePair) {
	var nodes []string
	for node, _ := range context.activeNodes {
		nodes = append(nodes, node)
	}
	diskSpace := "100 TB"
	requests := []*messages.KeyValuePair{{Key: "orion01", Value: "4"}, {Key: "orion02", Value: "5"}}
	return nodes, diskSpace, requests
}

func ValidatePutRequest(metadata *messages.Metadata, context context) validationResult {
	log.Println("Checking file index for file")
	CalculateNumChunks(metadata, context)
	fileName, fileSize, numChunks, chunkSize, checkSum := UnpackMetadata(metadata)
	_, exists := FindFile(fileName, context)
	log.Println("Exists = " + strconv.FormatBool(exists))
	//chunkIndex := make(map[string]string)
	var chunkToNodeIndex []*messages.KeyValuePair
	//var nodeList []string
	if !exists {
		//add to bloom filter
		chunkToNodeIndex = GetChunkToNodeIndex(metadata, context)
		log.Println("Adding destination nodes to file index")
		context.fileToChunkToNodesIndex[fileName] = chunkToNodeIndex

		//adding file to ls directory to support ls client commands
		file, err := os.OpenFile(context.lsDirectory + fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Println(err.Error())
		}
		defer file.Close()
		metadataBytes := []byte(fileName + "," + strconv.Itoa(fileSize) + "," + strconv.Itoa(numChunks) + "," + strconv.Itoa(chunkSize) + "," + checkSum)
		log.Println("Added the following destination nodes for filename " + fileName)
		for _, pair := range chunkToNodeIndex {
			log.Println("-> " + pair.Key + " " + pair.Value)
		}
		w := bufio.NewWriter(file)
		w.Write(metadataBytes)
		w.Flush()
	} else {
		chunkToNodeIndex = make([]*messages.KeyValuePair, 0)
	}
	return validationResult{fileExists: exists, chunkToNodeIndex: chunkToNodeIndex}
}

func FindChunks(fileName string, context context) locationResult {
	log.Println("Checking file index for file")
	chunkToNodeIndex, exists := FindFile(fileName, context)
	log.Println("Exists = " + strconv.FormatBool(exists))

	if !exists {
		log.Println("File [" + fileName + "] doesn't exist")
	} else {
		log.Println("File [" + fileName + "] located at the following locations:")
		for _, pair := range chunkToNodeIndex {
			log.Println(pair.Key + " @ " + pair.Value)
		}
	}
	return locationResult{fileExists: exists, chunkToNodeIndex: chunkToNodeIndex}
}

func PackagePutResponse(validationResult validationResult, metadata *messages.Metadata, context *context) *messages.Wrapper {
	putResponse := &messages.PutResponse{Available: !validationResult.fileExists, Metadata: metadata, ChunkToNodeIndex: validationResult.chunkToNodeIndex}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutResponseMessage{PutResponseMessage: putResponse},
	}
	log.Println("Sending put response to client")
	return wrapper
}

func PackageDeleteResponse(result locationResult) *messages.Wrapper {
	deleteResponse := &messages.DeleteResponse{Available: result.fileExists, ChunkNodePairs: result.chunkToNodeIndex}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteResponseMessage{DeleteResponseMessage: deleteResponse},
	}
	log.Println("Sending delete response to client")
	return wrapper
}

func PackageGetResponse(result locationResult) *messages.Wrapper {
	getResponse := &messages.GetResponse{Exists: result.fileExists, ChunkNodePairs: result.chunkToNodeIndex}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetResponseMessage{GetResponseMessage: getResponse},
	}
	log.Println("Sending get response to client")
	return wrapper
}

func PackageLSResponse(listing string) *messages.Wrapper {
	lsResponse := &messages.LSResponse{Listing: listing}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_LsResponse{LsResponse: lsResponse},
	}
	log.Println("Sending ls response to client")
	return wrapper
}

func PackageInfoResponse(nodes []string, diskSpace string, requestsPerNode []*messages.KeyValuePair) *messages.Wrapper {
	infoResponse := &messages.InfoResponse{Nodes: nodes, AvailableDiskSpace: diskSpace, RequestsPerNode: requestsPerNode}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_InfoResponse{InfoResponse: infoResponse},
	}
	return wrapper
}

func RegisterNode(msg *messages.Wrapper_RegistrationMessage, context *context) {
	node := msg.RegistrationMessage.GetNode()
	port := msg.RegistrationMessage.GetPort()
	context.activeNodes[node + ":" + port] = 0
	log.Println(node + ":" + port + " registered with controller")
	log.Print("Active nodes: ")
	for node, port := range context.activeNodes {
		log.Printf("-> %s:%s \n", node, port)
	}
}

func DeleteFileFromIndex(fileName string, context context) {
	delete(context.fileToChunkToNodesIndex, fileName)
	//delete from ls directory
	localDirectory := context.lsDirectory + fileName
	err := os.Remove(localDirectory)
	if err != nil {
		log.Fatalln()
	}
	log.Println("Deleted " + fileName + " from file index")
}

func RecordHeartBeat(node string, context context) {
	context.activeNodes[node] = context.activeNodes[node] + 1
	fmt.Println("<3" + node + "<3")
}

func AnalyzeHeartBeats(context context) {
	//copy the active nodes map
	counts := make(map[string]int)
	for node, heartBeats := range context.activeNodes {
		counts[node] = heartBeats
		fmt.Println("Set up: " + node + " = " + strconv.Itoa(heartBeats))
	}

	for {
		time.Sleep(10 * time.Second)
		for node, heartBeats := range context.activeNodes {
			if count, nodeExists := counts[node]; !nodeExists {
				//if node on active nodes list isn't in our map, add it
				counts[node] = heartBeats
				fmt.Println("Adding " + node + " to heartbeat counter")
			} else {
				fmt.Println(node + ": " + "count=" + strconv.Itoa(counts[node]) + " beats=" + strconv.Itoa(heartBeats))
				if heartBeats == count {
					//node is down, initiate recovery
					go InitiateRecovery(node)
					//remove node from counts and from active nodes
					delete(counts, node)
					delete(context.activeNodes, node)
					fmt.Println("Node " + node + " is offline")
				} else {
					//node isn't down, update counts
					counts[node] = heartBeats
				}
			}
		}
	}
}

func InitiateRecovery(node string) {
	return
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
			validationResult := ValidatePutRequest(metadata, context)
			wrapper := PackagePutResponse(validationResult, metadata, &context)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_GetRequestMessage:
			log.Println("Get request message received")
			fileName := msg.GetRequestMessage.FileName
			results := FindChunks(fileName, context)
			if results.fileExists {
				log.Println("file exists")
			} else {
				log.Println("file doesn't exist")
			}
			wrapper := PackageGetResponse(results)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_DeleteRequestMessage:
			log.Println("Delete request message received")
			fileName := msg.DeleteRequestMessage.FileName
			results := FindChunks(fileName, context)
			if results.fileExists {
				DeleteFileFromIndex(fileName, context)
			}
			wrapper := PackageDeleteResponse(results)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_HeartbeatMessage:
			node := msg.HeartbeatMessage.Node
			log.Println("Heartbeat received from " + node)
			RecordHeartBeat(node, context)
		case *messages.Wrapper_LsRequest:
			directory := msg.LsRequest.Directory
			listing := GetListing(directory, context)
			wrapper := PackageLSResponse(listing)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_InfoRequest:
			nodeList, diskSpace, requestsPerNode := GetInfo(context)
			wrapper := PackageInfoResponse(nodeList, diskSpace, requestsPerNode)
			messageHandler.Send(wrapper)
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
	var lsDirectory string
	if _, isOrion := IsHostOrion(); isOrion {
		lsDirectory = "/bigdata/bpporter/ls/"
	} else {
		lsDirectory = "/Users/pport/677/ls/"
	}
	return context{activeNodes: make(map[string]int),
		fileToChunkToNodesIndex: make(map[string][]*messages.KeyValuePair),
		bloomFilter: make(map[string]int),
		chunkSize: chunkSize,
		lsDirectory: lsDirectory},
		err
}

type context struct {
	activeNodes map[string]int
	//                                      chunkIndex     //
	//                                   |---------------| //
	//                           file      chunk  node     //
	fileToChunkToNodesIndex map[string][]*messages.KeyValuePair
	//nodeToChunkIndex
	bloomFilter map[string]int
	chunkSize int
	lsDirectory string

	//what we need

	//bloom filter

	//create a new file everytime a node is registered, just lists the chunks stored there, add to it with every
	//save, how do we delete from it? could be append only? only read last line, to remove, read last line, remove
	//one chunk, write new line?
	//node -> [chunk01, chunk02] (if a node goes down, we know we need to duplicate these chunks

	//fileToChunkToNodesIndex moves to files in ls directory, add lines to file for every chunk (chunk, node, node, node
	//chunk -> [node1, node2, node3] (if we need to duplicate chunks, we know where to get them

	//Initiate recovery

	//Node to node passes
}

type validationResult struct {
	fileExists bool
	chunkToNodeIndex []*messages.KeyValuePair
}

type locationResult struct {
	fileExists bool
	chunkToNodeIndex []*messages.KeyValuePair
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
	go AnalyzeHeartBeats(context)
	for {
		if conn, err := listener.Accept(); err == nil {
			go HandleConnection(conn, context)
		}
	}
}
