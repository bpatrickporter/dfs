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

func GetIndexAndFileName(chunkName string) (string, string) {
	splitIndex := strings.Index(chunkName, "_")
	index := chunkName[0:splitIndex]
	fileName := chunkName[splitIndex + 1:]
	return index, fileName
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

func FindFile(fileName string, context context) (map[string][]string, bool) {
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

func GetChunkToNodesIndex(metadata *messages.Metadata, context context, chunkToNodeIndex map[string][]string) []string {
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
		chunkToNodeIndex[currentChunk] = []string{node}
		//add back up nodes
		forwardListIndex1 := (i + 1) % numNodes
		forwardListIndex2 := (i + 2) % numNodes
		forwardNode1 := destinationNodes[forwardListIndex1]
		forwardNode2 := destinationNodes[forwardListIndex2]
		chunkToNodeIndex[currentChunk] = append(chunkToNodeIndex[currentChunk], forwardNode1)
		chunkToNodeIndex[currentChunk] = append(chunkToNodeIndex[currentChunk], forwardNode2)
	}
	for chunk, nodeList := range chunkToNodeIndex {
		log.Print("-> " + chunk + " ")
		for i := range nodeList {
			log.Print(nodeList[i] + " ")
		}
	}
	return destinationNodes
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
	chunkToNodesIndex := make(map[string][]string)
	var nodeList []string
	if !exists {
		//add to bloom filter
		nodeList = GetChunkToNodesIndex(metadata, context, chunkToNodesIndex)
		log.Println("Adding destination nodes to file index")
		context.fileToChunkToNodesIndex[fileName] = chunkToNodesIndex

		//adding file to ls directory to support ls client commands
		file, err := os.OpenFile(context.lsDirectory + fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Println(err.Error())
		}
		defer file.Close()
		metadataBytes := []byte(fileName + "," + strconv.Itoa(fileSize) + "," + strconv.Itoa(numChunks) + "," + strconv.Itoa(chunkSize) + "," + checkSum)

		log.Println("Current status of nodeToChunksIndex")
		for node, chunkList := range context.nodeToChunksIndex {
			log.Println(node)
			for i := range chunkList {
				log.Println("->" + chunkList[i])
			}
		}
		w := bufio.NewWriter(file)
		w.Write(metadataBytes)
		w.Flush()
	} else {
		nodeList = make([]string, 0)
	}
	return validationResult{fileExists: exists, nodeList: nodeList}
}

func FindChunks(fileName string, context context) locationResult {
	log.Println("Checking file index for file")
	chunkToNodeMap, exists := FindFile(fileName, context)
	log.Println("Exists = " + strconv.FormatBool(exists))

	if !exists {
		log.Println("File [" + fileName + "] doesn't exist")
	} else {
		log.Println("File [" + fileName + "] located at the following locations:")
		for chunk, nodes := range chunkToNodeMap {
			log.Print(chunk + " @ ")
			for node := range nodes {
				log.Print(" " + nodes[node])
			}
			log.Println()
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

func PackageDeleteResponse(result locationResult) *messages.Wrapper {
	//chunk node
	chunks := make([]string, 0)
	nodeLists := make([]*messages.ListOfStrings, 0)

	if result.fileExists {
		for chunk, nodeList := range result.chunkLocation {
			chunks = append(chunks, chunk)
			list := messages.ListOfStrings{Strings: nodeList}
			nodeLists = append(nodeLists, &list)
		}
		log.Println("Sending response with the following node locations: ")
		for i := range chunks {
			log.Print(chunks[i] + " @ " + nodeLists[i].String())
		}
	}

	deleteResponse := &messages.DeleteResponse{Available: result.fileExists, Chunks: chunks, NodeLists: nodeLists}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteResponseMessage{DeleteResponseMessage: deleteResponse},
	}
	log.Println("Sending delete response to client")
	return wrapper
}

func PackageGetResponse(result locationResult) *messages.Wrapper {
	//pairs := make([]*messages.KeyValuePair, 0)
	chunks := make([]string, 0)
	nodes := make([]string, 0)


	if result.fileExists {
		for chunk, nodeList := range result.chunkLocation {
			chunks = append(chunks, chunk)
			nodes = append(nodes, nodeList[0])
		}
		log.Println("Sending response with the following node locations: ")
		for i := range chunks {
			log.Println(chunks[i] + " @ " + nodes[i])
		}
	}

	getResponse := &messages.GetResponse{Exists: result.fileExists, Chunks: chunks, Nodes: nodes}
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
	context.nodeToChunksIndex[node + ":" + port] = make([]string, 0)
	log.Println(node + ":" + port + " registered with controller")
	log.Print("Active nodes: ")
	for node, port := range context.activeNodes {
		log.Printf("-> %s:%s \n", node, port)
	}
}

func DeleteFileFromIndexes(fileName string, context context) {
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
					go InitiateRecovery(node, context)
					//remove node from counts and from active nodes
					log.Println("Recovery Initiated")
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

func PackageRecoveryInstruction(receiver string, chunk string) *messages.Wrapper {
	msg := messages.RecoveryInstruction{Receiver: receiver, Chunk: chunk}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_RecoveryInstructionMessage{RecoveryInstructionMessage: &msg},
	}
	return wrapper
}

func InitiateRecovery(node string, context context) {
	//find out what chunks the node had
	chunkList := context.nodeToChunksIndex[node]
	for i := range chunkList {
		chunk := chunkList[i]
		_, fileName := GetIndexAndFileName(chunk)
		chunkToNodeIndex := context.fileToChunkToNodesIndex[fileName]
		//where is this chunk?
		nodeList := chunkToNodeIndex[chunk]
		//pick a node that has the chunk but isn't the downed node
		sender, found := FindSender(node, nodeList)
		if !found {
			log.Println("Recovery aborted: no sender node found for " + chunk)
			break
		}
		//pick a node that doesn't have the chunk
		receiver, found := FindReceiver(nodeList, context.activeNodes)
		if !found {
			log.Println("Recovery aborted: no receiver node found for " + chunk)
			break
		}
		log.Println("Recovery: " + sender + " > " + receiver + " : " + chunkList[i])
		wrapper := PackageRecoveryInstruction(receiver, chunk)
		//sender sends put request to receiver

		var conn net.Conn
		var err error
		for {
			if conn, err = net.Dial("tcp", sender); err != nil {
				log.Println("trying conn again" + sender)
				time.Sleep(1000 * time.Millisecond)
			} else {
				fmt.Print(err.Error())
				break
			}
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		messageHandler.Close()

		UpdateIndexes(context, receiver, chunk)
	}
	delete(context.nodeToChunksIndex, node)
	log.Println("Recovery Complete")
	return
}

func UpdateIndexes(context context, receiver string, chunk string) {
	chunkList := context.nodeToChunksIndex[receiver]
	chunkList = append(chunkList, chunk)
	context.nodeToChunksIndex[receiver] = chunkList //redundant?

	_, fileName := GetIndexAndFileName(chunk)
	chunkToNodeIndex := context.fileToChunkToNodesIndex[fileName]
	nodeList := chunkToNodeIndex[chunk]
	nodeList = append(nodeList, receiver)
	chunkToNodeIndex[chunk] = nodeList //redundant?
}

func FindReceiver(nodeList []string, activeNodes map[string]int) (string, bool) {
	var foundReceiver bool
	for node, _ := range activeNodes {
		foundReceiver = true
		for j := range nodeList {
			if node == nodeList[j] {
				foundReceiver = false
			}
		}
		if foundReceiver {
			return node, foundReceiver
		}
	}
	return "", foundReceiver
}

func FindSender(node string, nodeList []string) (string, bool) {
	for i := range nodeList {
		if nodeList[i] != node {
			return nodeList[i], true
		}
	}
	return "", false
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
				DeleteFileFromIndexes(fileName, context)
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
			fmt.Println("Received an empty message, terminating connection.")
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
		nodeToChunksIndex: make(map[string][]string),
		fileToChunkToNodesIndex: make(map[string]map[string][]string),
		bloomFilter: make(map[string]int),
		chunkSize: chunkSize,
		lsDirectory: lsDirectory},
		err
}

type context struct {
	activeNodes map[string]int
	nodeToChunksIndex map[string][]string
	fileToChunkToNodesIndex map[string]map[string][]string
	bloomFilter map[string]int
	chunkSize int
	lsDirectory string

	//TODO
	//[   ]Initiate recovery
	//[   ]Deny service if active nodes < 3
	//[   ]bloom filter

	//TODO - in memory file/node indexes
	//[   ] move to disk
	//node -> [chunk01, chunk02] (if a node goes down, we know we need to duplicate these chunks
	//nodeToChunksIndex

	//[   ] move to disk
	//chunk -> [node1, node2, node3] (if we need to duplicate chunks, we know where to get them
	//fileToChunkToNodesIndex moves to files in ls directory, add lines to file for every chunk (chunk, node, node, node

	//[   ] deleting chunks from nodeToChunks index

}

type validationResult struct {
	fileExists bool
	nodeList []string
}

type locationResult struct {
	fileExists bool
	//                chunk   nodes //
	chunkLocation map[string][]string
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
