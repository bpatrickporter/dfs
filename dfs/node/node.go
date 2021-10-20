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
	"time"
)

func HandleArgs() (string, string, string, string) {
	listeningPort := os.Args[1]
	rootDir := os.Args[2]
	controllerName := os.Args[3]
	controllerPort := os.Args[4]
	return listeningPort, rootDir, controllerName, controllerPort
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

	if host, isOrion := IsHostOrion(); isOrion {
		file, err = os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/" + host + "_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	} else {
		file, err = os.OpenFile("logs/node_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Node start up complete")
}

func RegisterWithController(context context) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}
	var conn net.Conn
	for {
		if conn, err = net.Dial("tcp", context.controllerName + ":" + context.controllerPort); err != nil {
			log.Println(err.Error())
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
	log.Println("Connected to controller: " + context.controllerName + ":" + context.controllerPort)
	registration := &messages.Registration{Node: hostname, Port: context.listeningPort}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_RegistrationMessage{RegistrationMessage: registration},
	}
	messageHandler := messages.NewMessageHandler(conn)
	messageHandler.Send(wrapper)
	log.Println("Node registered with controller")
	messageHandler.Close()
	return err
}

func SendHeartBeats(context context) {
	for {
		time.Sleep(5 * time.Second)
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalln()
		}
		conn, err := net.Dial("tcp", context.controllerName + ":" + context.controllerPort)
		if err != nil {
			log.Fatalln(err.Error())
		}
		log.Println("Connected to controller: " + context.controllerName + ":" + context.controllerPort)
		heartBeat := &messages.Heartbeat{Node: hostname + ":" + context.listeningPort}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_HeartbeatMessage{HeartbeatMessage: heartBeat},
		}
		messageHandler := messages.NewMessageHandler(conn)
		messageHandler.Send(wrapper)
		log.Println("Heart beat sent to controller")
		messageHandler.Close()
	}
}

func UnpackMetadata(metadata *messages.Metadata) (string, int, int, int, string) {
	fileName := metadata.GetFileName()
	fileSize := int(metadata.GetFileSize())
	numChunks := int(metadata.GetNumChunks())
	chunkSize := int(metadata.GetChunkSize())
	checkSum := metadata.GetCheckSum()
	return fileName, fileSize, numChunks, chunkSize, checkSum
}

func UnpackChunkMetadata(metadata *messages.ChunkMetadata) (string, int, string) {
	chunkName := metadata.ChunkName
	chunkSize := metadata.ChunkSize
	checkSum := metadata.ChunkCheckSum
	return chunkName, int(chunkSize), checkSum
}

func PackageMetadata(context context, chunkName string) (*messages.Metadata, *messages.ChunkMetadata){
	contents, err := os.ReadFile(context.rootDir + "meta_" + chunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	string := string(contents)
	slices := strings.Split(string, ",")
	fileName := slices[0]
	fileSize, _ := strconv.Atoi(slices[1])
	numChunks, _ := strconv.Atoi(slices[2])
	standardChunkSize, _ := strconv.Atoi(slices[3])
	actualChunkSize, _ := strconv.Atoi(slices[4])
	checkSum := slices[5]
	chunkCheckSum := slices[6]
	metadata := &messages.Metadata{FileName: fileName, FileSize: int32(fileSize), NumChunks: int32(numChunks), ChunkSize: int32(standardChunkSize), CheckSum: checkSum}
	chunkMetadata := &messages.ChunkMetadata{ChunkName: chunkName, ChunkSize: int32(actualChunkSize), ChunkCheckSum: chunkCheckSum}

	return metadata, chunkMetadata
}

func WriteMetadataFile(metadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, context context) error {
	fileName, fileSize, numChunks, standardChunkSize, checkSum := UnpackMetadata(metadata)
	chunkName, actualChunkSize, chunkCheckSum := UnpackChunkMetadata(chunkMetadata)

	file, err := os.Create(context.rootDir + "meta_"+ chunkName)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
		return err
	}

	metadataBytes := []byte(fileName + "," + strconv.Itoa(fileSize) + "," + strconv.Itoa(numChunks) + "," + strconv.Itoa(int(standardChunkSize)) + "," + strconv.Itoa(int(actualChunkSize)) + "," + checkSum + "," + chunkCheckSum)


	w := bufio.NewWriter(file)
	w.Write(metadataBytes)
	w.Flush()
	return err
}

func WriteChunk(fileMetadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, messageHandler *messages.MessageHandler, context context) error {

	conn := messageHandler.GetConn()
	chunkName, _, _ := UnpackChunkMetadata(chunkMetadata)
	err := WriteMetadataFile(fileMetadata, chunkMetadata, context)
	if err != nil {
		log.Fatalln(err.Error())
	}

	file, err := os.Create(context.rootDir + chunkName)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
		return err
	}
	log.Println(chunkName)

	writer := bufio.NewWriter(file)

	buffer := make([]byte, chunkMetadata.ChunkSize)
	numBytes, err := io.ReadFull(conn, buffer)
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + " read " + strconv.Itoa(numBytes) + " bytes")

	checkSum := messages.GetChunkCheckSum(buffer)
	oldCheckSum := chunkMetadata.ChunkCheckSum
	log.Println(chunkMetadata.ChunkName + " New Checksum: " + checkSum)
	log.Println(chunkMetadata.ChunkName + " Old Checksum: " + oldCheckSum)

	reader := bytes.NewReader(buffer)
	n, err := io.CopyN(writer, reader, int64(chunkMetadata.ChunkSize))
	if err != nil {
		log.Println(err.Error())
	}
	log.Println(chunkMetadata.ChunkName + "Wrote " + strconv.Itoa(int(n)) + " bytes")
	return err
}

func VerifyCheckSumsMatch(metadata *messages.ChunkMetadata, context context) bool {
	file, err := os.Open(context.rootDir + metadata.ChunkName)
	if err != nil {
		fmt.Println(err.Error())
	}
	checkSum := messages.GetCheckSum(*file)
	checkSumResults := strings.Compare(metadata.ChunkCheckSum, checkSum) == 0
	log.Println("Original Checksum: " + metadata.ChunkCheckSum)
	log.Println("New Checksum     : " + checkSum)
	log.Println("Checksums match: " + strconv.FormatBool(checkSumResults))
	return checkSumResults
}

func SendAck(results bool, messageHandler *messages.MessageHandler) error {
	ack := &messages.Ack{CheckSumMatched: results}
	response := &messages.Wrapper{
		Msg: &messages.Wrapper_AcknowledgeMessage{AcknowledgeMessage: ack},
	}
	err := messageHandler.Send(response)
	fmt.Println("Ack status: " + strconv.FormatBool(ack.CheckSumMatched))
	log.Println("Acknowledgment sent to client")
	return err
}

func DeleteChunk(chunkName string, context context) {
	log.Println("Delete chunk request received for " + chunkName)
	err := os.Remove(context.rootDir + chunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	err = os.Remove(context.rootDir + "meta_" + chunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	log.Println("File deleted")
}

func SendChunk(chunkName string, context context, messageHandler *messages.MessageHandler) {
	log.Println(chunkName)
	file, err := os.Open(context.rootDir + chunkName)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer file.Close()

	metadata, chunkMetadata := PackageMetadata(context, chunkName)
	message := &messages.GetResponseChunk{ChunkMetadata: chunkMetadata, Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetResponseChunkMessage{GetResponseChunkMessage: message},
	}
	messageHandler.Send(wrapper)
	writer := bufio.NewWriter(messageHandler.GetConn())
	bytes, err := io.Copy(writer, file)
	log.Println("Sent " + strconv.Itoa(int(bytes)) + " bytes")
}

func HandleConnection(conn net.Conn, context context) {
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			metadata := msg.PutRequestMessage.Metadata
			chunkMetadata := msg.PutRequestMessage.ChunkMetadata
			WriteChunk(metadata, chunkMetadata, messageHandler, context)
			//results := VerifyCheckSumsMatch(metadata, context)
			//VerifyCheckSumsMatch(chunkMetadata, context)
			//SendAck(results, messageHandler)
			messageHandler.Close()
			return
		case *messages.Wrapper_DeleteRequestMessage:
			chunkName := msg.DeleteRequestMessage.FileName
			DeleteChunk(chunkName, context)
			messageHandler.Close()
			return
		case *messages.Wrapper_GetRequestMessage:
			chunkName := msg.GetRequestMessage.FileName
			SendChunk(chunkName, context, messageHandler)
			messageHandler.Close()
			return
		case nil:
			log.Println("Received an empty message, terminating client")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func InitializeContext() context {
	listeningPort, rootDir, controllerName, controllerPort := HandleArgs()
	return context{rootDir: rootDir, listeningPort: listeningPort, controllerName: controllerName, controllerPort: controllerPort}
}

type context struct {
	rootDir string
	listeningPort string
	controllerName string
	controllerPort string
}

func main() {
	context := InitializeContext()
	InitializeLogger()
	err  := RegisterWithController(context)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	//go SendHeartBeats(context)

	listener, err := net.Listen("tcp", ":" + context.listeningPort)
	if err != nil {
		log.Fatalln(err.Error())
	}

	for {
		if conn, err := listener.Accept(); err == nil {
			go HandleConnection(conn, context)
		}
	}
}
