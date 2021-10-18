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

func HandleArgs() (string, string, string, string) {
	listeningPort := os.Args[1]
	rootDir := os.Args[2]
	controllerName := os.Args[3]
	controllerPort := os.Args[4]
	return listeningPort, rootDir, controllerName, controllerPort
}

func InitializeLogger() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}
	file, err := os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/" + hostname  + "_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	//file, err := os.OpenFile("logs/node_logs.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
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
	conn, err := net.Dial("tcp", context.controllerName + ":" + context.controllerPort)
	if err != nil {
		log.Fatalln(err.Error())
		return err
	}
	defer conn.Close()
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
	chunkSize, _ := strconv.Atoi(slices[3])
	checkSum := slices[4]
	chunkCheckSum := slices[5]
	metadata := &messages.Metadata{FileName: fileName, FileSize: int32(fileSize), NumChunks: int32(numChunks), ChunkSize: int32(chunkSize), CheckSum: checkSum}
	chunkMetadata := &messages.ChunkMetadata{ChunkName: chunkName, ChunkSize: int32(chunkSize), ChunkCheckSum: chunkCheckSum}
	return metadata, chunkMetadata
}

func WriteMetadataFile(metadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, context context) error {
	fileName, fileSize, numChunks, chunkSize, checkSum := UnpackMetadata(metadata)
	chunkName, chunkSize, chunkCheckSum := UnpackChunkMetadata(chunkMetadata)

	log.Println("Trying to create [" + context.rootDir + "meta_" + chunkName + "]")
	file, err := os.Create(context.rootDir + "meta_"+ chunkName)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
		return err
	}
	log.Println("Metadata file created for" + chunkName)

	metadataBytes := []byte(fileName + "," + strconv.Itoa(fileSize) + "," + strconv.Itoa(numChunks) + "," + strconv.Itoa(int(chunkSize))+ "," + checkSum + "," + chunkCheckSum)

	w := bufio.NewWriter(file)
	r := bytes.NewReader(metadataBytes)
	count, _ := r.Read(metadataBytes)
	r2 := bytes.NewReader(metadataBytes)
	bs, err:= io.CopyN(w, r2, int64(count))
	fmt.Println("wrote metadata bytes: " + strconv.Itoa(int(bs)))
	if err != nil {
		log.Fatalln(err.Error())
	}
	return err
}

func WriteChunk(metadata *messages.Metadata, chunkMetadata *messages.ChunkMetadata, messageHandler *messages.MessageHandler, context context) error {
	conn := messageHandler.GetConn()
	chunkName, chunkSize, _ := UnpackChunkMetadata(chunkMetadata)
	err := WriteMetadataFile(metadata, chunkMetadata, context)
	if err != nil {
		log.Fatalln(err.Error())
	}

	//create file for chunk
	log.Println("Trying to create [" + context.rootDir + chunkName + "]")
	file, err := os.Create(context.rootDir + chunkName)
	defer file.Close()
	if err != nil {
		fmt.Println(err.Error())
		log.Println(err.Error())
		return err
	}
	log.Println("Chunk file created for " + chunkName)

	buffer := make([]byte, chunkSize)
	writer := bufio.NewWriter(file)

	log.Println("Preparing to write chunk")
	numBytes, err := conn.Read(buffer)
	log.Println("Read from conn: " + strconv.Itoa(numBytes))
	if err != nil {
		fmt.Println(err.Error())
	}
	reader := bytes.NewReader(buffer)

	log.Println("read bytes into buffer")
	_, err = io.CopyN(writer, reader, int64(numBytes))
	log.Println("Copied buffer to writer")
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	log.Println("File chunk complete")
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

func HandleConnection(conn net.Conn, context context) {
	log.Println("Accepted a connection from " + conn.RemoteAddr().String())
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			metadata := msg.PutRequestMessage.Metadata
			chunkMetadata := msg.PutRequestMessage.ChunkMetadata
			log.Println("Put request received for")
			log.Println("-> " + metadata.String())
			WriteChunk(metadata, chunkMetadata, messageHandler, context)
			//results := VerifyCheckSumsMatch(metadata, context)
			VerifyCheckSumsMatch(chunkMetadata, context)
			//SendAck(results, messageHandler)
		case *messages.Wrapper_DeleteRequestMessage:
			fileName := msg.DeleteRequestMessage.FileName
			log.Println("Delete chunk request received for " + fileName)
			err := os.Remove(context.rootDir + fileName)
			if err != nil {
				log.Fatalln(err.Error())
			} else {
				log.Println("Chunk deleted")
			}
		case *messages.Wrapper_GetRequestMessage:
			chunkName := msg.GetRequestMessage.FileName
			log.Println("Get chunk message received for " + chunkName)
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
			log.Println("Get response chunk message sent")
			writer := bufio.NewWriter(messageHandler.GetConn())
			bytes, err := io.Copy(writer, file)
			log.Println("Sent " + strconv.Itoa(int(bytes)) + " bytes")
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
	listener, err := net.Listen("tcp", ":" + context.listeningPort)
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
