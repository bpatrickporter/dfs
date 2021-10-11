package main

import (
	"bufio"
	"bytes"
	"dfs/messages"
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"net"
	"os"
	"strconv"
)

func HandleArgs() (string, string, string, int, error) {
	host := os.Args[1]
	port := os.Args[2]
	file := os.Args[3]
	chunkSize, err := strconv.Atoi(os.Args[4])
	return host, port, file, chunkSize, err
}

func ReceiveAck(conn net.Conn) {
	for {
		prefix := make([]byte, 8)
		if b, _ := conn.Read(prefix); b == 0 {
			break
		}

		payloadSize := binary.LittleEndian.Uint64(prefix)
		payload := make([]byte, payloadSize)
		conn.Read(payload)

		ack := &messages.Ack{}
		err := proto.Unmarshal(payload, ack)
		if err != nil {
			fmt.Println(err.Error())
		}

		fmt.Printf("Ack status: " + strconv.FormatBool(ack.CheckSumMatched) + "\n")
		return
	}
}

func InitialLogger() {
	file, err := os.OpenFile("logs/client_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Client start up complete")
}

func GetFileMetadata(fileName string) (int, int32, string) {
	f, _ := os.Open(fileName)
	defer f.Close()
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	numChunks := int32(fileSize)/int32(64)
	checkSum := messages.GetCheckSum(*f)
	return int(fileSize), numChunks, checkSum
}

func main() {

	host, port, file, chunkSize, err := HandleArgs()
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	InitialLogger()
	conn, err := net.Dial("tcp", host + ":" + port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()
	msgHandler := messages.NewMessageHandler(conn)
	_, chunks, checkSum := GetFileMetadata(file)
	metadata := &messages.Metadata{FileName: file, NumChunks: chunks, ChunkSize: int32(chunkSize), CheckSum: checkSum}
	msg := messages.PutRequest{Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	msgHandler.Send(wrapper)

	f, _ := os.Open(file)
	buffer := make([]byte, chunkSize)
	reader := bytes.NewReader(buffer)
	writer := bufio.NewWriter(conn)
	for {
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

	fmt.Println("Waiting for ack..")
	ReceiveAck(conn)
}

