package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"file_transfer/messages"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"net"
	"os"
	"strconv"
)

func HandleArgs(args []string) (string, int, error){
	chunkSize, err := strconv.Atoi(args[4])
	return args[3], chunkSize, err
}

func SendMetadata(metadata *messages.Metadata, conn net.Conn) error {
	serialized, err := proto.Marshal(metadata)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	conn.Write(prefix)
	conn.Write(serialized)

	return nil
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

func main() {

	file, chunkSize, err := HandleArgs(os.Args)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	conn, err := net.Dial("tcp", os.Args[1] + ":" + os.Args[2])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()

	f, _ := os.Open(file)
	defer f.Close()

	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	numChunks := int32(fileSize)/int32(chunkSize)
	checkSum := messages.GetCheckSum(*f)

	var metadata *messages.Metadata
	metadata = &messages.Metadata{FileName: file, NumChunks: numChunks, ChunkSize: int32(chunkSize), CheckSum: checkSum}
	SendMetadata(metadata, conn)

	f, _ = os.Open(file)

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

