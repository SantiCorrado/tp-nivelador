package client

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const CLIENT_BUFFER_MAX_SIZE = 1024
const SIZE_LENGTH = 4
const AGENCY_ID_LENGTH = 4
const TYPE_LENGTH = 1

const TYPE_AGENCY_ID = byte(1)
const TYPE_BET = byte(2)
const TYPE_END = byte(3)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
	input  *os.File
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	inputFile, err := openFile(config.InputFile)
	if err != nil {
		logger.Warn("open-file", logger.Fail)
		return nil, err
	}
	client := &Client{conn: conn, config: config, input: inputFile}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func openFile(filePath string) (*os.File, error) {
	const action = "open-file"
	file, err := os.Open(filePath)
	if err != nil {
		logger.Error(action, logger.Fail, "file-path", filePath)
		return nil, err
	}

	logger.Info(action, logger.Success, "file-path", filePath)
	return file, nil
}

func encodeID(id string) ([]byte, error) {
	encoded := make([]byte, AGENCY_ID_LENGTH)
	e_id, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(encoded, (uint32)(e_id))
	return encoded, nil
}

func sendMessage(client *Client, messagetype byte, message []byte) error {
	header := make([]byte, TYPE_LENGTH+SIZE_LENGTH)
	header[0] = messagetype
	binary.BigEndian.PutUint32(header[1:], (uint32)(len(message)))
	if err := safe_socket.SendAll(client.conn, header); err != nil {
		return err
	}
	if err := safe_socket.SendAll(client.conn, message); err != nil {
		return err
	}
	return nil
}

func sendgreetings(client *Client) error {
	id, err := encodeID(client.config.AgencyId)
	if err != nil {
		logger.Error("encode-id", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}
	return sendMessage(client, TYPE_AGENCY_ID, id)
}

func (client *Client) Run() error {
	const mainAction = "enviando registros de agencia"
	defer client.conn.Close()
	defer client.input.Close()
	scanner := bufio.NewScanner(client.input)

	i := 0
	// Enviar mensaje inicial
	if err := sendgreetings(client); err != nil {
		return err
	}
	buffer := []byte{}
	for scanner.Scan() {
		bline := []byte(scanner.Text() + "\n")
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", i}
		logger.Info(mainAction, logger.InProgress, messageArgs...)
		if len(buffer)+len(bline) > CLIENT_BUFFER_MAX_SIZE {
			//enviar registros acumulados
			if err := sendMessage(client, TYPE_BET, buffer); err != nil {
				logger.Error("send-message", logger.Fail, messageArgs...)
				return err
			}
			buffer = []byte{}
		}
		buffer = append(buffer, bline...)
		i++
	}
	if len(buffer) > 0 {
		//enviar ultimos registros acumulados
		if err := sendMessage(client, TYPE_BET, buffer); err != nil {
			logger.Error("send-message", logger.Fail, "Error enviando mensaje final", "agency-id", client.config.AgencyId, "output-file", client.config.OutputFile)
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		bufiocheck := []any{"cliente: ", client.config.AgencyId, "error leyendo archivo de entrada"}
		logger.Error("read-input", logger.Fail, bufiocheck...)
		return err
	}

	//Fin archivo, enviar mensaje de fin
	if err := sendMessage(client, TYPE_END, nil); err != nil {
		return err
	}

	final_message, err := safe_socket.RecvAll(client.conn, CLIENT_BUFFER_MAX_SIZE)
	if err != nil {
		logger.Error("recv-response", logger.Fail)
		return err
	}
	file, err := os.Create(client.config.OutputFile)
	if err != nil {
		log.Fatalf("Error al crear el archivo: %s", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{string(final_message)})

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
