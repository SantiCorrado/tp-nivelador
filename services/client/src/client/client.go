package client

import (
	"bufio"
	"encoding/csv"
	"log"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512
const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

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

func (client *Client) Run() error {
	const mainAction = "enviando registros de agencia"
	defer client.conn.Close()
	defer client.input.Close()
	scanner := bufio.NewScanner(client.input)

	i := 0

	initialMessage := client.config.AgencyId + "\n"
	if err := safe_socket.SendAll(client.conn, []byte(initialMessage)); err != nil {
		logger.Error("send-message", logger.Fail, "Error enviando mensaje inicial", "agency-id", client.config.AgencyId, "output-file", client.config.OutputFile)
		return err
	}

	for scanner.Scan() {
		line := scanner.Text()

		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", i}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		if err := safe_socket.SendAll(client.conn, []byte(line+"\n")); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}
		i++
	}
	if err := scanner.Err(); err != nil {
		bufiocheck := []any{"cliente: ", client.config.AgencyId, "error leyendo archivo de entrada"}
		logger.Error("read-input", logger.Fail, bufiocheck...)
		return err
	}

	if err := safe_socket.SendAll(client.conn, []byte("EOF\n")); err != nil {
		logger.Error("send-message", logger.Fail, "Error enviando mensaje final", "agency-id", client.config.AgencyId, "output-file", client.config.OutputFile)
		return err
	}

	final_message, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
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
