package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	for len(bytes) > 0 {
		n, err := socket.Write(bytes)
		if err != nil {
			return err
		}
		bytes = bytes[n:]
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	r := 0
	for r < size {
		n, err := socket.Read(buff[r:])
		if err != nil {
			return nil, err
		}
		r += n
	}
	return buff[:n], nil
}
