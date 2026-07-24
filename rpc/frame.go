package rpc

import (
	"encoding/binary"
	"io"
	"net"
)

func writeFrame(conn net.Conn, data []byte) error {
	length := uint32(len(data))
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], length)

	if _, err := conn.Write(header[:]); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

func readFrame(conn net.Conn) ([]byte, error) {
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
