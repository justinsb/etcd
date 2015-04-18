package streams

import (
	"os"
	"log"
	"hash/crc32"
	"encoding/binary"
	"fmt"
)

var crc32c_table = crc32.MakeTable(crc32.Castagnoli)

const EntryHeaderLength = 8

type entryHeader struct {
	payloadSize uint32
	checksum    uint32
}

func (s*entryHeader) ReadAt(fd *os.File, offset int64) error {
	var header  [8]byte
	_, err := fd.ReadAt(header[0:8], offset)
	if err != nil {
		return err
	}

	s.checksum = binary.LittleEndian.Uint32(header[0:4])
	//log.Print("crc ", crc)
	s.payloadSize = binary.LittleEndian.Uint32(header[4:8])
	//log.Print("length ", length)

	return nil
}

func (s*entryHeader) WriteAt(fd *os.File, offset int64) error {
	var header  [8]byte
	binary.LittleEndian.PutUint32(header[0:4], s.checksum)
	binary.LittleEndian.PutUint32(header[4:8], s.payloadSize)

	_, err := fd.WriteAt(header[0:8], offset)
	if err != nil {
		return err
	}

	return nil
}

func (s*entryHeader) Init(value []byte) {
	s.checksum = crc32.Checksum(value, crc32c_table)
	s.payloadSize = uint32(len(value))
}

type AppendStream struct {
	streamKey string

	nextOffset int64
	fd   *os.File
	clone      bool
}

func NewAppendStream(basedir, streamKey string) (*AppendStream, error) {
	log.Print("NewAppendStream ", streamKey)

	filePath := basedir + "/" + streamKey
	fd, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Print("Error opening file ", streamKey, err)
		return nil, err
	}
	s := new(AppendStream)
	s.streamKey = streamKey
	s.fd = fd
	return s, nil
}

func (s *AppendStream) GetTail() (int64) {
	tail := s.nextOffset
	return tail
}

func (s *AppendStream) Read(pos int64) ([]byte, error) {
	var header entryHeader
	err := header.ReadAt(s.fd, pos)
	if err != nil {
		return nil, fmt.Errorf("Not a valid offset")
	}

	// TODO: Sanity check length
	value := make([]byte, header.payloadSize)
	_, err = s.fd.ReadAt(value, pos+EntryHeaderLength)
	if err != nil {
		return nil, fmt.Errorf("Not a valid offset")
	}

	actualCrc := crc32.Checksum(value, crc32c_table)

	if header.checksum != actualCrc {
		return nil, fmt.Errorf("Not a valid offset")
	}

	return value, nil
}

func (s *AppendStream) Append(value []byte) (int64, error) {
	var err error

	pos := s.nextOffset

	log.Print("Append ", s.streamKey, "@", pos)

	var header entryHeader
	header.Init(value)

	err = header.WriteAt(s.fd, pos)
	if err != nil {
		return 0, err
	}

	_, err = s.fd.WriteAt(value, pos+EntryHeaderLength)
	if err != nil {
		return 0, err
	}

	s.nextOffset += int64(EntryHeaderLength + len(value))

	return pos, nil
}

func (s *AppendStream) Clone() (*AppendStream) {
	log.Print("Clone of ", s.streamKey)

	clone := &AppendStream{}
	*clone = *s
	clone.clone = true
	return clone
}


func (s *AppendStream) SaveNoCopy() ([]byte, error) {
	log.Print("SaveNoCopy of ", s.streamKey)

	n := s.nextOffset

	value := make([]byte, n)

	_, err := s.fd.ReadAt(value, 0)
	if err != nil {
		return nil, err
	}

	return value, nil
}


func (s *AppendStream) Recovery(state []byte) error {
	log.Print("Recovery of ", s.streamKey)

	n, err := s.fd.WriteAt(state, 0)
	if err != nil {
		return err
	}
	s.nextOffset = int64(n)
	return nil
}
