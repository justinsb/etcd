package streams

import (
	"os"
	"log"
	"hash/crc32"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
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

	fd   *os.File
	clone      bool

	offsetMutex     sync.Mutex
	offsetCondition sync.Cond
	nextOffset      int64
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
	s.offsetCondition.L = &s.offsetMutex
	return s, nil
}

func (s *AppendStream) GetTail() (int64) {
	tail := atomic.LoadInt64(&s.nextOffset)
	return tail
}

type TailOptions struct {
	Count int
}

type StreamListener interface {
	GotValue(pos int64, value []byte) error
	End(err error)
}

func (s *AppendStream) Tail(startPos int64, options TailOptions, listener StreamListener) {
	pos := startPos
	count := 0

	tail := atomic.LoadInt64(&s.nextOffset)

	for {
		if pos >= tail {
			s.offsetMutex.Lock()
			tail = s.nextOffset
			for pos >= tail {
				log.Print("Waiting ", pos, " vs ", tail)
				s.offsetCondition.Wait()
				tail = s.nextOffset
			}
			s.offsetMutex.Unlock()
		}

		var header entryHeader
		err := header.ReadAt(s.fd, pos)
		if err != nil {
			listener.End(fmt.Errorf("Not a valid offset"))
			return
		}

		// TODO: Sanity check length
		value := make([]byte, header.payloadSize)
		_, err = s.fd.ReadAt(value, pos+EntryHeaderLength)
		if err != nil {
			listener.End(fmt.Errorf("Not a valid offset"))
			return
		}

		actualCrc := crc32.Checksum(value, crc32c_table)
		if header.checksum != actualCrc {
			listener.End(fmt.Errorf("Not a valid offset"))
			return
		}

		log.Print("Sending value @", pos)

		err = listener.GotValue(pos, value)
		if err != nil {
			log.Print("GotValue returned error; stopping");
			break
		}

		count++
		pos += int64(header.payloadSize + EntryHeaderLength)

		if options.Count != 0 && count >= options.Count {
			log.Print("Hit max count: ", options.Count)
			break
		}
	}

	listener.End(nil)
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

	log.Print("Append prelock ", s.streamKey)

	s.offsetMutex.Lock()
	defer s.offsetMutex.Unlock()

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

	atomic.AddInt64(&s.nextOffset, int64(EntryHeaderLength + len(value)))
	s.offsetCondition.Broadcast()

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

	n := atomic.LoadInt64(&s.nextOffset)

	value := make([]byte, n)

	_, err := s.fd.ReadAt(value, 0)
	if err != nil {
		return nil, err
	}

	return value, nil
}


func (s *AppendStream) Recovery(state []byte) error {
	s.offsetMutex.Lock()
	defer s.offsetMutex.Unlock()

	log.Print("Recovery of ", s.streamKey)

	n, err := s.fd.WriteAt(state, 0)
	if err != nil {
		return err
	}

	atomic.StoreInt64(&s.nextOffset, int64(n))
	s.offsetCondition.Broadcast()

	return nil
}
