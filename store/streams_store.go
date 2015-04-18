package store

import (
	"log"
	"github.com/coreos/etcd/store/streams"
	"sync"
	"strconv"
	"strings"
	"fmt"
)

const PREFIX string = "/2/"

type StreamsStore interface {
	StreamAppend(nodePath string, value []byte) (*Event, error)
	StreamGet(nodePath string) (*Event, error)
}

type streamsStore struct {
	basedir string
	streams map[string]*streams.AppendStream
	mutex   sync.Mutex
}

func (s *streamsStore) init(basedir string) {
	s.streams = make(map[string]*streams.AppendStream)
	s.basedir = basedir
}

func (s *streamsStore) StreamAppend(nodePath string, value []byte) (*Event, error) {
	stream, err := s.getStream(nodePath)
	if err != nil {
		return nil, err
	}
	pos, err := stream.Append(value)
	if err != nil {
		return nil, err
	}

	entryPath := nodePath + "/" + strconv.FormatInt(pos, 16)
	node := &NodeExtern{
		Key:           entryPath,
		ModifiedIndex: 0,
		CreatedIndex:  0,
	}

	return &Event{
		Action: Create,
		Node:   node,
	}, nil
}


func (s *streamsStore) StreamGet(nodePath string) (*Event, error) {
	lastSlash := strings.LastIndex(nodePath, "/")
	if lastSlash == -1 {
		return nil, fmt.Errorf("Invalid node path")
	}
	streamPath := nodePath[:lastSlash]
	streamOffset := nodePath[lastSlash+1:]

	stream, err := s.getStream(streamPath)
	if err != nil {
		return nil, err
	}

	if streamOffset == "info" {
		tail := stream.GetTail()

		infoString := strconv.FormatInt(tail, 16)

		nodePath := streamPath + "info"
		node := &NodeExtern{
			Key:           nodePath,
			Value: &infoString,
			ModifiedIndex: 0,
			CreatedIndex:  0,
		}

		return &Event{
			Action: Get,
			Node:   node,
		}, nil
	}

	pos, err := strconv.ParseInt(streamOffset, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("Not a valid offset")
	}

	value, err := stream.Read(pos)
	if err != nil {
		return nil, err
	}

	stringValue := string(value)

	node := &NodeExtern{
		Key:           nodePath,
		Value: &stringValue,
		ModifiedIndex: 0,
		CreatedIndex:  0,
	}

	return &Event{
		Action: Get,
		Node:   node,
	}, nil
}



func (s *streamsStore) getStream(key string) (*streams.AppendStream, error) {
	log.Print("getStream", key)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	stream, found := s.streams[key]
	if !found {
		if !strings.HasPrefix(key, PREFIX) {
			return nil, fmt.Errorf("Invalid prefix for stream")
		}
		streamId := key[len(PREFIX):]

		var err error
		stream, err = streams.NewAppendStream(s.basedir, streamId)
		if err != nil {
			log.Print("Error getting stream", err)
			return nil, err
		}
		s.streams[key] = stream
	}
	return stream, nil
}
