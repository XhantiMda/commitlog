package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	fileEncoding = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mutex        sync.Mutex
	memoryBuffer *bufio.Writer
	size         uint64
}

func newStore(file *os.File) (*store, error) {

	// get the file information
	fileInfo, err := os.Stat(file.Name())

	if err != nil {
		return nil, err
	}

	//get file size from the file info
	size := uint64(fileInfo.Size())

	//create a new store with a new Writer
	return &store{
		File:         file,
		size:         size,
		memoryBuffer: bufio.NewWriter(file),
	}, nil
}

func (store *store) Append(bytes []byte) (totalBytes uint64, position uint64, error error) {

	//obtain a lock on the store before performing any actions on it
	store.mutex.Lock()

	//release lock when done appending to the file
	defer store.mutex.Unlock()

	//the position of the bytes to be appended is equal to the current size of the store
	position = store.size

	//write the length of the bytes to store
	err := binary.Write(store.memoryBuffer, fileEncoding, uint64(len(bytes)))

	if err != nil {
		return 0, 0, err
	}

	//write the bytes to the store's buffered writer
	numberOfBytesWritten, err := store.memoryBuffer.Write(bytes)

	if err != nil {
		return 0, 0, err
	}

	numberOfBytesWritten += lenWidth

	//the store size grows by the number of bytes recently appended
	store.size += uint64(numberOfBytesWritten)

	return uint64(numberOfBytesWritten), position, nil
}

func (store *store) Read(position uint64) ([]byte, error) {

	//obtain a lock on the store before performing any actions
	store.mutex.Lock()

	//release the lock when done reading from the store
	defer store.mutex.Unlock()

	//ensure that the store has flushed all records to disk before we can read
	err := store.memoryBuffer.Flush()

	if err != nil {
		return nil, err
	}

	defaultSizeByteArray := make([]byte, lenWidth)

	if _, err := store.File.ReadAt(defaultSizeByteArray, int64(position)); err != nil {
		return nil, err
	}

	readBytesSize := fileEncoding.Uint64(defaultSizeByteArray)

	readBytes := make([]byte, readBytesSize)

	if _, err := store.File.ReadAt(readBytes, int64(position+lenWidth)); err != nil {
		return nil, err
	}

	return readBytes, nil
}

func (store *store) ReadAt(bytes []byte, offset int64) (int, error) {

	store.mutex.Lock()

	defer store.mutex.Unlock()

	err := store.memoryBuffer.Flush()

	if err != nil {
		return 0, err
	}

	return store.File.ReadAt(bytes, offset)
}

func (store *store) Close() error {

	store.mutex.Lock()

	defer store.mutex.Unlock()

	err := store.memoryBuffer.Flush()

	if err != nil {
		return err
	}

	return store.File.Close()
}
