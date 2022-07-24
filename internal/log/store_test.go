package log

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var (
	write = []byte("hello commit log")
	width = uint64(len(write)) + lenWidth
)

//In this test, we create a store with a temporary file and call two test helpers
//to test appending and reading from the store.
//Then we create the store again to test is it recovers its state after restarting.
func TestStoreAppendRead(t *testing.T) {

	file, err := ioutil.TempFile("", "store_append_read_test")

	require.NoError(t, err)

	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, store)
	testRead(t, store)
	testRead(t, store)

	store, err = newStore(file)
	require.NoError(t, err)
	testRead(t, store)
}

//
func testAppend(t *testing.T, store *store) {

	t.Helper()

	for i := uint64(1); i < 4; i++ {

		totalBytes, position, err := store.Append(write)
		require.NoError(t, err)
		require.Equal(t, position+totalBytes, width*i)

	}
}

func testRead(t *testing.T, store *store) {

	t.Helper()

	var readPosition uint64

	for i := uint64(1); i < 4; i++ {

		read, err := store.Read(readPosition)
		require.NoError(t, err)
		require.Equal(t, write, read)
		readPosition += width

	}
}

func testReadAt(t *testing.T, store *store) {

	t.Helper()

	for i, offset := uint64(1), int64(0); i < 4; i++ {

		bytes := make([]byte, lenWidth)
		bytesRead, err := store.ReadAt(bytes, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, bytesRead)
		offset += int64(bytesRead)
		size := fileEncoding.Uint64(bytes)

		bytes = make([]byte, size)
		bytesRead, err = store.ReadAt(bytes, offset)
		require.NoError(t, err)
		require.Equal(t, write, bytes)
		require.Equal(t, int(size), bytesRead)
		offset += int64(bytesRead)
	}
}

func TestStoreClose(t *testing.T) {

	file, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)

	defer os.Remove(file.Name())

	store, err := newStore(file)
	require.NoError(t, err)

	_, _, err = store.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(file.Name())
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	require.True(t, afterSize > beforeSize)
}

func openFile(name string) (file *os.File, size int64, err error) {

	file, err = os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)

	if err != nil {
		return nil, 0, err
	}

	fileInfo, err := file.Stat()

	if err != nil {
		return nil, 0, err
	}

	return file, fileInfo.Size(), nil
}
