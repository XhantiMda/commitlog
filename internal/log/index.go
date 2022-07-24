package log

import (
	"encoding/binary"
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	enc             = binary.BigEndian
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file      *os.File
	memoryMap gommap.MMap
	size      uint64
}

// newIndex creates an index for the given file.
// We create the index and
// save the current size of the file, so we can track the amount of data in the
// index file as we add index entries.
// We grow the file to the max index size before
// memory-mapping the file and then return the created index to the caller.
func newIndex(file *os.File, config Config) (*index, error) {

	index := &index{
		file: file,
	}

	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	index.size = uint64(fileInfo.Size())

	if err = os.Truncate(file.Name(), int64(config.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	index.memoryMap, err = gommap.Map(index.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)

	if err != nil {
		return nil, err
	}

	return index, nil
}

// Read takes in an offset and returns the associated record’s position in
// the store. The given offset is relative to the segment’s base offset; 0 is always
// the offset of the index’s first entry, 1 is the second entry, and so on.
func (index *index) Read(in int64) (out uint32, pos uint64, err error) {
	if index.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((index.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if index.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(index.memoryMap[pos : pos+offWidth])
	pos = enc.Uint64(index.memoryMap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
func (index *index) Write(off uint32, pos uint64) error {
	if uint64(len(index.memoryMap)) < index.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(index.memoryMap[index.size:index.size+offWidth], off)
	enc.PutUint64(index.memoryMap[index.size+offWidth:index.size+entWidth], pos)
	index.size += uint64(entWidth)
	return nil
}

// Name returns the index's fila path.
func (index *index) Name() string {
	return index.file.Name()
}

func (index *index) Close() error {

	if err := index.memoryMap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := index.file.Sync(); err != nil {
		return err
	}

	if err := index.file.Truncate(int64(index.size)); err != nil {
		return err
	}

	return index.file.Close()
}
