package log

import (
	"fmt"
	api "github.com/xhantimda/commitlog/api/v1"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

func newSegment(dir string, baseOffset uint64, conf Config) (*segment, error) {
	seg := &segment{
		baseOffset: baseOffset,
		config:     conf,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if seg.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if seg.index, err = newIndex(indexFile, conf); err != nil {
		return nil, err
	}

	// if index is empty, nextOffset = baseOffset
	if off, _, err := seg.index.Read(-1); err != nil {
		seg.nextOffset = baseOffset
	} else {
		seg.nextOffset = baseOffset + uint64(off) + 1
	}
	return seg, nil
}

// Append writes the record to the segment and returns the newly appended
// record’s offset.
func (seg *segment) Append(record *api.Record) (offset uint64, err error) {

	cur := seg.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// append the data to the store
	_, pos, err := seg.store.Append(p)
	if err != nil {
		return 0, err
	}

	// index offset are relative to the baseOffset
	// subtract the segment's next offset from its baseOffset
	off := uint32(cur - uint64(seg.baseOffset))

	// add an entry to the index
	err = seg.index.Write(off, pos)
	if err != nil {
		return 0, err
	}

	seg.nextOffset++
	return cur, nil

}

// Read returns the record for the given offset
func (seg *segment) Read(off uint64) (*api.Record, error) {

	// translate the absolute index into a relative offset
	relOffset := int64(off - seg.baseOffset)
	_, pos, err := seg.index.Read(relOffset)
	if err != nil {
		return nil, err
	}
	// use the record's position to retrieve the entry from the store
	bytes, err := seg.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(bytes, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size,
// either by writing too much to the store or the index
func (seg *segment) IsMaxed() bool {
	return seg.store.size >= seg.config.Segment.MaxStoreBytes ||
		seg.index.size >= seg.config.Segment.MaxIndexBytes
}

// Remove closes the segment and removes the index and store files.
func (seg *segment) Remove() error {
	if err := seg.Close(); err != nil {
		return err
	}
	if err := os.Remove(seg.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(seg.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close ensures that the store and index files are closed.
func (seg *segment) Close() error {
	if err := seg.index.Close(); err != nil {
		return err
	}
	if err := seg.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j,
// for example nearestMultiple(9, 4) == 8.
// We take the lesser multiple to make sure we stay under the user’s disk capacity.
// simplified example: https://www.geeksforgeeks.org/multiple-of-x-closest-to-n/
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}
