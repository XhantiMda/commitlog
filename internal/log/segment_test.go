package log

import (
	"github.com/stretchr/testify/require"
	api "github.com/xhantimda/commitlog/api/v1"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {

	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	wantRec := &api.Record{Value: []byte("hello world")}
	wantOff := uint64(16)

	conf := Config{}
	conf.Segment.MaxStoreBytes = 1024
	conf.Segment.MaxIndexBytes = entWidth * 3

	seg, err := newSegment(dir, wantOff, conf)
	require.NoError(t, err)

	// the segment's next offset should be equal to the base offset because segment is still empty
	require.Equal(t, wantOff, seg.nextOffset, seg.nextOffset)

	// neither index nor store should've reached max capacity after appending the message above
	require.False(t, seg.IsMaxed())

	// keep appending and reading from the segment without reaching max capacity
	for i := uint64(0); i < 3; i++ {
		off, err := seg.Append(wantRec)
		require.NoError(t, err)
		require.Equal(t, wantOff+i, off)

		got, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, wantRec.Value, got.Value)
	}

	// append record to reach max capacity
	_, err = seg.Append(wantRec)
	require.Equal(t, io.EOF, err)
	// index should be maxed
	require.True(t, seg.IsMaxed())

	// set the max store bytes to the number bytes already stored
	conf.Segment.MaxStoreBytes = uint64(len(wantRec.Value) * 3)
	conf.Segment.MaxIndexBytes = 1024

	// creating a new segment should just change config of the existing segment but the underlying store and index files should remain
	seg, err = newSegment(dir, wantOff, conf)
	require.NoError(t, err)

	// store should be maxed
	require.True(t, seg.IsMaxed())

	// should clear all store and index files
	err = seg.Remove()
	require.NoError(t, err)

	// create newSegment should create new store and index files
	seg, err = newSegment(dir, wantOff, conf)
	require.NoError(t, err)
	require.False(t, seg.IsMaxed())

}
