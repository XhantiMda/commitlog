package log

import (
	"fmt"
	api "github.com/xhantimda/commitlog/api/v1"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// NewLog creates and configures a Log instance.
func NewLog(dir string, conf Config) (*Log, error) {
	if conf.Segment.MaxStoreBytes == 0 {
		conf.Segment.MaxIndexBytes = 1024
	}
	if conf.Segment.MaxIndexBytes == 0 {
		conf.Segment.MaxIndexBytes = 1024
	}
	log := &Log{
		Dir:    dir,
		Config: conf,
	}
	return log, log.setup()
}

// setup ensures that the Log is set up with the segments that
// already exist on disk or, if the log is new and has no existing segments,
// bootstraps the initial segment
func (log *Log) setup() error {
	files, err := ioutil.ReadDir(log.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}

	if log.segments == nil {
		if err = log.newSegment(log.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment creates a new segment, appends that segment to the log’s
// slice of segments, and makes the new segment the active segment so that
// subsequent append calls write to it.
func (log *Log) newSegment(offset uint64) error {

	seg, err := newSegment(log.Dir, offset, log.Config)
	if err != nil {
		return err
	}

	log.segments = append(log.segments, seg)
	log.activeSegment = seg

	return nil
}

// Append appends a record to the log, if the segment has reached max capacity
// then creates a new segment and sets it as the new active segment.
func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mutex.Lock()
	defer log.mutex.Unlock()

	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if log.activeSegment.IsMaxed() {
		err = log.newSegment(off + 1)
	}

	return off, err
}

// Read reads the record stored at the given offset
func (log *Log) Read(offset uint64) (*api.Record, error) {
	log.mutex.RLock()
	defer log.mutex.RUnlock()

	var seg *segment
	for _, segment := range log.segments {
		// find segment that contains the offset based on the segments baseOffset
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			seg = segment
			break
		}
	}
	if seg == nil || seg.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	return seg.Read(offset)
}

// Close iterates over the segments and closes them
func (log *Log) Close() error {
	log.mutex.Lock()
	defer log.mutex.Unlock()

	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes the log and then removes its data.
func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Dir)
}

// Reset removes the log and then creates a new log to replace it.
func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

// LowestOffset returns the lowest offset in the log.
func (log *Log) LowestOffset() (uint64, error) {
	log.mutex.Lock()
	defer log.mutex.Unlock()

	return log.segments[0].baseOffset, nil
}

// HighestOffset returns the highest offset in the log.
func (log *Log) HighestOffset() (uint64, error) {
	log.mutex.Lock()
	defer log.mutex.Unlock()

	off := log.segments[len(log.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes all segments whose highest offset is lower than lowest.
func (log *Log) Truncate(lowest uint64) error {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	var segments []*segment
	for _, s := range log.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	log.segments = segments
	return nil
}

// Reader returns an io.Reader to read the whole log
func (log *Log) Reader() io.Reader {
	log.mutex.RLock()
	defer log.mutex.RUnlock()

	readers := make([]io.Reader, len(log.segments))
	for i, segment := range log.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

type originReader struct {
	*store
	off int64
}

type Log struct {
	mutex         sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}
