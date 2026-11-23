package pogreb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akrylysov/pogreb/fs"
	"github.com/akrylysov/pogreb/internal/assert"
)

// syncCountingFS counts file syncs and simulates disk sync latency.
type syncCountingFS struct {
	fs.FileSystem
	syncs int64
	delay time.Duration
}

func (sfs *syncCountingFS) OpenFile(name string, flag int, perm os.FileMode) (fs.File, error) {
	f, err := sfs.FileSystem.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &syncCountingFile{File: f, sfs: sfs}, nil
}

func (sfs *syncCountingFS) count() int64 {
	return atomic.LoadInt64(&sfs.syncs)
}

type syncCountingFile struct {
	fs.File
	sfs *syncCountingFS
}

func (f *syncCountingFile) Sync() error {
	atomic.AddInt64(&f.sfs.syncs, 1)
	time.Sleep(f.sfs.delay)
	return f.File.Sync()
}

func TestGroupCommit(t *testing.T) {
	sfs := &syncCountingFS{FileSystem: testFS, delay: 500 * time.Microsecond}
	opts := &Options{
		BackgroundSyncInterval: -1, // Sync every write.
		FileSystem:             sfs,
	}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	const (
		n        = 100000
		nworkers = 32
	)
	start := time.Now()
	errs := make([]error, nworkers)
	var wg sync.WaitGroup
	for w := 0; w < nworkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := make([]byte, 4)
			for i := w; i < n; i += nworkers {
				binary.LittleEndian.PutUint32(key, uint32(i))
				if err := db.Put(key, key); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, err := range errs {
		assert.Nil(t, err)
	}

	syncs := sfs.count()
	t.Logf("wrote %d records in %v with %d syncs (%.1f records per sync)", n, elapsed, syncs, float64(n)/float64(syncs))
	if syncs >= n/8 {
		t.Fatalf("expected writes to be committed in groups, got %d syncs for %d writes", syncs, n)
	}
	assert.Equal(t, uint32(n), db.Count())
	assert.Nil(t, db.Close())

	// Simulate a crash.
	// All acknowledged writes must survive, no partially written records may be read back.
	assert.Nil(t, touchFile(testFS, filepath.Join(testDBName, lockName)))
	assert.Nil(t, testFS.Remove(filepath.Join(testDBName, segmentMetaName(0, 1))))
	assert.Nil(t, testFS.Remove(filepath.Join(testDBName, indexMetaName)))

	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Equal(t, uint32(n), db.Count())
	key := make([]byte, 4)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint32(key, uint32(i))
		v, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, key, v)
	}
	assert.Nil(t, db.Close())
}
