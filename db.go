package pogreb

import (
	"bytes"
	"context"
	"math"
	"os"
	"sync"
	"time"

	"github.com/akrylysov/pogreb/fs"
	"github.com/akrylysov/pogreb/internal/errors"
	"github.com/akrylysov/pogreb/internal/hash"
)

const (
	// MaxKeyLength is the maximum size of a key in bytes.
	MaxKeyLength = math.MaxUint16

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = 512 << 20 // 512 MiB

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint32

	metaExt    = ".pmt"
	dbMetaName = "db" + metaExt
)

// DB represents the key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu             sync.RWMutex // Allows multiple database readers or a single writer.
	opts           *Options
	index          *index
	datalog        *datalog
	lock           fs.LockFile // Prevents opening multiple instances of the same database.
	hashSeed       uint32
	metrics        *Metrics
	syncWrites     bool
	syncMu         sync.Mutex
	syncCond       sync.Cond // Signaled when a group sync completes.
	writeSeq       uint64    // Number of writes that must be committed to disk before returning.
	syncedSeq      uint64    // Writes up to syncedSeq are committed to disk.
	syncErr        error     // Error of the sync that covered syncedSeq.
	syncing        bool      // A goroutine is performing a sync.
	cancelBgWorker context.CancelFunc
	closeWg        sync.WaitGroup
	maintenanceMu  sync.Mutex // Ensures there only one maintenance task running at a time.
}

type dbMeta struct {
	HashSeed uint32
}

// Open opens or creates a new DB.
// The DB must be closed after use, by calling Close method.
func Open(path string, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults(path)

	if err := opts.rootFS.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// Try to acquire a file lock.
	lock, acquiredExistingLock, err := createLockFile(opts)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, errors.Wrap(err, "creating lock file")
	}

	if acquiredExistingLock {
		// Lock file already existed, but the process managed to acquire it.
		// It means the database wasn't closed properly.
		// Start recovery process.
		if err := backupNonsegmentFiles(opts.FileSystem); err != nil {
			return nil, err
		}
	}

	index, err := openIndex(opts)
	if err != nil {
		return nil, errors.Wrap(err, "opening index")
	}

	datalog, err := openDatalog(opts)
	if err != nil {
		return nil, errors.Wrap(err, "opening datalog")
	}

	db := &DB{
		opts:       opts,
		index:      index,
		datalog:    datalog,
		lock:       lock,
		metrics:    &Metrics{},
		syncWrites: opts.BackgroundSyncInterval == -1,
	}
	db.syncCond.L = &db.syncMu
	if index.count() == 0 {
		// The index is empty, make a new hash seed.
		seed, err := hash.RandSeed()
		if err != nil {
			return nil, err
		}
		db.hashSeed = seed
	} else {
		if err := db.readMeta(); err != nil {
			return nil, errors.Wrap(err, "reading db meta")
		}
	}

	if acquiredExistingLock {
		if err := db.recover(); err != nil {
			return nil, errors.Wrap(err, "recovering")
		}
	}

	if db.opts.BackgroundSyncInterval > 0 || db.opts.BackgroundCompactionInterval > 0 {
		db.startBackgroundWorker()
	}

	return db, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (db *DB) writeMeta() error {
	m := dbMeta{
		HashSeed: db.hashSeed,
	}
	return writeGobFile(db.opts.FileSystem, dbMetaName, m)
}

func (db *DB) readMeta() error {
	m := dbMeta{}
	if err := readGobFile(db.opts.FileSystem, dbMetaName, &m); err != nil {
		return err
	}
	db.hashSeed = m.HashSeed
	return nil
}

func (db *DB) hash(data []byte) uint32 {
	return hash.Sum32WithSeed(data, db.hashSeed)
}

// newNullableTicker is a wrapper around time.NewTicker that allows creating a nil ticker.
// A nil ticker never ticks.
func newNullableTicker(d time.Duration) (<-chan time.Time, func()) {
	if d > 0 {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	return nil, func() {}
}

func (db *DB) startBackgroundWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	db.cancelBgWorker = cancel
	db.closeWg.Add(1)

	go func() {
		defer db.closeWg.Done()

		syncC, syncStop := newNullableTicker(db.opts.BackgroundSyncInterval)
		defer syncStop()

		compactC, compactStop := newNullableTicker(db.opts.BackgroundCompactionInterval)
		defer compactStop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-syncC:
				if err := db.Sync(); err != nil {
					logger.Printf("error synchronizing database: %v", err)
				}
			case <-compactC:
				if cr, err := db.Compact(); err != nil {
					logger.Printf("error compacting database: %v", err)
				} else if cr.CompactedSegments > 0 {
					logger.Printf("compacted database: %+v", cr)
				}
			}
		}
	}()
}

// Get returns the value for the given key stored in the DB or nil if the key doesn't exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	h := db.hash(key)
	db.metrics.Gets.Add(1)
	db.mu.RLock()
	defer db.mu.RUnlock()
	var retValue []byte
	err := db.index.get(h, func(sl slot) (bool, error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, value, err := db.datalog.readKeyValue(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			retValue = cloneBytes(value)
			return true, nil
		}
		db.metrics.HashCollisions.Add(1)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return retValue, nil
}

// GetAppend returns the value for the given key (appended into buffer) stored in the DB or nil if the key doesn't exist
func (db *DB) GetAppend(key, buf []byte) ([]byte, error) {
	h := db.hash(key)
	db.metrics.Gets.Add(1)
	db.mu.RLock()
	defer db.mu.RUnlock()
	var retValue []byte
	err := db.index.get(h, func(sl slot) (bool, error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, value, err := db.datalog.readKeyValue(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			retValue = append(buf, value...)
			return true, nil
		}
		db.metrics.HashCollisions.Add(1)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return retValue, nil
}

// Has returns true if the DB contains the given key.
func (db *DB) Has(key []byte) (bool, error) {
	h := db.hash(key)
	db.metrics.Gets.Add(1)
	found := false
	db.mu.RLock()
	defer db.mu.RUnlock()
	err := db.index.get(h, func(sl slot) (bool, error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			found = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func (db *DB) put(sl slot, key []byte) error {
	return db.index.put(sl, func(cursl slot) (bool, error) {
		if uint16(len(key)) != cursl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(cursl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			db.datalog.trackDel(cursl) // Overwriting existing key.
			return true, nil
		}
		return false, nil
	})
}

// Put sets the value for the given key. It updates the value for the existing key.
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) > MaxKeyLength {
		return errKeyTooLarge
	}
	if len(value) > MaxValueLength {
		return errValueTooLarge
	}
	h := db.hash(key)
	db.metrics.Puts.Add(1)
	db.mu.Lock()

	segID, offset, err := db.datalog.put(key, value)
	if err == nil {
		sl := slot{
			hash:      h,
			segmentID: segID,
			keySize:   uint16(len(key)),
			valueSize: uint32(len(value)),
			offset:    offset,
		}
		err = db.put(sl, key)
	}
	var seq uint64
	if err == nil && db.syncWrites {
		seq = db.nextWriteSeq()
	}
	db.mu.Unlock()
	if err != nil {
		return err
	}
	if db.syncWrites {
		// Wait for the write to become durable.
		// Concurrent writes are committed by a single group sync.
		return db.waitSync(seq)
	}
	return nil
}

func (db *DB) del(h uint32, key []byte, writeWAL bool) error {
	err := db.index.delete(h, func(sl slot) (b bool, e error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			db.datalog.trackDel(sl)
			var err error
			if writeWAL {
				err = db.datalog.del(key)
			}
			return true, err
		}
		return false, nil
	})
	return err
}

// Delete deletes the given key from the DB.
func (db *DB) Delete(key []byte) error {
	h := db.hash(key)
	db.metrics.Dels.Add(1)
	db.mu.Lock()
	err := db.del(h, key, true)
	var seq uint64
	if err == nil && db.syncWrites {
		seq = db.nextWriteSeq()
	}
	db.mu.Unlock()
	if err != nil {
		return err
	}
	if db.syncWrites {
		return db.waitSync(seq)
	}
	return nil
}

// Close closes the DB.
func (db *DB) Close() error {
	if db.cancelBgWorker != nil {
		db.cancelBgWorker()
	}
	db.closeWg.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.writeMeta(); err != nil {
		return err
	}
	if err := db.datalog.close(); err != nil {
		return err
	}
	if err := db.index.close(); err != nil {
		return err
	}
	if err := db.lock.Unlock(); err != nil {
		return err
	}
	return nil
}

// nextWriteSeq assigns a sequence number to a write that needs to be committed to disk.
// It must be called with db.mu held.
func (db *DB) nextWriteSeq() uint64 {
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	db.writeSeq++
	return db.writeSeq
}

// waitSync blocks until all writes up to and including seq are committed to disk.
// Concurrent writes are grouped together and committed by a single file system sync.
func (db *DB) waitSync(seq uint64) error {
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	for db.syncedSeq < seq {
		if db.syncing {
			db.syncCond.Wait()
			continue
		}
		// No sync is in progress - perform the sync for the whole group.
		db.syncing = true
		target := db.writeSeq
		db.syncMu.Unlock()
		err := db.sync()
		db.syncMu.Lock()
		db.syncedSeq = target
		db.syncErr = err
		db.syncing = false
		db.syncCond.Broadcast()
	}
	return db.syncErr
}

func (db *DB) sync() error {
	return db.datalog.sync()
}

// Items returns a new ItemIterator.
func (db *DB) Items() *ItemIterator {
	return &ItemIterator{db: db}
}

// Sync commits the contents of the database to the backing FileSystem.
func (db *DB) Sync() error {
	db.syncMu.Lock()
	db.writeSeq++
	seq := db.writeSeq
	db.syncMu.Unlock()
	return db.waitSync(seq)
}

// Count returns the number of keys in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.count()
}

// Metrics returns the DB metrics.
func (db *DB) Metrics() *Metrics {
	return db.metrics
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	var size int64
	files, err := db.opts.FileSystem.ReadDir(".")
	if err != nil {
		return 0, err
	}
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			return 0, err
		}
		size += info.Size()
	}
	return size, nil
}
