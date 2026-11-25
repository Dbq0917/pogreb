package pogreb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/akrylysov/pogreb/fs"
)

func TestBlockCompressionHighRepetitionRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pogreb-compression")
	opts := &Options{FileSystem: fs.OS}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatal(err)
	}

	const recordCount = 1000
	value := bytes.Repeat([]byte("highly-repetitive-json-log-record "), 32)
	keys := make([][]byte, recordCount)
	rawBytes := int64(0)
	for i := 0; i < recordCount; i++ {
		keys[i] = []byte(fmt.Sprintf("log-key-%06d", i))
		rawBytes += int64(encodedRecordSize(uint32(len(keys[i]) + len(value))))
		if err := db.Put(keys[i], value); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < recordCount; i += 37 {
		got, err := db.Get(keys[i])
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("read %d: err=%v match=%v", i, err, bytes.Equal(got, value))
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	segmentInfo, err := os.Stat(filepath.Join(dbPath, segmentName(0, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if segmentInfo.Size()*3 >= rawBytes {
		t.Fatalf("compression ratio too low: segment=%d raw=%d", segmentInfo.Size(), rawBytes)
	}

	db, err = Open(dbPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < recordCount; i += 53 {
		got, err := db.Get(keys[i])
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("reopen read %d: err=%v match=%v", i, err, bytes.Equal(got, value))
		}
	}
}
