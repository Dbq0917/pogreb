package fs

import (
	"bytes"
	"os"
	"testing"
)

func TestOSMMapFS(t *testing.T) {
	testFS(t, Sub(OSMMap, t.TempDir()))
}

func TestOSMMapGrowth(t *testing.T) {
	fsys := Sub(OSMMap, t.TempDir())
	f, err := fsys.OpenFile("test", os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	value := []byte("growing mapping")
	off := int64(initialMmapSize) + 1024
	if _, err := f.WriteAt(value, off); err != nil {
		t.Fatal(err)
	}
	got, err := f.Slice(off, off+int64(len(value)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("expected %q, got %q", value, got)
	}
}
