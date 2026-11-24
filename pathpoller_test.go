package path_poller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A file replaced by a rename, as an atomic write does, keeps its events: the
// rename takes the watched inode away with it
func TestReplacedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(path, []byte("0"), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := NewPathPoller()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Add(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	go func() {
		for err := range p.Errors {
			t.Errorf("poller error: %v", err)
		}
	}()

	for i, content := range []string{"1", "2", "3"} {
		tmp := filepath.Join(dir, ".cert.pem.tmp")
		if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
		timeout := time.After(2 * time.Second)
	wait:
		for {
			select {
			case event := <-p.Events:
				if event.Name == path {
					break wait
				}
			case <-timeout:
				t.Fatalf("no event for replacement %d", i+1)
			}
		}
		// Let the rest of this replacement's events through before the next one
		drain := time.After(200 * time.Millisecond)
	settle:
		for {
			select {
			case <-p.Events:
			case <-drain:
				break settle
			}
		}
	}
}
