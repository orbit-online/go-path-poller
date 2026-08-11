package path_poller

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/fsnotify/fsnotify"
)

type PathPoller struct {
	interval        time.Duration
	intervalChanged chan struct{}
	fullWatchList   []string
	watchCreate     []string
	watcher         *fsnotify.Watcher
	Events          chan fsnotify.Event
	Errors          chan error
}

func NewPathPoller() (*PathPoller, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create filesystem watcher: %w", err)
	}
	poller := PathPoller{
		interval:    10 * time.Second,
		watchCreate: []string{},
		watcher:     watcher,
		Events:      make(chan fsnotify.Event),
		Errors:      make(chan error),
	}
	return &poller, nil
}

func (p *PathPoller) SetInterval(interval time.Duration) {
	p.interval = interval
	p.intervalChanged <- struct{}{}
}

func (p *PathPoller) Add(path string) error {
	if i := slices.Index(p.fullWatchList, path); i >= 0 {
		return nil
	}
	if err := p.watcher.Add(path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("pathpoller: fsnotify cannot watch the file, but it exists: %w", err)
		}
		p.watchCreate = append(p.watchCreate, path)
	}
	p.fullWatchList = append(p.fullWatchList, path)
	return nil
}

func (p *PathPoller) Remove(path string) error {
	if i := slices.Index(p.fullWatchList, path); i >= 0 {
		p.fullWatchList = slices.Delete(p.fullWatchList, i, i+1)
	} else {
		return fsnotify.ErrNonExistentWatch
	}
	if i := slices.Index(p.watchCreate, path); i >= 0 {
		p.watchCreate = slices.Delete(p.watchCreate, i, i+1)
		return nil
	} else {
		return p.watcher.Remove(path)
	}
}

func (p *PathPoller) Run(ctx context.Context) error {
	defer p.watcher.Close()
	defer close(p.intervalChanged)
	defer close(p.Events)
	defer close(p.Errors)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.intervalChanged:
		case <-time.After(p.interval):
			// Run through the watchCreate list and send a CREATE event for any path that exists
			created := []string{}
			for _, path := range p.watchCreate {
				if _, err := os.Stat(path); err == nil {
					created = append(created, path)
				}
			}
			for _, path := range created {
				if i := slices.Index(p.watchCreate, path); i >= 0 {
					// Remove path from watchCreate list
					p.watchCreate = slices.Delete(p.watchCreate, i, i+1)
				} else {
					// shouldn't happen
					p.Errors <- fmt.Errorf("pathpoller: internal error, a path disappeared from the watchCreate list")
				}
				if err := p.watcher.Add(path); err != nil {
					// Add file to fsnotify watch list
					p.Errors <- fmt.Errorf("pathpoller: fsnotify cannot watch the file, but it exists: %w", err)
				}
				// Send the event
				p.Events <- fsnotify.Event{
					Op:   fsnotify.Create,
					Name: path,
				}
			}
		case event, ok := <-p.watcher.Events:
			if !ok {
				return fmt.Errorf("fsnotify watcher was closed")
			}
			// Rather than check what kind of event we're dealing with we just check if the file exists
			// and either remove or add it to the watchCreate list
			if _, err := os.Stat(event.Name); err == nil {
				if i := slices.Index(p.watchCreate, event.Name); i >= 0 {
					// File exists and was in the watchCreate list, remove it.
					// This can happen if the parent folder is watched in addition to the particular file
					p.watchCreate = slices.Delete(p.watchCreate, i, i+1)
				}
			} else {
				if i := slices.Index(p.fullWatchList, event.Name); i >= 0 {
					if i := slices.Index(p.watchCreate, event.Name); i < 0 {
						// File does not exist, is explicitly watched, and is not yet in the watchCreate list, add it
						p.watchCreate = append(p.watchCreate, event.Name)
					}
				}
			}
			// forward the event
			p.Events <- event
		case err, ok := <-p.watcher.Errors:
			if !ok {
				return fmt.Errorf("fsnotify watcher was closed")
			} else {
				// forward the error
				p.Errors <- err
			}
		}
	}
}
