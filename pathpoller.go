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
	ctx             context.Context
	Close           func()
	interval        time.Duration
	intervalChanged chan struct{}
	fullWatchList   []string
	watchCreate     []string
	watcher         *fsnotify.Watcher
	Events          chan fsnotify.Event
	Errors          chan error
}

func NewPathPoller(ctx context.Context) (*PathPoller, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create filesystem watcher: %w", err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	poller := PathPoller{
		ctx:         cancelCtx,
		Close:       cancel,
		interval:    10 * time.Second,
		watchCreate: []string{},
		watcher:     watcher,
		Events:      make(chan fsnotify.Event),
		Errors:      make(chan error),
	}
	go poller.run()
	return &poller, nil
}

func (d *PathPoller) SetInterval(interval time.Duration) {
	d.interval = interval
	d.intervalChanged <- struct{}{}
}

func (d *PathPoller) Add(path string) error {
	if i := slices.Index(d.fullWatchList, path); i >= 0 {
		return nil
	}
	if err := d.watcher.Add(path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("pathpoller: fsnotify cannot watch the file, but it exists: %w", err)
		}
		d.watchCreate = append(d.watchCreate, path)
	}
	d.fullWatchList = append(d.fullWatchList, path)
	return nil
}

func (d *PathPoller) Remove(path string) error {
	if i := slices.Index(d.fullWatchList, path); i >= 0 {
		d.fullWatchList = slices.Delete(d.fullWatchList, i, i+1)
	} else {
		return fsnotify.ErrNonExistentWatch
	}
	if i := slices.Index(d.watchCreate, path); i >= 0 {
		d.watchCreate = slices.Delete(d.watchCreate, i, i+1)
		return nil
	} else {
		return d.watcher.Remove(path)
	}
}

func (d *PathPoller) run() {
	defer d.watcher.Close()
	defer close(d.intervalChanged)
	defer close(d.Events)
	defer close(d.Errors)
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-d.intervalChanged:
		case <-time.After(d.interval):
			// Run through the watchCreate list and send a CREATE event for any path that exists
			created := []string{}
			for _, path := range d.watchCreate {
				if _, err := os.Stat(path); err == nil {
					created = append(created, path)
				}
			}
			for _, path := range created {
				if i := slices.Index(d.watchCreate, path); i >= 0 {
					// Remove path from watchCreate list
					d.watchCreate = slices.Delete(d.watchCreate, i, i+1)
				} else {
					// shouldn't happen
					d.Errors <- fmt.Errorf("pathpoller: internal error, a path disappeared from the watchCreate list")
				}
				if err := d.watcher.Add(path); err != nil {
					// Add file to fsnotify watch list
					d.Errors <- fmt.Errorf("pathpoller: fsnotify cannot watch the file, but it exists: %w", err)
				}
				// Send the event
				d.Events <- fsnotify.Event{
					Op:   fsnotify.Create,
					Name: path,
				}
			}
		case event, ok := <-d.watcher.Events:
			if !ok {
				d.Errors <- fmt.Errorf("fsnotify watcher was closed")
				return
			}
			// Rather than check what kind of event we're dealing with we just check if the file exists
			// and either remove or add it to the watchCreate list
			if _, err := os.Stat(event.Name); err == nil {
				if i := slices.Index(d.watchCreate, event.Name); i >= 0 {
					// File exists and was in the watchCreate list, remove it.
					// This can happen if the parent folder is watched in addition to the particular file
					d.watchCreate = slices.Delete(d.watchCreate, i, i+1)
				}
			} else {
				if i := slices.Index(d.fullWatchList, event.Name); i >= 0 {
					if i := slices.Index(d.watchCreate, event.Name); i < 0 {
						// File does not exist, is explicitly watched, and is not yet in the watchCreate list, add it
						d.watchCreate = append(d.watchCreate, event.Name)
					}
				}
			}
			// forward the event
			d.Events <- event
		case err, ok := <-d.watcher.Errors:
			if !ok {
				d.Errors <- fmt.Errorf("fsnotify watcher was closed")
				return
			} else {
				// forward the error
				d.Errors <- err
			}
		}
	}
}
