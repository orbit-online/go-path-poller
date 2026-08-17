package path_poller

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type PathNotifier struct {
	poller     *PathPoller
	nextFuncId atomic.Uint64
	mu         sync.Mutex
	funcs      map[string]map[uint64]func()
	channels   map[string][](chan struct{})
	started    bool
}

func NewPathNotifier() (*PathNotifier, error) {
	poller, err := NewPathPoller()
	if err != nil {
		return nil, fmt.Errorf("failed to create pathpoller: %w", err)
	}
	return &PathNotifier{
		poller:     poller,
		nextFuncId: atomic.Uint64{},
		mu:         sync.Mutex{},
		funcs:      map[string](map[uint64]func()){},
		channels:   map[string]([](chan struct{})){},
		started:    false,
	}, nil
}

func (n *PathNotifier) SetInterval(interval time.Duration) {
	n.poller.SetInterval(interval)
}

func (n *PathNotifier) AddFunc(path string, fn func()) (funcId uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	funcId = n.nextFuncId.Add(1)
	if _, ok := n.funcs[path]; !ok {
		err := n.poller.Add(path)
		if err != nil {
			return 0, err
		}
		n.funcs[path] = map[uint64]func(){}
	}
	n.funcs[path][funcId] = fn
	return funcId, err
}

func (n *PathNotifier) RemoveFunc(path string, funcId uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if funcs, ok := n.funcs[path]; ok {
		if _, ok := n.funcs[path][funcId]; ok {
			delete(funcs, funcId)
			if len(funcs) == 0 {
				delete(n.funcs, path)
				if _, ok := n.channels[path]; !ok {
					n.poller.Remove(path)
				}
			}
		}
	}
}

func (n *PathNotifier) AddChannel(path string) (chan struct{}, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	channel := make(chan struct{}, 1)
	if _, ok := n.channels[path]; ok {
		n.channels[path] = append(n.channels[path], channel)
	} else {
		err := n.poller.Add(path)
		if err != nil {
			return nil, err
		}
		n.channels[path] = [](chan struct{}){channel}
	}
	return channel, nil
}

func (n *PathNotifier) RemoveChannel(path string, channel chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if channels, ok := n.channels[path]; ok {
		if i := slices.Index(channels, channel); i >= 0 {
			close(n.channels[path][i])
			n.channels[path] = slices.Delete(n.channels[path], i, i+1)
			if len(n.channels[path]) == 0 {
				delete(n.channels, path)
				if _, ok := n.funcs[path]; !ok {
					n.poller.Remove(path)
				}
			}
		}
	}
}

func (n *PathNotifier) closeAll() {
	for _, channels := range n.channels {
		for _, channel := range channels {
			close(channel)
		}
	}
}

func (n *PathNotifier) Run(ctx context.Context) error {
	if n.started {
		return fmt.Errorf("Unable to run, PathNotifier was already started once")
	}
	n.started = true
	runErr := make(chan error)
	go func() {
		runErr <- n.poller.Run(ctx)
		defer close(runErr)
	}()
	defer n.closeAll()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-runErr:
			return err
		case event, ok := <-n.poller.Events:
			if !ok {
				return fmt.Errorf("Watcher was closed")
			}
			slog.Debug("File changed", "path", event.Name)
			if funcs, ok := n.funcs[event.Name]; ok {
				for _, fn := range funcs {
					go fn()
				}
			}
			if channels, ok := n.channels[event.Name]; ok {
				for _, channel := range channels {
					select {
					case channel <- struct{}{}:
					default:
					}
				}
			}
		case err, ok := <-n.poller.Errors:
			if !ok {
				return fmt.Errorf("Watcher was closed")
			}
			slog.Warn("Error while watching for file changes", "err", err)
		}
	}
}
