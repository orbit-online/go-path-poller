package path_poller

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
)

type PathNotifier struct {
	poller     *PathPoller
	nextFuncId atomic.Uint64
	funcs      map[string]map[uint64]func()
	channels   map[string][](chan struct{})
}

func NewPathNotifier() (*PathNotifier, error) {
	poller, err := NewPathPoller()
	if err != nil {
		return nil, fmt.Errorf("failed to create pathpoller: %w", err)
	}
	return &PathNotifier{
		poller:     poller,
		nextFuncId: atomic.Uint64{},
		funcs:      map[string](map[uint64]func()){},
		channels:   map[string]([](chan struct{})){},
	}, nil
}

func (p *PathNotifier) AddFunc(path string, fn func()) (funcId uint64, err error) {
	funcId = p.nextFuncId.Add(1)
	if _, ok := p.funcs[path]; !ok {
		err := p.poller.Add(path)
		if err != nil {
			return 0, err
		}
		p.funcs[path] = map[uint64]func(){}
	}
	p.funcs[path][funcId] = fn
	return funcId, err
}

func (p *PathNotifier) RemoveFunc(path string, funcId uint64) {
	if funcs, ok := p.funcs[path]; ok {
		if _, ok := p.funcs[path][funcId]; ok {
			delete(funcs, funcId)
			if len(funcs) == 0 {
				delete(p.funcs, path)
				if _, ok := p.channels[path]; !ok {
					p.poller.Remove(path)
				}
			}
		}
	}
}

func (p *PathNotifier) AddChannel(path string) (chan struct{}, error) {
	channel := make(chan struct{})
	if _, ok := p.channels[path]; ok {
		p.channels[path] = append(p.channels[path], channel)
	} else {
		err := p.poller.Add(path)
		if err != nil {
			return nil, err
		}
		p.channels[path] = [](chan struct{}){channel}
	}
	return channel, nil
}

func (p *PathNotifier) RemoveChannel(path string, channel chan struct{}) {
	if channels, ok := p.channels[path]; ok {
		if i := slices.Index(channels, channel); i >= 0 {
			p.channels[path] = slices.Delete(p.channels[path], i, i+1)
			if len(p.channels[path]) == 0 {
				delete(p.channels, path)
				if _, ok := p.funcs[path]; !ok {
					p.poller.Remove(path)
				}
			}
		}
	}
}

func (p *PathNotifier) WatchPaths(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-p.poller.Events:
			if !ok {
				return fmt.Errorf("Watcher was closed")
			}
			slog.Debug("File changed", "path", event.Name)
			if funcs, ok := p.funcs[event.Name]; ok {
				for _, fn := range funcs {
					go fn()
				}
			}
			if channels, ok := p.channels[event.Name]; ok {
				for _, channel := range channels {
					select {
					case channel <- struct{}{}:
					default:
					}
				}
			}
		case err, ok := <-p.poller.Errors:
			if !ok {
				return fmt.Errorf("Watcher was closed")
			}
			slog.Warn("Error while watching for file changes", "err", err)
		}
	}
}
