package hls

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bluenviron/gohlslib/v2"
	"github.com/bluenviron/mediamtx/internal/conf"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/hls"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
)

const (
	maxRetryAttempts    = 3
	retryBaseDelay      = 100 * time.Millisecond
	watcherEventBuffer  = 100
	playlistUpdateLimit = time.Second / 2 // Limit updates to max 2 per second
)

var (
	ErrPlaylistUpdate = errors.New("playlist update failed")
	ErrWatcherSetup   = errors.New("file watcher setup failed")
)

type playlistResponse struct {
	header http.Header
	body   bytes.Buffer
}

func (r *playlistResponse) Header() http.Header {
	return r.header
}

func (r *playlistResponse) Write(b []byte) (int, error) {
	return r.body.Write(b)
}

func (r *playlistResponse) WriteHeader(statusCode int) {
	// No-op
}

type muxerInstance struct {
	variant         conf.HLSVariant
	segmentCount    int
	segmentDuration conf.Duration
	partDuration    conf.Duration
	segmentMaxSize  conf.StringSize
	directory       string
	pathName        string
	stream          *stream.Stream
	bytesSent       *uint64
	parent          *muxer

	ctx                 context.Context
	ctxCancel           func()
	hmuxer              *gohlslib.Muxer
	primaryPlaylistPath string
	streamPlaylistPath  string
	playlistMutex       sync.Mutex
	handleMutex         sync.Mutex
	errChan             chan error

	watcher        *fsnotify.Watcher
	watchDone      chan struct{}
	wg             sync.WaitGroup
	lastUpdateTime time.Time
}

func (mi *muxerInstance) Log(level logger.Level, format string, args ...interface{}) {
	mi.parent.Log(level, format, args...)
}

func (mi *muxerInstance) errorChan() chan error {
	return mi.errChan
}

func (mi *muxerInstance) initialize() error {
	mi.ctx, mi.ctxCancel = context.WithCancel(context.Background())
	mi.errChan = make(chan error, 10) // Buffer increased to handle more errors
	mi.watchDone = make(chan struct{})
	mi.lastUpdateTime = time.Now()

	var muxerDirectory string
	if mi.directory != "" {
		muxerDirectory = filepath.Join(mi.directory, mi.pathName)
		if err := mi.setupDirectory(muxerDirectory); err != nil {
			return err
		}
		mi.primaryPlaylistPath = filepath.Join(muxerDirectory, "index.m3u8")
		mi.streamPlaylistPath = filepath.Join(muxerDirectory, "main_stream.m3u8")
	}

	if err := mi.setupMuxer(muxerDirectory); err != nil {
		return err
	}

	if mi.directory != "" {
		if err := mi.setupWatcher(muxerDirectory); err != nil {
			return fmt.Errorf("%w: %v", ErrWatcherSetup, err)
		}

		// Start watching in a separate goroutine
		mi.wg.Add(1)
		go mi.watchSegments()
	}

	return nil
}

func (mi *muxerInstance) setupDirectory(dir string) error {
	// Ensure directory exists
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Cleanup any temporary files from previous crashes
	pattern := filepath.Join(dir, "*.tmp")
	tmpFiles, err := filepath.Glob(pattern)
	if err == nil {
		for _, f := range tmpFiles {
			os.Remove(f)
		}
	}

	return nil
}

func (mi *muxerInstance) setupMuxer(dir string) error {
	mi.hmuxer = &gohlslib.Muxer{
		Variant:            gohlslib.MuxerVariant(mi.variant),
		SegmentCount:       mi.segmentCount,
		SegmentMinDuration: time.Duration(mi.segmentDuration),
		PartMinDuration:    time.Duration(mi.partDuration),
		SegmentMaxSize:     uint64(mi.segmentMaxSize),
		Directory:          dir,
		OnEncodeError: func(err error) {
			mi.Log(logger.Warn, err.Error())
		},
	}

	if err := hls.FromStream(mi.stream, mi, mi.hmuxer); err != nil {
		return fmt.Errorf("failed to setup stream: %w", err)
	}

	if err := mi.hmuxer.Start(); err != nil {
		mi.stream.RemoveReader(mi)
		return fmt.Errorf("failed to start muxer: %w", err)
	}

	mi.Log(logger.Info, "is converting into HLS, %s",
		defs.FormatsInfo(mi.stream.ReaderFormats(mi)))

	mi.stream.StartReader(mi)
	return nil
}

func (mi *muxerInstance) setupWatcher(dir string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	mi.watcher = watcher

	if err := mi.watcher.Add(dir); err != nil {
		mi.watcher.Close()
		return err
	}

	return nil
}

func (mi *muxerInstance) watchSegments() {
	defer mi.wg.Done()
	defer close(mi.watchDone)

	for {
		select {
		case event, ok := <-mi.watcher.Events:
			if !ok {
				return
			}

			// Only process .ts file creation events
			if event.Op&fsnotify.Create == fsnotify.Create && filepath.Ext(event.Name) == ".ts" {
				// Rate limit updates
				if time.Since(mi.lastUpdateTime) < playlistUpdateLimit {
					continue
				}

				if err := mi.updatePlaylistsWithRetry(); err != nil {
					mi.Log(logger.Warn, "failed to update playlists after retries: %v", err)
				}
				mi.lastUpdateTime = time.Now()
			}

		case err, ok := <-mi.watcher.Errors:
			if !ok {
				return
			}
			mi.Log(logger.Warn, "file watcher error: %v", err)
			// Try to recreate watcher on critical errors
			if err := mi.recreateWatcher(); err != nil {
				mi.Log(logger.Error, "failed to recreate watcher: %v", err)
			}

		case <-mi.ctx.Done():
			return
		}
	}
}

func (mi *muxerInstance) recreateWatcher() error {
	if mi.watcher != nil {
		mi.watcher.Close()
	}

	return mi.setupWatcher(mi.hmuxer.Directory)
}

func (mi *muxerInstance) writePlaylistFile(path string, content []byte) error {
	tempPath := path + ".tmp"

	// Write to temporary file
	if err := os.WriteFile(tempPath, content, 0o644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath) // Clean up on error
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

func (mi *muxerInstance) getPlaylist(request string) ([]byte, error) {
	resp := &playlistResponse{
		header: make(http.Header),
	}

	req, err := http.NewRequest(http.MethodGet, request, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	mi.hmuxer.Handle(resp, req)
	return resp.body.Bytes(), nil
}

func (mi *muxerInstance) updatePlaylistsWithRetry() error {
	var lastErr error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if err := mi.updatePlaylists(); err != nil {
			lastErr = err
			// Exponential backoff
			delay := retryBaseDelay * time.Duration(1<<attempt)
			select {
			case <-time.After(delay):
				continue
			case <-mi.ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", lastErr)
			}
		}
		return nil
	}
	return fmt.Errorf("%w: %v", ErrPlaylistUpdate, lastErr)
}

func (mi *muxerInstance) updatePlaylists() error {
	mi.playlistMutex.Lock()
	defer mi.playlistMutex.Unlock()

	// Update primary playlist
	primaryContent, err := mi.getPlaylist("/index.m3u8")
	if err != nil {
		return fmt.Errorf("failed to get primary playlist: %w", err)
	}

	if err := mi.writePlaylistFile(mi.primaryPlaylistPath, primaryContent); err != nil {
		return fmt.Errorf("failed to write primary playlist: %w", err)
	}

	// Update stream playlist
	streamContent, err := mi.getPlaylist("/main_stream.m3u8")
	if err != nil {
		return fmt.Errorf("failed to get stream playlist: %w", err)
	}

	if err := mi.writePlaylistFile(mi.streamPlaylistPath, streamContent); err != nil {
		return fmt.Errorf("failed to write stream playlist: %w", err)
	}

	return nil
}

func (mi *muxerInstance) close() {
	if mi.ctxCancel != nil {
		mi.ctxCancel()
	}

	// Wait for watch goroutine to finish
	if mi.watchDone != nil {
		<-mi.watchDone
	}

	if mi.watcher != nil {
		mi.watcher.Close()
	}

	mi.wg.Wait()

	mi.stream.RemoveReader(mi)
	mi.hmuxer.Close()

	// Cleanup files
	if mi.hmuxer.Directory != "" {
		os.Remove(mi.primaryPlaylistPath)
		os.Remove(mi.streamPlaylistPath)

		// Cleanup any remaining temporary files
		pattern := filepath.Join(mi.hmuxer.Directory, "*.tmp")
		if tmpFiles, err := filepath.Glob(pattern); err == nil {
			for _, f := range tmpFiles {
				os.Remove(f)
			}
		}

		os.Remove(mi.hmuxer.Directory)
	}

	close(mi.errChan)
}

func (mi *muxerInstance) handleRequest(ctx *gin.Context) {
	mi.handleMutex.Lock()
	defer mi.handleMutex.Unlock()

	w := &responseWriterWithCounter{
		ResponseWriter: ctx.Writer,
		bytesSent:      mi.bytesSent,
	}

	mi.hmuxer.Handle(w, ctx.Request)
}
