package hls

import (
	"bytes"
	"context"
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
	"github.com/gin-gonic/gin"
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
}

type muxerInstance struct {
	variant         conf.HLSVariant
	segmentCount    int
	segmentDuration conf.StringDuration
	partDuration    conf.StringDuration
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
	wg                  sync.WaitGroup
	errChan             chan error
}

// Log implements logger.Writer
func (mi *muxerInstance) Log(level logger.Level, format string, args ...interface{}) {
	mi.parent.Log(level, format, args...)
}

func (mi *muxerInstance) errorChan() chan error {
	return mi.errChan
}

func (mi *muxerInstance) initialize() error {
	mi.ctx, mi.ctxCancel = context.WithCancel(context.Background())
	mi.errChan = make(chan error, 1)

	var muxerDirectory string
	if mi.directory != "" {
		muxerDirectory = filepath.Join(mi.directory, mi.pathName)
		err := os.MkdirAll(muxerDirectory, 0o755)
		if err != nil {
			return err
		}
		mi.primaryPlaylistPath = filepath.Join(muxerDirectory, "index.m3u8")
		mi.streamPlaylistPath = filepath.Join(muxerDirectory, "main_stream.m3u8")
	}

	mi.hmuxer = &gohlslib.Muxer{
		Variant:            gohlslib.MuxerVariant(mi.variant),
		SegmentCount:       mi.segmentCount,
		SegmentMinDuration: time.Duration(mi.segmentDuration),
		PartMinDuration:    time.Duration(mi.partDuration),
		SegmentMaxSize:     uint64(mi.segmentMaxSize),
		Directory:          muxerDirectory,
		OnEncodeError: func(err error) {
			mi.Log(logger.Warn, err.Error())
		},
	}

	err := hls.FromStream(mi.stream, mi, mi.hmuxer)
	if err != nil {
		return err
	}

	err = mi.hmuxer.Start()
	if err != nil {
		mi.stream.RemoveReader(mi)
		return err
	}

	mi.Log(logger.Info, "is converting into HLS, %s",
		defs.FormatsInfo(mi.stream.ReaderFormats(mi)))

	mi.stream.StartReader(mi)

	// Start playlist monitoring if directory is set
	if mi.directory != "" {
		mi.wg.Add(1)
		go mi.monitorPlaylists()
	}

	return nil
}

func (mi *muxerInstance) writePlaylistFile(path string, content []byte) error {
	// Write to temporary file
	tempPath := path + ".tmp"
	err := os.WriteFile(tempPath, content, 0644)
	if err != nil {
		return err
	}

	return os.Rename(tempPath, path)
}

func (mi *muxerInstance) getPlaylist(request string) ([]byte, error) {
	resp := &playlistResponse{
		header: make(http.Header),
	}

	req, err := http.NewRequest(http.MethodGet, request, nil)
	if err != nil {
		return nil, err
	}

	mi.hmuxer.Handle(resp, req)
	return resp.body.Bytes(), nil
}

func (mi *muxerInstance) monitorPlaylists() {
	defer mi.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastPrimaryContent []byte
	var lastStreamContent []byte

	for {
		select {
		case <-ticker.C:
			mi.playlistMutex.Lock()

			// Update primary playlist
			primaryContent, err := mi.getPlaylist("/index.m3u8")
			if err == nil && !bytes.Equal(primaryContent, lastPrimaryContent) {
				lastPrimaryContent = primaryContent
				err = mi.writePlaylistFile(mi.primaryPlaylistPath, primaryContent)
				if err != nil {
					mi.Log(logger.Warn, "error writing primary playlist: %v", err)
				}
			}

			// Update stream playlist
			streamContent, err := mi.getPlaylist("/main_stream.m3u8")
			if err == nil && !bytes.Equal(streamContent, lastStreamContent) {
				lastStreamContent = streamContent
				err = mi.writePlaylistFile(mi.streamPlaylistPath, streamContent)
				if err != nil {
					mi.Log(logger.Warn, "error writing stream playlist: %v", err)
				}
			}

			mi.playlistMutex.Unlock()

		case <-mi.ctx.Done():
			return
		}
	}
}

func (mi *muxerInstance) close() {
	if mi.ctxCancel != nil {
		mi.ctxCancel()
	}

	mi.wg.Wait()

	mi.stream.RemoveReader(mi)
	mi.hmuxer.Close()

	if mi.hmuxer.Directory != "" {
		if mi.primaryPlaylistPath != "" {
			os.Remove(mi.primaryPlaylistPath)
		}
		if mi.streamPlaylistPath != "" {
			os.Remove(mi.streamPlaylistPath)
		}
		os.Remove(mi.hmuxer.Directory)
	}

	close(mi.errChan)
}

func (mi *muxerInstance) handleRequest(ctx *gin.Context) {
	w := &responseWriterWithCounter{
		ResponseWriter: ctx.Writer,
		bytesSent:      mi.bytesSent,
	}

	mi.hmuxer.Handle(w, ctx.Request)
}
