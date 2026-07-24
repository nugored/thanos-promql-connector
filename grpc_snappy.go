package main

import (
	"io"
	"sync"

	"github.com/klauspost/compress/snappy"
	"google.golang.org/grpc/encoding"
)

const grpcSnappyName = "snappy"

func init() {
	encoding.RegisterCompressor(newGRPCSnappyCompressor())
}

type grpcSnappyCompressor struct {
	writersPool sync.Pool
	readersPool sync.Pool
}

func newGRPCSnappyCompressor() *grpcSnappyCompressor {
	c := &grpcSnappyCompressor{}
	c.readersPool = sync.Pool{
		New: func() any {
			return snappy.NewReader(nil)
		},
	}
	c.writersPool = sync.Pool{
		New: func() any {
			return snappy.NewBufferedWriter(nil)
		},
	}
	return c
}

func (c *grpcSnappyCompressor) Name() string {
	return grpcSnappyName
}

func (c *grpcSnappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	wr := c.writersPool.Get().(*snappy.Writer)
	wr.Reset(w)
	return &grpcSnappyWriteCloser{
		writer: wr,
		pool:   &c.writersPool,
	}, nil
}

func (c *grpcSnappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	rd := c.readersPool.Get().(*snappy.Reader)
	rd.Reset(r)
	return &grpcSnappyReader{
		reader: rd,
		pool:   &c.readersPool,
	}, nil
}

type grpcSnappyWriteCloser struct {
	writer *snappy.Writer
	pool   *sync.Pool
	closed bool
}

func (w *grpcSnappyWriteCloser) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.writer.Write(p)
}

func (w *grpcSnappyWriteCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	var err error
	if w.writer != nil {
		err = w.writer.Close()
		w.writer.Reset(nil)
		w.pool.Put(w.writer)
		w.writer = nil
	}
	return err
}

type grpcSnappyReader struct {
	reader *snappy.Reader
	pool   *sync.Pool
	closed bool
}

func (r *grpcSnappyReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	n, err := r.reader.Read(p)
	if err == io.EOF {
		r.close()
	}
	return n, err
}

func (r *grpcSnappyReader) ReadByte() (byte, error) {
	if r.closed {
		return 0, io.EOF
	}
	b, err := r.reader.ReadByte()
	if err == io.EOF {
		r.close()
	}
	return b, err
}

func (r *grpcSnappyReader) close() {
	if r.closed {
		return
	}
	r.closed = true
	r.reader.Reset(nil)
	r.pool.Put(r.reader)
	r.reader = nil
}
