package snappy_test

import (
	"bytes"
	"io"
	"testing"

	_ "main.go/pkg/snappy"
	"google.golang.org/grpc/encoding"
)

func TestSnappyGRPCCompressorRegistered(t *testing.T) {
	if encoding.GetCompressor("snappy") == nil {
		t.Fatal("snappy gRPC compressor is not registered")
	}
}

func TestSnappyGRPCCompressorReadRoundTrip(t *testing.T) {
	compressor := encoding.GetCompressor("snappy")
	if compressor == nil {
		t.Fatal("snappy gRPC compressor is not registered")
	}

	payload := bytes.Repeat([]byte("promql-connector grpc request payload "), 256)
	var compressed bytes.Buffer
	writer, err := compressor.Compress(&compressed)
	if err != nil {
		t.Fatalf("Compress() returned error: %v", err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("Write() returned error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	reader, err := compressor.Decompress(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("Decompress() returned error: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() returned error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decompressed payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	n, err := reader.Read(make([]byte, 1))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read() after EOF = %d, %v; want 0, EOF", n, err)
	}
}