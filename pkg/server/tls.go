package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type GRPCServerTLSConfig struct {
	CertFile     string
	KeyFile      string
	ClientCAFile string
}

func NewGRPCServerOptions(tlsConfig GRPCServerTLSConfig) ([]grpc.ServerOption, bool, error) {
	tlsEnabled := tlsConfig.CertFile != "" || tlsConfig.KeyFile != "" || tlsConfig.ClientCAFile != ""
	if !tlsEnabled {
		return nil, false, nil
	}
	if tlsConfig.CertFile == "" || tlsConfig.KeyFile == "" {
		return nil, false, fmt.Errorf("grpc-server-tls-cert and grpc-server-tls-key must both be set to enable gRPC TLS")
	}

	cert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
	if err != nil {
		return nil, false, fmt.Errorf("load gRPC server certificate: %w", err)
	}

	serverTLSConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
		Certificates: []tls.Certificate{cert},
	}
	if tlsConfig.ClientCAFile != "" {
		clientCAPEM, err := os.ReadFile(tlsConfig.ClientCAFile)
		if err != nil {
			return nil, false, fmt.Errorf("read gRPC client CA file: %w", err)
		}
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
			return nil, false, fmt.Errorf("parse gRPC client CA file %q: no certificates found", tlsConfig.ClientCAFile)
		}
		serverTLSConfig.ClientCAs = clientCAs
		serverTLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLSConfig))}, true, nil
}