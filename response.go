package drpc

import (
	"io"

	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/server"
)

var _ server.Response = &rpcResponse{}

type rpcResponse struct {
	rw     io.ReadWriter
	header metadata.Metadata
	codec  codec.Codec
}

func (r *rpcResponse) Codec() codec.Codec {
	return r.codec
}

func (r *rpcResponse) WriteHeader(hdr metadata.Metadata) {
	for k, v := range hdr {
		r.header[k] = v
	}
}

func (r *rpcResponse) Write(b []byte) error {
	return r.codec.Write(r.rw, &codec.Message{
		Header: r.header,
		Body:   b,
	}, nil)
}
