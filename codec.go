package drpc

import (
	"io"

	"go.unistack.org/micro/v3/codec"
	"storj.io/drpc"
)

type wrapMicroCodec struct{ codec.Codec }

func (w *wrapMicroCodec) Marshal(v drpc.Message) ([]byte, error) {
	if m, ok := v.(*codec.Frame); ok {
		return m.Data, nil
	}
	return w.Codec.Marshal(v.(interface{}))
}

func (w *wrapMicroCodec) Unmarshal(d []byte, v drpc.Message) error {
	if d == nil || v == nil {
		return nil
	}
	if m, ok := v.(*codec.Frame); ok {
		m.Data = d
		return nil
	}
	return w.Codec.Unmarshal(d, v.(interface{}))
}

func (w *wrapMicroCodec) ReadHeader(conn io.Reader, m *codec.Message, mt codec.MessageType) error {
	return nil
}

func (w *wrapMicroCodec) ReadBody(conn io.Reader, v interface{}) error {
	if m, ok := v.(*codec.Frame); ok {
		_, err := conn.Read(m.Data)
		return err
	}
	return codec.ErrInvalidMessage
}

func (w *wrapMicroCodec) Write(conn io.Writer, m *codec.Message, v interface{}) error {
	// if we don't have a body
	if v != nil {
		b, err := w.Marshal(v)
		if err != nil {
			return err
		}
		m.Body = b
	}
	// write the body using the framing codec
	_, err := conn.Write(m.Body)
	return err
}
