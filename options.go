package drpc

import (
	"github.com/unistack-org/micro/v3/server"
)

type (
	maxMsgSizeKey struct{}
)

//
// MaxMsgSize set the maximum message in bytes the server can receive and
// send.  Default maximum message size is 4 MB.
//
func MaxMsgSize(s int) server.Option {
	return server.SetOption(maxMsgSizeKey{}, s)
}
