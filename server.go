package drpc

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Meh, we need to get rid of this shit

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/unistack-org/micro/v3/server"
)

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	ArgType     reflect.Type
	ReplyType   reflect.Type
	ContextType reflect.Type
	method      reflect.Method
	stream      bool
}

// type reflectionType func(context.Context, server.Stream) error

type service struct {
	typ    reflect.Type
	method map[string]*methodType
	rcvr   reflect.Value
	name   string
}

// server represents an RPC Server.
type rServer struct {
	serviceMap map[string]*service
	mu         sync.RWMutex
	// reflection bool
}

// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// prepareEndpoint() returns a methodType for the provided method or nil
// in case if the method was unsuitable.
func prepareEndpoint(method reflect.Method) (*methodType, error) {
	mtype := method.Type
	mname := method.Name
	var replyType, argType, contextType reflect.Type
	var stream bool

	// Endpoint() must be exported.
	if method.PkgPath != "" {
		return nil, fmt.Errorf("Endpoint must be exported")
	}

	switch mtype.NumIn() {
	case 3:
		// assuming streaming
		argType = mtype.In(2)
		contextType = mtype.In(1)
		stream = true
	case 4:
		// method that takes a context
		argType = mtype.In(2)
		replyType = mtype.In(3)
		contextType = mtype.In(1)
	default:
		return nil, fmt.Errorf("method %v of %v has wrong number of ins: %v", mname, mtype, mtype.NumIn())
	}

	switch stream {
	case true:
		// check stream type
		streamType := reflect.TypeOf((*server.Stream)(nil)).Elem()
		if !argType.Implements(streamType) {
			return nil, fmt.Errorf("%v argument does not implement Streamer interface: %v", mname, argType)
		}
	default:
		// First arg need not be a pointer.
		if !isExportedOrBuiltinType(argType) {
			return nil, fmt.Errorf("%v argument type not exported: %v", mname, argType)
		}

		if replyType.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("method %v reply type not a pointer: %v", mname, replyType)
		}

		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			return nil, fmt.Errorf("method %v reply type not exported: %v", mname, replyType)
		}
	}

	// Endpoint() needs one out.
	if mtype.NumOut() != 1 {
		return nil, fmt.Errorf("method %v has wrong number of outs: %v", mname, mtype.NumOut())
	}
	// The return type of the method must be error.
	if returnType := mtype.Out(0); returnType != typeOfError {
		return nil, fmt.Errorf("method %v returns %v not error", mname, returnType.String())
	}
	return &methodType{method: method, ArgType: argType, ReplyType: replyType, ContextType: contextType, stream: stream}, nil
}

func (server *rServer) register(rcvr interface{}) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.serviceMap == nil {
		server.serviceMap = make(map[string]*service)
	}
	s := &service{}
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(s.rcvr).Type().Name()
	if sname == "" {
		return fmt.Errorf("rpc: no service name for type %v", s.typ.String())
	}
	if !isExported(sname) {
		return fmt.Errorf("rpc Register: type %s is not exported", sname)
	}
	if _, present := server.serviceMap[sname]; present {
		return fmt.Errorf("rpc: service already defined: " + sname)
	}
	s.name = sname
	s.method = make(map[string]*methodType)

	// Install the methods
	for m := 0; m < s.typ.NumMethod(); m++ {
		method := s.typ.Method(m)
		mt, err := prepareEndpoint(method)
		if mt != nil && err == nil {
			s.method[method.Name] = mt
		} else if err != nil {
			return err
		}
	}

	if len(s.method) == 0 {
		return fmt.Errorf("rpc Register: type %s has no exported methods of suitable type", sname)
	}
	server.serviceMap[s.name] = s
	return nil
}

func (m *methodType) prepareContext(ctx context.Context) reflect.Value {
	if contextv := reflect.ValueOf(ctx); contextv.IsValid() {
		return contextv
	}
	return reflect.Zero(m.ContextType)
}
