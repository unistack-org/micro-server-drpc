// Package drpc provides a drpc server
package drpc // import "go.unistack.org/micro-server-drpc/v3"

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	// nolint: staticcheck

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/errors"
	"go.unistack.org/micro/v3/logger"
	metadata "go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/register"
	"go.unistack.org/micro/v3/server"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"storj.io/drpc/drpcerr"
	gmetadata "storj.io/drpc/drpcmetadata"
)

const (
	defaultContentType = "application/drpc+proto"
)

/*
type grpcServerReflection struct {
	srv *grpc.Server
	s   *serverReflectionServer
}
*/

type grpcServer struct {
	handlers map[string]server.Handler
	//	srv         *grpc.Server
	exit        chan chan error
	wg          *sync.WaitGroup
	rsvc        *register.Service
	subscribers map[*subscriber][]broker.Subscriber
	rpc         *rServer
	opts        server.Options
	sync.RWMutex
	init       bool
	started    bool
	registered bool
}

func newGRPCServer(opts ...server.Option) server.Server {
	// create a grpc server
	g := &grpcServer{
		opts: server.NewOptions(opts...),
		rpc: &rServer{
			serviceMap: make(map[string]*service),
		},
		handlers:    make(map[string]server.Handler),
		subscribers: make(map[*subscriber][]broker.Subscriber),
		exit:        make(chan chan error),
	}

	return g
}

/*
type grpcRouter struct {
 h func(context.Context, server.Request, interface{}) error
 m func(context.Context, server.Message) error
}

func (r grpcRouter) ProcessMessage(ctx context.Context, msg server.Message) error {
 return r.m(ctx, msg)
}

func (r grpcRouter) ServeRequest(ctx context.Context, req server.Request, rsp server.Response) error {
 return r.h(ctx, req, rsp)
}

*/

func (g *grpcServer) configure(opts ...server.Option) error {
	g.Lock()
	defer g.Unlock()

	for _, o := range opts {
		o(&g.opts)
	}

	if err := g.opts.Register.Init(); err != nil {
		return err
	}
	if err := g.opts.Broker.Init(); err != nil {
		return err
	}
	if err := g.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := g.opts.Auth.Init(); err != nil {
		return err
	}
	if err := g.opts.Logger.Init(); err != nil {
		return err
	}
	if err := g.opts.Meter.Init(); err != nil {
		return err
	}
	if err := g.opts.Transport.Init(); err != nil {
		return err
	}

	g.wg = g.opts.Wait

	maxMsgSize := g.getMaxMsgSize()

	gopts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.UnknownServiceHandler(g.handler),
	}

	/*
		if creds := g.getCredentials(); creds != nil {
			gopts = append(gopts, grpc.Creds(creds))
		}

		if opts := g.getGrpcOptions(); opts != nil {
			gopts = append(gopts, opts...)
		}
	*/

	g.rsvc = nil
	restart := false
	if g.started {
		restart = true
		if err := g.Stop(); err != nil {
			return err
		}
	}
	g.srv = grpc.NewServer(gopts...)

	if restart {
		return g.Start()
	}

	g.init = true

	return nil
}

func (g *grpcServer) getMaxMsgSize() int {
	if g.opts.Context == nil {
		return codec.DefaultMaxMsgSize
	}
	s, ok := g.opts.Context.Value(maxMsgSizeKey{}).(int)
	if !ok {
		return codec.DefaultMaxMsgSize
	}
	return s
}

/*
func (g *grpcServer) getCredentials() credentials.TransportCredentials {
	if g.opts.TLSConfig != nil {
		return credentials.NewTLS(g.opts.TLSConfig)
	}
	return nil
}
*/
/*
func (g *grpcServer) getGrpcOptions() []grpc.ServerOption {
	if g.opts.Context == nil {
		return nil
	}

	opts, ok := g.opts.Context.Value(grpcOptions{}).([]grpc.ServerOption)
	if !ok || opts == nil {
		return nil
	}

	return opts
}
*/

func (g *grpcServer) handler(srv interface{}, stream grpc.ServerStream) (err error) {
	defer func() {
		if r := recover(); r != nil {
			g.RLock()
			config := g.opts
			g.RUnlock()
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Error(config.Context, "panic recovered: ", r)
				config.Logger.Error(config.Context, string(debug.Stack()))
			}
			err = errors.InternalServerError(g.opts.Name, "panic recovered: %v", r)
		} else if err != nil {
			g.RLock()
			config := g.opts
			g.RUnlock()
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Errorf(config.Context, "grpc handler got error: %s", err)
			}
		}
	}()

	if g.wg != nil {
		g.wg.Add(1)
		defer g.wg.Done()
	}

	fullMethod, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Errorf(codes.Internal, "method does not exist in context")
	}

	serviceName, methodName, err := serviceMethod(fullMethod)
	if err != nil {
		return status.New(StatusInvalidArgument, err.Error()).Err()
	}

	// get drpc metadata
	dmd, _ := gmetadata.Get(stream.Context())

	md := metadata.New(len(dmd))
	for k, v := range dmd {
		md.Set(k, v)
	}

	// timeout for server deadline
	to, ok := md.Get("timeout")
	if ok {
		md.Del("timeout")
	}

	// get content type
	ct := defaultContentType

	if ctype, ok := md.Get("content-type"); ok {
		ct = ctype
	} else if ctype, ok := md.Get("x-content-type"); ok {
		ct = ctype
		md.Del("x-content-type")
	}

	// create new context
	ctx := metadata.NewIncomingContext(stream.Context(), md)

	// get peer from context
	if p, ok := peer.FromContext(stream.Context()); ok {
		md["Remote"] = p.Addr.String()
		ctx = peer.NewContext(ctx, p)
	}

	// set the timeout if we have it
	if len(to) > 0 {
		if n, err := strconv.ParseUint(to, 10, 64); err == nil {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(n))
			defer cancel()
		}
	}

	g.rpc.mu.RLock()
	svc := g.rpc.serviceMap[serviceName]
	g.rpc.mu.RUnlock()

	/*
		if svc == nil && g.reflection && methodName == "ServerReflectionInfo" {
			rfl := &grpcServerReflection{srv: g.srv, s: &serverReflectionServer{s: g.srv}}
			svc = &service{}
			svc.typ = reflect.TypeOf(rfl)
			svc.rcvr = reflect.ValueOf(rfl)
			svc.name = reflect.Indirect(svc.rcvr).Type().Name()
			svc.method = make(map[string]*methodType)
			typ := reflect.TypeOf(rfl)
			if me, ok := typ.MethodByName("ServerReflectionInfo"); ok {
				g.rpc.mu.Lock()
				ep, err := prepareEndpoint(me)
				if ep != nil && err != nil {
					svc.method["ServerReflectionInfo"] = ep
				} else if err != nil {
					return status.New(codes.Unimplemented, err.Error()).Err()
				}
				g.rpc.mu.Unlock()
			}
		}
	*/

	if svc == nil {
		return status.New(codes.Unimplemented, fmt.Sprintf("unknown service %s", serviceName)).Err()
	}

	mtype := svc.method[methodName]
	if mtype == nil {
		return status.New(codes.Unimplemented, fmt.Sprintf("unknown service %s.%s", serviceName, methodName)).Err()
	}

	// process unary
	if !mtype.stream {
		return g.processRequest(ctx, stream, svc, mtype, ct)
	}

	// process stream
	return g.processStream(ctx, stream, svc, mtype, ct)
}

func (g *grpcServer) processRequest(ctx context.Context, stream grpc.ServerStream, service *service, mtype *methodType, ct string) error {
	// for {
	var argv, replyv reflect.Value

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}

	// Unmarshal request
	if err := stream.RecvMsg(argv.Interface()); err != nil {
		return err
	}

	if argIsValue {
		argv = argv.Elem()
	}

	// reply value
	replyv = reflect.New(mtype.ReplyType.Elem())

	function := mtype.method.Func
	var returnValues []reflect.Value

	cf, err := g.newCodec(ct)
	if err != nil {
		return errors.InternalServerError(g.opts.Name, err.Error())
	}
	b, err := cf.Marshal(argv.Interface())
	if err != nil {
		return err
	}

	// create a client.Request
	r := &rpcRequest{
		service:     g.opts.Name,
		contentType: ct,
		method:      fmt.Sprintf("%s.%s", service.name, mtype.method.Name),
		body:        b,
		payload:     argv.Interface(),
	}
	// define the handler func
	fn := func(ctx context.Context, req server.Request, rsp interface{}) (err error) {
		returnValues = function.Call([]reflect.Value{service.rcvr, mtype.prepareContext(ctx), reflect.ValueOf(argv.Interface()), reflect.ValueOf(rsp)})

		// The return value for the method is an error.
		if rerr := returnValues[0].Interface(); rerr != nil {
			err = rerr.(error)
		}

		return err
	}

	// wrap the handler func
	for i := len(g.opts.HdlrWrappers); i > 0; i-- {
		fn = g.opts.HdlrWrappers[i-1](fn)
	}

	statusCode := StatusOK
	statusDesc := ""

	// execute the handler
	if appErr := fn(ctx, r, replyv.Interface()); appErr != nil {
		switch verr := appErr.(type) {
		case *errors.Error:
			statusCode = microError(verr)
			statusDesc = verr.Error()
		case proto.Message:
			// user defined error that proto based we can attach it to grpc status
			statusCode = convertCode(appErr)
			statusDesc = appErr.Error()
		default:
			g.RLock()
			config := g.opts
			g.RUnlock()
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Warn(config.Context, "handler error will not be transferred properly, must return *errors.Error or drpcerr.Error")
			}
			// default case user pass own error type that not proto based
			statusCode = convertCode(verr)
			statusDesc = verr.Error()
		}

		return drpcerr.WithCode(fmt.Errorf(statusDesc), uint64(statusCode))
	}

	if err := stream.SendMsg(replyv.Interface()); err != nil {
		return err
	}

	return nil
}

/*
type reflectStream struct {
	stream server.Stream
}

func (s *reflectStream) Send(rsp *grpcreflect.ServerReflectionResponse) error {
	return s.stream.Send(rsp)
}

func (s *reflectStream) Recv() (*grpcreflect.ServerReflectionRequest, error) {
	req := &grpcreflect.ServerReflectionRequest{}
	err := s.stream.Recv(req)
	return req, err
}

func (s *reflectStream) SetHeader(gmetadata.MD) error {
	return nil
}

func (s *reflectStream) SendHeader(gmetadata.MD) error {
	return nil
}

func (s *reflectStream) SetTrailer(gmetadata.MD) {

}

func (s *reflectStream) Context() context.Context {
	return s.stream.Context()
}

func (s *reflectStream) SendMsg(m interface{}) error {
	return s.stream.Send(m)
}

func (s *reflectStream) RecvMsg(m interface{}) error {
	return s.stream.Recv(m)
}

func (g *grpcServerReflection) ServerReflectionInfo(ctx context.Context, stream server.Stream) error {
	return g.s.ServerReflectionInfo(&reflectStream{stream})
}
*/

func (g *grpcServer) processStream(ctx context.Context, stream grpc.ServerStream, service *service, mtype *methodType, ct string) error {
	opts := g.opts

	r := &rpcRequest{
		service:     opts.Name,
		contentType: ct,
		method:      fmt.Sprintf("%s.%s", service.name, mtype.method.Name),
		stream:      true,
	}

	ss := &rpcStream{
		ServerStream: stream,
		request:      r,
	}

	function := mtype.method.Func
	var returnValues []reflect.Value

	// Invoke the method, providing a new value for the reply.
	fn := func(ctx context.Context, req server.Request, stream interface{}) error {
		returnValues = function.Call([]reflect.Value{service.rcvr, mtype.prepareContext(ctx), reflect.ValueOf(stream)})
		if err := returnValues[0].Interface(); err != nil {
			return err.(error)
		}

		return nil
	}

	for i := len(opts.HdlrWrappers); i > 0; i-- {
		fn = opts.HdlrWrappers[i-1](fn)
	}

	statusCode := StatusOK
	statusDesc := ""

	if appErr := fn(ctx, r, ss); appErr != nil {
		switch verr := appErr.(type) {
		case *errors.Error:
			statusCode = microError(verr)
			statusDesc = verr.Error()
		case proto.Message:
			// user defined error that proto based we can attach it to grpc status
			statusCode = convertCode(appErr)
			statusDesc = appErr.Error()
		default:
			if g.opts.Logger.V(logger.ErrorLevel) {
				g.opts.Logger.Error(g.opts.Context, "handler error will not be transferred properly, must return *errors.Error or proto.Message")
			}
			// default case user pass own error type that not proto based
			statusCode = convertCode(verr)
			statusDesc = verr.Error()
		}
		return drpcerr.WithCode(fmt.Errorf(statusDesc), uint64(statusCode))
	}

	return nil
}

func (g *grpcServer) newCodec(ct string) (codec.Codec, error) {
	g.RLock()
	defer g.RUnlock()

	if idx := strings.IndexRune(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}

	if c, ok := g.opts.Codecs[ct]; ok {
		return c, nil
	}

	return nil, codec.ErrUnknownContentType
}

func (g *grpcServer) Options() server.Options {
	g.RLock()
	opts := g.opts
	g.RUnlock()

	return opts
}

func (g *grpcServer) Init(opts ...server.Option) error {
	if len(opts) == 0 && g.init {
		return nil
	}
	return g.configure(opts...)
}

func (g *grpcServer) NewHandler(h interface{}, opts ...server.HandlerOption) server.Handler {
	return newRPCHandler(h, opts...)
}

func (g *grpcServer) Handle(h server.Handler) error {
	if err := g.rpc.register(h.Handler()); err != nil {
		return err
	}

	g.handlers[h.Name()] = h
	return nil
}

func (g *grpcServer) NewSubscriber(topic string, sb interface{}, opts ...server.SubscriberOption) server.Subscriber {
	return newSubscriber(topic, sb, opts...)
}

func (g *grpcServer) Subscribe(sb server.Subscriber) error {
	sub, ok := sb.(*subscriber)
	if !ok {
		return fmt.Errorf("invalid subscriber: expected *subscriber")
	}
	if len(sub.handlers) == 0 {
		return fmt.Errorf("invalid subscriber: no handler functions")
	}

	if err := server.ValidateSubscriber(sb); err != nil {
		return err
	}

	g.Lock()
	if _, ok = g.subscribers[sub]; ok {
		g.Unlock()
		return fmt.Errorf("subscriber %v already exists", sub)
	}

	g.subscribers[sub] = nil
	g.Unlock()
	return nil
}

func (g *grpcServer) Register() error {
	g.RLock()
	rsvc := g.rsvc
	config := g.opts
	g.RUnlock()

	// if service already filled, reuse it and return early
	if rsvc != nil {
		if err := server.DefaultRegisterFunc(rsvc, config); err != nil {
			return err
		}
		return nil
	}

	service, err := server.NewRegisterService(g)
	if err != nil {
		return err
	}

	g.RLock()
	// Maps are ordered randomly, sort the keys for consistency
	handlerList := make([]string, 0, len(g.handlers))
	for n := range g.handlers {
		// Only advertise non internal handlers
		handlerList = append(handlerList, n)
	}

	sort.Strings(handlerList)

	subscriberList := make([]*subscriber, 0, len(g.subscribers))
	for e := range g.subscribers {
		// Only advertise non internal subscribers
		subscriberList = append(subscriberList, e)
	}
	sort.Slice(subscriberList, func(i, j int) bool {
		return subscriberList[i].topic > subscriberList[j].topic
	})

	endpoints := make([]*register.Endpoint, 0, len(handlerList)+len(subscriberList))
	for _, n := range handlerList {
		endpoints = append(endpoints, g.handlers[n].Endpoints()...)
	}
	for _, e := range subscriberList {
		endpoints = append(endpoints, e.Endpoints()...)
	}
	g.RUnlock()

	service.Nodes[0].Metadata["protocol"] = "grpc"
	service.Nodes[0].Metadata["transport"] = service.Nodes[0].Metadata["protocol"]
	service.Endpoints = endpoints

	g.RLock()
	registered := g.registered
	g.RUnlock()

	if !registered {
		if config.Logger.V(logger.InfoLevel) {
			config.Logger.Infof(config.Context, "Register [%s] Registering node: %s", config.Register.String(), service.Nodes[0].ID)
		}
	}

	// register the service
	if err := server.DefaultRegisterFunc(service, config); err != nil {
		return err
	}

	// already registered? don't need to register subscribers
	if registered {
		return nil
	}

	g.Lock()
	defer g.Unlock()

	for sb := range g.subscribers {
		handler := g.createSubHandler(sb, config)
		var opts []broker.SubscribeOption
		if queue := sb.Options().Queue; len(queue) > 0 {
			opts = append(opts, broker.SubscribeGroup(queue))
		}

		subCtx := config.Context
		if cx := sb.Options().Context; cx != nil {
			subCtx = cx
		}
		opts = append(opts, broker.SubscribeContext(subCtx))
		opts = append(opts, broker.SubscribeAutoAck(sb.Options().AutoAck))
		opts = append(opts, broker.SubscribeBodyOnly(sb.Options().BodyOnly))

		if config.Logger.V(logger.InfoLevel) {
			config.Logger.Infof(config.Context, "Subscribing to topic: %s", sb.Topic())
		}
		sub, err := config.Broker.Subscribe(subCtx, sb.Topic(), handler, opts...)
		if err != nil {
			return err
		}
		g.subscribers[sb] = []broker.Subscriber{sub}
	}

	g.registered = true
	g.rsvc = service

	return nil
}

func (g *grpcServer) Deregister() error {
	var err error

	g.RLock()
	config := g.opts
	g.RUnlock()

	service, err := server.NewRegisterService(g)
	if err != nil {
		return err
	}

	if config.Logger.V(logger.InfoLevel) {
		config.Logger.Infof(config.Context, "Deregistering node: %s", service.Nodes[0].ID)
	}

	if err := server.DefaultDeregisterFunc(service, config); err != nil {
		return err
	}

	g.Lock()
	g.rsvc = nil

	if !g.registered {
		g.Unlock()
		return nil
	}

	g.registered = false

	wg := sync.WaitGroup{}
	for sb, subs := range g.subscribers {
		for _, sub := range subs {
			wg.Add(1)
			go func(s broker.Subscriber) {
				defer wg.Done()
				if config.Logger.V(logger.InfoLevel) {
					config.Logger.Infof(config.Context, "Unsubscribing from topic: %s", s.Topic())
				}
				if err := s.Unsubscribe(g.opts.Context); err != nil {
					if config.Logger.V(logger.ErrorLevel) {
						config.Logger.Errorf(config.Context, "Unsubscribing from topic: %s err: %v", s.Topic(), err)
					}
				}
			}(sub)
		}
		g.subscribers[sb] = nil
	}
	wg.Wait()

	g.Unlock()
	return nil
}

func (g *grpcServer) Start() error {
	g.RLock()
	if g.started {
		g.RUnlock()
		return nil
	}
	g.RUnlock()

	config := g.Options()

	// micro: config.Transport.Listen(config.Address)
	var ts net.Listener

	if l := config.Listener; l != nil {
		ts = l
	} else {
		var err error

		// check the tls config for secure connect
		if tc := config.TLSConfig; tc != nil {
			ts, err = tls.Listen("tcp", config.Address, tc)
			// otherwise just plain tcp listener
		} else {
			ts, err = net.Listen("tcp", config.Address)
		}
		if err != nil {
			return err
		}
	}

	if config.MaxConn > 0 {
		ts = netutil.LimitListener(ts, config.MaxConn)
	}

	if config.Logger.V(logger.InfoLevel) {
		config.Logger.Infof(config.Context, "Server [grpc] Listening on %s", ts.Addr().String())
	}
	g.Lock()
	g.opts.Address = ts.Addr().String()
	if len(g.opts.Advertise) == 0 {
		g.opts.Advertise = ts.Addr().String()
	}
	g.Unlock()

	// only connect if we're subscribed
	if len(g.subscribers) > 0 {
		// connect to the broker
		if err := config.Broker.Connect(config.Context); err != nil {
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Errorf(config.Context, "Broker [%s] connect error: %v", config.Broker.String(), err)
			}
			return err
		}

		if config.Logger.V(logger.InfoLevel) {
			config.Logger.Infof(config.Context, "Broker [%s] Connected to %s", config.Broker.String(), config.Broker.Address())
		}
	}

	// use RegisterCheck func before register
	// nolint: nestif
	if err := g.opts.RegisterCheck(config.Context); err != nil {
		if config.Logger.V(logger.ErrorLevel) {
			config.Logger.Errorf(config.Context, "Server %s-%s register check error: %s", config.Name, config.ID, err)
		}
	} else {
		// announce self to the world
		if err := g.Register(); err != nil {
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Errorf(config.Context, "Server register error: %v", err)
			}
		}
	}

	// micro: go ts.Accept(s.accept)
	go func() {
		if err := g.srv.Serve(ts); err != nil {
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Errorf(config.Context, "gRPC Server start error: %v", err)
			}
			if err := g.Stop(); err != nil {
				if config.Logger.V(logger.ErrorLevel) {
					config.Logger.Errorf(config.Context, "gRPC Server stop error: %v", err)
				}
			}
		}
	}()

	go func() {
		t := new(time.Ticker)

		// only process if it exists
		if g.opts.RegisterInterval > time.Duration(0) {
			// new ticker
			t = time.NewTicker(g.opts.RegisterInterval)
		}

		// return error chan
		var ch chan error

	Loop:
		for {
			select {
			// register self on interval
			case <-t.C:
				g.RLock()
				registered := g.registered
				g.RUnlock()
				rerr := g.opts.RegisterCheck(g.opts.Context)
				// nolint: nestif
				if rerr != nil && registered {
					if config.Logger.V(logger.ErrorLevel) {
						config.Logger.Errorf(config.Context, "Server %s-%s register check error: %s, deregister it", config.Name, config.ID, rerr)
					}
					// deregister self in case of error
					if err := g.Deregister(); err != nil {
						if config.Logger.V(logger.ErrorLevel) {
							config.Logger.Errorf(config.Context, "Server %s-%s deregister error: %s", config.Name, config.ID, err)
						}
					}
				} else if rerr != nil && !registered {
					if config.Logger.V(logger.ErrorLevel) {
						config.Logger.Errorf(config.Context, "Server %s-%s register check error: %s", config.Name, config.ID, rerr)
					}
					continue
				}
				if err := g.Register(); err != nil {
					if config.Logger.V(logger.ErrorLevel) {
						config.Logger.Errorf(config.Context, "Server %s-%s register error: %s", config.Name, config.ID, err)
					}
				}
			// wait for exit
			case ch = <-g.exit:
				break Loop
			}
		}

		// deregister self
		if err := g.Deregister(); err != nil {
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Errorf(config.Context, "Server deregister error: %v", err)
			}
		}

		// wait for waitgroup
		if g.wg != nil {
			g.wg.Wait()
		}

		// stop the grpc server
		exit := make(chan bool)

		go func() {
			g.srv.GracefulStop()
			close(exit)
		}()

		select {
		case <-exit:
		case <-time.After(time.Second):
			g.srv.Stop()
		}

		// close transport
		ch <- nil

		if config.Logger.V(logger.InfoLevel) {
			config.Logger.Infof(config.Context, "Broker [%s] Disconnected from %s", config.Broker.String(), config.Broker.Address())
		}
		// disconnect broker
		if err := config.Broker.Disconnect(config.Context); err != nil {
			if config.Logger.V(logger.ErrorLevel) {
				config.Logger.Errorf(config.Context, "Broker [%s] disconnect error: %v", config.Broker.String(), err)
			}
		}
	}()

	// mark the server as started
	g.Lock()
	g.started = true
	g.Unlock()

	return nil
}

func (g *grpcServer) Stop() error {
	g.RLock()
	if !g.started {
		g.RUnlock()
		return nil
	}
	g.RUnlock()

	ch := make(chan error)
	g.exit <- ch

	err := <-ch
	g.Lock()
	g.rsvc = nil
	g.started = false
	g.Unlock()

	return err
}

func (g *grpcServer) String() string {
	return "grpc"
}

func (g *grpcServer) Name() string {
	return g.opts.Name
}

func NewServer(opts ...server.Option) server.Server {
	return newGRPCServer(opts...)
}
