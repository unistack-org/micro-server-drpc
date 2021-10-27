package drpc

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"

	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/errors"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"go.unistack.org/micro/v3/register"
	"go.unistack.org/micro/v3/server"
)

type handler struct {
	reqType reflect.Type
	ctxType reflect.Type
	method  reflect.Value
}

type subscriber struct {
	topic      string
	rcvr       reflect.Value
	typ        reflect.Type
	subscriber interface{}
	handlers   []*handler
	endpoints  []*register.Endpoint
	opts       server.SubscriberOptions
}

func newSubscriber(topic string, sub interface{}, opts ...server.SubscriberOption) server.Subscriber {
	options := server.NewSubscriberOptions(opts...)

	var endpoints []*register.Endpoint
	var handlers []*handler

	if typ := reflect.TypeOf(sub); typ.Kind() == reflect.Func {
		h := &handler{
			method: reflect.ValueOf(sub),
		}

		switch typ.NumIn() {
		case 1:
			h.reqType = typ.In(0)
		case 2:
			h.ctxType = typ.In(0)
			h.reqType = typ.In(1)
		}

		handlers = append(handlers, h)

		endpoints = append(endpoints, &register.Endpoint{
			Name:    "Func",
			Request: register.ExtractSubValue(typ),
			Metadata: map[string]string{
				"topic":      topic,
				"subscriber": "true",
			},
		})
	} else {
		hdlr := reflect.ValueOf(sub)
		name := reflect.Indirect(hdlr).Type().Name()

		for m := 0; m < typ.NumMethod(); m++ {
			method := typ.Method(m)
			h := &handler{
				method: method.Func,
			}

			switch method.Type.NumIn() {
			case 2:
				h.reqType = method.Type.In(1)
			case 3:
				h.ctxType = method.Type.In(1)
				h.reqType = method.Type.In(2)
			}

			handlers = append(handlers, h)

			endpoints = append(endpoints, &register.Endpoint{
				Name:    name + "." + method.Name,
				Request: register.ExtractSubValue(method.Type),
				Metadata: map[string]string{
					"topic":      topic,
					"subscriber": "true",
				},
			})
		}
	}

	return &subscriber{
		rcvr:       reflect.ValueOf(sub),
		typ:        reflect.TypeOf(sub),
		topic:      topic,
		subscriber: sub,
		handlers:   handlers,
		endpoints:  endpoints,
		opts:       options,
	}
}

func (g *grpcServer) createSubHandler(sb *subscriber, opts server.Options) broker.Handler {
	return func(p broker.Event) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if g.opts.Logger.V(logger.ErrorLevel) {
					g.opts.Logger.Error(g.opts.Context, "panic recovered: ", r)
					g.opts.Logger.Error(g.opts.Context, string(debug.Stack()))
				}
				err = errors.InternalServerError(g.opts.Name+".subscriber", "panic recovered: %v", r)
			}
		}()

		msg := p.Message()
		// if we don't have headers, create empty map
		if msg.Header == nil {
			msg.Header = make(map[string]string)
		}

		ct := msg.Header["Content-Type"]
		if len(ct) == 0 {
			msg.Header["Content-Type"] = defaultContentType
			ct = defaultContentType
		}
		cf, err := g.newCodec(ct)
		if err != nil {
			return err
		}

		hdr := make(map[string]string, len(msg.Header))
		for k, v := range msg.Header {
			if k == "Content-Type" {
				continue
			}
			hdr[k] = v
		}

		ctx := metadata.NewIncomingContext(sb.opts.Context, hdr)

		results := make(chan error, len(sb.handlers))

		for i := 0; i < len(sb.handlers); i++ {
			handler := sb.handlers[i]

			var isVal bool
			var req reflect.Value

			if handler.reqType.Kind() == reflect.Ptr {
				req = reflect.New(handler.reqType.Elem())
			} else {
				req = reflect.New(handler.reqType)
				isVal = true
			}
			if isVal {
				req = req.Elem()
			}

			if err = cf.Unmarshal(msg.Body, req.Interface()); err != nil {
				return err
			}

			fn := func(ctx context.Context, msg server.Message) error {
				var vals []reflect.Value
				if sb.typ.Kind() != reflect.Func {
					vals = append(vals, sb.rcvr)
				}
				if handler.ctxType != nil {
					vals = append(vals, reflect.ValueOf(ctx))
				}

				vals = append(vals, reflect.ValueOf(msg.Payload()))

				returnValues := handler.method.Call(vals)
				if rerr := returnValues[0].Interface(); rerr != nil {
					return rerr.(error)
				}
				return nil
			}

			for i := len(opts.SubWrappers); i > 0; i-- {
				fn = opts.SubWrappers[i-1](fn)
			}

			if g.wg != nil {
				g.wg.Add(1)
			}
			go func() {
				if g.wg != nil {
					defer g.wg.Done()
				}
				cerr := fn(ctx, &rpcMessage{
					topic:       sb.topic,
					contentType: ct,
					payload:     req.Interface(),
					header:      msg.Header,
					body:        msg.Body,
				})
				results <- cerr
			}()
		}
		var errors []string
		for i := 0; i < len(sb.handlers); i++ {
			if rerr := <-results; rerr != nil {
				errors = append(errors, rerr.Error())
			}
		}
		if len(errors) > 0 {
			err = fmt.Errorf("subscriber error: %s", strings.Join(errors, "\n"))
		}

		return err
	}
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Subscriber() interface{} {
	return s.subscriber
}

func (s *subscriber) Endpoints() []*register.Endpoint {
	return s.endpoints
}

func (s *subscriber) Options() server.SubscriberOptions {
	return s.opts
}
