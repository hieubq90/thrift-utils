package thrift_utils

import (
	"fmt"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/hieubq90/go-commons-pool"
)

type TProtocolType uint8

const (
	BINARY_PROTOCOL TProtocolType = iota + 1
	COMPACT_PROTOCOL
)

//
type MakeObjectFunction func(t thrift.TTransport, f thrift.TProtocolFactory) interface{}

type TClientFactory struct {
	tag              string
	endpoint         string
	protocolFactory  thrift.TProtocolFactory
	transportFactory thrift.TTransportFactory
	makeObjectFunc   MakeObjectFunction
}

func NewTClientFactory(tag, endpoint string, fn MakeObjectFunction, pType TProtocolType) *TClientFactory {
	if pType == BINARY_PROTOCOL {
		fmt.Printf("[%s] | create new TClientFactofy with binary protocol", tag)
		return &TClientFactory{
			tag:              tag,
			endpoint:         endpoint,
			protocolFactory:  thrift.NewTBinaryProtocolFactory(true, true),
			transportFactory: thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
			makeObjectFunc:   fn,
		}
	} else {
		fmt.Printf("[%s] | create new TClientFactofy with compact protocol", tag)
		return &TClientFactory{
			tag:              tag,
			endpoint:         endpoint,
			protocolFactory:  thrift.NewTCompactProtocolFactory(),
			transportFactory: thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
			makeObjectFunc:   fn,
		}
	}
}

// MakeObject function use to create new thrift client when needed
func (f *TClientFactory) MakeObject() (*pool.PooledObject, error) {
	fmt.Printf("[%s] | making new client for pool", f.tag)
	var transport thrift.TTransport
	var err error

	transport, err = thrift.NewTSocket(f.endpoint)
	if err != nil {
		return nil, err
	}

	transport = f.transportFactory.GetTransport(transport)
	err = transport.Open()
	if err != nil {
		fmt.Println(f.tag+" | error on opening connection to "+f.endpoint, err)
		return nil, err
	}

	tClient := f.makeObjectFunc(transport, f.protocolFactory)
	return pool.NewPooledObject(tClient), nil
}

func (f *TClientFactory) DestroyObject(object *pool.PooledObject) error {
	//do destroy
	return nil
}

func (f *TClientFactory) ValidateObject(object *pool.PooledObject) bool {
	//do validate
	return true
}

func (f *TClientFactory) ActivateObject(object *pool.PooledObject) error {
	//do activate
	return nil
}

func (f *TClientFactory) PassivateObject(object *pool.PooledObject) error {
	//do passivate
	return nil
}
