package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"plugin"
	"strconv"
	"time"

	"github.com/tessellator/executil"
	"github.com/tessellator/fnrun"
)

// -----------------------------------------------------------------------------
// type aliases

type eventSource func(ctx context.Context, invoker fnrun.Invoker) error

type eventSink func(ctx context.Context, result *fnrun.Result) error

// -----------------------------------------------------------------------------
// Sink Invoker
//
// This is a special type of invoker that also performs a side-effect of sending
// the result to a sink function.

type sinkInvoker struct {
	invoker fnrun.Invoker
	sink    eventSink
}

func (si *sinkInvoker) Invoke(ctx context.Context, input *fnrun.Input) (*fnrun.Result, error) {
	result, err := si.invoker.Invoke(ctx, input)
	if err != nil {
		return result, err
	}

	if si.sink == nil {
		return result, err
	}

	newErr := si.sink(ctx, result)
	if newErr != nil {
		return result, newErr
	}

	return result, err
}

// -----------------------------------------------------------------------------
// Main application

func getEventSource() (eventSource, error) {
	path := os.Getenv("SOURCE_PLUGIN_PATH")
	if path == "" {
		return nil, errors.New("SOURCE_PLUGIN_PATH is a required environment variable")
	}

	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	symbolName := os.Getenv("SOURCE_PLUGIN_SYMBOL")
	if symbolName == "" {
		return nil, errors.New("SOURCE_PLUGIN_SYMBOL is a required environment variable")
	}

	symSource, err := p.Lookup(symbolName)
	if err != nil {
		return nil, err
	}

	source, ok := symSource.(func(context.Context, fnrun.Invoker) error)
	if !ok {
		return nil, fmt.Errorf("Symbol %s could not be found in %s", symbolName, path)
	}

	return source, nil
}

func getEventSink() (eventSink, error) {
	path := os.Getenv("SINK_PLUGIN_PATH")
	if path == "" {
		return nil, nil
	}

	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	symbolName := os.Getenv("SINK_PLUGIN_SYMBOL")
	if symbolName == "" {
		return nil, fmt.Errorf("SINK_PLUGIN_SYMBOL is required when a SINK_PLUGIN_PATH is provided")
	}

	symSink, err := p.Lookup(symbolName)
	if err != nil {
		return nil, err
	}

	sink, ok := symSink.(func(ctx context.Context, result *fnrun.Result) error)
	if !ok {
		return nil, fmt.Errorf("Symbol %s could not be found in %s", symbolName, path)
	}

	return sink, nil
}

func getInvoker() (fnrun.Invoker, error) {
	cmd, err := executil.ParseCmd(os.Getenv("FUNCTION_COMMAND"))
	if err != nil {
		return nil, err
	}
	cmd.Env = os.Environ()

	maxFuncCount := 8
	maxFuncCountStr := os.Getenv("MAX_FUNCTION_COUNT")
	if maxFuncCountStr != "" {
		i, err := strconv.Atoi(maxFuncCountStr)
		if err == nil {
			maxFuncCount = i
		}
	}

	maxWaitMillis := 500
	maxWaitMillisStr := os.Getenv("MAX_WAIT_MILLIS")
	if maxWaitMillisStr != "" {
		i, err := strconv.Atoi(maxWaitMillisStr)
		if err == nil {
			maxWaitMillis = i
		}
	}

	maxExecMillis := 30000
	maxExecMillisStr := os.Getenv("MAX_EXEC_MILLIS")
	if maxExecMillisStr != "" {
		i, err := strconv.Atoi(maxExecMillisStr)
		if err == nil {
			maxExecMillis = i
		}
	}

	config := fnrun.InvokerPoolConfig{
		MaxInvokerCount: maxFuncCount,
		InvokerFactory:  fnrun.NewCmdInvokerFactory(cmd),
		MaxWaitDuration: time.Duration(maxWaitMillis) * time.Millisecond,
		MaxRunnableTime: time.Duration(maxExecMillis) * time.Millisecond,
	}
	pool, err := fnrun.NewInvokerPool(config)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	invoker, err := getInvoker()
	if err != nil {
		return err
	}

	eventSource, err := getEventSource()
	if err != nil {
		return err
	}

	eventSink, err := getEventSink()
	if err != nil {
		return err
	}

	return eventSource(context.Background(), &sinkInvoker{invoker: invoker, sink: eventSink})
}
