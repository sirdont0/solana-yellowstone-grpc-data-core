package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
	"github.com/sirdont0/solana-yellowstone-grpc-data-core/examples/pumpfuncreate"
)

func main() {
	settings, err := loadSettings()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	engine, err := core.New(settings.coreConfig(), func(_ context.Context, e core.Event) error {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = fmt.Println(string(b))
		return err
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	handlerPolicy, _ := parsePolicy(settings.HandlerErrorPolicy)
	if err = engine.RegisterWithPolicy(pumpfuncreate.New(), handlerPolicy); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if os.Getenv("CORE_REPLAY_ONLY") == "1" {
		err = engine.ReplayWithOptions(ctx, core.ReplayOptions{TolerantTail: settings.TolerantReplayTail})
	} else {
		err = engine.Run(ctx)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
