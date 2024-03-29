/*
  Copyright 2020 Tamás Gulácsi

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package dbcsv

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Wrap returns a new context with cancel that is canceled on interrupts.
func Wrap(ctx context.Context) (context.Context, context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	go func() {
		sig := <-sigCh
		signal.Stop(sigCh)
		cancel()
		go func() {
			time.Sleep(3 * time.Second)
			if p, _ := os.FindProcess(os.Getpid()); p != nil {
				_ = p.Signal(sig)
			}
			time.Sleep(2 * time.Second)
			os.Exit(1)
		}()
	}()
	return ctx, cancel
}
