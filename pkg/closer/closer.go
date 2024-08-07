package closer

import (
	"os"
	"os/signal"
	"sync"

	"github.com/gofiber/fiber/v2/log"
)

var globalCloser = New()

// Add adds `func() error` callback to the globalCloser.
func Add(f ...func() error) {
	globalCloser.Add(f...)
}

// Wait waits until all closer functions are done and blocks further execution.
func Wait() {
	globalCloser.Wait()
}

// CloseAll calls all registered closer functions asynchronously.
func CloseAll() {
	globalCloser.CloseAll()
}

// Closer manages a list of functions to be called when closing resources.
type Closer struct {
	done  chan struct{}
	funcs []func() error
	once  sync.Once
	mu    sync.Mutex
}

// New returns new Closer, if []os.Signal is specified Closer will automatically call CloseAll when one of signals is received from OS.
func New(sig ...os.Signal) *Closer {
	c := &Closer{done: make(chan struct{})}
	if len(sig) > 0 {
		go func() {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, sig...)
			<-ch
			signal.Stop(ch)
			c.CloseAll()
		}()
	}

	return c
}

// Add func to closer.
func (c *Closer) Add(f ...func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.funcs = append(c.funcs, f...)
}

// Wait blocks until all closer functions are done.
func (c *Closer) Wait() {
	<-c.done
}

// CloseAll calls all closer functions
func (c *Closer) CloseAll() {
	c.once.Do(func() {
		defer close(c.done)

		c.mu.Lock()
		funcs := c.funcs
		c.funcs = nil
		c.mu.Unlock()

		// call all Closer funcs async
		errs := make(chan error, len(funcs))
		for _, f := range funcs {
			go func(f func() error) {
				errs <- f()
			}(f)
		}

		for i := 0; i < cap(errs); i++ {
			if err := <-errs; err != nil {
				log.Warnf("Error returned from Closer: %v", err)
			}
		}
	})
}
