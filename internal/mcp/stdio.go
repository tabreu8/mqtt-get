package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/tabreu8/mqtt-get/internal/core"
)

// ServeStdio speaks MCP over newline-delimited JSON on in/out until in is
// closed or ctx is cancelled. Requests are handled concurrently, so a
// long wait_for_message never blocks other calls; notifications (resource
// updates) are written as they happen.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer, p *core.Principal) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sess := newSession(p, true)
	defer sess.close()

	var wmu sync.Mutex
	write := func(b []byte) {
		wmu.Lock()
		defer wmu.Unlock()
		_, _ = out.Write(b)
		_, _ = out.Write([]byte{'\n'})
	}
	writeJSON := func(v any) {
		b, err := json.Marshal(v)
		if err == nil {
			write(b)
		}
	}
	go func() {
		for {
			select {
			case b := <-sess.out:
				write(b)
			case <-sess.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		line = append([]byte(nil), line...)
		if line[0] == '[' {
			var reqs []*request
			if err := json.Unmarshal(line, &reqs); err != nil {
				writeJSON(errResponse(nil, codeParse, "parse error"))
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				var outs []*response
				for _, q := range reqs {
					if r := s.handle(ctx, sess, q); r != nil {
						outs = append(outs, r)
					}
				}
				if len(outs) > 0 {
					writeJSON(outs)
				}
			}()
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			writeJSON(errResponse(nil, codeParse, "parse error"))
			continue
		}
		if req.isNotification() {
			s.handle(ctx, sess, &req) // cancellations must apply immediately
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if r := s.handle(ctx, sess, &req); r != nil {
				writeJSON(r)
			}
		}()
	}
	cancel()
	wg.Wait()
	return sc.Err()
}
