package history

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tabreu8/mqtt-get/internal/store"
)

// Snapshot file format (gzip-compressed):
//
//	magic "MQGH1\n"
//	repeated: uvarint len(topic), topic, uvarint points,
//	          points × (varint time, byte qos, byte retained, uvarint len, payload)
//	end:      uvarint 0 (an empty topic name)
const magic = "MQGH1\n"

// Save writes all recorded points to path (atomically, through a temporary
// file). It holds each topic's lock only while copying its point headers.
func (h *History) Save(path string) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	err = h.write(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func (h *History) write(w io.Writer) error {
	zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	bw := bufio.NewWriterSize(zw, 64<<10)
	var tmp [binary.MaxVarintLen64]byte
	uvarint := func(v uint64) { bw.Write(tmp[:binary.PutUvarint(tmp[:], v)]) }
	bw.WriteString(magic)
	var pts []point
	for i := range h.shards {
		sh := &h.shards[i]
		sh.mu.RLock()
		names := make([]string, 0, len(sh.m))
		list := make([]*series, 0, len(sh.m))
		for name, s := range sh.m {
			names = append(names, name)
			list = append(list, s)
		}
		sh.mu.RUnlock()
		for j, s := range list {
			s.mu.Lock()
			pts = pts[:0]
			for k := 0; k < s.n; k++ {
				pts = append(pts, *s.at(k))
			}
			s.mu.Unlock()
			if len(pts) == 0 {
				continue
			}
			uvarint(uint64(len(names[j])))
			bw.WriteString(names[j])
			uvarint(uint64(len(pts)))
			for _, p := range pts {
				bw.Write(tmp[:binary.PutVarint(tmp[:], p.time)])
				var ret byte
				if p.retained {
					ret = 1
				}
				bw.WriteByte(p.qos)
				bw.WriteByte(ret)
				uvarint(uint64(p.n))
				bw.Write(p.bytes())
			}
		}
		clear(pts[:cap(pts)]) // don't keep payloads alive
	}
	uvarint(0)
	if err := bw.Flush(); err != nil {
		return err
	}
	return zw.Close()
}

// Load restores points saved by Save, applying the current rules: topics
// no rule matches are skipped, and limits (points, age, bytes) apply as if
// the messages had just been received. A missing file is not an error.
func (h *History) Load(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	r := bufio.NewReaderSize(zr, 64<<10)
	head := make([]byte, len(magic))
	if _, err := io.ReadFull(r, head); err != nil || string(head) != magic {
		return fmt.Errorf("%s: not a history snapshot", path)
	}
	const maxLen = 256 << 20
	for {
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if n == 0 {
			return nil
		}
		if n > 65535 {
			return fmt.Errorf("%s: corrupt topic length", path)
		}
		name := make([]byte, n)
		if _, err := io.ReadFull(r, name); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		count, err := binary.ReadUvarint(r)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		e := store.Entry{Topic: string(name)}
		for ; count > 0; count-- {
			if e.Time, err = binary.ReadVarint(r); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			var flags [2]byte
			if _, err := io.ReadFull(r, flags[:]); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			e.QoS, e.Retained = flags[0], flags[1] == 1
			pl, err := binary.ReadUvarint(r)
			if err != nil || pl > maxLen {
				return fmt.Errorf("%s: corrupt payload length", path)
			}
			e.Payload = make([]byte, pl)
			if _, err := io.ReadFull(r, e.Payload); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			h.Add(&e)
		}
	}
}
