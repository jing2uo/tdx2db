package tdx

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"time"
)

const responseHeaderLen = 16

var macHosts = []struct {
	Name string
	Addr string
	Port string
}{
	{"行情主站1", "121.36.248.138", "7709"},
	{"行情主站2", "123.60.47.136", "7709"},
	{"行情主站3", "121.37.207.165", "7709"},
}

var standardHosts = []struct {
	Name string
	Addr string
	Port string
}{
	{"通达信深圳双线主站1", "110.41.147.114", "7709"},
	{"通达信深圳双线主站2", "110.41.2.72", "7709"},
	{"通达信深圳双线主站3", "110.41.4.4", "7709"},
	{"通达信深圳双线主站4", "47.113.94.204", "7709"},
	{"通达信上海双线主站1", "124.70.176.52", "7709"},
	{"通达信上海双线主站2", "47.100.236.28", "7709"},
	{"通达信北京双线主站1", "121.36.54.217", "7709"},
}

type OnlineClient struct {
	conn net.Conn
}

func NewOnlineClient() *OnlineClient {
	return &OnlineClient{}
}

func (c *OnlineClient) Connect() error {
	return c.connectToHosts(macHosts)
}

func (c *OnlineClient) ConnectStandard() error {
	return c.connectToHosts(standardHosts)
}

func (c *OnlineClient) connectToHosts(hosts []struct {
	Name string
	Addr string
	Port string
}) error {
	if c.conn != nil {
		return nil
	}

	type candidate struct {
		addr string
		dur  time.Duration
	}
	var candidates []candidate
	for _, h := range hosts {
		addr := net.JoinHostPort(h.Addr, h.Port)
		start := time.Now()
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			continue
		}
		_ = conn.Close()
		candidates = append(candidates, candidate{addr: addr, dur: time.Since(start)})
	}
	if len(candidates) == 0 {
		return fmt.Errorf("no available TDX online server")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].dur < candidates[j].dur })

	conn, err := net.DialTimeout("tcp", candidates[0].addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect TDX server %s: %w", candidates[0].addr, err)
	}
	c.conn = conn
	return nil
}

func (c *OnlineClient) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *OnlineClient) Call(msgID uint16, body []byte) ([]byte, error) {
	return c.CallWithHead(1, msgID, body)
}

func (c *OnlineClient) CallWithHead(head byte, msgID uint16, body []byte) ([]byte, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("TDX online client is not connected")
	}

	payload := make([]byte, 2+len(body))
	binary.LittleEndian.PutUint16(payload[0:2], msgID)
	copy(payload[2:], body)

	packet := make([]byte, 10+len(payload))
	packet[0] = head
	// customize uint32 at [1:5] remains 0, compatible with synchronous calls.
	packet[5] = 1
	binary.LittleEndian.PutUint16(packet[6:8], uint16(len(payload)))
	binary.LittleEndian.PutUint16(packet[8:10], uint16(len(payload)))
	copy(packet[10:], payload)

	if _, err := c.conn.Write(packet); err != nil {
		return nil, fmt.Errorf("failed to send TDX request 0x%x: %w", msgID, err)
	}

	header := make([]byte, responseHeaderLen)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, fmt.Errorf("failed to read TDX response header: %w", err)
	}
	zippedSize := binary.LittleEndian.Uint16(header[12:14])
	unzipSize := binary.LittleEndian.Uint16(header[14:16])
	bodyBuf := make([]byte, zippedSize)
	if _, err := io.ReadFull(c.conn, bodyBuf); err != nil {
		return nil, fmt.Errorf("failed to read TDX response body: %w", err)
	}
	if zippedSize == unzipSize {
		return bodyBuf, nil
	}
	zr, err := zlib.NewReader(bytes.NewReader(bodyBuf))
	if err != nil {
		return nil, fmt.Errorf("failed to create zlib reader: %w", err)
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress TDX response: %w", err)
	}
	return out, nil
}
