package outbound

import (
	"context"
	"fmt"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/gun"
)

// ProbeDialContext opens a connection for a delay probe. Over gRPC it gets a
// transport of its own that obeys ctx and closes with the connection; the
// trace says how far it got. Other networks dial as usual and have no trace.
func (v *Vless) ProbeDialContext(ctx context.Context, metadata *C.Metadata) (_ C.Conn, trace *gun.Trace, err error) {
	if v.gunClient == nil {
		conn, err := v.DialContext(ctx, metadata)
		return conn, nil, err
	}
	c, trace := v.gunClient.DialProbe(ctx)
	defer func() {
		safeConnClose(c, err)
	}()
	c, err = v.StreamConnContext(ctx, c, metadata)
	if err != nil {
		return nil, trace, fmt.Errorf("%s connect error: %s", v.addr, err.Error())
	}
	return NewConn(c, v), trace, nil
}

// TransportHealth reports stream and byte timing of the live gRPC transports.
// ok is false for networks that have none.
func (v *Vless) TransportHealth() (health gun.Health, ok bool) {
	if v.gunClient == nil {
		return gun.Health{}, false
	}
	return v.gunClient.Health(), true
}

// ResetTransports closes every live gRPC transport, failing the streams on
// them at once. The next dial opens a fresh connection.
func (v *Vless) ResetTransports() {
	if v.gunClient != nil {
		_ = v.gunClient.Close()
	}
}
