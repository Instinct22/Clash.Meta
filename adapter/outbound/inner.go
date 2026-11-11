package outbound

import C "github.com/metacubex/mihomo/constant"

// Inner is the adapter the auto close wrapper keeps alive. Methods beyond
// ProxyAdapter, such as probe dialing, are only reachable through it.
func (p *autoCloseProxyAdapter) Inner() C.ProxyAdapter {
	return p.ProxyAdapter
}
