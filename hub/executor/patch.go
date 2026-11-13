package executor

type ProviderLoadedHook func(providerName string)

var DefaultProviderLoadedHook ProviderLoadedHook

// ProvidersReadyHook runs once per apply, after every proxy provider of that
// apply finished its initial load, failed ones included.
type ProvidersReadyHook func(count int)

var DefaultProvidersReadyHook ProvidersReadyHook
