package transport

import "github.com/gezibash/arc/v2/pkg/runtime"

const componentKey = "transport"

// Attach stores a Transport on the runtime.
func Attach(t Transport) runtime.Extension {
	return func(rt *runtime.Runtime) error {
		rt.Set(componentKey, t)
		rt.OnClose(t.Close)
		return nil
	}
}

// From retrieves the Transport from the runtime.
func From(rt *runtime.Runtime) Transport {
	if rt == nil {
		return nil
	}
	if t, ok := rt.Get(componentKey).(Transport); ok {
		return t
	}
	return nil
}
