package relay

import (
	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
)

// anyToProtoMap converts a map[string]any to protobuf LabelValue map.
func anyToProtoMap(m map[string]any) map[string]*relayv1.LabelValue {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]*relayv1.LabelValue, len(m))
	for k, v := range m {
		result[k] = anyToProtoValue(v)
	}
	return result
}

func anyToProtoValue(v any) *relayv1.LabelValue {
	switch val := v.(type) {
	case string:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_StringValue{StringValue: val}}
	case int64:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_IntValue{IntValue: val}}
	case float64:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_DoubleValue{DoubleValue: val}}
	case bool:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_BoolValue{BoolValue: val}}
	default:
		return &relayv1.LabelValue{Value: &relayv1.LabelValue_StringValue{StringValue: ""}}
	}
}

// protoToAnyMap converts a protobuf LabelValue map to map[string]any.
func protoToAnyMap(m map[string]*relayv1.LabelValue) map[string]any {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		switch val := v.GetValue().(type) {
		case *relayv1.LabelValue_StringValue:
			result[k] = val.StringValue
		case *relayv1.LabelValue_IntValue:
			result[k] = val.IntValue
		case *relayv1.LabelValue_DoubleValue:
			result[k] = val.DoubleValue
		case *relayv1.LabelValue_BoolValue:
			result[k] = val.BoolValue
		default:
			result[k] = ""
		}
	}
	return result
}

// anyToStringMap extracts only string-valued entries from a typed map.
func anyToStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}
