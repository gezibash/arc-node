package relay

import (
	relayv1 "github.com/gezibash/arc/v2/api/arc/relay/v1"
)

// protoToAnyMap converts a protobuf typed_labels map to map[string]any.
func protoToAnyMap(m map[string]*relayv1.LabelValue) map[string]any {
	if len(m) == 0 {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = protoValueToAny(v)
	}
	return result
}

// protoValueToAny converts a single LabelValue to a Go native type.
func protoValueToAny(v *relayv1.LabelValue) any {
	switch val := v.GetValue().(type) {
	case *relayv1.LabelValue_StringValue:
		return val.StringValue
	case *relayv1.LabelValue_IntValue:
		return val.IntValue
	case *relayv1.LabelValue_DoubleValue:
		return val.DoubleValue
	case *relayv1.LabelValue_BoolValue:
		return val.BoolValue
	default:
		return ""
	}
}

// anyToProtoMap converts a map[string]any to protobuf typed_labels map.
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

// anyToProtoValue converts a Go native type to a LabelValue.
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

// mergeProtoLabels builds a map[string]any from proto string labels and typed labels.
// typed_labels take priority over string labels for overlapping keys.
func mergeProtoLabels(stringLabels map[string]string, typedLabels map[string]*relayv1.LabelValue) map[string]any {
	result := make(map[string]any, len(stringLabels)+len(typedLabels))
	for k, v := range stringLabels {
		result[k] = v
	}
	for k, v := range typedLabels {
		result[k] = protoValueToAny(v)
	}
	return result
}
