package kv

import (
	"bytes"
	"encoding/json"
)

const (
	CompareModRev  = "MOD_REV"
	CompareVersion = "VERSION"
	CompareValue   = "VALUE"
)

type Compare struct {
	Key      string `json:"key"`
	Target   string `json:"target"`
	Operator string `json:"operator"`
	Value    any    `json:"value"`
}

type Op struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type OpResult struct {
	Key    string
	Record *Record
}

type Txn struct {
	If   []Compare `json:"if"`
	Then []Op      `json:"then"`
	Else []Op      `json:"else"`
}

type TxnResponse struct {
	Success   bool
	Rev       int64
	Responses []OpResult
}

func NewTransaction(raw ...string) Txn {
	var t Txn
	if len(raw) > 0 && raw[0] != "" {
		if err := json.Unmarshal([]byte(raw[0]), &t); err == nil {
			if t.If == nil {
				t.If = make([]Compare, 0)
			}
			if t.Then == nil {
				t.Then = make([]Op, 0)
			}
			if t.Else == nil {
				t.Else = make([]Op, 0)
			}
			return t
		}
	}
	t.If = make([]Compare, 0)
	t.Then = make([]Op, 0)
	t.Else = make([]Op, 0)
	return t
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

func toBytes(v any) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		return nil
	}
}

func compareInt64(actual, target int64, op string) bool {
	switch op {
	case "==":
		return actual == target
	case "!=":
		return actual != target
	case "<":
		return actual < target
	case "<=":
		return actual <= target
	case ">":
		return actual > target
	case ">=":
		return actual >= target
	default:
		return false
	}
}

func compareBytes(actual, target []byte, op string) bool {
	cmp := bytes.Compare(actual, target)
	switch op {
	case "==":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

func (e *MVCCEngine) evaluateCompare(comp Compare) bool {
	rec := e.getInternal(comp.Key)

	var modRev int64 = 0
	var version int64 = 0
	var val []byte = nil

	if rec != nil {
		modRev = rec.ModifiedRev
		version = rec.Version
		val = rec.Value
	}

	switch comp.Target {
	case CompareModRev, "ModRev", "mod_rev":
		target, ok := toInt64(comp.Value)
		if !ok {
			return false
		}
		return compareInt64(modRev, target, comp.Operator)

	case CompareVersion, "Version", "version":
		target, ok := toInt64(comp.Value)
		if !ok {
			return false
		}
		return compareInt64(version, target, comp.Operator)

	case CompareValue, "Value", "value":
		target := toBytes(comp.Value)
		return compareBytes(val, target, comp.Operator)

	default:
		return false
	}
}

func (e *MVCCEngine) Txn(txn *Txn) *TxnResponse {
	e.mu.Lock()
	defer e.mu.Unlock()

	success := true
	for _, comp := range txn.If {
		if !e.evaluateCompare(comp) {
			success = false
			break
		}
	}

	ops := txn.Else
	if success {
		ops = txn.Then
	}

	hasMutation := false
	for _, op := range ops {
		switch op.Type {
		case "PUT", "put", "SET", "set", "DELETE", "delete", "DEL", "del":
			hasMutation = true
		}
	}

	commitRev := e.CurrentRev
	if hasMutation {
		e.CurrentRev++
		commitRev = e.CurrentRev
	}

	responses := make([]OpResult, 0, len(ops))
	for _, op := range ops {
		switch op.Type {
		case "PUT", "put", "SET", "set":
			rec := e.putInternal(op.Key, op.Value, commitRev)
			responses = append(responses, OpResult{Key: op.Key, Record: rec})
		case "DELETE", "delete", "DEL", "del":
			prev, ok := e.deleteInternal(op.Key, commitRev)
			if ok {
				responses = append(responses, OpResult{Key: op.Key, Record: prev})
			} else {
				responses = append(responses, OpResult{Key: op.Key, Record: nil})
			}
		case "GET", "get":
			rec := e.getInternal(op.Key)
			responses = append(responses, OpResult{Key: op.Key, Record: rec})
		}
	}

	return &TxnResponse{
		Success:   success,
		Rev:       commitRev,
		Responses: responses,
	}
}
