package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jsndz/bottle/kv"
)

var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
	ErrConflict      = errors.New("resource conflict: version mismatch")
)

type Resource interface {
	GetMetaData() *Metadata
	GetTypeMeta() *TypeMeta
}

type Registry[T Resource] struct {
	store      *kv.KVStore
	resource   string
	namespaced bool
	validate   func(T) error
	defaulter  func(T)
}

func NewRegistry[T Resource](store *kv.KVStore, resource string, namespaced bool, validate func(T) error, defaulter func(T)) *Registry[T] {
	return &Registry[T]{
		store:      store,
		resource:   resource,
		namespaced: namespaced,
		validate:   validate,
		defaulter:  defaulter,
	}
}

func (r *Registry[T]) Key(meta *Metadata) string {
	if r.namespaced {
		ns := meta.Namespace
		if ns == "" {
			ns = "default"
		}
		return fmt.Sprintf("/registry/%s/%s/%s", r.resource, ns, meta.Name)
	}
	return fmt.Sprintf("/registry/%s/%s", r.resource, meta.Name)
}

func (r *Registry[T]) KeyPath(ns, name string) string {
	if r.namespaced {
		if ns == "" {
			ns = "default"
		}
		return fmt.Sprintf("/registry/%s/%s/%s", r.resource, ns, name)
	}
	return fmt.Sprintf("/registry/%s/%s", r.resource, name)
}

func (r *Registry[T]) KeyPrefix(ns string) string {
	if r.namespaced {
		if ns != "" {
			return fmt.Sprintf("/registry/%s/%s/", r.resource, ns)
		}
	}
	return fmt.Sprintf("/registry/%s/", r.resource)
}

func (r *Registry[T]) Create(obj T) (T, error) {
	if r.defaulter != nil {
		r.defaulter(obj)
	}
	if r.validate != nil {
		if err := r.validate(obj); err != nil {
			return obj, err
		}
	}

	metadata := obj.GetMetaData()
	if metadata.UID == "" {
		metadata.UID = uuid.New().String()
	}

	key := r.Key(metadata)
	if _, exists := r.store.Get(key); exists {
		return obj, ErrAlreadyExists
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return obj, err
	}

	cmd, err := json.Marshal(kv.Command{
		Op:    "PUT",
		Key:   key,
		Value: string(data),
	})
	if err != nil {
		return obj, err
	}

	r.store.Apply(cmd)
	metadata.ResourceVersion = r.store.GlobalRev
	return obj, nil
}

func (r *Registry[T]) Get(ns, name string) (T, error) {
	var zero T
	key := r.KeyPath(ns, name)
	rec, ok := r.store.Get(key)
	if !ok {
		return zero, ErrNotFound
	}

	var obj T
	if err := json.Unmarshal(rec.Value, &obj); err != nil {
		return zero, err
	}
	obj.GetMetaData().ResourceVersion = rec.ModifiedRev
	return obj, nil
}

func (r *Registry[T]) List(ns string) ([]T, error) {
	prefix := r.KeyPrefix(ns)
	recs := r.store.GetPrefix(prefix)

	items := make([]T, 0, len(recs))
	for _, rec := range recs {
		var item T
		if err := json.Unmarshal(rec.Value, &item); err == nil {
			item.GetMetaData().ResourceVersion = rec.ModifiedRev
			items = append(items, item)
		}
	}
	return items, nil
}

func (r *Registry[T]) Update(obj T) (T, error) {
	if r.defaulter != nil {
		r.defaulter(obj)
	}
	if r.validate != nil {
		if err := r.validate(obj); err != nil {
			return obj, err
		}
	}

	metadata := obj.GetMetaData()
	key := r.Key(metadata)

	rec, ok := r.store.Get(key)
	if !ok {
		return obj, ErrNotFound
	}

	if metadata.ResourceVersion != 0 && rec.ModifiedRev != metadata.ResourceVersion {
		return obj, ErrConflict
	}

	data, err := json.Marshal(obj)
	if err != nil {
		return obj, err
	}

	cmd, err := json.Marshal(kv.Command{
		Op:    "PUT",
		Key:   key,
		Value: string(data),
	})
	if err != nil {
		return obj, err
	}

	r.store.Apply(cmd)
	metadata.ResourceVersion = r.store.GlobalRev
	return obj, nil
}

func (r *Registry[T]) Delete(ns, name string) error {
	key := r.KeyPath(ns, name)
	if _, ok := r.store.Get(key); !ok {
		return ErrNotFound
	}

	cmd, err := json.Marshal(kv.Command{
		Op:  "DELETE",
		Key: key,
	})
	if err != nil {
		return err
	}

	r.store.Apply(cmd)
	return nil
}

type PodRegistry struct {
	*Registry[*Pod]
}

func NewPodRegistry(store *kv.KVStore) *PodRegistry {
	return &PodRegistry{
		Registry: NewRegistry[*Pod](store, "pods", true, ValidatePod, DefaultPod),
	}
}

func (r *PodRegistry) ListPods(ns string) (*PodList, error) {
	pods, err := r.List(ns)
	if err != nil {
		return nil, err
	}
	items := make([]Pod, 0, len(pods))
	for _, p := range pods {
		if p != nil {
			items = append(items, *p)
		}
	}
	return &PodList{
		TypeMeta: TypeMeta{Kind: "PodList", APIVersion: "v1"},
		Metadata: ListMeta{ResourceVersion: r.store.GlobalRev},
		Items:    items,
	}, nil
}

type NodeRegistry struct {
	*Registry[*Node]
}

func NewNodeRegistry(store *kv.KVStore) *NodeRegistry {
	return &NodeRegistry{
		Registry: NewRegistry[*Node](store, "nodes", false, ValidateNode, DefaultNode),
	}
}

func (r *NodeRegistry) ListNodes() (*NodeList, error) {
	nodes, err := r.List("")
	if err != nil {
		return nil, err
	}
	items := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n != nil {
			items = append(items, *n)
		}
	}
	return &NodeList{
		TypeMeta: TypeMeta{Kind: "NodeList", APIVersion: "v1"},
		Metadata: ListMeta{ResourceVersion: r.store.GlobalRev},
		Items:    items,
	}, nil
}
