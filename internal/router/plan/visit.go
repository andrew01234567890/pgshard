package plan

import (
	"strconv"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/andrew01234567890/pgshard/internal/pgparser/pg18/pgquerypb"
)

// visit walks node depth-first, calling fn on every Node; fn returns false
// to skip the subtree.
func visit(node *pgquerypb.Node, fn func(*pgquerypb.Node) bool) {
	if node == nil {
		return
	}
	if !fn(node) {
		return
	}
	visitMessage(node.ProtoReflect(), fn)
}

func visitMessage(m protoreflect.Message, fn func(*pgquerypb.Node) bool) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				visitChild(l.Get(i).Message(), fn)
			}
		case fd.Kind() == protoreflect.MessageKind && !fd.IsMap():
			visitChild(v.Message(), fn)
		}
		return true
	})
}

func visitChild(m protoreflect.Message, fn func(*pgquerypb.Node) bool) {
	if n, ok := m.Interface().(*pgquerypb.Node); ok {
		visit(n, fn)
		return
	}
	visitMessage(m, fn)
}

var _ proto.Message = (*pgquerypb.Node)(nil)

func parseInt(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }
