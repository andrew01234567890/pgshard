package controller

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// serverMethods is every method declared on Server in this package. Reflection
// cannot answer this: a method promoted from the embedded
// UnimplementedControllerServer is reported as (*Server).X, with the outer
// type's name, so a declared-but-unhandled RPC is indistinguishable from a
// handled one until a caller gets codes.Unimplemented back.
func serverMethods(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fset := token.NewFileSet()
	out := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			expr := fn.Recv.List[0].Type
			if star, ok := expr.(*ast.StarExpr); ok {
				expr = star.X
			}
			if id, ok := expr.(*ast.Ident); ok && id.Name == "Server" {
				out[fn.Name.Name] = true
			}
		}
	}
	return out
}

// TestEveryControllerRPCIsImplemented fails when the service declares an RPC
// the server does not handle. Server embeds UnimplementedControllerServer, so
// such an RPC compiles and answers codes.Unimplemented at run time: it reads
// as available to anyone generating a client from the proto and then refuses.
// Declaring one is a decision -- implement it, or leave it out of the proto --
// not something for a caller to discover from an error.
func TestEveryControllerRPCIsImplemented(t *testing.T) {
	have := serverMethods(t)
	if len(have) == 0 {
		t.Fatal("no methods on Server were found; the source scan is broken, not the service")
	}
	for _, m := range pgshardv1.Controller_ServiceDesc.Methods {
		if !have[m.MethodName] {
			t.Errorf("rpc %s is declared in the Controller service and implemented nowhere", m.MethodName)
		}
	}
	for _, s := range pgshardv1.Controller_ServiceDesc.Streams {
		if !have[s.StreamName] {
			t.Errorf("streaming rpc %s is declared in the Controller service and implemented nowhere", s.StreamName)
		}
	}
}
