package pgshardv1

import (
	"slices"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestDescriptorSetIncludesServices(t *testing.T) {
	want := []string{
		"pgshard.v1.Agent",
		"pgshard.v1.Controller",
		"pgshard.v1.Pooler",
		"pgshard.v1.VStream",
	}
	var got []string
	protoregistry.GlobalFiles.RangeFilesByPackage("pgshard.v1", func(fd protoreflect.FileDescriptor) bool {
		svcs := fd.Services()
		for i := 0; i < svcs.Len(); i++ {
			got = append(got, string(svcs.Get(i).FullName()))
		}
		return true
	})
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("services = %v, want %v", got, want)
	}
}
