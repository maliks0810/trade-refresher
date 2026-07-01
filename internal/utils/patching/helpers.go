package patching

import (
	"reflect"
	"testing"

	"github.com/undefinedlabs/go-mpatch"
)
func Patch(t *testing.T, target interface{}, redirection interface{}) (*mpatch.Patch) {
	p, err := mpatch.PatchMethod(target, redirection)
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func PatchInstance(t *testing.T, instance reflect.Type, method string, redirection interface{}) (*mpatch.Patch) {
	p, err := mpatch.PatchInstanceMethodByName(instance, method, redirection)
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func Unpatch(t *testing.T, target *mpatch.Patch) {
	if err := target.Unpatch(); err != nil {
		t.Fatal(err)
	}
}