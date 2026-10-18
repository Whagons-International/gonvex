package gonvex

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestMutationMiddlewareEnclosesValidationAndHostErrors(t *testing.T) {
	app := NewApp()
	app.Mutation("write", func(_ *MutationCtx, args struct{ Name string }) (string, error) { return args.Name, nil })
	var calls int
	var observed error
	app.UseMutationMiddleware(func(ctx *MutationCtx, path string, raw json.RawMessage, next MutationNext) (any, error) {
		calls++
		result, err := next(ctx, raw)
		observed = err
		return result, err
	})
	ctx := &MutationCtx{}
	if _, err := app.ExecuteMutation(ctx, "write", json.RawMessage(`{"unknown":true}`)); err == nil || observed == nil || calls != 1 {
		t.Fatalf("validation must be observed once: calls=%d err=%v observed=%v", calls, err, observed)
	}
	hostErr := errors.New("commit failed")
	_, err := app.RunMutation(ctx, "write", nil, func(ctx *MutationCtx, raw json.RawMessage) (any, error) {
		if _, err := app.ExecuteMutation(ctx, "write", json.RawMessage(`{"Name":"saved"}`)); err != nil {
			t.Fatal(err)
		}
		return nil, hostErr
	})
	if !errors.Is(err, hostErr) || observed != hostErr || calls != 2 {
		t.Fatalf("outer outcome must be observed without duplicate middleware: calls=%d err=%v observed=%v", calls, err, observed)
	}
	if _, err := app.ExecuteMutation(ctx, "write", nil); err != nil || calls != 3 {
		t.Fatalf("context leaked middleware marker: %d %v", calls, err)
	}
}

func TestMutationMiddlewareCanStripBeforeTypedDecode(t *testing.T) {
	app := NewApp()
	app.Mutation("write", func(_ *MutationCtx, args struct{ Name string }) (string, error) { return args.Name, nil })
	app.UseMutationMiddleware(func(ctx *MutationCtx, _ string, raw json.RawMessage, next MutationNext) (any, error) {
		var args map[string]json.RawMessage
		if err := json.Unmarshal(raw, &args); err != nil {
			return next(ctx, raw)
		}
		delete(args, "support")
		clean, _ := json.Marshal(args)
		return next(ctx, clean)
	})
	result, err := app.ExecuteMutation(nil, "write", json.RawMessage(`{"Name":"saved","support":{"appVersion":"5.3.67"}}`))
	if err != nil || result != "saved" {
		t.Fatalf("result=%v err=%v", result, err)
	}
}
