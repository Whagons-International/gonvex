package manifest

import (
	"strings"
	"testing"
)

func manifestForContractTest() Manifest {
	artifact := validCronArtifact()
	function := artifact.Functions["jobs.run"]
	return Manifest{
		Project: "project-a",
		Functions: map[string]FunctionEntry{
			"jobs.run": {
				Kind: function.Kind, Handler: function.Handler, File: function.File,
				Args: function.Args, Result: function.Result,
			},
		},
		Schema: EmptySchema(),
		Module: &artifact,
	}
}

func TestTypeScriptContractRejectsDivergentFunctionMaps(t *testing.T) {
	current := manifestForContractTest()
	if err := current.ValidateTypeScriptContract(); err != nil {
		t.Fatalf("valid contract: %v", err)
	}
	current.Functions["jobs.run"] = FunctionEntry{
		Kind: FunctionKindReducer, Handler: "run", File: "gonvex/index.ts",
		Args: ModuleSchema{"kind": "any"}, Result: ModuleSchema{"kind": "any"},
	}
	if err := current.ValidateTypeScriptContract(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("divergent contract error = %v", err)
	}
}

func TestModuleArtifactRejectsInvalidPortableSchemasAndReducerPolicies(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ModuleFunction)
		want   string
	}{
		{
			name: "root optional",
			mutate: func(function *ModuleFunction) {
				function.Args = ModuleSchema{"kind": "optional", "value": map[string]any{"kind": "string"}}
			},
			want: "allowed only on object fields",
		},
		{
			name: "missing offline policy",
			mutate: func(function *ModuleFunction) {
				function.Kind = FunctionKindReducer
				function.Dependencies.NonOptimisticReason = "explicit test operation"
			},
			want: "offline policy",
		},
		{
			name: "missing optimistic contract",
			mutate: func(function *ModuleFunction) {
				function.Kind = FunctionKindReducer
				function.Offline = map[string]any{"mode": "forbidden"}
			},
			want: "optimistic transaction or nonOptimisticReason",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := validCronArtifact()
			function := artifact.Functions["jobs.run"]
			test.mutate(&function)
			artifact.Functions["jobs.run"] = function
			artifact.Hash, _ = artifact.ComputedHash()
			if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestModuleArtifactReservesControlPlaneFunctionNamespace(t *testing.T) {
	artifact := validCronArtifact()
	function := artifact.Functions["jobs.run"]
	delete(artifact.Functions, "jobs.run")
	artifact.Functions["control.accounts.me"] = function
	artifact.Hash, _ = artifact.ComputedHash()
	if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "host-reserved Control Plane namespace") {
		t.Fatalf("reserved namespace error = %v", err)
	}
}

func TestModuleArtifactHashCoversSourcesAndFunctionContract(t *testing.T) {
	for _, mutate := range []func(*ModuleArtifact){
		func(artifact *ModuleArtifact) { artifact.Files["gonvex/index.ts"] = "dGFtcGVyZWQ=" },
		func(artifact *ModuleArtifact) {
			function := artifact.Functions["jobs.run"]
			function.Handler = "differentHandler"
			artifact.Functions["jobs.run"] = function
		},
	} {
		artifact := validCronArtifact()
		artifact.Files = map[string]string{"gonvex/index.ts": "ZXhwb3J0IHt9"}
		artifact.Hash, _ = artifact.ComputedHash()
		mutate(&artifact)
		if err := artifact.Validate(); err == nil || !strings.Contains(err.Error(), "canonical contract hash") {
			t.Fatalf("tampered artifact error = %v", err)
		}
	}
}
