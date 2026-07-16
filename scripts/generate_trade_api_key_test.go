package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateTradeAPIKeyInstructions(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{input: "local", expected: "local"},
		{input: "sandbox", expected: "sandbox"},
		{input: "development", expected: "development"},
		{input: "dev", expected: "development"},
		{input: "qa", expected: "qa"},
		{input: "QA", expected: "qa"},
		{input: "production", expected: "production"},
		{input: "prod", expected: "production"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			output := runKeyScript(t, test.input)
			for _, expected := range []string{
				"Environment: " + test.expected,
				"Azure Key Vault secret name: trade-refresher-api-key",
				"Azure Key Vault secret value: " + strings.Repeat("a", 64),
			} {
				if !strings.Contains(output, expected) {
					t.Fatalf("output is missing %q:\n%s", expected, output)
				}
			}
		})
	}
}

func TestGenerateTradeAPIKeyDoesNotUseAzureCLI(t *testing.T) {
	contents, err := os.ReadFile("generate-trade-api-key.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"az keyvault", "az login", "--vault-name"} {
		if strings.Contains(string(contents), forbidden) {
			t.Fatalf("script must not modify Azure Key Vault: found %q", forbidden)
		}
	}
}

func runKeyScript(t *testing.T, environment string) string {
	t.Helper()
	directory := t.TempDir()
	openssl := filepath.Join(directory, "openssl")
	contents := "#!/bin/sh\nprintf '%s' '" + strings.Repeat("a", 64) + "'\n"
	if err := os.WriteFile(openssl, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("bash", "generate-trade-api-key.sh", environment)
	command.Env = append(os.Environ(), "PATH="+directory+":"+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s", err, output)
	}
	return string(output)
}
