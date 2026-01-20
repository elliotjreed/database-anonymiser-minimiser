package anonymiser

import (
	"regexp"
	"testing"
)

func TestGetFakerFunc(t *testing.T) {
	validFunctions := []string{
		"name", "firstName", "lastName", "email", "phone",
		"address", "city", "country", "company", "uuid",
		"username", "password", "ipv4", "date", "text", "number",
	}

	for _, name := range validFunctions {
		t.Run(name, func(t *testing.T) {
			fn := GetFakerFunc(name)
			if fn == nil {
				t.Errorf("GetFakerFunc(%q) returned nil", name)
			}
		})
	}

	t.Run("invalid function", func(t *testing.T) {
		fn := GetFakerFunc("invalid")
		if fn != nil {
			t.Error("GetFakerFunc(invalid) should return nil")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		fn := GetFakerFunc("")
		if fn != nil {
			t.Error("GetFakerFunc(\"\") should return nil")
		}
	})
}

func TestListFakerFunctions(t *testing.T) {
	functions := ListFakerFunctions()

	expectedCount := 16
	if len(functions) != expectedCount {
		t.Errorf("ListFakerFunctions() returned %d functions, want %d", len(functions), expectedCount)
	}

	// Check for duplicates
	seen := make(map[string]bool)
	for _, fn := range functions {
		if seen[fn] {
			t.Errorf("Duplicate function name: %s", fn)
		}
		seen[fn] = true
	}

	// Verify all listed functions are valid
	for _, fn := range functions {
		if GetFakerFunc(fn) == nil {
			t.Errorf("Listed function %q is not valid", fn)
		}
	}
}

func TestGenerateFakeValue(t *testing.T) {
	tests := []struct {
		funcName string
		validate func(string) bool
		desc     string
	}{
		{
			funcName: "name",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "firstName",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "lastName",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "email",
			validate: func(s string) bool {
				return regexp.MustCompile(`^[^@]+@[^@]+\.[^@]+$`).MatchString(s)
			},
			desc: "should return valid email format",
		},
		{
			funcName: "phone",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "address",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "city",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "country",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "company",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "uuid",
			validate: func(s string) bool {
				return regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(s)
			},
			desc: "should return valid UUID format",
		},
		{
			funcName: "username",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "password",
			validate: func(s string) bool { return len(s) >= 32 },
			desc:     "should return password of at least 32 characters",
		},
		{
			funcName: "ipv4",
			validate: func(s string) bool {
				return regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`).MatchString(s)
			},
			desc: "should return valid IPv4 format",
		},
		{
			funcName: "date",
			validate: func(s string) bool {
				return regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(s)
			},
			desc: "should return valid date format (YYYY-MM-DD)",
		},
		{
			funcName: "text",
			validate: func(s string) bool { return len(s) > 0 },
			desc:     "should return non-empty string",
		},
		{
			funcName: "number",
			validate: func(s string) bool {
				return regexp.MustCompile(`^\d{8}$`).MatchString(s)
			},
			desc: "should return 8-digit number string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.funcName, func(t *testing.T) {
			value := GenerateFakeValue(tt.funcName)
			if !tt.validate(value) {
				t.Errorf("GenerateFakeValue(%q) = %q, %s", tt.funcName, value, tt.desc)
			}
		})
	}

	t.Run("invalid function returns empty", func(t *testing.T) {
		value := GenerateFakeValue("invalid")
		if value != "" {
			t.Errorf("GenerateFakeValue(invalid) = %q, want empty string", value)
		}
	})
}

func TestGenerateFakeValue_Uniqueness(t *testing.T) {
	// Test that generated values vary (not always the same)
	// Note: There's a small theoretical chance of collision, but very unlikely

	t.Run("uuid uniqueness", func(t *testing.T) {
		seen := make(map[string]bool)
		for i := 0; i < 100; i++ {
			val := GenerateFakeValue("uuid")
			if seen[val] {
				t.Errorf("Duplicate UUID generated: %s", val)
			}
			seen[val] = true
		}
	})

	t.Run("email variation", func(t *testing.T) {
		seen := make(map[string]bool)
		for i := 0; i < 10; i++ {
			val := GenerateFakeValue("email")
			seen[val] = true
		}
		// We should get at least some variety
		if len(seen) < 5 {
			t.Error("Expected more variation in generated emails")
		}
	})
}

func TestFakerFunctionDirectCalls(t *testing.T) {
	// Test that each function in fakerFunctions can be called directly
	for name, fn := range fakerFunctions {
		t.Run(name, func(t *testing.T) {
			result := fn()
			if result == "" {
				t.Errorf("faker function %q returned empty string", name)
			}
		})
	}
}
