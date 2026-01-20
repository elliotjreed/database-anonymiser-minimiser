package anonymiser

import (
	"sync"
	"testing"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

func TestNew(t *testing.T) {
	cfg := &config.Config{}
	anon := New(cfg)

	if anon == nil {
		t.Fatal("New() returned nil")
	}
	if anon.config != cfg {
		t.Error("New() did not store config correctly")
	}
	if anon.consistencyMap == nil {
		t.Error("New() did not initialize consistencyMap")
	}
}

func TestAnonymiseRow(t *testing.T) {
	t.Run("no config for table", func(t *testing.T) {
		cfg := &config.Config{}
		anon := New(cfg)

		row := map[string]any{
			"id":    1,
			"name":  "John",
			"email": "john@example.com",
		}

		result := anon.AnonymiseRow("users", row)

		if result["name"] != "John" {
			t.Errorf("name should be unchanged, got %v", result["name"])
		}
		if result["email"] != "john@example.com" {
			t.Errorf("email should be unchanged, got %v", result["email"])
		}
	})

	t.Run("faker template", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"email": "{{faker.email}}",
					},
				},
			},
		}
		anon := New(cfg)

		row := map[string]any{
			"id":    1,
			"email": "john@example.com",
		}

		result := anon.AnonymiseRow("users", row)

		if result["email"] == "john@example.com" {
			t.Error("email should have been anonymised")
		}
		if result["email"] == "" {
			t.Error("email should not be empty after anonymisation")
		}
		if result["id"] != 1 {
			t.Errorf("id should be unchanged, got %v", result["id"])
		}
	})

	t.Run("static value replacement", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"role": "user",
					},
				},
			},
		}
		anon := New(cfg)

		row := map[string]any{
			"id":   1,
			"role": "admin",
		}

		result := anon.AnonymiseRow("users", row)

		if result["role"] != "user" {
			t.Errorf("role = %v, want 'user'", result["role"])
		}
	})

	t.Run("null value replacement", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"phone": "null",
					},
				},
			},
		}
		anon := New(cfg)

		row := map[string]any{
			"id":    1,
			"phone": "123-456-7890",
		}

		result := anon.AnonymiseRow("users", row)

		if result["phone"] != nil {
			t.Errorf("phone = %v, want nil", result["phone"])
		}
	})

	t.Run("empty string rule sets null", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"phone": "",
					},
				},
			},
		}
		anon := New(cfg)

		row := map[string]any{
			"phone": "123-456-7890",
		}

		result := anon.AnonymiseRow("users", row)

		if result["phone"] != nil {
			t.Errorf("phone = %v, want nil", result["phone"])
		}
	})

	t.Run("non-existent column is skipped", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"nonexistent": "{{faker.email}}",
					},
				},
			},
		}
		anon := New(cfg)

		row := map[string]any{
			"id":   1,
			"name": "John",
		}

		result := anon.AnonymiseRow("users", row)

		if result["name"] != "John" {
			t.Errorf("name = %v, want 'John'", result["name"])
		}
		if _, exists := result["nonexistent"]; exists {
			t.Error("nonexistent column should not be added")
		}
	})

	t.Run("consistency mapping", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"email": "{{faker.email}}",
					},
				},
			},
		}
		anon := New(cfg)

		row1 := map[string]any{"email": "same@example.com"}
		row2 := map[string]any{"email": "same@example.com"}
		row3 := map[string]any{"email": "different@example.com"}

		result1 := anon.AnonymiseRow("users", row1)
		result2 := anon.AnonymiseRow("users", row2)
		result3 := anon.AnonymiseRow("users", row3)

		// Same original value should get same anonymised value
		if result1["email"] != result2["email"] {
			t.Errorf("same original value should get same anonymised value: %v != %v",
				result1["email"], result2["email"])
		}

		// Different original value should get different anonymised value
		if result1["email"] == result3["email"] {
			t.Error("different original values should likely get different anonymised values")
		}
	})

	t.Run("multiple columns with different rules", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"email":    "{{faker.email}}",
						"password": "redacted",
						"phone":    "null",
					},
				},
			},
		}
		anon := New(cfg)

		row := map[string]any{
			"id":       1,
			"email":    "john@example.com",
			"password": "secret123",
			"phone":    "123-456-7890",
		}

		result := anon.AnonymiseRow("users", row)

		if result["email"] == "john@example.com" {
			t.Error("email should have been anonymised")
		}
		if result["password"] != "redacted" {
			t.Errorf("password = %v, want 'redacted'", result["password"])
		}
		if result["phone"] != nil {
			t.Errorf("phone = %v, want nil", result["phone"])
		}
		if result["id"] != 1 {
			t.Errorf("id should be unchanged, got %v", result["id"])
		}
	})
}

func TestAnonymiseRow_Concurrent(t *testing.T) {
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]string{
					"email": "{{faker.email}}",
				},
			},
		},
	}
	anon := New(cfg)

	var wg sync.WaitGroup
	iterations := 100

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			row := map[string]any{"email": "test@example.com"}
			anon.AnonymiseRow("users", row)
		}(i)
	}

	wg.Wait()
	// If we get here without a race condition, the test passes
}

func TestShouldTruncate(t *testing.T) {
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"logs":   {Truncate: true},
			"users":  {Truncate: false},
			"orders": {},
		},
	}
	anon := New(cfg)

	tests := []struct {
		table string
		want  bool
	}{
		{"logs", true},
		{"users", false},
		{"orders", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			got := anon.ShouldTruncate(tt.table)
			if got != tt.want {
				t.Errorf("ShouldTruncate(%q) = %v, want %v", tt.table, got, tt.want)
			}
		})
	}
}

func TestGetRetainConfig(t *testing.T) {
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"users":  {Retain: config.RetainConfig{Count: 100}},
			"orders": {Retain: config.RetainConfig{Count: 0}},
			"logs":   {},
		},
	}
	anon := New(cfg)

	tests := []struct {
		table string
		want  int
	}{
		{"users", 100},
		{"orders", 0},
		{"logs", 0},
		{"nonexistent", 0},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			got := anon.GetRetainConfig(tt.table)
			if got.Count != tt.want {
				t.Errorf("GetRetainConfig(%q).Count = %v, want %v", tt.table, got.Count, tt.want)
			}
		})
	}
}

func TestHasAnonymisation(t *testing.T) {
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]string{
					"email": "{{faker.email}}",
				},
			},
			"orders":  {},
			"logs":    {Columns: map[string]string{}},
			"archive": {Columns: nil},
		},
	}
	anon := New(cfg)

	tests := []struct {
		table string
		want  bool
	}{
		{"users", true},
		{"orders", false},
		{"logs", false},
		{"archive", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			got := anon.HasAnonymisation(tt.table)
			if got != tt.want {
				t.Errorf("HasAnonymisation(%q) = %v, want %v", tt.table, got, tt.want)
			}
		})
	}
}

func TestParseFakerTemplate(t *testing.T) {
	tests := []struct {
		template string
		wantFunc string
		wantOK   bool
	}{
		{"{{faker.email}}", "email", true},
		{"{{faker.name}}", "name", true},
		{"{{faker.firstName}}", "firstName", true},
		{"{{faker.uuid}}", "uuid", true},
		{"faker.email", "", false},
		{"{{email}}", "", false},
		{"{faker.email}", "", false},
		{"", "", false},
		{"static value", "", false},
		{"null", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.template, func(t *testing.T) {
			gotFunc, gotOK := ParseFakerTemplate(tt.template)
			if gotFunc != tt.wantFunc || gotOK != tt.wantOK {
				t.Errorf("ParseFakerTemplate(%q) = (%q, %v), want (%q, %v)",
					tt.template, gotFunc, gotOK, tt.wantFunc, tt.wantOK)
			}
		})
	}
}

func TestIsFakerTemplate(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"{{faker.email}}", true},
		{"{{faker.name}}", true},
		{"{{faker.firstName}}", true},
		{"faker.email", false},
		{"{{email}}", false},
		{"{faker.email}", false},
		{"", false},
		{"static value", false},
		{"null", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsFakerTemplate(tt.input)
			if got != tt.want {
				t.Errorf("IsFakerTemplate(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetAnonymisedColumns(t *testing.T) {
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]string{
					"email": "{{faker.email}}",
					"phone": "null",
				},
			},
			"orders": {},
		},
	}
	anon := New(cfg)

	t.Run("table with columns", func(t *testing.T) {
		cols := anon.GetAnonymisedColumns("users")
		if len(cols) != 2 {
			t.Errorf("len(GetAnonymisedColumns) = %d, want 2", len(cols))
		}

		hasEmail := false
		hasPhone := false
		for _, col := range cols {
			if col == "email" {
				hasEmail = true
			}
			if col == "phone" {
				hasPhone = true
			}
		}
		if !hasEmail || !hasPhone {
			t.Error("GetAnonymisedColumns should contain 'email' and 'phone'")
		}
	})

	t.Run("table without columns", func(t *testing.T) {
		cols := anon.GetAnonymisedColumns("orders")
		if cols != nil {
			t.Errorf("GetAnonymisedColumns(orders) = %v, want nil", cols)
		}
	})

	t.Run("nonexistent table", func(t *testing.T) {
		cols := anon.GetAnonymisedColumns("nonexistent")
		if cols != nil {
			t.Errorf("GetAnonymisedColumns(nonexistent) = %v, want nil", cols)
		}
	})
}

func TestClearConsistencyMap(t *testing.T) {
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]string{
					"email": "{{faker.email}}",
				},
			},
		},
	}
	anon := New(cfg)

	// Generate a value and cache it
	row := map[string]any{"email": "test@example.com"}
	result1 := anon.AnonymiseRow("users", row)

	// Clear the map
	anon.ClearConsistencyMap()

	// Same input should now generate a different value
	result2 := anon.AnonymiseRow("users", row)

	// Note: There's a small chance they could be the same randomly
	// but this is very unlikely with email generation
	if result1["email"] == result2["email"] {
		t.Log("Warning: Same value generated after clear (may be coincidental)")
	}
}

func TestValidateRules(t *testing.T) {
	t.Run("valid rules", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"email": "{{faker.email}}",
						"name":  "{{faker.name}}",
						"role":  "user",
						"phone": "null",
					},
				},
			},
		}
		anon := New(cfg)

		errors := anon.ValidateRules()
		if len(errors) != 0 {
			t.Errorf("ValidateRules() returned errors for valid rules: %v", errors)
		}
	})

	t.Run("invalid faker function", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"email": "{{faker.invalidFunc}}",
					},
				},
			},
		}
		anon := New(cfg)

		errors := anon.ValidateRules()
		if len(errors) != 1 {
			t.Errorf("ValidateRules() returned %d errors, want 1", len(errors))
		}
	})

	t.Run("mixed valid and invalid", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {
					Columns: map[string]string{
						"email":   "{{faker.email}}",
						"invalid": "{{faker.unknownFunc}}",
					},
				},
				"orders": {
					Columns: map[string]string{
						"bad": "{{faker.anotherBadFunc}}",
					},
				},
			},
		}
		anon := New(cfg)

		errors := anon.ValidateRules()
		if len(errors) != 2 {
			t.Errorf("ValidateRules() returned %d errors, want 2", len(errors))
		}
	})

	t.Run("nil configuration", func(t *testing.T) {
		cfg := &config.Config{}
		anon := New(cfg)

		errors := anon.ValidateRules()
		if len(errors) != 0 {
			t.Errorf("ValidateRules() returned errors for nil config: %v", errors)
		}
	})

	t.Run("nil table config", func(t *testing.T) {
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": nil,
			},
		}
		anon := New(cfg)

		errors := anon.ValidateRules()
		if len(errors) != 0 {
			t.Errorf("ValidateRules() returned errors for nil table config: %v", errors)
		}
	})
}
