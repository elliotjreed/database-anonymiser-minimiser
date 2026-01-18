package anonymiser

import (
	"github.com/brianvoe/gofakeit/v6"
)

// FakerFunc is a function that generates fake data.
type FakerFunc func() string

// fakerFunctions maps faker template names to their implementations.
var fakerFunctions = map[string]FakerFunc{
	"name":      func() string { return gofakeit.Name() },
	"firstName": func() string { return gofakeit.FirstName() },
	"lastName":  func() string { return gofakeit.LastName() },
	"email":     func() string { return gofakeit.Email() },
	"phone":     func() string { return gofakeit.Phone() },
	"address":   func() string { return gofakeit.Street() },
	"city":      func() string { return gofakeit.City() },
	"country":   func() string { return gofakeit.Country() },
	"company":   func() string { return gofakeit.Company() },
	"uuid":      func() string { return gofakeit.UUID() },
	"username":  func() string { return gofakeit.Username() },
	"password":  func() string { return gofakeit.Password(true, true, true, true, false, 32) },
	"ipv4":      func() string { return gofakeit.IPv4Address() },
	"date":      func() string { return gofakeit.Date().Format("2006-01-02") },
	"text":      func() string { return gofakeit.Sentence(10) },
	"number":    func() string { return gofakeit.DigitN(8) },
}

// GetFakerFunc returns the faker function for a given name.
// Returns nil if the function doesn't exist.
func GetFakerFunc(name string) FakerFunc {
	return fakerFunctions[name]
}

// ListFakerFunctions returns all available faker function names.
func ListFakerFunctions() []string {
	names := make([]string, 0, len(fakerFunctions))
	for name := range fakerFunctions {
		names = append(names, name)
	}
	return names
}

// GenerateFakeValue generates a fake value for the given faker function name.
// Returns empty string if the function doesn't exist.
func GenerateFakeValue(funcName string) string {
	if fn := GetFakerFunc(funcName); fn != nil {
		return fn()
	}
	return ""
}
