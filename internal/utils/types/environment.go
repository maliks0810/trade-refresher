package types

// Type Environment is used to standardize the references to the executing Go runtime environment
type Environment string

// IsLocal method returns a boolean indicating if the runtime environment is set to "local"
func (e Environment) IsLocal() bool {
	return e == "local"
}

// IsProduction method returns a boolean indicating if the runtime environment is set to "production"
func (e Environment) IsProduction() bool {
	return e == "production"
}

// String method returns the string representation of the runtime environment
func (e Environment) String() string {
	return string(e)
}