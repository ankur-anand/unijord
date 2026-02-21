package postgres

// ToastConfig controls TOAST placeholder behavior.
type ToastConfig struct {
	Placeholder string `json:"placeholder" yaml:"placeholder" toml:"placeholder"`
}

func (t ToastConfig) PlaceholderValue() string {
	if t.Placeholder != "" {
		return t.Placeholder
	}
	return "__unijord_cdc_unavailable_value"
}
