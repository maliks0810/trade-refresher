package utils

func SliceFind[T any](a []T, fn func(u *T) bool) *T {
	for k, v := range a {
		if fn(&v) {
			return &a[k]
		}
	}
	var empty T
	return &empty
}
