//go:build nopgxregisterdefaulttypes

package gaussdbtype

func registerDefaultPgTypeVariants[T any](m *Map, name string) {
}
