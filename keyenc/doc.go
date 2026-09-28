// Package keyenc encodes key components so the byte order of a composite
// key is the order of its values: AppendString and AppendFloat64 build the
// components, String and Float64 read them back, and PrefixEnd bounds a
// prefix scan.
package keyenc
