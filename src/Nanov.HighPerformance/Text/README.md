# Nanov.HighPerformance.Text

Text and encoding helpers that avoid intermediate string allocations.

- `Ascii.Equals`-style vectorized span-vs-literal comparisons (header names, magic strings).
- UTF-8 encode/decode over `Span<byte>`, bounded-string interning.

Namespace: `Nanov.HighPerformance.Text`
