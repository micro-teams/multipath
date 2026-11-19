// The conformance suite, a module of its own so that its test-only fault proxy never becomes part of
// the MultiPath module; it uses the Go module from this repository.
module github.com/micro-teams/multipath/conformance

go 1.24

require github.com/micro-teams/multipath/go v0.0.0

replace github.com/micro-teams/multipath/go => ../go
