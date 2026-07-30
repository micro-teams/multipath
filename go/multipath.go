// Package multipath manages redundant network paths to one origin: rank the lines, race reads,
// and replay a failed write over the next line under the same idempotency key.
//
// The implementation is MP-3/MP-4 and follows the TypeScript package deliberately, so that the two
// cannot drift into two different meanings of "a line". The shared contract is the registry JSON.
package multipath
