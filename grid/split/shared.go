package split

import "crypto/sha256"

type TestKey [sha256.Size]byte

type RunKey int64

type TestStatus uint8

type RunTestStatus map[RunKey]map[TestKey]TestStatus
