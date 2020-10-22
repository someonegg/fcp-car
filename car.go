// Copyright 2020 QINIU. All rights reserved.

// Package car provides helper fuctions for filecoin package car.
package car

type FcpType string

const (
	Fcp32G FcpType = "32g"
)

func FilecoinPackageSize(t FcpType) int64 {
	switch t {
	case Fcp32G:
		return 33822867456 // 31.5Gi
	}
	panic("Unknown FcpType")
}
