// Copyright 2020 QINIU. All rights reserved.

// Package car provides helper fuctions for filecoin package car.
package car

import "errors"

type FcpType string

const (
	Fcp32G FcpType = "32g"
)

var ErrUnknownFcpType = errors.New("Unknown FcpType")

// FilecoinPackageSize returns required size for the package type.
func FilecoinPackageSize(t FcpType) (int64, error) {
	switch t {
	case Fcp32G:
		return 33822867456, nil // 31.5Gi
	}
	return 0, ErrUnknownFcpType
}
