/*
Copyright (c) 2020 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package single

import (
	"errors"
	"unsafe"
)
var (
	EOpNotSupp = errors.New("Operation not supported!")
	EServerAccessDenied = errors.New("Access Denied to Server!")
	EDiskFailure = errors.New("Disk Failure")
	EExist = errors.New("Exists")
	ENotFound = errors.New("Not Found")
	
	EBeingDeleted = errors.New("Busy Being Deleted")
)

func BoilDownError(err error) error {
	return err
}

type ObjectSvc interface{
	PutObj(objectId []byte,data []byte) (err error)
	Append(objectId []byte,data []byte) (pos ByteRange,err error)
	ReadObj(objectId []byte,pos ByteRange, ops *RdOps, dst unsafe.Pointer) (err error)
	DeleteObj(objectId []byte) (err error)
	Info(objectId []byte) (lng int64,err error)
}

///
