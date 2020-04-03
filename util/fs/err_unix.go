// +build !plan9

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


package fs

import (
	"os"
	"syscall"
)

func getErrno(e error) syscall.Errno {
	for {
		switch v := e.(type) {
		case *os.PathError: e = v.Err
		case *os.LinkError: e = v.Err
		case syscall.Errno: return v
		case *syscall.Errno: if v==nil { return 0 }; return *v
		default: return 0
		}
	}
}

func IsIO(e error) bool { return getErrno(e)==syscall.EIO }
func IsNOTDIR(e error) bool { return getErrno(e)==syscall.ENOTDIR }
func IsENAMETOOLONG(e error) bool { return getErrno(e)==syscall.ENAMETOOLONG }
