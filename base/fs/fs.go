/*
Copyright (c) 2020 byte-mug

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
	"io"
	//"io/ioutil"
	"os"
	"path/filepath"
	
	"github.com/byte-mug/hblobstore/base"
	
)


func translate(e error) error {
	if IsIO(e) { return base.EDiskFailure }
	if os.IsExist(e) { return base.EExist }
	if os.IsNotExist(e) { return base.ENotFound }
	if os.IsPermission(e) { return base.EServerAccessDenied }
	if IsNOTDIR(e) { return base.EServerAccessDenied }
	
	return e
}

type fsfolder struct{
	dir string
}

func (fs *fsfolder) path(name []byte) string {
	return filepath.Join(fs.dir,"obj-"+string(name)+".bin")
}

func ServeFile(dir string) base.ObjectLayer {
	return &fsfolder{dir:dir}
}

func (fs *fsfolder) HeadObject(name []byte) (size int64,err error) {
	i,err := os.Stat(fs.path(name))
	if err!=nil { return -1,translate(err) }
	return i.Size(),nil
}

// Gets the object {name} and puts it into the response body.
// Method should fill the response body.
func (fs *fsfolder) GetObject(name []byte, rang base.ByteRange, rc base.ReqCtx) error {
	f,err := os.Open(fs.path(name))
	if err!=nil { return translate(err) }
	defer f.Close()
	var r io.Reader = f
	bgn := rang.Begin()
	if lng,ok := rang.Length(); ok {
		r = io.NewSectionReader(f,int64(bgn),int64(lng))
	} else if bgn>0 {
		f.Seek(int64(bgn),io.SeekStart)
	}
	
	io.Copy(rc.GetBodyBuffer(),r)
	return nil
}

// Puts the object {name}. The Content is stored in the request context.
func (fs *fsfolder) PutObject(name []byte, rc base.ReqCtx) error {
	f,err := os.OpenFile(fs.path(name),os.O_WRONLY|os.O_CREATE|os.O_EXCL,0666)
	if err!=nil { return translate(err) }
	defer f.Close()
	f.Write(rc.GetRequestBody())
	return nil
}

func (fs *fsfolder) AppendObject(name []byte, rc base.ReqCtx) error {
	f,err := os.OpenFile(fs.path(name),os.O_WRONLY|os.O_CREATE|os.O_APPEND,0666)
	if err!=nil { return translate(err) }
	defer f.Close()
	f.Write(rc.GetRequestBody())
	return nil
}

func (fs *fsfolder) DeleteObject(name []byte) error {
	pth := fs.path(name)
	_,err := os.Lstat(pth)
	if err!=nil { return translate(err) }
	return os.Remove(pth)
}


