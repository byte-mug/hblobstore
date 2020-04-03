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


package files

import (
	"sync"
	"os"
	"io"
	"path/filepath"
	
	"unsafe"
	
	"github.com/byte-mug/hblobstore/single"
	. "github.com/byte-mug/hblobstore/util/fs"
)

func translate(e error) error {
	if IsIO(e) { return single.EDiskFailure }
	if os.IsExist(e) { return single.EExist }
	if os.IsNotExist(e) { return single.ENotFound }
	if os.IsPermission(e) { return single.EServerAccessDenied }
	if IsNOTDIR(e) { return single.EServerAccessDenied }
	
	return e
}

type singleFile struct {
	sync.WaitGroup
	f *os.File
	l int64
	
	// Append-Mutex
	am sync.Mutex
}
func (s *singleFile) Append(buf []byte) (pos single.ByteRange,err error) {
	var w int
	s.am.Lock(); defer s.am.Unlock()
	w,err = s.f.WriteAt(buf,s.l)
	pos[0] = s.l
	pos[1] = int64(w)
	if err==nil { s.l += int64(w) }
	return
}
func (s *singleFile) CreateContent(buf []byte) (err error) {
	var w int
	s.am.Lock(); defer s.am.Unlock()
	if s.l!=0 { return single.EExist } // We lost the race!
	w,err = s.f.WriteAt(buf,0)
	if err==nil { s.l += int64(w) }
	return
}

func makSF(f *os.File) (*singleFile,error) {
	s,err := f.Stat()
	if err!=nil { return nil,err }
	return &singleFile{
		f:f,
		l:s.Size(),
	},nil
}


type multiFiles struct {
	dir string
	
	// File-Map.
	fm   sync.Map
	fme  sync.Map // Needed for sweeps.
	fmwg sync.WaitGroup
	fml  sync.Mutex
}
func (fs *multiFiles) path(name []byte) string {
	return filepath.Join(fs.dir,"obj-"+string(name)+".bin")
}
func (fs *multiFiles) borrowFile(path string,create int) (*singleFile,error) {
	// Acquire a lightweight shared lock on FMWG.
	fs.fmwg.Add(1); defer fs.fmwg.Done()
	
	// XXX: O_EXCL opens the door for race conditions on the returned
	//      file-object, since, other threads will be capable to use the same
	//      object, potentially undermining the guarantees of O_EXCL.
	
	inst,ok := fs.fm.Load(path)
	if !ok {
		if _,ok = fs.fme.Load(path); ok { return nil,single.EBeingDeleted }
		f,err := os.OpenFile(path,os.O_RDWR|create,0666)
		if err!=nil { return nil,translate(err) }
		sf,err := makSF(f)
		if err!=nil { f.Close(); return nil,translate(err) }
		
		// Insert a new k-v-pair, and on conflict, return the existing value.
		inst,ok = fs.fm.LoadOrStore(path,sf)
		
		// OK==true indicates, that Insert has failed, discard the existing object.
		if ok { f.Close() }
	} else if (create&os.O_EXCL)!=0 {
		// The file had been opened before, so our guarantee, O_EXCL, isn't held.
		return nil,single.EExist
	}
	
	sf := inst.(*singleFile)
	sf.Add(1)
	return sf,nil
}
func (fs *multiFiles) clearFile(path string) (found bool) {
	fs.fml.Lock()
	
	// Load and delete the instance (Pseudo-Atomic).
	inst,ok := fs.fm.Load(path)
	if ok { fs.fm.Delete(path) }
	
	fs.fml.Unlock()
	
	// If the load wasn't successful, return false.
	if !ok { return false }
	
	// Acquire/Release an exclusive lock on FMWG.
	fs.fmwg.Wait()
	
	sf := inst.(*singleFile)
	
	// Wait for all users of the *singleFile to release it.
	sf.Wait()
	
	// Close the file/release the resource.
	sf.f.Close()
	return true
}
func (fs *multiFiles) deleteFile(path string) (found bool,err error) {
	if _,done := fs.fme.LoadOrStore(path,single.EBeingDeleted); done { return }
	defer fs.fme.Delete(path)
	found = fs.clearFile(path)
	err = translate(os.Remove(path))
	return
}
func (fs *multiFiles) PutObj(objectId []byte,data []byte) (err error) {
	var sf *singleFile
	if sf,err = fs.borrowFile(fs.path(objectId),os.O_CREATE|os.O_EXCL); err!=nil { return }
	defer sf.Done()
	return sf.CreateContent(data)
}
func (fs *multiFiles) Append(objectId []byte,data []byte) (pos single.ByteRange,err error) {
	var sf *singleFile
	if sf,err = fs.borrowFile(fs.path(objectId),os.O_CREATE); err!=nil { return }
	defer sf.Done()
	return sf.Append(data)
}

func (fs *multiFiles) ReadObj(objectId []byte,pos single.ByteRange, ops *single.RdOps, dst unsafe.Pointer) (err error) {
	var sf *singleFile
	if sf,err = fs.borrowFile(fs.path(objectId),0); err!=nil { return }
	defer sf.Done()
	
	off := pos.Begin64()
	
	lng,ok := pos.Length64()
	if !ok { lng = sf.l }
	
	_,err = io.Copy(
		ops.GetBodyBuffer(dst),
		io.NewSectionReader(sf.f,off,lng),
	)
	err = translate(err)
	return
}

func (fs *multiFiles) DeleteObj(objectId []byte) (err error) {
	_,err = fs.deleteFile(fs.path(objectId))
	return
}
func (fs *multiFiles) Info(objectId []byte) (lng int64,err error) {
	var sf *singleFile
	if sf,err = fs.borrowFile(fs.path(objectId),0); err!=nil { return }
	defer sf.Done()
	lng = sf.l
	return
}

func ServeFile(dir string) single.ObjectSvc {
	return &multiFiles{dir:dir}
}

///
