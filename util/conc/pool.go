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


package conc

import (
	"sync"
	"sync/atomic"
)

type strent struct {
	sync.RWMutex
	
	key,value string
	
	valid,dead,rem bool
}

// null-pointer-check.
// Returns true if e!=nil and if e is not stale.
func (e *strent) notNil() bool {
	// is e a nil-pointer?
	if e==nil { return false }
	
	// is e stale?
	return !e.rem
}
func (e *strent) keep() bool {
	e.Lock(); defer e.Unlock()
	if e.dead {
		e.rem = true
		return false
	}
	return true
}
func (e *strent) set(v string,ok bool) bool {
	e.Lock(); defer e.Unlock()
	if e.rem { return false }
	e.value = v
	e.valid = ok
	e.dead  = !ok
	return true
}
func (e *strent) get() (v string,ok bool) {
	e.RLock(); defer e.RUnlock()
	return e.value,e.valid
}

func (e *strent) loadOrStore(v string) (string,bool,bool) {
	e.Lock(); defer e.Unlock()
	if e.rem { return "",false,false }
	if e.valid { return e.value,true,true }
	e.value = v
	e.valid = true
	e.dead  = false
	return v,false,true
}


type strmap map[string]*strent

type Strpool struct {
	amended int32
	mu sync.Mutex
	
	// read-map and write-map.
	rm,wm strmap
	
	misses int
}
func (s *Strpool) isAmended() bool { return atomic.LoadInt32(&s.amended)!=0 }

func (s *Strpool) missLocked() {
	s.misses++
	if s.misses < len(s.wm) { return }
	s.rm,s.wm = s.wm,nil
	atomic.StoreInt32(&s.amended,0)
}
func (s *Strpool) dirtyLocked() {
	if s.isAmended() { return }
	s.wm = make(strmap)
	atomic.StoreInt32(&s.amended,1)
	for k,v := range s.rm {
		if v.keep() {
			s.wm[k] = v
		}
	}
}
func (s *Strpool) sync() {
	s.mu.Lock()
	s.mu.Unlock()
}
func (s *Strpool) get(k []byte) *strent {
	if se := s.rm[string(k)]; se.notNil() { return se }
	
	if !s.isAmended() { return nil }
	
	s.mu.Lock(); defer s.mu.Unlock()
	
	// During lock acquisition, wm might have been promoted to rm.
	// That means, we check rm again.
	if se := s.rm[string(k)]; se!=nil { return se }
	
	// Lookup wm.
	se := s.wm[string(k)]
	
	// Record an rm miss.
	s.missLocked()
	
	return se
}

func (s *Strpool) create(k []byte) *strent {
	if se := s.rm[string(k)]; se.notNil() { return se }
	
	s.mu.Lock(); defer s.mu.Unlock()
	
	// During lock acquisition, wm might have been promoted to rm.
	// That means, we check rm again.
	if se := s.rm[string(k)]; se!=nil { return se }
	
	// Lookup wm.
	if se := s.wm[string(k)]; se!=nil { return se }
	
	// Create a dirty map, if none is present.
	s.dirtyLocked()
	
	// Create an empty entry.
	se := new(strent)
	se.key = string(k)
	
	// Add the entry.
	s.wm[se.key] = se
	
	return se
}

func (s *Strpool) Store(k []byte,val string) {
	for {
		ok := s.create(k).set(val,true)
		if ok { return }
		
		// Wait for the writer to finish.
		s.sync()
	}
}
func (s *Strpool) Load(k []byte) (string,bool) {
	e := s.get(k)
	if e==nil { return "",false }
	return e.get()
}
func (s *Strpool) Delete(k []byte) {
	if e := s.get(k); e!=nil { e.set("",false) }
}
func (s *Strpool) LoadOrStore(k []byte,val string) (actual string,loaded bool) {
	var ok bool
	for {
		actual,loaded,ok = s.create(k).loadOrStore(val)
		if ok { return }
		
		// Wait for the writer to finish.
		s.sync()
	}
}

//
