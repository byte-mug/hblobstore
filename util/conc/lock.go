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

// This structure works like sync.RWMutex, except, that a writer can yield control
// over a resource at any time.
type FairLock struct{
	rc int64
	rl sync.RWMutex
	wl sync.Mutex
}

// Acquires a shared lock on f.
func (f *FairLock) RLock() {
	atomic.AddInt64(&f.rc,1)
	f.rl.RLock()
}
// Releases a shared lock on f.
func (f *FairLock) RUnlock() {
	f.rl.RUnlock()
	atomic.AddInt64(&f.rc,-1)
}

// Acquires an exclusive lock on f.
func (f *FairLock) Lock() {
	f.wl.Lock()
	f.rl.Lock()
}
// Releases an exclusive lock on f.
func (f *FairLock) Unlock() {
	f.rl.Unlock()
	f.wl.Unlock()
}

// Temporarily yields exclusive the lock. This allows pending readers to access
// the resource, but excludes other writers.
func (f *FairLock) Yield() {
	if atomic.LoadInt64(&f.rc)<1 { return }
	f.rl.Unlock()
	f.rl.Lock()
}


//
