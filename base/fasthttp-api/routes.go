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


package fhapi

import (
	"github.com/valyala/fasthttp"
	"github.com/byte-mug/hblobstore/base"
	
	fhr "github.com/byte-mug/golibs/radixroute/fasthttpradix"
)

func statusFrom(err error) int {
	switch base.BoilDownError(err) {
	case nil:
		return fasthttp.StatusOK
	case base.EOpNotSupp:
		return fasthttp.StatusNotImplemented
	case base.EServerAccessDenied,base.EDiskFailure:
		return fasthttp.StatusInternalServerError
	case base.ENotFound:
		return fasthttp.StatusNotFound
	case base.EExist:
		return fasthttp.StatusPreconditionFailed
	}
	return fasthttp.StatusNotFound
}

type apiOL struct{
	base.ObjectLayer
}
func(h *apiOL) headObject(ctx *fasthttp.RequestCtx) {
	
	sz,err := h.HeadObject(ctx.UserValue("object").([]byte))
	if err!=nil {
		ctx.SetBodyString(err.Error())
		ctx.SetStatusCode(statusFrom(err))
	} else {
		ctx.Response.Header.AddBytesV("X-Length",fasthttp.AppendUint(make([]byte,0,10),int(sz)))
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func(h *apiOL) getObject(ctx *fasthttp.RequestCtx) {
	var rang base.ByteRange
	rang[0],_ = fasthttp.ParseUint(ctx.Request.Header.Peek("X-Offset"))
	rang[1],_ = fasthttp.ParseUint(ctx.Request.Header.Peek("X-Length"))
	//
	
	err := h.GetObject(ctx.UserValue("object").([]byte),rang,Wrap(ctx))
	if err!=nil {
		ctx.SetBodyString(err.Error())
		ctx.SetStatusCode(statusFrom(err))
	} else {
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
}
func(h *apiOL) putObject(ctx *fasthttp.RequestCtx) {
	err := h.PutObject(ctx.UserValue("object").([]byte),Wrap(ctx))
	if err!=nil {
		ctx.SetBodyString(err.Error())
		ctx.SetStatusCode(statusFrom(err))
	} else {
		ctx.SetBodyString("")
		ctx.SetStatusCode(fasthttp.StatusCreated)
	}
}

func(h *apiOL) postObject(ctx *fasthttp.RequestCtx) {
	err := h.AppendObject(ctx.UserValue("object").([]byte),Wrap(ctx))
	if err!=nil {
		ctx.SetBodyString(err.Error())
		ctx.SetStatusCode(statusFrom(err))
	} else {
		ctx.SetBodyString("")
		ctx.SetStatusCode(fasthttp.StatusCreated)
	}
}

func(h *apiOL) deleteObject(ctx *fasthttp.RequestCtx) {
	err := h.DeleteObject(ctx.UserValue("object").([]byte))
	if err!=nil {
		ctx.SetBodyString(err.Error())
		ctx.SetStatusCode(statusFrom(err))
	} else {
		ctx.SetStatusCode(fasthttp.StatusAccepted)
	}
}

func methodNotAllowed(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
	ctx.Response.Header.SetContentLength(0)
}

func RegisterBase(router *fhr.Router) {
	go func(){ recover() }()
	router.Handle("HEAD","/*path",methodNotAllowed)
}
func RegisterObjectLayer(ol base.ObjectLayer, router *fhr.Router) {
	h := &apiOL{ol}
	router.Handle("OPTIONS","/o/:object",h.headObject)
	router.Handle("GET","/o/:object",h.getObject)
	router.Handle("PUT","/o/:object",h.putObject)
	router.Handle("POST","/o/:object",h.postObject)
	router.Handle("DELETE","/o/:object",h.deleteObject)
}

