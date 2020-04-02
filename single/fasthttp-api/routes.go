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


package fhapi

import (
	"github.com/valyala/fasthttp"
	"github.com/byte-mug/hblobstore/single"
	
	fhr "github.com/byte-mug/golibs/radixroute/fasthttpradix"
)

func setError(err error,ctx *fasthttp.RequestCtx, isR bool) {
	ctx.SetBodyString(err.Error())
	switch single.BoilDownError(err) {
	case nil:
		ctx.Response.Header.Add("X-Error","nil_error")
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	case single.EOpNotSupp:
		ctx.SetStatusCode(fasthttp.StatusNotImplemented)
	case single.EServerAccessDenied:
		ctx.Response.Header.Add("X-Error","access_denied")
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	case single.EDiskFailure:
		ctx.Response.Header.Add("X-Error","disk_failure")
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	case single.EBeingDeleted:
		if isR {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
		} else {
			ctx.Response.Header.Add("X-Error","being_deleted")
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		}
	case single.ENotFound:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	case single.EExist:
		ctx.SetStatusCode(fasthttp.StatusConflict)
	}
	ctx.Response.Header.Add("X-Error","unknown_error")
	ctx.SetStatusCode(fasthttp.StatusInternalServerError)
}

type apiOL struct{
	single.ObjectSvc
}
func(h *apiOL) headObject(ctx *fasthttp.RequestCtx) {
	sz,err := h.Info(ctx.UserValue("object").([]byte))
	if err!=nil {
		setError(err,ctx,true)
	} else {
		ctx.Response.Header.AddBytesV("X-Length",fasthttp.AppendUint(make([]byte,0,10),int(sz)))
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func(h *apiOL) getObject(ctx *fasthttp.RequestCtx) {
	var rang single.ByteRange
	rang[0],_ = fasthttp.ParseUint(ctx.Request.Header.Peek("X-Offset"))
	rang[1],_ = fasthttp.ParseUint(ctx.Request.Header.Peek("X-Length"))
	//
	
	err := h.ReadObj(ctx.UserValue("object").([]byte),rang,&ops,asPtr(ctx))
	if err!=nil {
		setError(err,ctx,true)
	} else {
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
}
func(h *apiOL) putObject(ctx *fasthttp.RequestCtx) {
	err := h.PutObj(ctx.UserValue("object").([]byte),ctx.Request.Body())
	if err!=nil {
		setError(err,ctx,false)
	} else {
		ctx.SetBodyString("")
		ctx.SetStatusCode(fasthttp.StatusCreated)
	}
}
func(h *apiOL) postObject(ctx *fasthttp.RequestCtx) {
	rang,err := h.Append(ctx.UserValue("object").([]byte),ctx.Request.Body())
	if err!=nil {
		setError(err,ctx,false)
	} else {
		ctx.Response.Header.AddBytesV("X-Offset",fasthttp.AppendUint(make([]byte,0,10),int(rang[0])))
		ctx.Response.Header.AddBytesV("X-Length",fasthttp.AppendUint(make([]byte,0,10),int(rang[1])))
		ctx.SetBodyString("")
		ctx.SetStatusCode(fasthttp.StatusCreated)
	}
}

func(h *apiOL) deleteObject(ctx *fasthttp.RequestCtx) {
	err := h.DeleteObj(ctx.UserValue("object").([]byte))
	if err!=nil {
		setError(err,ctx,false)
	} else {
		ctx.SetStatusCode(fasthttp.StatusCreated)
	}
}

func RegisterObjectSvc(ol single.ObjectSvc, router *fhr.Router) {
	h := &apiOL{ol}
	router.Handle("OPTIONS","/o/:object",h.headObject  )
	router.Handle("GET"    ,"/o/:object",h.getObject   )
	router.Handle("PUT"    ,"/o/:object",h.putObject   )
	router.Handle("POST"   ,"/o/:object",h.postObject  )
	router.Handle("DELETE" ,"/o/:object",h.deleteObject)
}

///
