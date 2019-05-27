// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/crfs/stargz"
	"golang.org/x/sys/unix"
)

const (
	LayerModeMetadata  = "1"
	LayerModeDirectory = "2"
	LayerModeFile      = "4"
)

func createEntry(reader *stargz.Reader, root *stargz.TOCEntry, fullpath, workdir, target string, mode string) error {
	destpath := filepath.Join(target, fullpath)
	skipChmod := false

	if mode != LayerModeDirectory {
		_, err := os.Lstat(destpath)
		if err == nil {
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
	}

	switch root.Type {
	case "reg":
		fr, err := reader.OpenFile(fullpath)
		if err != nil {
			return err
		}
		fw, err := os.OpenFile(destpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(root.Mode))
		if err != nil {
			return err
		}
		defer fw.Close()

		copied, err := io.Copy(fw, fr)
		if err != nil {
			os.Remove(destpath)
			return err
		}
		if copied != root.Size {
			os.Remove(destpath)
			return fmt.Errorf("wrong file size for %s", target)
		}
	case "dir":
		err := os.Mkdir(destpath, os.FileMode(root.Mode))
		if err != nil && !os.IsExist(err) {
			return err
		}
		if mode == LayerModeDirectory {
			wg := sync.WaitGroup{}
			root.ForeachChild(func(baseName string, ent *stargz.TOCEntry) bool {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := createEntry(reader, ent, filepath.Join(fullpath, baseName), workdir, target, LayerModeFile)
					if err != nil {
						fmt.Printf("cannot create %s: %v\n", filepath.Join(fullpath, baseName), err)
					}
				}()
				return true
			})
			wg.Wait()
		}
		skipChmod = true
	case "symlink":
		err := os.Symlink(root.LinkName, destpath)
		if err != nil && !os.IsExist(err) {
			return err
		}
		skipChmod = true
	case "hardlink":
		err := os.Link(root.LinkName, destpath)
		if err != nil && !os.IsExist(err) {
			return err
		}
	case "char":
		err := syscall.Mknod(root.LinkName, unix.S_IFCHR, int(unix.Mkdev(uint32(root.DevMajor), uint32(root.DevMinor))))
		if err != nil && !os.IsExist(err) {
			return err
		}
	case "block":
		err := syscall.Mknod(root.LinkName, unix.S_IFBLK, int(unix.Mkdev(uint32(root.DevMajor), uint32(root.DevMinor))))
		if err != nil && !os.IsExist(err) {
			return err
		}
	case "fifo":
		err := syscall.Mknod(root.LinkName, unix.S_IFIFO, int(unix.Mkdev(uint32(root.DevMajor), uint32(root.DevMinor))))
		if err != nil && !os.IsExist(err) {
			return err
		}
	case "chunk":
		return nil
	}

	if os.Geteuid() == 0 {
		if err := os.Lchown(destpath, root.Uid, root.Gid); err != nil {
			return fmt.Errorf("cannot lchown %s with %d:%d: %v", destpath, root.Uid, root.Gid, err)
		}
	}

	if !skipChmod {
		if err := os.Chmod(destpath, os.FileMode(root.Mode)); err != nil {
			return err
		}
	}

	return nil
}

func serve(sockfd *os.File, reader *stargz.Reader, workdir, target string) error {
	buffer := make([]byte, 4096)
	for {
		l, err := sockfd.Read(buffer)
		if err != nil {
			return err
		}

		parts := strings.Split(string(buffer[:l]), ":")
		if len(parts) != 3 {
			if _, err := sockfd.Write([]byte("1")); err != nil {
				return err
			}
		}
		parentdir, path, mode := parts[0], parts[1], parts[2]

		fullpath := filepath.Join(parentdir, path)

		if fullpath == "." {
			fullpath = ""
		}
		root, found := reader.Lookup(fullpath)
		if !found {
			if _, err := sockfd.Write([]byte("0")); err != nil {
				return err
			}
			continue
		}
		err = createEntry(reader, root, fullpath, workdir, target, mode)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			if _, err := sockfd.Write([]byte("1")); err != nil {
				return err
			}
		}

		if _, err := sockfd.Write([]byte("0")); err != nil {
			return err
		}
	}
	return nil
}

type urlReaderAt struct {
	contentLength int64
	url           string
	cache         []byte
	cacheRange    string
	client        *http.Client
	cond          *sync.Cond
	destFile      string
	done          bool
	chunks        []bool
	requests      chan int64
	destFileFD    *os.File
}

func (r *urlReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	r.cond.L.Lock()
	r.requestChunks(off, int64(len(p)))
	for !r.hasChunks(off, int64(len(p))) {
		r.cond.Wait()
	}
	r.cond.L.Unlock()

	return r.destFileFD.ReadAt(p, off)
}

const chunkSize = int64(1048576)

func (r *urlReaderAt) hasChunks(off, len int64) bool {
	if r.done {
		return true
	}
	for n := off / chunkSize; n <= (off+len)/chunkSize; n++ {
		if !r.chunks[n] {
			return false
		}
	}
	return true
}

func (r *urlReaderAt) requestChunks(off, len int64) {
	for n := off / chunkSize; n <= (off+len)/chunkSize; n++ {
		if !r.chunks[n] {
			r.requests <- n
		}
	}
}

func numChunk(off int64) int64 {
	return off / chunkSize
}

func (r *urlReaderAt) fetchChunk(nChunk int64) error {
	off := nChunk * chunkSize
	len := (nChunk + 1) * chunkSize

	if off+len > r.contentLength {
		len = r.contentLength - off
	}

	rangeVal := fmt.Sprintf("bytes=%d-%d", off, off+len-1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequest("GET", r.url, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Range", rangeVal)

	if r.client == nil {
		r.client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	res, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if location := res.Header.Get("Location"); location != "" {
		r.url = location
		return r.fetchChunk(nChunk)
	}

	buf := make([]byte, len)

	n, err := io.ReadFull(res.Body, buf)
	if err != nil {
		return err
	}

	_, err = r.destFileFD.WriteAt(buf[:n], off)
	return err
}

func (r *urlReaderAt) download() error {
	defer func() {
		r.done = true
		r.cond.L.Lock()
		r.cond.Broadcast()
		r.cond.L.Unlock()
	}()

	genC := make(chan int64)
	done := make(chan interface{})

	go func() {
		// iterate backward as the TOC is at the end
		for nChunk := r.contentLength / chunkSize; nChunk >= int64(0); nChunk-- {
			genC <- nChunk
		}
		done <- true
	}()

	for {
		var nextChunk int64
		select {
		case n := <-r.requests:
			nextChunk = n
		case n := <-genC:
			nextChunk = n
		case <-done:
			return nil
		}

		if r.chunks[nextChunk] {
			continue
		}

		if err := r.fetchChunk(nextChunk); err != nil {
			return err
		}
		r.chunks[nextChunk] = true
		r.cond.L.Lock()
		r.cond.Broadcast()
		r.cond.L.Unlock()
	}
	return nil
}

func openLayer(data, workdir string) (*io.SectionReader, error) {
	if strings.HasPrefix(data, "file://") {
		path := data[len("file://"):]
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		fi, err := f.Stat()
		if err != nil {
			return nil, err
		}
		return io.NewSectionReader(f, 0, fi.Size()), nil
	}
	if strings.HasPrefix(data, "https://") || strings.HasPrefix(data, "http://") {
		res, err := http.Head(data)
		if err != nil {
			return nil, err
		}
		if res.ContentLength == 0 {
			return nil, fmt.Errorf("invalid Content-Length for %s", data)
		}

		m := sync.Mutex{}
		cond := sync.NewCond(&m)
		destFile := filepath.Join(workdir, filepath.Base(data))
		destFileFD, err := os.OpenFile(destFile, os.O_RDWR|os.O_CREATE, 0700)
		chunks := make([]bool, res.ContentLength/chunkSize+1)
		r := &urlReaderAt{url: data, contentLength: res.ContentLength, cond: cond, destFile: destFile, destFileFD: destFileFD, chunks: chunks, requests: make(chan int64, 10)}
		go r.download()

		return io.NewSectionReader(r, 0, res.ContentLength), nil
	}
	return nil, fmt.Errorf("source %s is not supported", data)
}

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "wrong number of args")
		os.Exit(1)
	}

	dataB, err := base64.StdEncoding.DecodeString(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid data %s: %v", os.Args[1], err)
		os.Exit(1)
	}
	workdir := os.Args[2]
	target := os.Args[3]

	data := string(dataB)

	if os.Getenv("_SOCKFD") == "" {
		fmt.Fprintln(os.Stderr, "_SOCKFD not specified")
		os.Exit(1)
	}

	sockfd, err := strconv.Atoi(os.Getenv("_SOCKFD"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid _SOCKFD")
		os.Exit(1)
	}

	sock := os.NewFile(uintptr(sockfd), "SOCK FD")

	sr, err := openLayer(data, workdir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open source %s: %v\n", data, err)
		os.Exit(1)
	}

	r, err := stargz.Open(sr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open stargz file %v: %v\n", data, err)
		os.Exit(1)
	}

	if err := serve(sock, r, workdir, target); err != nil {
		fmt.Fprintln(os.Stderr, "error: %v", err)
		os.Exit(1)
	}
}
