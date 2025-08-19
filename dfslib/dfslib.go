/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	cs "github.com/beel-ww/dfs/definitions"
)

// A Chunk is the unit of reading/writing in DFS.
type Chunk [32]byte

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", e)
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	// - WriteModeTimeoutError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {
	if _, err := os.Stat(localPath); err != nil {
		return nil, LocalPathError(localPath)
	}
	files, err := localFiles(localPath)
	if err != nil {
		panic(err)
	}

	client := &DFSClient{
		ClientID:   genID(),
		LocalPath:  localPath,
		ServerAddr: serverAddr,
		LocalFiles: files,
	}

	c, err := rpc.DialHTTP("tcp", serverAddr)
	if err != nil {
		client.Connected = false
		client.DFSClient = nil
	}
	// Start RPC listener so this client can serve chunks
	if err := client.Listen(net.JoinHostPort(localIP, "8888")); err != nil {
		return client, err
	}

	client.Connected = true
	client.DFSClient = c

	var done bool
	if client.Connected {
		err = c.Call("Server.RegisterClient", cs.RegInfo{
			ClientID:    client.ClientID,
			ClientAddr:  net.JoinHostPort(localIP, "8888"),
			ClientFiles: files,
		}, &done)

		if !done || err != nil {
			fmt.Println("failed to register, server will not be able to make calls to this client")
		}
	}
	go func() {
		for range 2 * time.Second {
			var alive bool
			err := client.DFSClient.Call("Server.Heartbeat", client.ClientID, &alive)
			if !alive || err != nil {
				client.Connected = false
			}
		}
	}()
	return client, nil
}

type DFSClient struct {
	ClientID   string
	LocalPath  string
	ServerAddr string
	LocalFiles []string
	Connected  bool
	DFSClient  *rpc.Client
}

func (d *DFSClient) Listen(addr string) error {
	err := rpc.Register(d)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	fmt.Println("listening to server")

	return nil
}

func (d *DFSClient) LocalFileExists(fname string) (exists bool, err error) {
	okName := nameCheck(fname)
	if !okName {
		return false, BadFilenameError(fname)
	}

	// absPath := filepath.Join(d.LocalPath, fname+".dfs")
	// _, err = os.Stat(absPath)
	// if os.IsNotExist(err) {
	// 	return false, nil
	// } else if err != nil {
	// 	return false, err
	// }

	return slices.Contains(d.LocalFiles, fname), nil
}

func (d *DFSClient) GlobalFileExists(fname string) (exists bool, err error) {
	okName := nameCheck(fname)
	if !okName {
		return false, BadFilenameError(fname)
	}

	if !d.Connected {
		return false, DisconnectedError(d.ServerAddr)
	}
	err = d.DFSClient.Call("Server.FileExists", fname, &exists)
	if err != nil {
		return exists, err
	}

	return exists, nil
}

func (d *DFSClient) Open(fname string, mode FileMode) (f DFSFile, err error) {
	okName := nameCheck(fname)
	if !okName {
		return nil, BadFilenameError(fname)
	}

	switch mode {
	case READ, WRITE:
		var succ bool
		err := d.DFSClient.Call("Server.Open", cs.OpenReq{
			ClientID: d.ClientID,
			Fname:    fname,
			Mode:     cs.FileMode(mode),
		}, &succ)
		if err != nil {
			return nil, fmt.Errorf("server rejected open: %w", err)
		}
		if !succ {
			return nil, fmt.Errorf("open rejected: file may already have a writer or not exist in READ mode")
		}

		contents, err := os.OpenFile(filepath.Join(d.LocalPath, fname+".dfs"), os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}

		// Load version map
		chunkVMap, err := versMap(filepath.Join(d.LocalPath, fname))
		if err != nil {
			return nil, err
		}

		return &DFile{
			Mode:   mode,
			DaFile: contents,
			Client: d,
			VMap:   chunkVMap,
		}, nil
	case DREAD:
		exists, err := d.LocalFileExists(fname + ".dfs")
		if err != nil || !exists {
			return nil, FileDoesNotExistError(fname)
		}

		contents, err := os.OpenFile(filepath.Join(d.LocalPath, fname+".dfs"), os.O_RDONLY, 0444)
		if err != nil {
			panic(err)
		}

		m, err := versMap(filepath.Join(d.LocalPath, fname))
		if err != nil {
			return nil, err
		}
		return &DFile{
			Mode:   mode,
			DaFile: contents,
			Client: nil,
			VMap:   m,
		}, nil
	}

	return nil, BadFileModeError(mode)
}

func (d *DFSClient) UMountDFS() error {
	if !d.Connected {
		return DisconnectedError(d.ServerAddr)
	}

	d.DFSClient.Close()
	return nil
}

func (d *DFSClient) GetChunk(r cs.ChunkReq, chunk *Chunk) (err error) {
	abspath := filepath.Join(d.LocalPath, r.Fname+".dfs")
	f, err := os.OpenFile(abspath, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}
	defer f.Close()

	var b Chunk
	_, err = f.ReadAt(b[:], int64(r.ChunkNum*cs.ChunkLength))
	if err != nil {
		return err
	}
	*chunk = b
	return nil
}

type DFile struct {
	Mode   FileMode
	DaFile *os.File
	Client *DFSClient
	VMap   map[uint8]uint16
}

func (f *DFile) Read(chunkNum uint8, chunk *Chunk) (err error) {
	switch f.Mode {
	case READ, WRITE:
		if !f.Client.Connected {
			return DisconnectedError(f.Client.ServerAddr)
		}
		//to-do
		fmt.Printf("attempting to read %s", f.DaFile.Name())
		var latest bool
		f.Client.DFSClient.Call("Server.CheckVers", cs.CheckReq{
			Fname:    strings.TrimSuffix(f.DaFile.Name(), ".dfs"),
			ChunkNum: chunkNum,
			Vers:     f.VMap[chunkNum],
		}, &latest)

		var data Chunk
		if !latest {
			err := f.Client.DFSClient.Call("Server.GetChunk", cs.ChunkReq{
				Fname:    strings.TrimSuffix(f.DaFile.Name(), ".dfs"),
				ChunkNum: chunkNum,
			}, &data)
			if err != nil {
				return err
			}
			*chunk = data
			return nil
		}
		_, err := f.DaFile.ReadAt(data[:], int64(chunkNum)*int64(cs.ChunkLength))
		if err != nil && err != io.EOF {
			panic(err)
		}
		*chunk = data
		fmt.Println(data)

		return nil
	case DREAD:
		//to-do
		_, existLocally := f.VMap[chunkNum]
		if !existLocally {
			return ChunkUnavailableError(chunkNum)
		}
		var data Chunk
		_, err := f.DaFile.ReadAt(data[:], int64(cs.ChunkLength)*int64(chunkNum))
		if err != nil {
			return err
		}
		*chunk = data
		return nil
	}
	return ChunkUnavailableError(chunkNum)
}

func (f *DFile) Write(chunkNum uint8, chunk *Chunk) (err error) {
	if !f.Client.Connected {
		return DisconnectedError(f.Client.ServerAddr)
	}

	switch f.Mode {
	case READ, DREAD:
		return BadFileModeError(f.Mode)
	case WRITE:
		//to-do
		_, err := f.DaFile.WriteAt(chunk[:], int64(chunkNum)*int64(cs.ChunkLength))
		if err != nil {
			panic(err)
		}

		var vers *uint16
		err = f.Client.DFSClient.Call("Server.WriteChunk", cs.ChunkReq{
			ClientID: f.Client.ClientID,
			Fname:    strings.TrimSuffix(f.DaFile.Name(), ".dfs"),
			ChunkNum: chunkNum,
		}, &vers)
		if err != nil {
			return err
		}

		v := *vers
		f.VMap[chunkNum] = v
	}
	return nil
}

func (f *DFile) Close() (err error) {
	switch f.Mode {
	case READ, WRITE:
		var succ bool
		err = f.Client.DFSClient.Call("Server.Close", cs.CloseReq{
			ClientID: f.Client.ClientID,
			Fname:    strings.TrimSuffix(f.DaFile.Name(), ".dfs"),
		}, &succ)
		if err != nil || !succ {
			fmt.Println("the server failed to register the file close")
		}
		saveMap(filepath.Join(f.Client.LocalPath, strings.TrimSuffix(f.DaFile.Name(), ".dfs")), f.VMap)
		err = f.DaFile.Close()
		return err
	case DREAD:
		saveMap(filepath.Join(f.Client.LocalPath, strings.TrimSuffix(f.DaFile.Name(), ".dfs")), f.VMap)
		err = f.DaFile.Close()
		return err
	}

	return errors.New("file didn't close")
}

// helpers
func localFiles(absPath string) (files []string, err error) {
	var clientFiles []string
	allFiles, err := os.ReadDir(absPath)
	if err != nil {
		return nil, err
	}
	for _, file := range allFiles {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".dfs") {
			name := strings.TrimSuffix(file.Name(), ".dfs")
			clientFiles = append(clientFiles, name)
		}
	}

	return clientFiles, nil
}

var okName = regexp.MustCompile(`^[a-z0-9]{1,16}$`)

func nameCheck(fname string) bool {
	return okName.MatchString(fname)
}

func genID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal("rand.Read somehow returned an error")
	}

	return hex.EncodeToString(b)
}

// used for saving chunk versions map to disk
func versMap(fname string) (vmap map[uint8]uint16, err error) {
	_, ferr := os.Stat(fname)
	if os.IsNotExist(ferr) {
		return map[uint8]uint16{}, nil
	}

	var m map[uint8]uint16
	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&m); err != nil {
		if err == io.EOF {
			return make(map[uint8]uint16), nil
		} else {
			return nil, fmt.Errorf("failed to decode version map")
		}
	}

	return m, nil
}

// used for saving chunk versions map to disk
// func saveMap(fname string, vmap map[uint8]uint16) {
// 	file, err := os.Create(fname)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()

//		encoder := gob.NewEncoder(file)
//		if err := encoder.Encode(vmap); err != nil {
//			log.Fatal("failed to save version map to file")
//		}
//	}

func saveMap(fname string, vmap map[uint8]uint16) error {
	file, err := os.Create(fname)
	if err != nil {
		return fmt.Errorf("failed to create version map file: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(vmap); err != nil {
		return fmt.Errorf("failed to encode version map: %w", err)
	}

	// Ensure contents are written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync version map file: %w", err)
	}

	return nil
}
