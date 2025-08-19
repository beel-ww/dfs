package definitions

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

type Chunk [32]byte

type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE
)

const ChunkLength uint8 = 32

type RegInfo struct {
	ClientID    string
	ClientAddr  string
	ClientFiles []string
}

type CheckReq struct {
	Fname    string
	ChunkNum uint8
	Vers     uint16
}

type ChunkReq struct {
	ClientID string
	Fname    string
	ChunkNum uint8
}

type OpenReq struct {
	ClientID string
	Fname    string
	Mode     FileMode
}

type CloseReq struct {
	ClientID string
	Fname    string
}

type DFSServer interface {
	RegisterClient(r RegInfo, succ *bool) (err error)

	FileExists(fname string, exists *bool) (err error)

	CheckVers(c CheckReq, latest *bool) (err error)

	GetChunk(r ChunkReq, reply *Chunk) (err error)

	Heartbeat(clientID string, alive *bool) error

	RemoveClient(clientID string, removed *bool) (err error)
}

type ClientInfo struct {
	RPCClient *rpc.Client
	Files     []string
	WritingTo []string
	LastSeen  time.Time
}

type ChunkInfo struct {
	LatestVers uint16
	Owners     []string
}

type FileInfo struct {
	Lock   *sync.RWMutex
	Chunks map[uint8]ChunkInfo
	Writer string
}

type Server struct {
	Clients     map[string]ClientInfo
	GlobalFiles map[string]FileInfo
}

func (s *Server) RegisterClient(r RegInfo, succ *bool) (err error) {
	client, err := rpc.DialHTTP("tcp", r.ClientAddr)
	if err != nil {
		return err
	}

	s.Clients[r.ClientID] = ClientInfo{
		RPCClient: client,
		Files:     r.ClientFiles,
		LastSeen:  time.Now(),
	}
	*succ = true

	for _, file := range r.ClientFiles {
		_, existsGlobally := s.GlobalFiles[file]
		if !existsGlobally {
			s.GlobalFiles[file] = FileInfo{
				Lock:   new(sync.RWMutex),
				Chunks: make(map[uint8]ChunkInfo),
				Writer: "",
			}
		}
	}

	return nil
}

func (s *Server) FileExists(fname string, exists *bool) (err error) {
	_, ok := s.GlobalFiles[fname]
	*exists = ok

	return nil
}

func (s *Server) CheckVers(c CheckReq, latest *bool) (err error) {
	_, ok := s.GlobalFiles[c.Fname]
	if !ok {
		return errors.New("file does not exist")
	}

	chnk, ok := s.GlobalFiles[c.Fname].Chunks[c.ChunkNum]
	if !ok || c.Vers == chnk.LatestVers {
		*latest = true
		return nil
	}

	*latest = false
	return nil
}

func (s *Server) Open(o OpenReq, succ *bool) (err error) {
	switch o.Mode {
	case READ:
		_, exists := s.GlobalFiles[o.Fname]
		if exists {
			*succ = true
			return nil
		}

		return errors.New("file does not exist yet, and cannot be created in READ mode")
	case WRITE:
		f, exists := s.GlobalFiles[o.Fname]
		if exists {
			if f.Writer != "" && f.Writer != o.ClientID {
				*succ = false
				return fmt.Errorf("file %s is already opened for WRITE by client %s", o.Fname, f.Writer)
			}
			f.Writer = o.ClientID
			s.GlobalFiles[o.Fname] = f
			*succ = true
			return nil
		}

		s.GlobalFiles[o.Fname] = FileInfo{
			Lock:   &sync.RWMutex{},
			Chunks: make(map[uint8]ChunkInfo),
			Writer: o.ClientID,
		}
		*succ = true
		return nil
	}

	return errors.New("bad file open arguments")
}

func (s *Server) Close(r CloseReq, succ *bool) (err error) {
	if s.GlobalFiles[r.Fname].Writer == r.ClientID {
		x := s.GlobalFiles[r.Fname]
		x.Writer = ""
		s.GlobalFiles[r.Fname] = x
	}
	*succ = true

	return nil
}

func (s *Server) GetChunk(r ChunkReq, reply *Chunk) (err error) {
	chnkInfo, ok := s.GlobalFiles[r.Fname].Chunks[r.ChunkNum]
	if !ok {
		return errors.New("chunk does not exist yet")
	}

	s.GlobalFiles[r.Fname].Lock.RLock()
	defer s.GlobalFiles[r.Fname].Lock.RUnlock()
	for _, client := range chnkInfo.Owners {
		c, connected := s.Clients[client]
		if connected && c.RPCClient != nil {
			err := c.RPCClient.Call("DFSClient.GetChunk", r, reply)
			if err != nil {
				return err
			}
			// if successful then it can be assumed the client requesting the chunk has the latest version as well
			newOwner := s.GlobalFiles[r.Fname].Chunks[r.ChunkNum]
			newOwner.Owners = append(newOwner.Owners, r.ClientID)
			s.GlobalFiles[r.Fname].Chunks[r.ChunkNum] = newOwner

			return nil
		}
	}

	return errors.New("currently connected clients do not have the latest chunk")
}

func (s *Server) WriteChunk(r ChunkReq, vers *uint16) (err error) {
	//to-do
	file, exists := s.GlobalFiles[r.Fname]
	if !exists {
		return errors.New("file not registered in server")
	}

	// log.Printf("WriteChunk: Writer=%q, ClientID=%q, Fname=%q, ChunkNum=%d",
	// 	file.Writer, r.ClientID, r.Fname, r.ChunkNum)

	if file.Writer != r.ClientID {
		return errors.New("client is not currently designated as the file's writer")
	}
	file.Lock.Lock()
	defer file.Lock.Unlock()

	info, ok := file.Chunks[r.ChunkNum]
	if !ok {
		info = ChunkInfo{
			LatestVers: 0,
			Owners:     []string{},
		}
	}

	info.LatestVers++
	info.Owners = []string{r.ClientID} //the writer is now currently the only one with the newest version of the chunk

	file.Chunks[r.ChunkNum] = info
	s.GlobalFiles[r.Fname] = file
	*vers = info.LatestVers

	return nil
}

func (s *Server) Heartbeat(clientID string, alive *bool) (err error) {
	client, exists := s.Clients[clientID]
	if !exists {
		return errors.New("client not found")
	}
	client.LastSeen = time.Now()
	s.Clients[clientID] = client
	*alive = true

	return nil
}

func (s *Server) DisconnectClient() {
	timeout := time.Now().Add(-4 * time.Second)

	if len(s.Clients) > 0 {
		for client, info := range s.Clients {
			if info.LastSeen.Before(timeout) && info.RPCClient != nil {
				info.RPCClient = nil
				fmt.Printf("removing client %s and unblocking files it's writing to\n", client)
				s.Clients[client] = info
				for _, file := range info.Files {
					f := s.GlobalFiles[file]
					if f.Writer == client {
						f.Writer = ""
						s.GlobalFiles[file] = f
					}
				}
			}
		}
	}
}
