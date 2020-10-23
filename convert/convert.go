// Copyright 2020 QINIU. All rights reserved.

// Package convert generates filecoin package car.
package convert

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	"github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-ipfs-files"
	pi "github.com/ipfs/go-ipfs-posinfo"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	ibalancer "github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	car "github.com/ipld/go-car"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/multiformats/go-multihash"
)

const defaultHashFunction = uint64(multihash.BLAKE2B_MIN + 31)

const unixfsChunkSize uint64 = 1 << 20
const unixfsLinksPerLevel = 1024

type refNode struct {
	cid cid.Cid

	fullPath string
	offset   uint64
	size     uint64
}

func (n *refNode) RawData() []byte {
	in, err := os.Open(n.fullPath)
	if err != nil {
		panic(err)
	}
	defer in.Close() //nolint:errcheck

	_, err = in.Seek(int64(n.offset), 0)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, n.size)
	_, err = io.ReadFull(in, buf)
	if err != nil {
		panic(err)
	}

	return buf
}

func (n *refNode) Cid() cid.Cid {
	return n.cid
}

func (n *refNode) String() string {
	return fmt.Sprintf("[Block %s]", n.Cid())
}

func (n *refNode) Loggable() map[string]interface{} {
	return map[string]interface{}{
		"path":   n.fullPath,
		"offset": n.offset,
		"size":   n.size,
		"block":  n.Cid().String(),
	}
}

func (n *refNode) Links() []*ipld.Link {
	return nil
}

func (n *refNode) ResolveLink(path []string) (*ipld.Link, []string, error) {
	return nil, nil, dag.ErrLinkNotFound
}

func (n *refNode) Resolve(path []string) (interface{}, []string, error) {
	return nil, nil, dag.ErrLinkNotFound
}

func (n *refNode) Tree(p string, depth int) []string {
	return nil
}

func (n *refNode) Copy() ipld.Node {
	nn := *n
	return &nn
}

func (n *refNode) Size() (uint64, error) {
	return n.size, nil
}

func (n *refNode) Stat() (*ipld.NodeStat, error) {
	return &ipld.NodeStat{
		CumulativeSize: int(n.size),
		DataSize:       int(n.size),
	}, nil
}

type memoryDag struct {
	mu    sync.Mutex
	nodes map[string]ipld.Node
}

func newMemoryDag() *memoryDag {
	return &memoryDag{nodes: make(map[string]ipld.Node)}
}

func (d *memoryDag) Get(ctx context.Context, cid cid.Cid) (ipld.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n, ok := d.nodes[cid.KeyString()]; ok {
		return n, nil
	}
	return nil, ipld.ErrNotFound
}

func (d *memoryDag) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(chan *ipld.NodeOption, len(cids))
	for _, c := range cids {
		if n, ok := d.nodes[c.KeyString()]; ok {
			out <- &ipld.NodeOption{Node: n}
		} else {
			out <- &ipld.NodeOption{Err: ipld.ErrNotFound}
		}
	}
	close(out)
	return out
}

func (d *memoryDag) Add(ctx context.Context, node ipld.Node) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.add(node)
	return nil
}

func (d *memoryDag) AddMany(ctx context.Context, nodes []ipld.Node) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, node := range nodes {
		d.add(node)
	}
	return nil
}

func (d *memoryDag) add(node ipld.Node) {
	if fn, ok := node.(*pi.FilestoreNode); ok {
		sz, err := node.Size()
		if err != nil {
			panic(err)
		}
		rn := &refNode{
			cid:      node.Cid(),
			fullPath: fn.PosInfo.FullPath,
			offset:   fn.PosInfo.Offset,
			size:     sz,
		}
		d.nodes[node.Cid().KeyString()] = rn
	} else {
		d.nodes[node.Cid().KeyString()] = node
	}
}

func (d *memoryDag) Remove(ctx context.Context, c cid.Cid) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.nodes, c.KeyString())
	return nil
}

func (d *memoryDag) RemoveMany(ctx context.Context, cids []cid.Cid) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range cids {
		delete(d.nodes, c.KeyString())
	}
	return nil
}

type blockStore struct {
	d *memoryDag
}

func (s blockStore) Get(cid cid.Cid) (blocks.Block, error) {
	return s.d.Get(context.Background(), cid)
}

func uvarintLen(x uint64) uint64 {
	var buf [binary.MaxVarintLen64]byte
	return uint64(binary.PutUvarint(buf[:], x))
}

func carNodeSize(dagS ipld.DAGService, node ipld.Node) uint64 {
	sz, err := node.Size()
	if err != nil {
		panic(err)
	}
	for _, link := range node.Links() {
		sz -= link.Size
	}

	sz += 1 + 1 + 3 + 1 + 32 // CID
	sz += uvarintLen(sz)

	for _, link := range node.Links() {
		lnode, err := dagS.Get(context.Background(), link.Cid)
		if err != nil {
			panic(err)
		}
		sz += carNodeSize(dagS, lnode)
	}

	return sz
}

func carSize(dagS ipld.DAGService, root ipld.Node) uint64 {
	sz := uint64(0x3d) // HEAD
	sz += carNodeSize(dagS, root)
	return sz
}

func convertToCAR(ctx context.Context, in io.Reader, out io.Writer, noCopy bool) (cid.Cid, uint64, error) {
	dagS := newMemoryDag()

	prefix, err := dag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, 0, err
	}
	prefix.MhType = defaultHashFunction

	params := ihelper.DagBuilderParams{
		Maxlinks:  unixfsLinksPerLevel,
		RawLeaves: true,
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   126,
		},
		Dagserv: dagS,
		NoCopy:  noCopy,
	}

	db, err := params.New(chunk.NewSizeSplitter(in, int64(unixfsChunkSize)))
	if err != nil {
		return cid.Undef, 0, err
	}
	root, err := ibalancer.Layout(db)
	if err != nil {
		return cid.Undef, 0, err
	}

	carsz := carSize(dagS, root)

	// entire DAG selector
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	allSelector := ssb.ExploreRecursive(selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge())).Node()

	sc := car.NewSelectiveCar(ctx, blockStore{dagS}, []car.Dag{{Root: root.Cid(), Selector: allSelector}})
	if err = sc.Write(out); err != nil {
		return root.Cid(), carsz, err
	}

	return root.Cid(), carsz, nil
}

// ConvertToCAR reads input to memory, then write out in car format.
func ConvertToCAR(ctx context.Context, in io.Reader, out io.Writer) (cid.Cid, uint64, error) {
	return convertToCAR(ctx, in, out, false)
}

// FileConvertToCAR converts an input file to a car format output file.
func FileConvertToCAR(ctx context.Context, inPath, outPath string) (cid.Cid, uint64, error) {
	inF, err := os.Open(inPath)
	if err != nil {
		return cid.Undef, 0, err
	}
	defer inF.Close() //nolint:errcheck

	inStat, err := inF.Stat()
	if err != nil {
		return cid.Undef, 0, err
	}

	inFile, err := files.NewReaderPathFile(inPath, inF, inStat)
	if err != nil {
		return cid.Undef, 0, err
	}

	outF, err := os.Create(outPath)
	if err != nil {
		return cid.Undef, 0, err
	}
	defer outF.Close() //nolint:errcheck

	return convertToCAR(ctx, inFile, outF, true)
}
