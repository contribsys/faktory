package webui

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"
)

var (
	fileTimestamp = time.Now()
)

type DummyFile struct {
	Path string
	Dir  bool
	Len  int64
}

func (f *DummyFile) Name() string {
	_, name := filepath.Split(f.Path)
	return name
}

func (f *DummyFile) Mode() os.FileMode {
	mode := os.FileMode(0644)
	if f.Dir {
		return mode | os.ModeDir
	}
	return mode
}

func (f *DummyFile) ModTime() time.Time {
	return fileTimestamp
}

func (f *DummyFile) Size() int64 {
	return f.Len
}

func (f *DummyFile) IsDir() bool {
	return f.Mode().IsDir()
}

func (f *DummyFile) Sys() interface{} {
	return nil
}

type AssetFile struct {
	*bytes.Reader
	io.Closer
	*DummyFile
}

func NewAssetFile(name string, content []byte) *AssetFile {
	return &AssetFile{
		bytes.NewReader(content),
		ioutil.NopCloser(nil),
		&DummyFile{name, false, int64(len(content))}}
}

func (f *AssetFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("not a directory")
}

func (f *AssetFile) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *AssetFile) Size() int64 {
	return f.DummyFile.Size()
}

type AssetDirectory struct {
	AssetFile
	ChildrenRead int
	Children     []os.FileInfo
}

func NewAssetDirectory(name string, children []string, fs *AssetFS) *AssetDirectory {
	fileinfos := make([]os.FileInfo, 0, len(children))
	for _, child := range children {
		_, err := fs.AssetDir(filepath.Join(name, child))
		fileinfos = append(fileinfos, &DummyFile{child, err == nil, 0})
	}
	return &AssetDirectory{
		AssetFile{
			bytes.NewReader(nil),
			ioutil.NopCloser(nil),
			&DummyFile{name, true, 0},
		},
		0,
		fileinfos}
}

func (f *AssetDirectory) Readdir(count int) ([]os.FileInfo, error) {
	if count <= 0 {
		return f.Children, nil
	}
	if f.ChildrenRead+count > len(f.Children) {
		count = len(f.Children) - f.ChildrenRead
	}
	rv := f.Children[f.ChildrenRead : f.ChildrenRead+count]
	f.ChildrenRead += count
	return rv, nil
}

func (f *AssetDirectory) Stat() (os.FileInfo, error) {
	return f, nil
}

type AssetFS struct {
	Asset    func(path string) ([]byte, error)
	AssetDir func(path string) ([]string, error)
	Prefix   string
}

func (fs *AssetFS) Open(name string) (http.File, error) {
	name = path.Join(fs.Prefix, name)
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	if children, err := fs.AssetDir(name); err == nil {
		return NewAssetDirectory(name, children, fs), nil
	}
	b, err := fs.Asset(name)
	if err != nil {
		return nil, err
	}
	return NewAssetFile(name, b), nil
}
