package core

import (
	"sort"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/topic"
)

// TreeNode summarizes part of the topic namespace.
type TreeNode struct {
	Name string `json:"name"`
	Path string `json:"path"`
	// Topics is the number of topics with a value at or below this node.
	Topics int `json:"topics"`
	// HasValue is true when Path itself is a topic with a value.
	HasValue     bool        `json:"has_value,omitempty"`
	LastUpdate   time.Time   `json:"last_update"`
	Children     []*TreeNode `json:"children,omitempty"`
	MoreChildren int         `json:"more_children,omitempty"`
	children     map[string]*TreeNode
}

// TopicTree summarizes the topics below prefix ("" = everything) as a tree
// limited to depth levels and maxChildren children per node, so large
// namespaces can be explored step by step.
func (s *Service) TopicTree(prefix string, depth, maxChildren int) (*TreeNode, error) {
	prefix = strings.Trim(prefix, "/")
	if depth <= 0 {
		depth = 2
	}
	if depth > 10 {
		depth = 10
	}
	if maxChildren <= 0 {
		maxChildren = 50
	}
	filter := "#"
	if prefix != "" {
		if topic.HasWildcard(prefix) {
			return nil, Invalidf("prefix must be a plain topic prefix without wildcards")
		}
		filter = prefix + "/#"
	}
	entries, _ := s.store.Query(filter, 0)
	root := &TreeNode{Name: prefix, Path: prefix, children: map[string]*TreeNode{}}
	for _, e := range entries {
		ts := time.Unix(0, e.Time).UTC()
		touch(root, ts)
		rel := e.Topic
		if prefix != "" {
			if e.Topic == prefix {
				root.HasValue = true
				continue
			}
			rel = strings.TrimPrefix(e.Topic, prefix+"/")
		}
		levels := strings.Split(rel, "/")
		n := root
		for i := 0; i < len(levels) && i < depth; i++ {
			c := n.children[levels[i]]
			if c == nil {
				p := levels[i]
				if n.Path != "" {
					p = n.Path + "/" + levels[i]
				}
				c = &TreeNode{Name: levels[i], Path: p, children: map[string]*TreeNode{}}
				n.children[levels[i]] = c
			}
			touch(c, ts)
			if i == len(levels)-1 {
				c.HasValue = true
			}
			n = c
		}
	}
	finish(root, maxChildren)
	return root, nil
}

func touch(n *TreeNode, ts time.Time) {
	n.Topics++
	if ts.After(n.LastUpdate) {
		n.LastUpdate = ts
	}
}

func finish(n *TreeNode, max int) {
	for _, c := range n.children {
		n.Children = append(n.Children, c)
	}
	n.children = nil
	sort.Slice(n.Children, func(i, j int) bool { return n.Children[i].Name < n.Children[j].Name })
	if len(n.Children) > max {
		n.MoreChildren = len(n.Children) - max
		n.Children = n.Children[:max]
	}
	for _, c := range n.Children {
		finish(c, max)
	}
}
