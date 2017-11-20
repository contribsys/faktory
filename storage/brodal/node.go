/*
adapted from https://github.com/cngkaygusuz/BrodalOkasakiHeap

The MIT License (MIT)

Copyright (c) 2015 ckaygusu

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
package brodal

/*
This is the structure that denotes a single node of the Brodal-Okasaki heap.
I liberally added new fields to this struct. The original description does not need that much information per node.
*/
type HeapNode struct {
	// This is the value that a node has.
	value HeapElement

	// Subqueue mechanism implements "data-structure-bootstrapping". While merging, childrenHead field is cleared
	// and moved to subqueueHead.
	subqueueHead *HeapNode

	// Children of a node is held in a doubly-linked-list fashion. The parent has a reference to the head of the list.
	childrenHead *HeapNode

	// Every children has a reference to her parent. Leftsibling is the previous, rightsibling is the next element.
	parent       *HeapNode
	rightsibling *HeapNode
	leftsibling  *HeapNode

	// Rank of a node.
	rank int
}

/*
Create a new node.
*/
func newHeapNode(value HeapElement) *HeapNode {
	return &HeapNode{
		value: value,
	}
}

/*
A node adopts the other node, being its parent.
Here we adjust the necessary fields accordingly to make this connection.
*/
func (bon *HeapNode) adopt(other *HeapNode) {

	// Parent relations
	other.parent = bon

	// Sibling relations
	bon.putNodeAmongChildren(other)
}

/*
Inserting the newly-parented node into the doubly-linked-list of children.
*/
func (bon *HeapNode) putNodeAmongChildren(other *HeapNode) {
	var prev *HeapNode
	var next *HeapNode

	prev = nil
	next = bon.childrenHead

	for next != nil && other.rank > next.rank {
		prev = next
		next = next.rightsibling
	}

	if prev == nil && next == nil {
		// This means children list is empty.
		bon.childrenHead = other
	} else if prev == nil && next != nil {
		// We get to this state when there are only 1 child on the list, and our new node has smaller rank than
		// the existing one.
		bon.childrenHead = other

		other.rightsibling = next
		next.leftsibling = other
	} else if prev != nil && next == nil {
		// We got to the end of the list, our new node has the highest rank.
		prev.rightsibling = other
		other.leftsibling = prev
	} else if prev != nil && next != nil {
		// Standard case, we hit somewhere in between the list.
		prev.rightsibling = other
		next.leftsibling = other

		other.leftsibling = prev
		other.rightsibling = next
	}
}

/*
A node goes rogue, severing its ties with its parent and siblings.
*/
func (bon *HeapNode) rogue() {
	if bon.parent == nil {
		return // If no parent, this node hasn't been adopted yet. No need to go through.
	}
	parent := bon.parent

	if parent.childrenHead == bon {
		parent.childrenHead = bon.rightsibling // This can set "nil" to parent.childrenHead
	} else {
		bon.leftsibling.rightsibling = bon.rightsibling
		if bon.rightsibling != nil {
			bon.rightsibling.leftsibling = bon.leftsibling
		}
	}
	bon.parent = nil
	bon.leftsibling = nil
	bon.rightsibling = nil
}

/*
Essentially same functionality as "rogue", but this one works for subqueued children.
*/
func (bon *HeapNode) rogueSubqueue() {
	if bon.parent == nil {
		return
	}
	parent := bon.parent

	if parent.subqueueHead == bon {
		parent.subqueueHead = bon.rightsibling
	} else {
		bon.leftsibling.rightsibling = bon.rightsibling
		if bon.rightsibling != nil {
			bon.rightsibling.leftsibling = bon.leftsibling
		}
	}
	bon.parent = nil
	bon.leftsibling = nil
	bon.rightsibling = nil
}

/*
Essential skew-linking procedure is described for "Heap.insertSkew"
This function simply performs the linking procedure given the nodes.
*/
func skewLink(firnode *HeapNode, secnode *HeapNode, newnode *HeapNode) *HeapNode {
	if firnode == nil || secnode == nil {
		// This happens when the parent has less than 2 children.
		return newnode
	} else if firnode.rank != secnode.rank {
		// Inequal ranks. We are going to just simply insert the new node.
		return newnode
	} else {
		// Here is the skew linking.
		// Get the minimum valued node among those 3, make her parent and the other two her children.
		currRank := firnode.rank // We can also use secnode to get this value.

		minnode, node1, node2 := minOf3(firnode, secnode, newnode)

		minnode.rogue()
		node1.rogue()
		node2.rogue()

		minnode.adopt(node1)
		minnode.adopt(node2)
		minnode.rank = currRank + 1

		return minnode
	}
}

/*
This function links the two nodes according to binomilal heap linking procedure. The description is given for
"Heap.insertBinomial"
The returning node is assumed to be rogue for code-simplifying reasons.
*/
func simpleLink(existingnode *HeapNode, newnode *HeapNode) *HeapNode {
	existingnode.rogue()

	if existingnode.value.Value() < newnode.value.Value() {
		existingnode.adopt(newnode)
		existingnode.rank++
		return existingnode
	} else {
		newnode.adopt(existingnode)
		newnode.rank++
		return newnode
	}
}

/*
Return the minimum-valued child.
*/
func (bon *HeapNode) getMinChild() *HeapNode {
	if !bon.hasChildren() {
		return nil
	}

	minchild := bon.childrenHead
	checknode := minchild.rightsibling

	for checknode != nil {
		if checknode.value.Value() < minchild.value.Value() {
			minchild = checknode
		}
		checknode = checknode.rightsibling
	}

	return minchild
}

func (bon *HeapNode) hasChildren() bool {
	return bon.childrenHead != nil
}

/*
Return the smallest ranked 2 children of a given node.
The doubly-linked-list of children is rank ordered, so we don't need to do any searching.
*/
func (bon *HeapNode) getSmallestRankChildren() (*HeapNode, *HeapNode) {
	if bon.childrenHead == nil {
		return nil, nil
	} else {
		return bon.childrenHead, bon.childrenHead.rightsibling
	}
}

/*
Simple.
*/
func (bon *HeapNode) getSameRankChild(rank int) *HeapNode {
	child := bon.childrenHead
	for child != nil {
		if child.rank == rank {
			return child
		}
		child = child.rightsibling
	}
	return nil
}

/*
Minimum of 3 algorithm.
First returning value is the minimum, other two are the rest.
I decided to return all of them in a sense, because if we don't do that, we need to do additional work back in the
calling function to determine which one was the smallest.
*/
func minOf3(n1 *HeapNode, n2 *HeapNode, n3 *HeapNode) (*HeapNode, *HeapNode, *HeapNode) {
	if n1.value.Value() < n2.value.Value() {
		if n1.value.Value() < n3.value.Value() {
			return n1, n2, n3
		} else { // n3 <= n1 <= n2
			return n3, n1, n2
		}
	} else { // n2 <= n1 ? n3
		if n2.value.Value() < n3.value.Value() {
			return n2, n1, n3
		} else { // n3 <= n2 <= n1
			return n3, n1, n2
		}
	}
}

/*
Put subqueue elements to a container, and return them.
I use this to iterate over elements. We can't trust traversing the children-linked-list because the operations that
may be performed can modify this list, hence we may not traverse all of the elements.
*/
func (bon *HeapNode) subqueueIterator() []*HeapNode {
	itercont := make([]*HeapNode, 0, 8)
	child := bon.subqueueHead
	for child != nil {
		itercont = append(itercont, child)
		child = child.rightsibling
	}
	return itercont
}

/*
Read subqueueIterator. Same thing for the children.
*/
func (bon *HeapNode) childrenIterator() []*HeapNode {
	itercont := make([]*HeapNode, 0, 8)
	child := bon.childrenHead
	for child != nil {
		itercont = append(itercont, child)
		child = child.rightsibling
	}
	return itercont
}

func (bon *HeapNode) moveChildrenToSubqueue() {
	// There is a bug here. Care to find out?

	bon.subqueueHead = bon.childrenHead
	bon.childrenHead = nil
}
