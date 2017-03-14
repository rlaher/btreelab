#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) :
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize,
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique)
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now

  //Calculate max number of keys per block
  SIZE_T blockSize = buffercache->GetBlockSize();
  maxNumKeys = (blockSize - sizeof(NodeMetadata))/(16);
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) {
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) {
      return rc;
    }

    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) {
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) {
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;

      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock

  return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc; // error checker
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) {
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) {
      	if (op==BTREE_OP_LOOKUP) {
      	  return b.GetVal(offset,value);
	  } else {
	  // BTREE_OP_UPDATE
      rc=b.SetVal(offset,value);
      if (rc) {  return rc; }

      rc=b.Serialize(buffercache,node);
      if (rc) {  return rc; }

	    return ERROR_NOERROR;
	}
      }
    }
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) {
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) {
      } else {
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) {
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) {
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) {
      if (offset==0) {
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) {
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) {
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) {
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) {
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) {
    os << "\" ]";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // ROHAN TAKE 1

  // Creating B+ tree without linking leaf nodes.
  // Will link leaf nodes for extra credit

  ERROR_T rc;
  VALUE_T val = value;

  // Lookup to see if value exists.
  // If it does, rc will return NOERROR -> return ERROR_CONFLICT due to duplicate key
  // If lookup returns NONEXISTENT, then we can insert value into tree
  rc = LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, val);
  if (rc==ERROR_NOERROR) {
    return ERROR_CONFLICT;
  }
  else if (rc!=ERROR_NONEXISTENT) {
    return rc;
  }

  BTreeNode leafNode;
  BTreeNode rootNode;
  BTreeNode rightLeafNode;
  SIZE_T leafPtr;
  SIZE_T rightLeafPtr;
  rootNode.Unserialize(buffercache,superblock.info.rootnode); // Set root.

  // If no keys exist in tree yet
  if(rootNode.info.numkeys == 0) {
    AllocateNode(leafPtr); // Allocate a new block
    leafNode = BTreeNode(BTREE_LEAF_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);
    leafNode.Serialize(buffercache,leafPtr);
    rc = leafNode.Unserialize(buffercache,leafPtr);
    if (rc) { return rc; }

    // Insert value into node
    leafNode.SetKey(0, key); // Assign key to offset 0 within LeafNode
    leafNode.SetVal(0, value); // Assign value to offset 0 within leafNode
    leafNode.info.numkeys++;
    leafNode.Serialize(buffercache,leafPtr); // Serialize again and write updated valuesize

    // Link leafNode to root of tree
    rc = rootNode.Unserialize(buffercache, superblock.info.rootnode);
    if (rc) { return rc; }
    rootNode.SetKey(0,key);
    rootNode.SetPtr(0,leafPtr);
    rootNode.info.numkeys++;

    // Create a node to the right of new leafNode
    AllocateNode(rightLeafPtr);
    rightLeafNode = BTreeNode(BTREE_LEAF_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);
    rc = rightLeafNode.Serialize(buffercache,rightLeafPtr);
    if (rc) { return rc; }

    // Connect rightLeafNode to root
    rootNode.SetPtr(1,rightLeafPtr);
    rc = rootNode.Serialize(buffercache,superblock.info.rootnode);
    if (rc) { return rc; }
  }

  // If tree already exists
  else {
    // Get leafNode from last pointer where we want to insert key
    std::vector<SIZE_T> ptrTrail; // Follow pointers to spot for insertion
    ptrTrail.push_back(superblock.info.rootnode); // Dynamically resize and add root to end
    CreatePtrTrail(superblock.info.rootnode,key,ptrTrail);
    leafPtr = ptrTrail.back();
    ptrTrail.pop_back();

    rc = leafNode.Unserialize(buffercache, leafPtr);
    if(rc) { return rc; }

    // Walk across the leafNode & increment key count
    leafNode.info.numkeys++;

    // If that was the only key in the leafNode
    if (leafNode.info.numkeys == 1) {
      rc = leafNode.SetKey(0,key);
      if(rc) { return rc; }
      rc = leafNode.SetVal(0,value);
      if(rc) { return rc; }
    }
    // If there were other keys in the leafNode
    else {
      KEY_T testkey;
      KEY_T keyInsPos;
      VALUE_T valueInsPos;
      bool inserted = false;
      // Loop through leafNode to find spot to insert
      for (SIZE_T offset=0; offset<leafNode.info.numkeys-1; offset++) {
        rc=leafNode.GetKey(offset,testkey);
        if(rc) { return rc; }
        if(key<testkey) {
          // Shift over all following keys by 1 space
          for (unsigned int offset2=leafNode.info.numkeys-2; offset2>=offset; offset2--){
              rc = leafNode.GetKey((SIZE_T)offset2, keyInsPos);
              if (rc) { return rc; }
              rc = leafNode.GetVal((SIZE_T)offset2, valueInsPos);
              if (rc) { return rc; }
              rc = leafNode.SetKey((SIZE_T)offset2+1, keyInsPos);
              if (rc) { return rc; }
              rc = leafNode.SetVal((SIZE_T)offset2+1, valueInsPos);
              if (rc) { return rc; }
          }

          // Insert new key in spot found above
          rc = leafNode.SetKey(offset,key);
          if (rc) { return rc; }
          rc = leafNode.SetVal(offset,value);
          if (rc) { return rc; }
          inserted = true;
          rc = leafNode.SetKey(offset,key);
          if (rc) { return rc; }
          rc = leafNode.SetVal(offset, value);
          if (rc) { return rc;}
          break; // Remove?
        }
      }
      if (!inserted) {
        rc=leafNode.SetKey(leafNode.info.numkeys-1,key);
        if (rc) { return rc; }
        rc=leafNode.SetVal(leafNode.info.numkeys-1,value);
        if (rc) { return rc; }
      }
    }

    leafNode.Serialize(buffercache, leafPtr); // Write back to disk
    // Check if the node length is over 2/3, and call TreeBalance if necessary
      if((int)leafNode.info.numkeys > (int)(2*maxNumKeys/3)) {
          SIZE_T parentPtr = ptrTrail.back();
          ptrTrail.pop_back();
          rc = TreeBalance(parentPtr, ptrTrail);
          if (rc) { return rc; }
      }
    }

  return ERROR_NOERROR;
}

// Return path to node where key should go
ERROR_T BTreeIndex::CreatePtrTrail(const SIZE_T &node, const KEY_T &key, std::vector<SIZE_T> &ptrTrail){
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc = b.Unserialize(buffercache, node);

  if(rc!=ERROR_NOERROR){
    return rc;
  }

  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      // Scan through key/ptr pairs and recurse if possible
    for(offset=0;offset<b.info.numkeys; offset++){
      rc=b.GetKey(offset,testkey);
      if(rc) { return rc; }
      if(key < testkey){
            // OK, so we now have the first key that's larger
            // so we ned to recurse on the ptr immediately previous to
            // this one, if it exists
        rc=b.GetPtr(offset,ptr);
        if (rc) { return rc; }
          //If there is no error on finding the appropriate pointer, push it onto our stack.
        ptrTrail.push_back(ptr);
        return CreatePtrTrail(ptr, key, ptrTrail);
      }
    }

      //if we get here, we need to go to the next pointer, if it exists.
    if(b.info.numkeys>0){
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
        //If there is no error on finding the appropriate pointer, push it onto our stack.
      ptrTrail.push_back(ptr);
      return CreatePtrTrail(ptr, key, ptrTrail);
    } else {
        // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
    case BTREE_LEAF_NODE:
    ptrTrail.push_back(node);
    return ERROR_NOERROR;
    break;
    default:
        // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;

}

//TreeBalance takes a path of pointers and a node at the bottom of that path. It will split the node and recursively walk up the parent path
// guaranteeing the sanity of each parent.
ERROR_T BTreeIndex::TreeBalance(const SIZE_T &node, std::vector<SIZE_T> ptrPath)
{
  BTreeNode b;
  BTreeNode leftNode;
  BTreeNode rightNode;
  ERROR_T rc;
  SIZE_T offset;

  int newType;
  rc = b.Unserialize(buffercache, node);
  if (rc) { return rc;}

  //Allocate 2 new nodes, fill them from the place you're splitting
  SIZE_T leftPtr;
  SIZE_T rightPtr;
  AllocateNode(leftPtr);
  if(b.info.nodetype == BTREE_LEAF_NODE){
    newType = BTREE_LEAF_NODE;
  }else{
    newType = BTREE_INTERIOR_NODE;
  }
  leftNode = BTreeNode(newType, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  rc = leftNode.Serialize(buffercache, leftPtr);
  AllocateNode(rightPtr);
  rightNode = BTreeNode(newType, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  rc = rightNode.Serialize(buffercache, rightPtr);
  //Unserialize to write to new nodes
  rc = leftNode.Unserialize(buffercache, leftPtr);
  if (rc) { return rc;}
  rc = rightNode.Unserialize(buffercache, rightPtr);
  if (rc) { return rc;}

  //Variables to hold spot/key vals
  KEY_T keySpot;
  KEY_T testKey;
  VALUE_T valSpot;
  SIZE_T ptrSpot;

  //Find splitting point
  int midpoint = (b.info.numkeys+0.5)/2;

  //If A leafNode
  if(b.info.nodetype==BTREE_LEAF_NODE){
  //Build left leaf node, include the splitting key (this is a <= B+ tree)
    for(offset = 0; (int)offset < midpoint; offset++){
      //std::cout<<":::: OFFSET for building new left leaf node = "<<offset<<std::endl;
      leftNode.info.numkeys++;

    //Get old node values
      rc = b.GetKey(offset, keySpot);
      if (rc) { return rc;}
      rc = b.GetVal(offset, valSpot);
      if (rc) { return rc;}
    //set values in new left node.
      rc = leftNode.SetKey(offset, keySpot);
      if (rc) { return rc;}
      rc = leftNode.SetVal(offset, valSpot);
      if (rc) { return rc;}
    }
  //Build right leaf node
    int spot=0;
    for(offset = midpoint; offset<b.info.numkeys; offset++){

    //Get values from old node.
      rightNode.info.numkeys++;
      rc = b.GetKey(offset, keySpot);
      if (rc) { return rc;}
      rc = b.GetVal(offset, valSpot);
      if (rc) { return rc;}
    //set values in new right node.
      rc = rightNode.SetKey(spot, keySpot);
      if (rc) { return rc;}
      rc = rightNode.SetVal(spot, valSpot);
      if (rc) { return rc;}
      spot++;
    }
  } else {//if it's an interior node.
      //Build left interior node
  for(offset = 0; (int)offset < midpoint; offset++){
    leftNode.info.numkeys++;
        //Get old key and pointers
    rc = b.GetKey(offset, keySpot);
    if (rc) { return rc;}
    rc = b.GetPtr(offset, ptrSpot);
    if (rc) { return rc;}
        //Set new key and Pointer vals
    rc = leftNode.SetKey(offset, keySpot);
    if (rc) { return rc;}
    rc = leftNode.SetPtr(offset, ptrSpot);
  }
      //Build Right interior node
  int spot=0;
  for(offset = midpoint; offset<b.info.numkeys; offset++){
    rightNode.info.numkeys++;
    //Get values from old node.
    rc = b.GetKey(offset, keySpot);
    if (rc) { return rc;}
    rc = b.GetPtr(offset, ptrSpot);
    if (rc) { return rc;}
    //set values in new right node.
    rc = rightNode.SetKey(spot, keySpot);
    if (rc) { return rc;}
    rc = rightNode.SetPtr(spot, ptrSpot);
    if (rc) { return rc;}
    spot++;
  }
  rc = b.GetPtr(offset, ptrSpot);
  if (rc) { return rc;}
  rc = rightNode.SetPtr(spot, ptrSpot);
  if (rc) { return rc;}
}
  //Serialize the new nodes
rc = leftNode.Serialize(buffercache, leftPtr);
if (rc) { return rc;}
rc = rightNode.Serialize(buffercache, rightPtr);
if (rc) { return rc;}
rc = b.Serialize(buffercache, node);

  //Find split key
KEY_T splitKey;
rc = b.GetKey(midpoint-1, splitKey);
if (rc) { return rc;}

  //If we're all the way up at the root, we need to make a new root.
if (b.info.nodetype == BTREE_ROOT_NODE) {
  SIZE_T newRootPtr;
  BTreeNode newRootNode;
  AllocateNode(newRootPtr);
  newRootNode = BTreeNode(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  superblock.info.rootnode = newRootPtr;
    newRootNode.info.rootnode = newRootPtr;
    newRootNode.info.numkeys = 1;
    newRootNode.SetKey(0, splitKey);
    newRootNode.SetPtr(0, leftPtr);
    newRootNode.SetPtr(1, rightPtr);
  rc = newRootNode.Serialize(buffercache, newRootPtr);
  if(rc) {return rc;}
}
else{
//Find the parent node
  SIZE_T parentPtr = ptrPath.back();
  ptrPath.pop_back();
  BTreeNode parentNode;
  rc = parentNode.Unserialize(buffercache, parentPtr);
  if(rc) {return rc;}

    if (parentNode.info.nodetype == BTREE_SUPERBLOCK) {
        AllocateNode(parentPtr);

    }
    //Increment the key count for the given node.
    //parentNode.info.numkeys++;

    BTreeNode newParentNode = BTreeNode(parentNode.info.nodetype, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    newParentNode.info.numkeys = parentNode.info.numkeys + 1;
    newParentNode.info.freelist = parentNode.info.freelist;

    bool newKeyInserted = false;
    for (offset = 0; offset < newParentNode.info.numkeys - 1; offset++) {
        rc = parentNode.GetKey(offset, testKey);
        //    if(rc){ return rc;}
        if (newKeyInserted) {
            rc = parentNode.GetKey(offset, keySpot);
            newParentNode.SetKey(offset + 1, keySpot);

            rc = parentNode.GetPtr(offset + 1, ptrSpot);
            newParentNode.SetPtr(offset + 2, ptrSpot);
        } else {
            if (splitKey < testKey) {
                newKeyInserted = true;
                newParentNode.SetPtr(offset, leftPtr);
                newParentNode.SetKey(offset, splitKey);
                newParentNode.SetPtr(offset+1, rightPtr);
                offset = offset - 1;

            } else {
                rc = parentNode.GetKey(offset, keySpot);
                if (rc) {return rc;}
                rc = newParentNode.SetKey(offset, keySpot);
                if (rc) {return rc;}

                rc = parentNode.GetPtr(offset, ptrSpot);
                if (rc) {return rc;}
                rc = newParentNode.SetPtr(offset, ptrSpot);
                if (rc) {return rc;}
            }
        }
    }
    if (newKeyInserted == false) {
        newKeyInserted = true;
        newParentNode.SetPtr(offset, leftPtr);
        newParentNode.SetKey(offset, splitKey);
        newParentNode.SetPtr(offset+1, rightPtr);
    }

    newParentNode.Serialize(buffercache, parentPtr);

  if((int)newParentNode.info.numkeys > (int)(2*maxNumKeys/3)){
    rc = TreeBalance(parentPtr, ptrPath);
    if(rc){ return rc;}
  }
}
  //Deallocate the old (too large) node
DeallocateNode(node);
return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T val = value;
  return LookupOrUpdateInternal(superblock.info.rootnode,BTREE_OP_UPDATE,key,val);
}


ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit
  //
  //
  return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);

  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) {
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) {
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) {
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) {
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) {
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) {
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  ERROR_T rc = SanityWalk(superblock.info.rootnode);
  return rc;
}
//We'll use this for walking the tree for our sanity check.
ERROR_T BTreeIndex::SanityWalk(const SIZE_T &node) const{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  KEY_T tempkey;
  SIZE_T ptr;
  VALUE_T value;
  rc = b.Unserialize(buffercache, node);
  if(rc!=ERROR_NOERROR){
    return rc;
  }

  //Check to see if the nodes have proper lengths
  if(b.info.numkeys>(unsigned int)(2*maxNumKeys/3)){
    std::cout << "Current Node of type "<<b.info.nodetype<<" has "<<b.info.numkeys<<" keys. Which is over the 2/3 threshold of the maximum of "<<maxNumKeys<<" keys."<<std::endl;
  }
  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
        //Scan through key/ptr pairs
        //and recurse if possible
    for(offset=0; offset<b.info.numkeys; offset++){
      rc = b.GetKey(offset,testkey);
      if(rc) {return rc; }
        //If keys are not in proper size order
      if(offset+1<b.info.numkeys-1){
        rc = b.GetKey(offset+1, tempkey);
        if(tempkey < testkey){
          std::cout<<"The keys are not properly sorted!"<<std::endl;
        }
      }
      rc=b.GetPtr(offset,ptr);
      if(rc){return rc;}
          return SanityWalk(ptr);
    }
      //If we get here, we need to go to the next pointer, if it exists.
    if(b.info.numkeys>0){
      rc = b.GetPtr(b.info.numkeys, ptr);
      if(rc) { return rc; }
        return SanityWalk(ptr/*, allTreeNodes*/);
    }else{
        //There are no keys at all on this node, so nowhere to go
      std::cout << "The keys on this interior node are nonexistent."<<std::endl;
      return ERROR_NONEXISTENT;
    }
    break;
    case BTREE_LEAF_NODE:
    for(offset=0; offset<b.info.numkeys;offset++){
      rc = b.GetKey(offset, testkey);
      if(rc) {
        std::cout << "Leaf Node is missing key"<<std::endl;
        return rc;
      }
      rc =b.GetVal(offset, value);
      if(rc){
        std::cout << "leaf node key is missing associated value"<<std::endl;
        return rc;
      }
        //If keys are not in proper size order
      if(offset+1<b.info.numkeys){
        rc = b.GetKey(offset+1, tempkey);
        if(tempkey < testkey){
          std::cout<<"The keys are not properly sorted!"<<std::endl;
        }
      }
    }
    break;
    default:
    return ERROR_INSANE;
    break;
  }
  return ERROR_NOERROR;
}


ostream & BTreeIndex::Print(ostream &os) const
{
  Display(os, BTREE_DEPTH_DOT);
  return os;
}
