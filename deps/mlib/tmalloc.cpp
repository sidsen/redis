// thread local allocator
//
// (c) Copyright 2014 Microsoft Corporation
// Written by Marcos K. Aguilera
//

#include "stdafx.h"
#include "tmalloc.h"
#include "tmalloc-include.h"

__declspec(thread) _TMThreadInfo *_TMthreadinfo=0;

// add padding information
void FixedAllocator::addPadding(void *buf){
  PadBefore *padbefore = (PadBefore*) buf;
  PadAfter *padafter = (PadAfter*) ((char*) buf + sizeof(PadBefore) + Size);
  padbefore->allocated=-1LL;
  padbefore->tag = Tag;
  padbefore->status = 0;
  padbefore->next = 0;
  memcpy(padbefore->magic, PADBEFOREMAGIC, sizeof(padbefore->magic));
  memcpy(padafter->magic, PADAFTERMAGIC, sizeof(padafter->magic));
}

// Check padding information.
//   buf: pointer to buffer
//   alloc: whether buf is allocated or not
void FixedAllocator::checkPadding(void *buf, bool alloc){
  PadBefore *padbefore = (PadBefore*) buf;
  PadAfter *padafter = (PadAfter*) ((char*)buf + sizeof(PadBefore) + Size);
  if (alloc) assert(padbefore->allocated != -1LL);
  else assert(padbefore->allocated == -1LL);
  //assert(padbefore->tag == Tag);
  assert(memcmp(padbefore->magic, PADBEFOREMAGIC, sizeof(padbefore->magic))==0);
  assert(memcmp(padafter->magic, PADAFTERMAGIC, sizeof(padafter->magic))==0);
}

// must be called with FreeUnits_lock held
void FixedAllocator::grow(int inc){
  PadBefore *pbprev, *savenext;
  char *buf, *tmp;
  int i;
  assert(inc>=0);
  if (inc==0) return;
  buf = (char*) malloc((size_t)Realsize * inc); assert(buf);

  // set up FreeUnits
  //FreeUnits_lock.lock();
  pbprev = FreeUnitsHead;
  savenext = FreeUnitsHead->next;
  tmp = buf;
  for (i=0; i < inc; ++i){
    addPadding(tmp);  // add padding information
    pbprev->next = (PadBefore*) tmp; // add to link list
    pbprev = (PadBefore*) tmp;
    tmp += Realsize;
  }
  pbprev->next = savenext;
  //FreeUnits_lock.unlock();
}

FixedAllocator::FixedAllocator(int size, int startpool, int incgrow, u64 tag) :
  FreeUnits_lock() {
  assert(size > 0 && startpool >= 0 && incgrow > 0);

  // set up initial empty linked list
  FreeUnitsHead = new(malloc(sizeof(PadBefore))) PadBefore;
  FreeUnitsTail = new(malloc(sizeof(PadBefore))) PadBefore;
  FreeUnitsHead->next = FreeUnitsTail;
  FreeUnitsTail->next = 0;

  Size = size;
  Realsize = Size + sizeof(PadBefore) + sizeof(PadAfter);
  IncGrow = incgrow;
  NAllocated = 0;
  Tag = tag;
  if (startpool)
    grow(startpool);
}

FixedAllocator::~FixedAllocator(){
  FreeUnitsHead->~PadBefore(); free(FreeUnitsHead);
  FreeUnitsTail->~PadBefore(); free(FreeUnitsTail);
}

void *FixedAllocator::alloc(u64 reqsize){
  PadBefore *pb;

  if (reqsize==-1LL) reqsize = Size; // for bookkeeping purposes only, not really used
  else if (reqsize > Size) return 0; // size too large for fixed block
  FreeUnits_lock.lock();
  if (FreeUnitsHead->next == FreeUnitsTail) // empty linked list
    grow();
  pb = FreeUnitsHead->next;
  FreeUnitsHead->next = pb->next; // remove from linked list
  pb->next = 0;
  pb->allocated = reqsize;
  ++NAllocated;
  FreeUnits_lock.unlock();
  checkPadding((char*) pb, true);
  return (void*)((char*) pb + sizeof(PadBefore));
}

void FixedAllocator::free(void *tofree){
  char *buf = (char*) tofree;
  buf -= sizeof(PadBefore);
  PadBefore *pb = (PadBefore*) buf;
  checkPadding(buf,true);
  FreeUnits_lock.lock();
  pb->next = FreeUnitsHead->next; // add block to beginning of linked list
  pb->allocated = -1LL; // mark as not allocated
  FreeUnitsHead->next = pb;  
  --NAllocated;
  FreeUnits_lock.unlock();
  return;
}

size_t FixedAllocator::getSize(void *buf){
  PadBefore *pb = (PadBefore*) ((char*)buf - sizeof(PadBefore));
  return (size_t)pb->allocated;
}

u64 FixedAllocator::getTag(void *buf){
  PadBefore *pb = (PadBefore*) ((char*)buf - sizeof(PadBefore));
  return (size_t)pb->tag;
}

void FixedAllocator::setStatus(void *buf, u32 status){
  PadBefore *pb = (PadBefore*)((char*)buf-sizeof(PadBefore));
  pb->status = status;
}
u32 FixedAllocator::getStatus(void *buf){
  PadBefore *pb = (PadBefore*)((char*)buf-sizeof(PadBefore));
  return pb->status;
}

// add padding information
void FixedAllocatorNolock::addPadding(void *buf){
  PadBefore *padbefore = (PadBefore*) buf;
  PadAfter *padafter = (PadAfter*) ((char*) buf + sizeof(PadBefore) + Size);
  padbefore->allocated=-1LL;
  padbefore->tag = Tag;
  padbefore->status = 0;
  padbefore->next = 0;
  memcpy(padbefore->magic, PADBEFOREMAGIC, sizeof(padbefore->magic)); 
  memcpy(padafter->magic, PADAFTERMAGIC, sizeof(padafter->magic));
}

// Check padding information.
//   buf: pointer to buffer
//   alloc: whether buffer is supposed to be allocated or not
void FixedAllocatorNolock::checkPadding(void *buf, bool alloc){
  PadBefore *padbefore = (PadBefore*) buf;
  PadAfter *padafter = (PadAfter*) ((char*) buf + sizeof(PadBefore) + Size);
  if (alloc) assert(padbefore->allocated != -1LL);
  else assert(padbefore->allocated == -1LL);
  //assert(padbefore->tag == Tag);
  assert(memcmp(padbefore->magic, PADBEFOREMAGIC, sizeof(padbefore->magic))==0);
  assert(memcmp(padafter->magic, PADAFTERMAGIC, sizeof(padafter->magic))==0);
}

void FixedAllocatorNolock::grow(int inc){
  PadBefore *pbprev, *savenext;
  char *buf, *tmp;
  int i;
  size_t allocsize;

  assert(inc>=0);
  if (inc==0) return;
  allocsize = (size_t)Realsize * inc;
  if (PageSize) // round up to the next page size
    allocsize = (allocsize + PageSize - 1) / PageSize * PageSize;

  if (PageAllocFunc){
    buf = (char*) PageAllocFunc(allocsize);
    if (!buf) buf = (char*) malloc(allocsize); // use malloc if PageAllocFunc fails
  }
  else buf = (char*) malloc(allocsize);
  assert(buf);

  // set up FreeUnits
  pbprev = FreeUnitsHead;
  savenext = FreeUnitsHead->next;
  tmp = buf;
  for (i=0; i < inc; ++i){
    addPadding(tmp);  // add padding information
    pbprev->next = (PadBefore*) tmp; // add to link list
    pbprev = (PadBefore*) tmp;
    tmp += Realsize;
  }
  pbprev->next = savenext;
}

FixedAllocatorNolock::FixedAllocatorNolock(int size, int startpool, int incgrow, u64 tag, unsigned pagesize, void *(*pageallocfunc)(size_t)){
  assert(size > 0 && startpool >= 0 && incgrow > 0);

  // set up initial empty linked list
  FreeUnitsHead = new(malloc(sizeof(PadBefore))) PadBefore; // not allocated with pageallocfunc since this is likely much smaller than page size
  FreeUnitsTail = new(malloc(sizeof(PadBefore))) PadBefore;
  FreeUnitsHead->next = FreeUnitsTail;
  FreeUnitsTail->next = 0;

  Size = size;
  Realsize = Size + sizeof(PadBefore) + sizeof(PadAfter);
  IncGrow = incgrow;
  NAllocated = 0;
  Tag = tag;
  PageSize = pagesize;
  PageAllocFunc = pageallocfunc;

  // adjust IncGrow if pagesize is set
  if (PageSize && (size_t) IncGrow * Realsize < PageSize)
    IncGrow = PageSize / Realsize;

  // adjust startpool if pagesize and startpool are set
  if (PageSize && startpool && (size_t) startpool * Realsize < PageSize)
    startpool = PageSize / Realsize;

  if (startpool)
    grow(startpool);
}

FixedAllocatorNolock::~FixedAllocatorNolock(){
  FreeUnitsHead->~PadBefore(); free(FreeUnitsHead);
  FreeUnitsTail->~PadBefore(); free(FreeUnitsTail);
}

void *FixedAllocatorNolock::alloc(u64 reqsize){
  PadBefore *pb;

  if (reqsize==-1LL) reqsize = Size; // for bookkeeping purposes only, not really used
  else if (reqsize > Size) return 0; // size too large for fixed block
  if (FreeUnitsHead->next == FreeUnitsTail) // empty linked list
    grow();
  pb = FreeUnitsHead->next;
  FreeUnitsHead->next = pb->next; // remove from linked list
  pb->next = 0;
  pb->allocated = reqsize;
  ++NAllocated;
  assert((checkPadding((char*) pb, true),1));
  return (void*)((char*) pb + sizeof(PadBefore));
}

void FixedAllocatorNolock::free(void *tofree){
  char *buf = (char*) tofree;
  buf -= sizeof(PadBefore);
  PadBefore *pb = (PadBefore*) buf;
  assert((checkPadding(buf,true),1));
  pb->next = FreeUnitsHead->next; // add block to beginning of linked list
  pb->allocated = -1LL; // mark as not allocated
  FreeUnitsHead->next = pb;  
  --NAllocated;
  return;
}

size_t FixedAllocatorNolock::getSize(void *buf){
  PadBefore *pb = (PadBefore*) ((char*)buf - sizeof(PadBefore));
  return (size_t)pb->allocated;
}

void FixedAllocatorNolock::setSize(void *buf, size_t newsize){
  PadBefore *pb = (PadBefore*) ((char*)buf - sizeof(PadBefore));
  size_t oldsize = getSize(buf);
  assert(newsize <= oldsize);
  if (newsize <= oldsize) pb->allocated = newsize;
}

u64 FixedAllocatorNolock::getTag(void *buf){
  PadBefore *pb = (PadBefore*) ((char*)buf - sizeof(PadBefore));
  return (size_t)pb->tag;
}

void FixedAllocatorNolock::setStatus(void *buf, u32 status){
  PadBefore *pb = (PadBefore*)((char*)buf-sizeof(PadBefore));
  pb->status = status;
}
u32 FixedAllocatorNolock::getStatus(void *buf){
  PadBefore *pb = (PadBefore*)((char*)buf-sizeof(PadBefore));
  return pb->status;
}

int VariableAllocatorNolock::ceillog2(size_t n){ // computes ceiling(log2(n))
  if (n == 0) return 0;
  --n;
  int i=0;
  while (n){
    n >>= 1;
    ++i;
  }
  return i;
}

VariableAllocatorNolock::VariableAllocatorNolock(u64 tag, unsigned pagesize, void *(*pageallocfunc)(size_t)){
  int i;
  int currsize;
  PageSize = pagesize;
  PageAllocFunc = pageallocfunc;
  FixedPools = (FixedAllocatorNolock*) malloc((size_t)((FixedAllocatorNolock*)0 + 1) * VARALLOC_NPOOLS);
  currsize = 1 << VARALLOC_FIRSTPOOL;
  for (i=0; i < VARALLOC_NPOOLS; ++i){
    if (i <= 2){ // sizes 32..128
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 1024, tag, pagesize, pageallocfunc); // allocate 1024 at a time
    } else if (i <= 5){ // sizes 256..1024
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 256, tag, pagesize, pageallocfunc); // allocate 256 at a time
    } else if (i <= 10){ // sizes 2048..32K
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 32, tag, pagesize, pageallocfunc); // allocate 32 at a time
    } else if (i <= 15){ // size 64K..1M
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 16, tag, pagesize, pageallocfunc); // allocate 16 at a time
    } else if (i <= 20){ // size 2M..32MB
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 8, tag, pagesize, pageallocfunc); // allocate 8 at a time
    } else if (i <= 25){ // size 64MB..1GB
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 2, tag, pagesize, pageallocfunc); // allocate 2 at a time
    } else { // size 2GB
      new((void*)(FixedPools+i)) FixedAllocatorNolock(currsize, 0, 1, tag, pagesize, pageallocfunc); // allocate 1 at a time
    }
    currsize <<= 1;
  }
}

VariableAllocatorNolock::~VariableAllocatorNolock(){
  int i;
  for (i=0; i < VARALLOC_NPOOLS; ++i){
    FixedPools[i].~FixedAllocatorNolock();
  }
  free(FixedPools);
}

void *VariableAllocatorNolock::alloc(size_t size){
  int pool = ceillog2(size);
  pool -= VARALLOC_FIRSTPOOL;
  if (pool < 0) pool = 0;
  if (pool >= VARALLOC_NPOOLS) return 0; // too big
  return FixedPools[pool].alloc(size);
}

void VariableAllocatorNolock::free(void *ptr){
  size_t size = getSize(ptr);
  assert(size != -1LL); // if size==-1LL then ptr has been freed already
  int pool = ceillog2(size);
  pool -= VARALLOC_FIRSTPOOL;
  if (pool < 0) pool = 0;
  assert(pool < VARALLOC_NPOOLS);
  FixedPools[pool].free(ptr);
}

u64 VariableAllocatorNolock::getTag(void *ptr){
  return FixedAllocatorNolock::getTag(ptr);
}

void VariableAllocatorNolock::checkBuf(void *buf, bool alloc){
  size_t size = FixedAllocatorNolock::getSize(buf);
  assert(size != -1LL); // if size==-1LL then ptr has been freed already
  int pool = ceillog2(size);
  pool -= VARALLOC_FIRSTPOOL;
  if (pool < 0) pool = 0;
  assert(pool < VARALLOC_NPOOLS);
  FixedPools[pool].checkBuf(buf, alloc);
}

void VariableAllocatorNolock::setStatus(void *buf, u32 status){
  FixedAllocatorNolock::setStatus(buf,status);
}
u32 VariableAllocatorNolock::getStatus(void *buf){
  return FixedAllocatorNolock::getStatus(buf);
}

FixedAllocatorMultipool::FixedAllocatorMultipool(int size, int startpool, int incgrow, int npools){
  int i;
  // hack to allocate an array of FixedAllocator with the non-default constructor
  int sizeone = (int)((FixedAllocator *)0 + 1);
  Pools = (FixedAllocator*) malloc(sizeone*npools);
  NPools = npools;
  for (i=0; i < NPools; ++i)
    new(Pools+i) FixedAllocator(size, startpool, incgrow); // call placement constructor
}

FixedAllocatorMultipool::~FixedAllocatorMultipool(){
  int i;
  for (i=0; i < NPools; ++i) Pools[i].~FixedAllocator(); // call destructors
  free(Pools);
}

// allocate new buffer
void *FixedAllocatorMultipool::alloc(void){
  int i = AtomicInc32(&NextPool) % NPools;
  return Pools[i].alloc();
}
// free buffer
void FixedAllocatorMultipool::free(void *buf){
  int i = AtomicInc32(&NextPool) % NPools;
  return Pools[i].free(buf);
}

void *_tmallocnogc(size_t size);
void *_tmalloc(size_t size);
void _tfree(void *buf);

_TMLinkListNode *_TMLinkListNode::_TMnewnode(int n){ // allocate a new superbuffer with space for n buffers
  _TMLinkListNode *retval;
  int toalloc;
  if (n <= 0) toalloc=1;
  else toalloc = n;
  retval = (_TMLinkListNode*) _tmallocnogc(sizeof(_TMLinkListNode) + (toalloc-1) * sizeof(void*));
  assert(retval);
  new(retval) _TMLinkListNode;
  retval->nbufs = n;
  retval->next = 0;
  return retval;
}

void _TMLinkListNode::_TMfreenode(_TMLinkListNode *node){
  node->~_TMLinkListNode();
  _tfree(node);
}

DestMapItem::DestMapItem(void *dest){
  destthread = dest;
  superbuffer = _TMLinkListNode::_TMnewnode(_TM_SUPERBUFFER_SIZE);
  pos = 0;
}

void _TMThreadInfo::addNode(_TMLinkListNode *node){
  _TMLinkListNode *res;
  Align4 _TMLinkListNode *currhead;
  res = headLinkList;
  do { // try to slap head using compare-and-swap until successful
    currhead = res;
    node->next = currhead;
    res = (_TMLinkListNode*) CompareSwapPtr((void * volatile *) &headLinkList, currhead, node);
  } while (res != currhead);
}

static void _TMfreelist(){
  int i, nbufs;
  void **ptr;
  _TMLinkListNode *curr, *next;
  curr = _TMthreadinfo->headLinkList;
  if (!curr) return; // empty
  next = curr->next;
  if (!next) return;  // only one element; keep it there
                      // This check is an optimization: code below would do nothing in this case

  curr->next = 0; // detach rest of list from link list
  curr = next;

  // traverse linklist from second node
  while (curr){
    next = curr->next;
    nbufs = curr->nbufs;
    ptr = curr->bufs;
    for (i = 0; i < nbufs; ++i){
      assert(VariableAllocatorNolock::getStatus(*ptr)==1); // status should be waiting
      VariableAllocatorNolock::setStatus(*ptr,0); // set status to free
      _TMthreadinfo->allocator.free(*ptr); // return *ptr to local pool
      ++ptr;
    }
    _TMLinkListNode::_TMfreenode(curr);
    curr = next;
  }
}

void *operator new(size_t size){ return _tmalloc(size); }
void operator delete(void *p){ _tfree(p); }
void *operator new[](size_t size){ return _tmalloc(size); }
void operator delete[](void *p){ _tfree(p); }

// returns 0 if ok, non-zero if error
static int SetPrivilege(HANDLE hToken, LPCTSTR lpszPrivilege, bool enable){
  TOKEN_PRIVILEGES tp;
  LUID luid;
  int res, err;
 
  if(!LookupPrivilegeValue(0, lpszPrivilege, &luid)){
    //printf("LookupPrivilegeValue() error: %u\n", GetLastError());
    return 0;
  }
 
  tp.PrivilegeCount = 1;
  tp.Privileges[0].Luid = luid;
 
  tp.Privileges[0].Attributes = enable ? SE_PRIVILEGE_ENABLED : 0;
 
  // Enable the privilege (or disable all privileges).
  res = AdjustTokenPrivileges(hToken, 0, &tp, sizeof(TOKEN_PRIVILEGES), 0, 0);
  err = GetLastError();
  if (!res){
    printf("AdjustTokenPrivileges() failed: %u\n", GetLastError());
    return -1;
  }
  if (err){
    printf("AdjustTokenPrivileges() error: %u\n", GetLastError());
    return -1;
  }
  return 0;
}

static size_t LargePageSize = 0;

void _tmEnableLargePage()
{
	HANDLE hToken;
  int res;
 
	// Open a handle to the access token for the calling process. That is this running program
	if(!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hToken))
		printf("OpenProcessToken() error %u\n", GetLastError());
	// Call the user defined SetPrivilege() function to enable and set the needed privilege
	res = SetPrivilege(hToken, (LPCTSTR) _T("SeLockMemoryPrivilege"), true);
  if (res){ // error
    LargePageSize = 0;
  	printf("Cannot use large pages: SetPrivilege(SeLockMemoryPrivilege) failed\n");
    fflush(stdout);
    return;
  }

  printf("Using large pages\n"); fflush(stdout);
	LargePageSize = GetLargePageMinimum();
}

void *_tmlargemalloc(size_t size){
  void *retval;
  if (size==0) size=1;
  size = (size + LargePageSize - 1) / LargePageSize * LargePageSize; // round up to next LargePageSize
  retval = VirtualAlloc(0, size, MEM_LARGE_PAGES|MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE);
  if (!retval){
    //printf("Cannot allocate largepage. Error %d. Trying normal allocation\n", GetLastError());
    retval = malloc(size);
  }
  assert(retval);
  return retval;
}

void *_tmallocnuma(size_t size){
  void *retval;
  unsigned short ushort_numanode;
  u32 numanode;
  int res;
  int processor;
  
  assert(_TMthreadinfo);
  if (_TMthreadinfo->myprocessor + 1 == 0){
#ifdef _TM_NUMA_WARNING_NOREPORTPROCESSOR	 
	fprintf(stderr, "_tmalloc warning: malloc being called before _tmreportprocessor(); allocating from processor 0\n");
#endif
	processor = 0;
  }
  else processor = _TMthreadinfo->myprocessor;




  int bigproc = processor % GetActiveProcessorCount(0);
  int group = processor / GetActiveProcessorCount(0);

  PROCESSOR_NUMBER pn;
  ZeroMemory(&pn, sizeof(PROCESSOR_NUMBER));
  pn.Group = group;
  pn.Number = bigproc;

  res = GetNumaProcessorNodeEx(&pn, &ushort_numanode);
  //printf("GetNumaProcessorNodeEx: proc %d relativeProc %d group %d res %d node %u size %d \n", processor, bigproc, group, res, ushort_numanode, size);

  if (res == 0){
	  printf("Error in GetNumaProcessorNodeEx: %d\n", GetLastError());
	  numanode = 0;
  }
  else numanode = ushort_numanode;
  if (size==0) size=1;
  size = (size + _TM_PAGESIZE - 1) / _TM_PAGESIZE * _TM_PAGESIZE; // round up to next LargePageSize
  retval = VirtualAllocExNuma(GetCurrentProcess(), 0, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE, numanode);
  if (!retval){
    printf("Cannot allocate NUMA page. Error %d. Trying normal allocation\n", GetLastError());
    retval = malloc(size);
  }
  assert(retval);
  return retval;
}

int _tpreinit(){
#ifdef _TM_USELARGEPAGES
  _tmEnableLargePage();
#endif
  return 1;
}

int _tpreinited = _tpreinit();

void _tinit(){
#ifndef BYPASS_THREADALLOCATOR
  _TMthreadinfo = new(malloc(sizeof(_TMThreadInfo))) _TMThreadInfo;
#endif
}

void _tmreportprocessor(int processor){
  if (!_TMthreadinfo) _tinit();
  assert(_TMthreadinfo);
  _TMthreadinfo->myprocessor = (u32)processor;
}

void *_tmallocnogc(size_t size){
  void *retval;
  retval = _TMthreadinfo->allocator.alloc(size);
  VariableAllocatorNolock::setStatus(retval, 2);
  return retval;
}

#ifdef BYPASS_THREADALLOCATOR
void *_tmalloc(size_t size){ return malloc(size); }
void _tfree(void *buf){ return free(buf); }
void *_trealloc(void *buf, size_t size){ return realloc(buf, size); }
#else

void *_tmalloc(size_t size){
  void *retval;
  if (!_TMthreadinfo) _tinit();
  _TMfreelist();
  retval = _TMthreadinfo->allocator.alloc(size);
  VariableAllocatorNolock::setStatus(retval, 2);
#ifdef _TM_FILLBUFFERS
  memset(retval, _TM_FILLALLOC, size);
#endif
  return retval;
}

void _tfree(void *buf){
  DestMapItem *dmi;
  void *destthread;

  if (!buf) return;

  if (!_TMthreadinfo) _tinit();

  assert((_TMthreadinfo->allocator.checkBuf(buf,true),1));
  assert(VariableAllocatorNolock::getStatus(buf)==2);
  VariableAllocatorNolock::setStatus(buf,1); // set status to waiting
#ifdef _TM_FILLBUFFERS
  memset(buf, _TM_FILLFREE, VariableAllocatorNolock::getSize(buf));
#endif

  destthread = (void*) VariableAllocatorNolock::getTag(buf);
  if (destthread == _TMthreadinfo){ // local buffer
    VariableAllocatorNolock::setStatus(buf,0); // set status to free
    _TMthreadinfo->allocator.free(buf);
    return;
  }

  // otherwise, add to current superbuffer
  dmi = _TMthreadinfo->destMap.lookup(destthread);
  if (!dmi){
    dmi = new(malloc(sizeof(DestMapItem))) DestMapItem(destthread);
    _TMthreadinfo->destMap.insert(dmi);
  }
  assert(dmi->pos < dmi->superbuffer->nbufs);
  dmi->superbuffer->bufs[dmi->pos] = buf;
  ++dmi->pos;
  if (dmi->pos >= dmi->superbuffer->nbufs){
    // superbuffer is full. Send it to the destination thread and allocate a new one
    _TMThreadInfo *dt = (_TMThreadInfo*) destthread;
    dt->addNode(dmi->superbuffer);
    dmi->superbuffer = _TMLinkListNode::_TMnewnode(_TM_SUPERBUFFER_SIZE);
    dmi->pos = 0;
    _TMfreelist();
  }
}

size_t _tgetsize(void *buf){
  return VariableAllocatorNolock::getSize(buf);
}

void *_trealloc(void *ptr, size_t size){
  if (size==0){ // if new size is 0, free location
    _tfree(ptr);
    return 0;
  }
  size_t oldsize = _tgetsize(ptr);
  if (size <= oldsize){
    //VariableAllocatorNolock::setSize(ptr, size);
    return ptr;
  }
  // size > old size
  void *newptr = _tmalloc(size);
  memcpy(newptr, ptr, oldsize);
  _tfree(ptr);
  return newptr;
}

void *_tcalloc(size_t n_elements, size_t elem_size) {
	void* mem;
	size_t req = 0;
	if (n_elements != 0) {
		req = n_elements * elem_size;
		if (((n_elements | elem_size) & ~(size_t)0xffff) &&
			(req / n_elements != elem_size))
			assert(0); //req = MAX_SIZE_T; /* force downstream failure on overflow */
	}
	mem = _tmalloc(req);
	if (mem) // != 0 && calloc_must_clear(mem2chunk(mem)))
		memset(mem, 0, req);
	return mem;
}

#endif

void *orig_malloc(size_t size){ return malloc(size); }
void orig_free(void *buf){ free(buf); }
void *orig_realloc(void *ptr, size_t size){ return realloc(ptr, size); }

#if defined(_TM_USELARGEPAGES) && defined(_TM_NUMAALLOC)
#error We do not currently support both _TM_NUMAALLOC and _TM_USELARGEPAGES
#elif defined(_TM_USELARGEPAGES)
_TMThreadInfo::_TMThreadInfo()
  : destMap(_TM_DESTMAP_HASHTABLE_SIZE), allocator((u64)this, LargePageSize, LargePageSize ? _tmlargemalloc : 0)
{
  headLinkList = 0;
  myprocessor = -1;
}
#elif defined(_TM_NUMAALLOC)
_TMThreadInfo::_TMThreadInfo()
: destMap(_TM_DESTMAP_HASHTABLE_SIZE), allocator((u64)this, _TM_PAGESIZE, _tmallocnuma)
{
  headLinkList = 0;
  myprocessor = -1;
}
#else
_TMThreadInfo::_TMThreadInfo()
  : destMap(_TM_DESTMAP_HASHTABLE_SIZE), allocator((u64)this)
{
  headLinkList = 0;
  myprocessor = -1;
}
#endif
