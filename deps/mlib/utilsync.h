// utilsync.h
//
// General utility functions for interprocess synchronization
//
// (c) Copyright 2014 Microsoft Corporation
// Written by Marcos K. Aguilera


#ifndef _UTILSYNC_H
#define _UTILSYNC_H

//#include <intrin.h>
//#include <tchar.h>
//#include <assert.h>
//#include <string.h>
//#include <stdio.h>


// these definitions for fetch-and-add, atomic-increment, and atomic-decrement are compiler specific
#define FetchAndAdd32(ptr32, val32) _InterlockedExchangeAdd((long*)ptr32,val32)
#define FetchAndAdd64(ptr64, val64) _InterlockedExchangeAdd64((long*)ptr64,val64)
#define AtomicInc32(ptr32) _InterlockedIncrement((long*)ptr32)
#define AtomicInc64(ptr64) _InterlockedIncrement64((__int64*)ptr64)
#define AtomicDec32(ptr32) _InterlockedDecrement((long*)ptr32)
#define AtomicDec64(ptr64) _InterlockedIncrement64((__int64*)ptr64)
#define CompareSwap32(ptr32,cmp32,val32) _InterlockedCompareExchange(ptr32,val32,cmp32)
#define CompareSwap64(ptr64,cmp64,val64) _InterlockedCompareExchange(ptr64,val64,cmp64)
#define CompareSwapPtr(ptr,cmp,val) _InterlockedCompareExchangePointer(ptr,val,cmp)

// definitions to align a variable in 4-byte or 64-byte boundaries, also compiler specific
#define Align4 __declspec(align(4))
#define Align8 __declspec(align(8))
#define Align16 __declspec(align(16))
#define Align64 __declspec(align(64))

 
#define SYNC_TYPE 3
// possible values:
//    1 = use lightweight windows synchronization (SRWLocks, etc)
//    2 = use regular windows synchronization (CreateMutex/WaitForSingleObject/ReleaseMutex)
//        (no support for condition variables)
//    3 = C++11

#define WINDOWS_SEMAPHORE // uses windows semaphore instead of C++11 semaphores built with mutex and condition_variable

#include "inttypes.h"

#if SYNC_TYPE==1
class RWLock {
  friend class CondVar;
protected:
  SRWLOCK SRWLock;
public:
  RWLock(){
    InitializeSRWLock(&SRWLock);
  }

  void lock(void){ AcquireSRWLockExclusive(&SRWLock); }
  void lockRead(void){ AcquireSRWLockShared(&SRWLock); }
  void unlock(void){ ReleaseSRWLockExclusive(&SRWLock); }
  void unlockRead(void){  ReleaseSRWLockShared(&SRWLock); }
  int trylock(void){ return TryAcquireSRWLockExclusive(&SRWLock); }
  int trylockRead(void){ return TryAcquireSRWLockShared(&SRWLock); }
  // the try functions return true if lock was gotten, false if someone else holds the lock
};
class CondVar {
  CONDITION_VARIABLE CV;
public:
  CondVar(){ InitializeConditionVariable(&CV); }
  void wakeOne(void){ WakeConditionVariable(&CV); }
  void wakeAll(void){ WakeAllConditionVariable(&CV); }

  // Release write lock and sleep atomically.
  // When the thread is woken up, lock is reacquired.
  // Returns true if successful (lock acquired), false if timeout (lock not acquired)
  bool unlockSleepLock(RWLock *lock, int mstimeout){ return SleepConditionVariableSRW(&CV, &lock->SRWLock, mstimeout, 0) != 0; }

  // Release read lock and sleep atomically.
  // When the thread is woken up, lock is reacquired.
  // Returns true if successful (lock acquired), false if timeout (lock not acquired)
  bool unlockReadSleepLockRead(RWLock *lock, int mstimeout){ return SleepConditionVariableSRW(&CV, &lock->SRWLock, mstimeout, CONDITION_VARIABLE_LOCKMODE_SHARED) != 0; }
};
#elif SYNC_TYPE==2
class RWLock {
  //friend class CondVar;
protected:
  HANDLE mutex;
public:
  RWLock(){ mutex = CreateMutex(0, 0, 0); }
  ~RWLock(){ CloseHandle(mutex); }
  void lock(void){ while (WaitForSingleObject(mutex,INFINITE) != WAIT_OBJECT_0) ; }
  void lockRead(void){ lock(); }
  void unlock(void){ ReleaseMutex(mutex); }
  void unlockRead(void){  unlock(); }
  int trylock(void){ return WaitForSingleObject(mutex,0)==0; }
  int trylockRead(void){ return trylock(); }
  // the try functions return true if lock was gotten, false if someone else holds the lock
};
#elif SYNC_TYPE==3
#include <mutex>
class RWLock {
  //friend class CondVar;
protected:
  std::mutex m;
public:
  RWLock(){ }
  ~RWLock(){ }
  void lock(void){ m.lock(); }
  void lockRead(void){ lock(); }
  void unlock(void){ m.unlock(); }
  void unlockRead(void){  unlock(); }
  int trylock(void){ return m.try_lock(); }
  int trylockRead(void){ return trylock(); }
  // the try functions return true if lock was gotten, false if someone else holds the lock
};
#endif

#ifdef WINDOWS_SEMAPHORE
class Semaphore {
  static const int MAXSEMAPHOREVALUE=2147483647; // largest LONG
private:
  HANDLE SemaphoreObject;
public:
  Semaphore(int initialValue=0){
    SemaphoreObject = CreateSemaphore(0, initialValue, MAXSEMAPHOREVALUE, 0);
  }
  ~Semaphore(){ CloseHandle(SemaphoreObject); }

  // returns true if timeout expired, false if semaphore has been signaled
  // if msTimeout=INFINITE then wait forever
  bool wait(int msTimeout){
    u32 res;
    res = WaitForSingleObject(SemaphoreObject, msTimeout);
    if (res == WAIT_OBJECT_0) return false;
    if (res == WAIT_TIMEOUT) return true;
    assert(0);
    return false;
  }

  void signal(void){
    int res = ReleaseSemaphore(SemaphoreObject, 1, 0); assert(res);
  }
};
#else
// Semaphore built with condition variable and mutex
#include <condition_variable>
class Semaphore {
  static const int MAXSEMAPHOREVALUE=2147483647; // largest LONG
private:
  Align4 u32 value;
  std::mutex m;
  std::condition_variable cv;
public:
  Semaphore(int initialValue=0){
    value = initialValue;
  }

  // returns true if timeout expired, false if semaphore has been signaled
  // if msTimeout=INFINITE then wait forever
  bool wait(int msTimeout){
    std::unique_lock<std::mutex> uniquelck(m);
    while (value==0){
      if (msTimeout != INFINITE){
        std::_Cv_status s;
        s = cv.wait_for(uniquelck, std::chrono::milliseconds(msTimeout));
        if (s == std::_Cv_status::timeout) return true;
      }
      else
        cv.wait(uniquelck);
    }
    AtomicDec32(&value);
    return false;
  }

  void signal(void){
    std::unique_lock<std::mutex> uniquelck(m);
    AtomicInc32(&value);
    cv.notify_one();
  }
};
#endif

//#include <concrtrm.h>
//#include <ppl.h>

#include <concrt.h>

#define EventSync EventSyncTwo  // use windows events

// event synchronization based on concurrency::set/wait
class EventSyncZero {
private:
  concurrency::event e;
public:
  void set(){ e.set(); }
  void reset(){ e.reset(); }
  int wait(unsigned timeout = concurrency::COOPERATIVE_TIMEOUT_INFINITE){  // returns 0 if wait satisfied, non-0 otherwise
    return (int) e.wait(timeout); 
  }
};

// event synchronization based on concurrency::set/wait_for_multiple
class EventSyncZeroAlt {
private:
  concurrency::event e;
public:
  void set(){ e.set(); }
  void reset(){ e.reset(); }
  int wait(unsigned timeout = concurrency::COOPERATIVE_TIMEOUT_INFINITE){  // returns 0 if wait satisfied, non-0 otherwise
    concurrency::event *eptr = &e;
    size_t status;
    bool waitinfinite;
    if (timeout == concurrency::COOPERATIVE_TIMEOUT_INFINITE){
      waitinfinite = true;
      timeout = 1000000; // avoid calling wait_for_multiple with infinity to prevent spinning in the library
    } else waitinfinite = false;
    do {
      status = concurrency::event::wait_for_multiple(&eptr, 1, true, timeout);
    } while (waitinfinite && status == concurrency::COOPERATIVE_WAIT_TIMEOUT);
    return status == concurrency::COOPERATIVE_WAIT_TIMEOUT;
  }
};

class EventSyncOne {
private:
  SRWLOCK SRWLock;
public:
  EventSyncOne(){
    InitializeSRWLock(&SRWLock);
    AcquireSRWLockExclusive(&SRWLock);
  }
  void set(){ ReleaseSRWLockExclusive(&SRWLock); }
  int wait(void){  // returns 0 if wait satisfied, non-0 otherwise
    AcquireSRWLockExclusive(&SRWLock);
    return 0;
  }
};

class EventSyncTwo {
private:
  HANDLE Event;
public:
  EventSyncTwo(){
    Event = CreateEvent(0, true, false, 0);
  }
  ~EventSyncTwo(){
    CloseHandle(Event);
  }
  void set(){ int res = SetEvent(Event); assert(res); }
  void reset(){ int res = ResetEvent(Event); assert(res); }
  int wait(unsigned timeout = INFINITE){  // returns 0 if wait satisfied, non-0 otherwise
    int res;
    res = WaitForSingleObject(Event, timeout);
    return res;
  }
};

class EventSyncThree {
private:
  HANDLE Semaphore;
public:
  EventSyncThree(){
    Semaphore = CreateSemaphore(0, 0, 10, 0);
  }
  ~EventSyncThree(){
    CloseHandle(Semaphore);
  }
  void set(){ int res = ReleaseSemaphore(Semaphore, 1, 0); assert(res); }
  int wait(void){  // returns 0 if wait satisfied, non-0 otherwise
    int res;
    res = WaitForSingleObject(Semaphore, INFINITE); assert(res==0);
    return 0;
  }
};

class EventSyncFour {
private:
  CONDITION_VARIABLE cv;
  CRITICAL_SECTION cs;
  int flag;
public:
  EventSyncFour(){
    InitializeConditionVariable(&cv);
    InitializeCriticalSection(&cs);
    flag = 0;
  }
  void set(){ 
    flag = 1;
    WakeConditionVariable(&cv);
  }
  int wait(void){  // returns 0 if wait satisfied, non-0 otherwise
    int res;
    EnterCriticalSection(&cs);
    while (flag == 0){
      res = SleepConditionVariableCS(&cv, &cs, INFINITE);
      assert(res);
    }
    return 0;
  }
};

#endif
