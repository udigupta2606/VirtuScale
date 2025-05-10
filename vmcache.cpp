
// utilities 
#include <algorithm> 
#include <functional> 
#include <numeric>
#include <set>
#include <vector>
#include <span> //for span, a lightweight array view
#include <iostream> //for input and output

// time and error handling
#include <exception> //for exception handling
#include <sys/time.h> //for time-related functions
#include <errno.h>  //for error handling
#include <cassert> //to catch incorrect assumptions like invalid state
#include <csignal> //for signal handling to intercept page faults

// multithreading and synchronization 
#include <atomic> //provide atomic operations
#include <mutex> //for mutexes
#include <thread> //for threads 

//memory management
#include <sys/mman.h> //for memory management declarations
#include <immintrin.h> //for Intel intrinsics

// file device io
#include <fcntl.h> //for file control options
#include <libaio.h> //for asynchronous I/O
#include <sys/ioctl.h> //for ioctl system call for device-specific input/output operations
#include <sys/stat.h> //for file status
#include <sys/types.h> //for data types used in system calls
#include <unistd.h> //for POSIX operating system API POSIX  "Portable Operating System Interface" 

//expmap
#include "expmap.h" //for expmap library

#include "tpcc/TPCCWorkload.h" //for TPCC workload 

using namespace std;

__thread uint16_t workerThreadId = 0;
__thread int32_t tpcchistorycounter = 0;

// typedefs
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef u64 PID; //page id type

static const u64 pageSize=4096; //page size 4kb 

struct alignas(4096) Page{
    bool dirty; //indicates if the page is dirty
};

static const int16_t maxWorkerThreads=128; //maximum number of worker threads

#define die(msg) do{ perror(msg); exit(EXIT_FAILURE);}while(0) //macro to handle error & exit program

u64 rdtsc() { //function to read the time-stamp counter
    u32 lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return static_cast<u64>(lo)|(static_cast<u64>(hi)<<32);
}

// exmap helper function 
static int exmapAction(int exmapfd,exmap_opcode op ,u16 len)
{
    struct exmap_action_params params_free={ 
        .interface=workerThreadId,
        .iov_len=len,
        .opcode=(u16)op,
    };
    return ioctl(exmapfd,EXMAP_ACTION,&params_free); //perform the action on the exmap file descriptor
}

//alocate memory using huge pages
void* allocHuge(size_t size){
    void* p=mmap(NULL,size,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0); //map memory
    madvise(p,size,MADV_HUGEPAGE); //advise the kernel to use huge pages
    return p; //return the pointer to the allocated memory
}

//use when lock is not free
void yield(u64 counter){
    _mm_pause(); //pause the thread for a short period
}

struct PageState{
    atomic<u64> stateAndVersion; //atomic variable to store state and version

    static const u64 Unlocked=0; //unlocked state
    static const u64 MaxShared=252; //maximum shared state
    static const u63 Locked=253; //locked state
    static const u64 Marked=254; //marked state
    static const u64 Evicted=255; //evicted state

    PageState(){}

    void init(){
        stateAndVersion.store(sameVersion(0,Evicted),memory_order_release); //initialize the state and version
    }

    static inline u64 sameVersion(u64 oldStateAndVersion,u64 newState){
        return ((oldStateAndVersion<<8)>>8)|newState<<56;}
    }

    static inline u64 nextVersion(u64 oldStateAndVersion,u64 newState){
        return (((oldStateAndVersion<<8)>>8)+1)|newState<<56;
    }

    bool tryLockX(u64 oldStateAndVersion)
    {
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion,sameVersion(oldStateAndVersion,Locked));
    }

    void unlockX()
    {
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(),Unlocked),memory_order_release);
    }

    void unlockXEvicted(){
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(),Evicted),memory_order_release);
    }

    void downgradeLock(){
        assert(getState() == Locked);
        stateAndVersion.store(nextVersion(stateAndVersion.load(),1),memory_order_release);
    }

    bool tryLockS(u64 oldStateAndVersion)
    {
        u64 s=getState(oldStateAndVersion);
        if(s<MaxShared)
        {
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion,sameVersion(oldStateAndVersion,s+1));
        }
        if(s==Marked)
        {
            return stateAndVersion.compare_exchange_strong(oldStateAndVersion,sameVersion(oldStateAndVersion,1));
        }
        return false;
    }

    void unlockS(){
        while(true)
        {
            u64 oldStateAndVersion=stateAndVersion.load();
            u64 state=getState(oldStateAndVersion);
            assert(state>0 && state<=MaxShared);
            if (stateAndVersion.compare_exchange_strong(oldStateAndVersion,sameVersion(oldStateAndVersion,state-1)))
            {
                return;
            }   
        }
    }
    bool tryMark(u64 oldStateAndVersion) {
        assert(getState(oldStateAndVersion)==Unlocked);
        return stateAndVersion.compare_exchange_strong(oldStateAndVersion, sameVersion(oldStateAndVersion, Marked));
     }
  
     static u64 getState(u64 v) { return v >> 56; };
     u64 getState() { return getState(stateAndVersion.load()); }
  
     void operator=(PageState&) = delete;

};











