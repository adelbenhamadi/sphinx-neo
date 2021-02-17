#include "neo/platform/thread.h"
#include "neo/platform/mutex.h"
#include "neo/platform/atomic.h"
#include "neo/core/die.h"

#if !USE_WINDOWS
#include <sys/time.h> // for gettimeofday

// define this if you want to run gprof over the threads model - to track children threads also.
#define USE_GPROF 0

#endif

namespace NEO {

int g_iThreadStackSize = 1024 * 1024;

char CSphString::EMPTY[] = "";


//////////////////////////////////////////////////////////////////////////


// This is a working context for a thread wrapper. It wraps every thread to
// store information about it's stack size, cleanup threads and something else.
// This struct always should be allocated in the heap, cause wrapper need
// to see it all the time and it frees it out of the heap by itself. Wrapper thread function
// receives as an argument a pointer to ThreadCall_t with one function pointer to
// a main thread function. Afterwards, thread can set up one or more cleanup functions
// which will be executed by a wrapper in the linked list order after it dies.
struct ThreadCall_t
{
	void			(*m_pCall)(void* pArg);
	void* m_pArg;
#if USE_GPROF
	pthread_mutex_t	m_dlock;
	pthread_cond_t	m_dwait;
	itimerval		m_ditimer;
#endif
	ThreadCall_t* m_pNext;
};
static SphThreadKey_t g_tThreadCleanupKey;
static SphThreadKey_t g_tMyThreadStack;


#if USE_WINDOWS
#define SPH_THDFUNC DWORD __stdcall
#else
#define SPH_THDFUNC void *
#endif

SPH_THDFUNC sphThreadProcWrapper(void* pArg)
{
	// This is the first local variable in the new thread. So, its address is the top of the stack.
	// We need to know thread stack size for both expression and query evaluating engines.
	// We store expressions as a linked tree of structs and execution is a calls of mutually
	// recursive methods. Before executing we compute tree height and multiply it by a constant
	// with experimentally measured value to check whether we have enough stack to execute current query.
	// The check is not ideal and do not work for all compilers and compiler settings.
	char	cTopOfMyStack;
	assert(sphThreadGet(g_tThreadCleanupKey) == NULL);
	assert(sphThreadGet(g_tMyThreadStack) == NULL);

#if SPH_ALLOCS_PROFILER
	MemCategoryStack_t* pTLS = sphMemStatThdInit();
#endif

#if USE_GPROF
	// Set the profile timer value
	setitimer(ITIMER_PROF, &((ThreadCall_t*)pArg)->m_ditimer, NULL);

	// Tell the calling thread that we don't need its data anymore
	pthread_mutex_lock(&((ThreadCall_t*)pArg)->m_dlock);
	pthread_cond_signal(&((ThreadCall_t*)pArg)->m_dwait);
	pthread_mutex_unlock(&((ThreadCall_t*)pArg)->m_dlock);
#endif

	ThreadCall_t* pCall = (ThreadCall_t*)pArg;
	MemorizeStack(&cTopOfMyStack);
	pCall->m_pCall(pCall->m_pArg);
	SafeDelete(pCall);

	ThreadCall_t* pCleanup = (ThreadCall_t*)sphThreadGet(g_tThreadCleanupKey);
	while (pCleanup)
	{
		pCall = pCleanup;
		pCall->m_pCall(pCall->m_pArg);
		pCleanup = pCall->m_pNext;
		SafeDelete(pCall);
	}

#if SPH_ALLOCS_PROFILER
	sphMemStatThdCleanup(pTLS);
#endif

	return 0;
}

#if !USE_WINDOWS
void* sphThreadInit(bool bDetached)
#else
void* sphThreadInit(bool)
#endif
{
	static bool bInit = false;
#if !USE_WINDOWS
	static pthread_attr_t tJoinableAttr;
	static pthread_attr_t tDetachedAttr;
#endif

	if (!bInit)
	{
#if SPH_DEBUG_LEAKS || SPH_ALLOCS_PROFILER
		sphMemStatInit();
#endif

		// we're single-threaded yet, right?!
		if (!sphThreadKeyCreate(&g_tThreadCleanupKey))
			sphDie("FATAL: sphThreadKeyCreate() failed");

		if (!sphThreadKeyCreate(&g_tMyThreadStack))
			sphDie("FATAL: sphThreadKeyCreate() failed");

#if !USE_WINDOWS
		if (pthread_attr_init(&tJoinableAttr))
			sphDie("FATAL: pthread_attr_init( joinable ) failed");

		if (pthread_attr_init(&tDetachedAttr))
			sphDie("FATAL: pthread_attr_init( detached ) failed");

		if (pthread_attr_setdetachstate(&tDetachedAttr, PTHREAD_CREATE_DETACHED))
			sphDie("FATAL: pthread_attr_setdetachstate( detached ) failed");
#endif
		bInit = true;
	}
#if !USE_WINDOWS
	if (pthread_attr_setstacksize(&tJoinableAttr, g_iThreadStackSize + PTHREAD_STACK_MIN))
		sphDie("FATAL: pthread_attr_setstacksize( joinable ) failed");

	if (pthread_attr_setstacksize(&tDetachedAttr, g_iThreadStackSize + PTHREAD_STACK_MIN))
		sphDie("FATAL: pthread_attr_setstacksize( detached ) failed");

	return bDetached ? &tDetachedAttr : &tJoinableAttr;
#else
	return NULL;
#endif
}


#if SPH_DEBUG_LEAKS || SPH_ALLOCS_PROFILER
void sphThreadDone(int iFD)
{
	sphMemStatDump(iFD);
	sphMemStatDone();
}
#else
void sphThreadDone(int)
{
}
#endif


bool sphThreadCreate(SphThread_t* pThread, void (*fnThread)(void*), void* pArg, bool bDetached)
{
	// we can not put this on current stack because wrapper need to see
	// it all the time and it will destroy this data from heap by itself
	ThreadCall_t* pCall = new ThreadCall_t;
	pCall->m_pCall = fnThread;
	pCall->m_pArg = pArg;
	pCall->m_pNext = NULL;

	// create thread
#if USE_WINDOWS
	sphThreadInit(bDetached);
	*pThread = CreateThread(NULL, g_iThreadStackSize, sphThreadProcWrapper, pCall, 0, NULL);
	if (*pThread)
		return true;
#else

#if USE_GPROF
	getitimer(ITIMER_PROF, &pCall->m_ditimer);
	pthread_cond_init(&pCall->m_dwait, NULL);
	pthread_mutex_init(&pCall->m_dlock, NULL);
	pthread_mutex_lock(&pCall->m_dlock);
#endif

	void* pAttr = sphThreadInit(bDetached);
	errno = pthread_create(pThread, (pthread_attr_t*)pAttr, sphThreadProcWrapper, pCall);

#if USE_GPROF
	if (!errno)
		pthread_cond_wait(&pCall->m_dwait, &pCall->m_dlock);

	pthread_mutex_unlock(&pCall->m_dlock);
	pthread_mutex_destroy(&pCall->m_dlock);
	pthread_cond_destroy(&pCall->m_dwait);
#endif

	if (!errno)
		return true;

#endif

	// thread creation failed so we need to cleanup ourselves
	SafeDelete(pCall);
	return false;
}


bool sphThreadJoin(SphThread_t* pThread)
{
#if USE_WINDOWS
	DWORD uWait = WaitForSingleObject(*pThread, INFINITE);
	CloseHandle(*pThread);
	*pThread = NULL;
	return (uWait == WAIT_OBJECT_0 || uWait == WAIT_ABANDONED);
#else
	return pthread_join(*pThread, NULL) == 0;
#endif
}

// Adds a function call (a new task for a wrapper) to a linked list
// of thread contexts. They will be executed one by one right after
// the main thread ends its execution. This is a way for a wrapper
// to free local resources allocated by its main thread.
void sphThreadOnExit(void (*fnCleanup)(void*), void* pArg)
{
	ThreadCall_t* pCleanup = new ThreadCall_t;
	pCleanup->m_pCall = fnCleanup;
	pCleanup->m_pArg = pArg;
	pCleanup->m_pNext = (ThreadCall_t*)sphThreadGet(g_tThreadCleanupKey);
	sphThreadSet(g_tThreadCleanupKey, pCleanup);
}


bool sphThreadKeyCreate(SphThreadKey_t* pKey)
{
#if USE_WINDOWS
	* pKey = TlsAlloc();
	return *pKey != TLS_OUT_OF_INDEXES;
#else
	return pthread_key_create(pKey, NULL) == 0;
#endif
}


void sphThreadKeyDelete(SphThreadKey_t tKey)
{
#if USE_WINDOWS
	TlsFree(tKey);
#else
	pthread_key_delete(tKey);
#endif
}


void* sphThreadGet(SphThreadKey_t tKey)
{
#if USE_WINDOWS
	return TlsGetValue(tKey);
#else
	return pthread_getspecific(tKey);
#endif
}

void* sphMyStack()
{
	return sphThreadGet(g_tMyThreadStack);
}


int64_t sphGetStackUsed()
{
	BYTE cStack;
	BYTE* pStackTop = (BYTE*)sphMyStack();
	if (!pStackTop)
		return 0;
	int64_t iHeight = pStackTop - &cStack;
	return (iHeight >= 0) ? iHeight : -iHeight;
}

void sphSetMyStackSize(int iStackSize)
{
	g_iThreadStackSize = iStackSize;
	sphThreadInit(false);
}


void MemorizeStack(void* PStack)
{
	sphThreadSet(g_tMyThreadStack, PStack);
}


bool sphThreadSet(SphThreadKey_t tKey, void* pValue)
{
#if USE_WINDOWS
	return TlsSetValue(tKey, pValue) != FALSE;
#else
	return pthread_setspecific(tKey, pValue) == 0;
#endif
}

#if !USE_WINDOWS
bool sphIsLtLib()
{
#ifndef _CS_GNU_LIBPTHREAD_VERSION
	return false;
#else
	char buff[64];
	confstr(_CS_GNU_LIBPTHREAD_VERSION, buff, 64);

	if (!strncasecmp(buff, "linuxthreads", 12))
		return true;
	return false;
#endif
}
#endif


/// microsecond precision timestamp
int64_t sphMicroTimer()
{
#if USE_WINDOWS
	// Windows time query
	static int64_t iBase = 0;
	static int64_t iStart = 0;
	static int64_t iFreq = 0;

	LARGE_INTEGER iLarge;
	if (!iBase)
	{
		// get start QPC value
		QueryPerformanceFrequency(&iLarge); iFreq = iLarge.QuadPart;
		QueryPerformanceCounter(&iLarge); iStart = iLarge.QuadPart;

		// get start UTC timestamp
		// assuming it's still approximately the same moment as iStart, give or take a msec or three
		FILETIME ft;
		GetSystemTimeAsFileTime(&ft);

		iBase = (int64_t(ft.dwHighDateTime) << 32) + int64_t(ft.dwLowDateTime);
		iBase = (iBase - 116444736000000000ULL) / 10; // rebase from 01 Jan 1601 to 01 Jan 1970, and rescale to 1 usec from 100 ns
	}

	// we can't easily drag iBase into parens because iBase*iFreq/1000000 overflows 64bit int!
	QueryPerformanceCounter(&iLarge);
	return iBase + (iLarge.QuadPart - iStart) * 1000000 / iFreq;

#else
	// UNIX time query
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return int64_t(tv.tv_sec) * int64_t(1000000) + int64_t(tv.tv_usec);
#endif // USE_WINDOWS
}


#if USE_WINDOWS
template<> long CSphAtomic_T<long>::GetValue() const
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedExchangeAdd(&m_iValue, 0);
}

template<> int64_t CSphAtomic_T<int64_t>::GetValue() const
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedExchangeAdd64(&m_iValue, 0);
}

template<> long CSphAtomic_T<long>::Inc()
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedIncrement(&m_iValue) - 1;
}

template<> int64_t CSphAtomic_T<int64_t>::Inc()
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedIncrement64(&m_iValue) - 1;
}

template<> long CSphAtomic_T<long>::Dec()
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedDecrement(&m_iValue) + 1;
}

template<> int64_t CSphAtomic_T<int64_t>::Dec()
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedDecrement64(&m_iValue) + 1;
}

template<> long CSphAtomic_T<long>::Add(long iValue)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedExchangeAdd(&m_iValue, iValue);
}

template<> int64_t CSphAtomic_T<int64_t>::Add(int64_t iValue)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedExchangeAdd64(&m_iValue, iValue);
}

template<> long CSphAtomic_T<long>::Sub(long iValue)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedExchangeAdd(&m_iValue, -iValue);
}

template<> int64_t CSphAtomic_T<int64_t>::Sub(int64_t iValue)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedExchangeAdd64(&m_iValue, -iValue);
}

template<> void CSphAtomic_T<long>::SetValue(long iValue)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	InterlockedExchange(&m_iValue, iValue);
}

template<> void CSphAtomic_T<int64_t>::SetValue(int64_t iValue)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	InterlockedExchange64(&m_iValue, iValue);
}

template<> long CSphAtomic_T<long>::CAS(long iOldVal, long iNewVal)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedCompareExchange(&m_iValue, iNewVal, iOldVal);
}

template<> int64_t CSphAtomic_T<int64_t>::CAS(int64_t iOldVal, int64_t iNewVal)
{
	assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
	return InterlockedCompareExchange64(&m_iValue, iNewVal, iOldVal);
}

#endif

// fast check if we are built with right endianess settings
const char* sphCheckEndian()
{
	const char* sErrorMsg = "Oops! It seems that sphinx was built with wrong endianess (cross-compiling?)\n"
#if USE_LITTLE_ENDIAN
		"either reconfigure and rebuild, defining ac_cv_c_bigendian=yes in the environment of ./configure script,\n"
		"either ensure that '#define USE_LITTLE_ENDIAN = 0' in config/config.h\n";
#else
		"either reconfigure and rebuild, defining ac_cv_c_bigendian=no in the environment of ./configure script,\n"
		"either ensure that '#define USE_LITTLE_ENDIAN = 1' in config/config.h\n";
#endif

	char sMagic[] = "\x01\x02\x03\x04\x05\x06\x07\x08";
	unsigned long* pMagic;
	unsigned long uResult;
	pMagic = (unsigned long*)sMagic;
	uResult = 0xFFFFFFFF & (*pMagic);
#if USE_LITTLE_ENDIAN
	if (uResult == 0x01020304 || uResult == 0x05060708)
#else
	if (uResult == 0x08070605 || uResult == 0x04030201)
#endif
		return sErrorMsg;
	return NULL;
}

struct ThdJob_t
{
	ISphJob* m_pItem;
	ThdJob_t* m_pNext;
	ThdJob_t* m_pPrev;

	ThdJob_t()
		: m_pItem(NULL)
		, m_pNext(NULL)
		, m_pPrev(NULL)
	{}

	~ThdJob_t()
	{
		SafeDelete(m_pItem);
	}
};

#ifdef USE_VTUNE
#include "ittnotify.h"
static void SetThdName(const char*)
{
	__itt_thread_set_name("job");
}
#else
static void SetThdName(const char*) {}
#endif

class CSphThdPool : public ISphThdPool
{
	CSphSemaphore					m_tWorkSem;
	CSphMutex						m_tJobLock;

	CSphFixedVector<SphThread_t>	m_dWorkers;
	ThdJob_t* m_pHead;
	ThdJob_t* m_pTail;

	volatile bool					m_bShutdown;

	CSphAtomic						m_tStatActiveWorkers;
	int								m_iStatQueuedJobs;

public:
	explicit CSphThdPool(int iThreads, const char* sName)
		: m_dWorkers(0)
		, m_pHead(NULL)
		, m_pTail(NULL)
		, m_bShutdown(false)
		, m_iStatQueuedJobs(0)
	{
		Verify(m_tWorkSem.Init(sName));

		iThreads = Max(iThreads, 1);
		m_dWorkers.Reset(iThreads);
		ARRAY_FOREACH(i, m_dWorkers)
		{
			sphThreadCreate(m_dWorkers.Begin() + i, Tick, this);
		}
	}

	virtual ~CSphThdPool()
	{
		Shutdown();

		Verify(m_tWorkSem.Done());
	}

	virtual void Shutdown()
	{
		if (m_bShutdown)
			return;

		m_bShutdown = true;

		ARRAY_FOREACH(i, m_dWorkers)
			m_tWorkSem.Post();

		ARRAY_FOREACH(i, m_dWorkers)
			sphThreadJoin(m_dWorkers.Begin() + i);

		while (m_pHead && m_pHead != m_pTail)
		{
			ThdJob_t* pNext = m_pHead->m_pNext;
			SafeDelete(m_pHead);
			m_pHead = pNext;
		}
	}

	virtual void AddJob(ISphJob* pItem)
	{
		assert(pItem);
		assert(!m_bShutdown);

		ThdJob_t* pJob = new ThdJob_t;
		pJob->m_pItem = pItem;

		m_tJobLock.Lock();

		if (!m_pHead)
		{
			m_pHead = pJob;
			m_pTail = pJob;
		}
		else
		{
			pJob->m_pNext = m_pHead;
			m_pHead->m_pPrev = pJob;
			m_pHead = pJob;
		}

		m_iStatQueuedJobs++;

		m_tWorkSem.Post();
		m_tJobLock.Unlock();
	}

	virtual void StartJob(ISphJob* pItem)
	{
		// FIXME!!! start thread only in case of no workers available to offload call site
		SphThread_t tThd;
		sphThreadCreate(&tThd, Start, pItem, true);
	}

private:
	static void Tick(void* pArg)
	{
		SetThdName("job");

		CSphThdPool* pPool = (CSphThdPool*)pArg;

		while (!pPool->m_bShutdown)
		{
			pPool->m_tWorkSem.Wait();

			if (pPool->m_bShutdown)
				break;

			pPool->m_tJobLock.Lock();

			ThdJob_t* pJob = pPool->m_pTail;
			if (pPool->m_pHead == pPool->m_pTail) // either 0 or 1 job case
			{
				pPool->m_pHead = pPool->m_pTail = NULL;
			}
			else
			{
				pJob->m_pPrev->m_pNext = NULL;
				pPool->m_pTail = pJob->m_pPrev;
			}

			if (pJob)
				pPool->m_iStatQueuedJobs--;

			pPool->m_tJobLock.Unlock();

			if (!pJob)
				continue;

			pPool->m_tStatActiveWorkers.Inc();

			pJob->m_pItem->Call();
			SafeDelete(pJob);

			pPool->m_tStatActiveWorkers.Dec();

			// FIXME!!! work stealing case (check another job prior going to sem)
		}
	}

	static void Start(void* pArg)
	{
		ISphJob* pJob = (ISphJob*)pArg;
		if (pJob)
			pJob->Call();
		SafeDelete(pJob);
	}

	virtual int GetActiveWorkerCount() const
	{
		return m_tStatActiveWorkers.GetValue();
	}

	virtual int GetTotalWorkerCount() const
	{
		return m_dWorkers.GetLength();
	}

	virtual int GetQueueLength() const
	{
		return m_iStatQueuedJobs;
	}
};


ISphThdPool* sphThreadPoolCreate(int iThreads, const char* sName)
{
	return new CSphThdPool(iThreads, sName);
}

int sphCpuThreadsCount()
{
#if USE_WINDOWS
	SYSTEM_INFO tInfo;
	GetSystemInfo(&tInfo);
	return tInfo.dwNumberOfProcessors;
#else
	return sysconf(_SC_NPROCESSORS_ONLN);
#endif
}

}