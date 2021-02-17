#pragma once
#include "neo/int/types.h"

namespace NEO {

	extern int g_iThreadStackSize;

	/// microsecond precision timestamp
	/// current UNIX timestamp in seconds multiplied by 1000000, plus microseconds since the beginning of current second
	int64_t		sphMicroTimer();


	/// my thread handle and thread func magic
#if USE_WINDOWS
	typedef HANDLE SphThread_t;
	typedef DWORD SphThreadKey_t;
#else
	typedef pthread_t SphThread_t;
	typedef pthread_key_t SphThreadKey_t;
#endif

	/// my threading initialize routine
	void* sphThreadInit(bool bDetached = false);

	/// my threading deinitialize routine
	void sphThreadDone(int iFD);

	/// my create thread wrapper
	bool sphThreadCreate(SphThread_t* pThread, void (*fnThread)(void*), void* pArg, bool bDetached = false);

	/// my join thread wrapper
	bool sphThreadJoin(SphThread_t* pThread);

	/// add (cleanup) callback to run on thread exit
	void sphThreadOnExit(void (*fnCleanup)(void*), void* pArg);

	/// alloc thread-local key
	bool sphThreadKeyCreate(SphThreadKey_t* pKey);

	/// free thread-local key
	void sphThreadKeyDelete(SphThreadKey_t tKey);

	/// get thread-local key value
	void* sphThreadGet(SphThreadKey_t tKey);

	/// get the pointer to my thread's stack
	void* sphMyStack();

	/// get size of used stack
	int64_t sphGetStackUsed();

	/// set the size of my thread's stack
	void sphSetMyStackSize(int iStackSize);

	/// store the address in the TLS
	void MemorizeStack(void* PStack);

	/// set thread-local key value
	bool sphThreadSet(SphThreadKey_t tKey, void* pValue);

#if !USE_WINDOWS
	/// what kind of threading lib do we have? The number of frames in the stack depends from it
	bool sphIsLtLib();
#endif

	int sphCpuThreadsCount();

	//////////////////////////////////////////////////////////////////////////


	struct ISphJob
	{
		ISphJob() {}
		virtual ~ISphJob() {}
		virtual void Call() = 0;
	};

	struct ISphThdPool
	{
		ISphThdPool() {}
		virtual ~ISphThdPool() {}
		virtual void Shutdown() = 0;
		virtual void AddJob(ISphJob* pItem) = 0;
		virtual void StartJob(ISphJob* pItem) = 0;

		virtual int GetActiveWorkerCount() const = 0;
		virtual int GetTotalWorkerCount() const = 0;
		virtual int GetQueueLength() const = 0;
	};

	ISphThdPool* sphThreadPoolCreate(int iThreads, const char* sName = NULL);

	////////////////////////////////////////////////////////////////////////////

	/// scoped thread scheduling helper
	/// either makes the current thread low priority while the helper lives, or does noething
	class ScopedThreadPriority_c
	{
	private:
		bool m_bRestore;

	public:
		ScopedThreadPriority_c(bool bLowPriority)
		{
			m_bRestore = false;
			if (!bLowPriority)
				return;

#if USE_WINDOWS
			if (!SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_IDLE))
				return;
#else
			struct sched_param p;
			p.sched_priority = 0;
#ifdef SCHED_IDLE
			int iPolicy = SCHED_IDLE;
#else
			int iPolicy = SCHED_OTHER;
#endif
			if (pthread_setschedparam(pthread_self(), iPolicy, &p))
				return;
#endif

			m_bRestore = true;
		}

		~ScopedThreadPriority_c()
		{
			if (!m_bRestore)
				return;

#if USE_WINDOWS
			if (!SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_NORMAL))
				return;
#else
			struct sched_param p;
			p.sched_priority = 0;
			if (pthread_setschedparam(pthread_self(), SCHED_OTHER, &p))
				return;
#endif
		}
	};

}