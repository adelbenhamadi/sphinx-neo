#include "neo/core/globals.h"
#include "neo/platform/mutex.h"
#include "neo/core/die.h"

namespace NEO {

// Windows mutex implementation
#if USE_WINDOWS


	CSphMutex::CSphMutex()
	{
		m_hMutex = CreateMutex(NULL, FALSE, NULL);
		if (!m_hMutex)
			sphDie("CreateMutex() failed");
	}

	CSphMutex::~CSphMutex()
	{
		if (CloseHandle(m_hMutex) == FALSE)
			sphDie("CloseHandle() failed");
	}

	bool CSphMutex::Lock()
	{
		DWORD uWait = WaitForSingleObject(m_hMutex, INFINITE);
		return (uWait != WAIT_FAILED && uWait != WAIT_TIMEOUT);
	}

	bool CSphMutex::TimedLock(int iMsec)
	{
		DWORD uWait = WaitForSingleObject(m_hMutex, iMsec);
		return (uWait != WAIT_FAILED && uWait != WAIT_TIMEOUT);
	}

	bool CSphMutex::Unlock()
	{
		return ReleaseMutex(m_hMutex) == TRUE;
	}

	bool CSphAutoEvent::Init(CSphMutex*)
	{
		m_bSent = false;
		m_hEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
		m_bInitialized = (m_hEvent != 0);
		return m_bInitialized;
	}

	bool CSphAutoEvent::Done()
	{
		if (!m_bInitialized)
			return true;

		m_bInitialized = false;
		return CloseHandle(m_hEvent) == TRUE;
	}

	void CSphAutoEvent::SetEvent()
	{
		::SetEvent(m_hEvent);
		m_bSent = true;
	}

	bool CSphAutoEvent::WaitEvent()
	{
		if (m_bSent)
		{
			m_bSent = false;
			return true;
		}
		DWORD uWait = WaitForSingleObject(m_hEvent, INFINITE);
		return !(uWait == WAIT_FAILED || uWait == WAIT_TIMEOUT);
	}

	CSphSemaphore::CSphSemaphore()
		: m_bInitialized(false)
	{
	}

	CSphSemaphore::~CSphSemaphore()
	{
		assert(!m_bInitialized);
	}

	bool CSphSemaphore::Init(const char*)
	{
		assert(!m_bInitialized);
		m_hSem = CreateSemaphore(NULL, 0, INT_MAX, NULL);
		m_bInitialized = (m_hSem != NULL);
		return m_bInitialized;
	}

	bool CSphSemaphore::Done()
	{
		if (!m_bInitialized)
			return true;

		m_bInitialized = false;
		return (CloseHandle(m_hSem) == TRUE);
	}

	void CSphSemaphore::Post()
	{
		assert(m_bInitialized);
		ReleaseSemaphore(m_hSem, 1, NULL);
	}

	bool CSphSemaphore::Wait()
	{
		assert(m_bInitialized);
		DWORD uWait = WaitForSingleObject(m_hSem, INFINITE);
		return !(uWait == WAIT_FAILED || uWait == WAIT_TIMEOUT);
	}

#else

// UNIX mutex implementation

	CSphMutex::CSphMutex()
	{
		m_pMutex = new pthread_mutex_t;
		if (pthread_mutex_init(m_pMutex, NULL))
			sphDie("pthread_mutex_init() failed %s", strerror(errno));
	}

	CSphMutex::~CSphMutex()
	{
		if (pthread_mutex_destroy(m_pMutex))
			sphDie("pthread_mutex_destroy() failed %s", strerror(errno));
		SafeDelete(m_pMutex);
	}

	bool CSphMutex::Lock()
	{
		return (pthread_mutex_lock(m_pMutex) == 0);
	}

	bool CSphMutex::TimedLock(int iMsec)
	{
		// pthread_mutex_timedlock is not available on Mac Os. Fallback to lock without a timer.
#if defined (HAVE_PTHREAD_MUTEX_TIMEDLOCK)
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);

		int ns = ts.tv_nsec + (iMsec % 1000) * 1000000;
		ts.tv_sec += (ns / 1000000000) + (iMsec / 1000);
		ts.tv_nsec = (ns % 1000000000);

		int iRes = pthread_mutex_timedlock(m_pMutex, &ts);
		return iRes == 0;

#else
		int iRes = EBUSY;
		int64_t tmTill = sphMicroTimer() + iMsec;
		do
		{
			iRes = pthread_mutex_trylock(m_pMutex);
			if (iRes != EBUSY)
				break;
			sphSleepMsec(1);
		} while (sphMicroTimer() < tmTill);
		if (iRes == EBUSY)
			iRes = pthread_mutex_trylock(m_pMutex);

		return iRes != EBUSY;

#endif
	}

	bool CSphMutex::Unlock()
	{
		return (pthread_mutex_unlock(m_pMutex) == 0);
	}

	bool CSphAutoEvent::Init(CSphMutex* pMutex)
	{
		m_bSent = false;
		assert(pMutex);
		if (!pMutex)
			return false;
		m_pMutex = pMutex->GetInternalMutex();
		m_bInitialized = (pthread_cond_init(&m_tCond, NULL) == 0);
		return m_bInitialized;
	}

	bool CSphAutoEvent::Done()
	{
		if (!m_bInitialized)
			return true;

		m_bInitialized = false;
		return (pthread_cond_destroy(&m_tCond)) == 0;
	}

	void CSphAutoEvent::SetEvent()
	{
		if (!m_bInitialized)
			return;

		pthread_cond_signal(&m_tCond); // locking is done from outside
		m_bSent = true;
	}

	bool CSphAutoEvent::WaitEvent()
	{
		if (!m_bInitialized)
			return true;
		pthread_mutex_lock(m_pMutex);
		if (!m_bSent)
			pthread_cond_wait(&m_tCond, m_pMutex);
		m_bSent = false;
		pthread_mutex_unlock(m_pMutex);
		return true;
	}


	CSphSemaphore::CSphSemaphore()
		: m_bInitialized(false)
		, m_pSem(NULL)
	{
	}


	CSphSemaphore::~CSphSemaphore()
	{
		assert(!m_bInitialized);
		sem_close(m_pSem);
	}


	bool CSphSemaphore::Init(const char* sName)
	{
		assert(!m_bInitialized);
		m_pSem = sem_open(sName, O_CREAT, 0, 0);
		m_sName = sName;
		m_bInitialized = (m_pSem != SEM_FAILED);
		return m_bInitialized;
	}

	bool CSphSemaphore::Done()
	{
		if (!m_bInitialized)
			return true;

		m_bInitialized = false;
		int iRes = sem_close(m_pSem);
		sem_unlink(m_sName.cstr());
		return (iRes == 0);
	}

	void CSphSemaphore::Post()
	{
		assert(m_bInitialized);
		sem_post(m_pSem);
	}

	bool CSphSemaphore::Wait()
	{
		assert(m_bInitialized);
		int iRes = sem_wait(m_pSem);
		return (iRes == 0);
	}

#endif

	//rlock


#if USE_WINDOWS

// Windows rwlock implementation

	CSphRwlock::CSphRwlock()
		: m_bInitialized(false)
		, m_hWriteMutex(NULL)
		, m_hReadEvent(NULL)
		, m_iReaders(0)
	{}


	bool CSphRwlock::Init(bool)
	{
		assert(!m_bInitialized);
		assert(!m_hWriteMutex && !m_hReadEvent && !m_iReaders);

		m_hReadEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
		if (!m_hReadEvent)
			return false;

		m_hWriteMutex = CreateMutex(NULL, FALSE, NULL);
		if (!m_hWriteMutex)
		{
			CloseHandle(m_hReadEvent);
			m_hReadEvent = NULL;
			return false;
		}
		m_bInitialized = true;
		return true;
	}


	bool CSphRwlock::Done()
	{
		if (!m_bInitialized)
			return true;

		if (!CloseHandle(m_hReadEvent))
			return false;
		m_hReadEvent = NULL;

		if (!CloseHandle(m_hWriteMutex))
			return false;
		m_hWriteMutex = NULL;

		m_iReaders = 0;
		m_bInitialized = false;
		return true;
	}


	bool CSphRwlock::ReadLock()
	{
		assert(m_bInitialized);

		DWORD uWait = WaitForSingleObject(m_hWriteMutex, INFINITE);
		if (uWait == WAIT_FAILED || uWait == WAIT_TIMEOUT)
			return false;

		// got the writer mutex, can't be locked for write
		// so it's OK to add the reader lock, then free the writer mutex
		// writer mutex also protects readers counter
		InterlockedIncrement(&m_iReaders);

		// reset writer lock event, we just got ourselves a reader
		if (!ResetEvent(m_hReadEvent))
			return false;

		// release writer lock
		return ReleaseMutex(m_hWriteMutex) == TRUE;
	}


	bool CSphRwlock::WriteLock()
	{
		assert(m_bInitialized);

		// try to acquire writer mutex
		DWORD uWait = WaitForSingleObject(m_hWriteMutex, INFINITE);
		if (uWait == WAIT_FAILED || uWait == WAIT_TIMEOUT)
			return false;

		// got the writer mutex, no pending readers, rock'n'roll
		if (!m_iReaders)
			return true;

		// got the writer mutex, but still have to wait for all readers to complete
		uWait = WaitForSingleObject(m_hReadEvent, INFINITE);
		if (uWait == WAIT_FAILED || uWait == WAIT_TIMEOUT)
		{
			// wait failed, well then, release writer mutex
			ReleaseMutex(m_hWriteMutex);
			return false;
		}
		return true;
	}


	bool CSphRwlock::Unlock()
	{
		assert(m_bInitialized);

		// are we unlocking a writer?
		if (ReleaseMutex(m_hWriteMutex))
			return true; // yes we are

		if (GetLastError() != ERROR_NOT_OWNER)
			return false; // some unexpected error

		// writer mutex wasn't mine; we must have a read lock
		if (!m_iReaders)
			return true; // could this ever happen?

		// atomically decrement reader counter
		if (InterlockedDecrement(&m_iReaders))
			return true; // there still are pending readers

		// no pending readers, fire the event for write lock
		return SetEvent(m_hReadEvent) == TRUE;
	}

#else

// UNIX rwlock implementation (pthreads wrapper)

	CSphRwlock::CSphRwlock()
		: m_bInitialized(false)
	{
		m_pLock = new pthread_rwlock_t;
	}

	bool CSphRwlock::Init(bool bPreferWriter)
	{
		assert(!m_bInitialized);
		assert(m_pLock);

		pthread_rwlockattr_t tAttr;
		pthread_rwlockattr_t* pAttr = NULL;

		// Mac OS X knows nothing about PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
#ifndef __APPLE__
		while (bPreferWriter)
		{
			bool bOk = (pthread_rwlockattr_init(&tAttr) == 0);
			assert(bOk);
			if (!bOk)
				break;

			bOk = (pthread_rwlockattr_setkind_np(&tAttr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP) == 0);
			assert(bOk);
			if (!bOk)
			{
				pthread_rwlockattr_destroy(&tAttr);
				break;
			}

			pAttr = &tAttr;
			break;
		}
#endif
		m_bInitialized = (pthread_rwlock_init(m_pLock, pAttr) == 0);

		if (pAttr)
			pthread_rwlockattr_destroy(&tAttr);

		return m_bInitialized;
	}

	bool CSphRwlock::Done()
	{
		assert(m_pLock);
		if (!m_bInitialized)
			return true;

		m_bInitialized = !(pthread_rwlock_destroy(m_pLock) == 0);
		return !m_bInitialized;
	}

	bool CSphRwlock::ReadLock()
	{
		assert(m_bInitialized);
		assert(m_pLock);

		return pthread_rwlock_rdlock(m_pLock) == 0;
	}

	bool CSphRwlock::WriteLock()
	{
		assert(m_bInitialized);
		assert(m_pLock);

		return pthread_rwlock_wrlock(m_pLock) == 0;
	}

	bool CSphRwlock::Unlock()
	{
		assert(m_bInitialized);
		assert(m_pLock);

		return pthread_rwlock_unlock(m_pLock) == 0;
	}

#endif

}