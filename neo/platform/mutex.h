#pragma once
#include "neo/core/config.h"
#include "neo/int/non_copyable.h"

#if defined(__clang__)
#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__( x ) // no-op
#endif

#define CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

#define SCOPED_CAPABILITY \
    THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define GUARDED_BY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#define PT_GUARDED_BY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define ACQUIRED_BEFORE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define ACQUIRED_AFTER( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))

#define REQUIRES( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))

#define REQUIRES_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(requires_shared_capability(__VA_ARGS__))

#define ACQUIRE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))

#define ACQUIRE_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))

#define RELEASE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))

#define RELEASE_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))

#define TRY_ACQUIRE( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))

#define TRY_ACQUIRE_SHARED( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

#define EXCLUDES( ... ) \
    THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define ASSERT_CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(assert_capability(x))

#define ASSERT_SHARED_CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_capability(x))

#define RETURN_CAPABILITY( x ) \
    THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define NO_THREAD_SAFETY_ANALYSIS \
    THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

// Replaced by RELEASE and RELEASE_SHARED
#define UNLOCK_FUNCTION( ... ) \
	THREAD_ANNOTATION_ATTRIBUTE__(unlock_function(__VA_ARGS__))

namespace NEO {
	/// mutex implementation
	class CAPABILITY("mutex") CSphMutex : public ISphNoncopyable
	{
	public:
		CSphMutex();
		~CSphMutex();

		bool Lock() ACQUIRE();
		bool Unlock() RELEASE();
		bool TimedLock(int iMsec) TRY_ACQUIRE(false);

	protected:
#if USE_WINDOWS
		HANDLE m_hMutex;
#else
		pthread_mutex_t* m_pMutex;
	public:
		inline pthread_mutex_t* GetInternalMutex() RETURN_CAPABILITY(this)
		{
			return m_pMutex;
		}
#endif
	};

	// event implementation
	class CSphAutoEvent : public ISphNoncopyable
	{
	public:
		CSphAutoEvent() {}
		~CSphAutoEvent() {}

		bool Init(CSphMutex* pMutex);
		bool Done();
		void SetEvent();
		bool WaitEvent();

	protected:
		bool m_bInitialized;
		bool m_bSent;
#if USE_WINDOWS
		HANDLE m_hEvent;
#else
		pthread_cond_t m_tCond;
		pthread_mutex_t* m_pMutex;
#endif
	};

	// semaphore implementation
	class CSphSemaphore : public ISphNoncopyable
	{
	public:
		CSphSemaphore();
		~CSphSemaphore();

		bool Init(const char* sName = NULL);
		bool Done();
		void Post();
		bool Wait();

	protected:
		bool m_bInitialized;
#if USE_WINDOWS
		HANDLE	m_hSem;
#else
		sem_t* m_pSem;
		CSphString m_sName;	// unnamed semaphores are deprecated and removed in some OS
#endif
	};


	/// scoped mutex lock
	template < typename T >
	class SCOPED_CAPABILITY CSphScopedLock : public ISphNoncopyable
	{
	public:
		/// lock on creation
		explicit CSphScopedLock(T& tMutex) ACQUIRE(tMutex)
			: m_tMutexRef(tMutex)
		{
			m_tMutexRef.Lock();
		}

		/// unlock on going out of scope
		~CSphScopedLock() RELEASE()
		{
			m_tMutexRef.Unlock();
		}

	protected:
		T& m_tMutexRef;
	};


	/// rwlock implementation
	class CAPABILITY("mutex") CSphRwlock : public ISphNoncopyable
	{
	public:
		CSphRwlock();
		~CSphRwlock() {
#if !USE_WINDOWS
			SafeDelete(m_pLock);
#endif
		}

		bool Init(bool bPreferWriter = false);
		bool Done();

		bool ReadLock() ACQUIRE_SHARED();
		bool WriteLock() ACQUIRE();
		bool Unlock() UNLOCK_FUNCTION();

	private:
		bool				m_bInitialized;
#if USE_WINDOWS
		HANDLE				m_hWriteMutex;
		HANDLE				m_hReadEvent;
		LONG				m_iReaders;
#else
		pthread_rwlock_t* m_pLock;
#endif
	};


	/// scoped shared (read) lock
	class SCOPED_CAPABILITY CSphScopedRLock : ISphNoncopyable
	{
	public:
		/// lock on creation
		CSphScopedRLock(CSphRwlock& tLock) ACQUIRE_SHARED(tLock)
			: m_tLock(tLock)
		{
			m_tLock.ReadLock();
		}

		/// unlock on going out of scope
		~CSphScopedRLock() RELEASE()
		{
			m_tLock.Unlock();
		}

	protected:
		CSphRwlock& m_tLock;
	};

	/// scoped shared (write) lock
	class SCOPED_CAPABILITY CSphScopedWLock : ISphNoncopyable
	{
	public:
		/// lock on creation
		CSphScopedWLock(CSphRwlock& tLock) ACQUIRE_SHARED(tLock)
			: m_tLock(tLock)
		{
			m_tLock.WriteLock();
		}

		/// unlock on going out of scope
		~CSphScopedWLock() RELEASE()
		{
			m_tLock.Unlock();
		}

	protected:
		CSphRwlock& m_tLock;
	};

}