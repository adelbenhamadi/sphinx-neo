#pragma once
#include "neo/int/types.h"
#include "neo/platform/mutex.h"

#if (USE_WINDOWS) || (HAVE_SYNC_FETCH)
#define NO_ATOMIC 0
#else
#define NO_ATOMIC 1
#endif

namespace NEO {
	// interlocked (atomic) operation

	template<typename TLONG>
	class CSphAtomic_T : public ISphNoncopyable
	{
		volatile mutable TLONG	m_iValue;
#if NO_ATOMIC
		mutable CSphMutex		m_tLock;
#endif

	public:
		explicit CSphAtomic_T(TLONG iValue = 0)
			: m_iValue(iValue)
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
		}

		~CSphAtomic_T()
		{}

		inline operator TLONG() const
		{
			return GetValue();
		}

		inline CSphAtomic_T& operator= (TLONG iArg)
		{
			SetValue(iArg);
			return *this;
		}

		inline TLONG operator++ (int) // postfix
		{
			return Inc();
		}

		inline TLONG operator++ ()
		{
			TLONG iPrev = Inc();
			return ++iPrev;
		}

		inline CSphAtomic_T& operator+= (TLONG iArg)
		{
			Add(iArg);
			return *this;
		}

		inline TLONG operator-- (int) // postfix
		{
			return Dec();
		}

		inline TLONG operator-- ()
		{
			TLONG iPrev = Dec();
			return --iPrev;
		}

		inline CSphAtomic_T& operator-= (TLONG iArg)
		{
			Sub(iArg);
			return *this;
		}


#ifdef HAVE_SYNC_FETCH
		TLONG GetValue() const
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			return __sync_fetch_and_add(&m_iValue, 0);
		}

		// return value here is original value, prior to operation took place
		inline TLONG Inc()
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			return __sync_fetch_and_add(&m_iValue, 1);
		}

		inline TLONG Dec()
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			return __sync_fetch_and_sub(&m_iValue, 1);
		}

		inline TLONG Add(TLONG iValue)
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			return __sync_fetch_and_add(&m_iValue, iValue);
		}

		inline TLONG Sub(TLONG iValue)
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			return __sync_fetch_and_sub(&m_iValue, iValue);
		}

		void SetValue(TLONG iValue)
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			for (;; )
			{
				TLONG iOld = GetValue();
				if (__sync_bool_compare_and_swap(&m_iValue, iOld, iValue))
					break;
			}
		}

		inline TLONG CAS(TLONG iOldVal, TLONG iNewVal)
		{
			assert((((size_t)&m_iValue) % (sizeof(&m_iValue))) == 0 && "unaligned atomic!");
			return __sync_val_compare_and_swap(&m_iValue, iOldVal, iNewVal);
		}

#elif USE_WINDOWS
		TLONG GetValue() const;
		TLONG Inc();
		TLONG Dec();
		TLONG Add(TLONG);
		TLONG Sub(TLONG);
		void SetValue(TLONG iValue);
		TLONG CAS(TLONG, TLONG);
#endif

#if NO_ATOMIC
		TLONG GetValue() const NO_THREAD_SAFETY_ANALYSIS
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			return m_iValue;
		}

		inline TLONG Inc() NO_THREAD_SAFETY_ANALYSIS
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			return m_iValue++;
		}

		inline TLONG Dec() NO_THREAD_SAFETY_ANALYSIS
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			return m_iValue--;
		}

		inline TLONG Add(TLONG iValue) NO_THREAD_SAFETY_ANALYSIS
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			TLONG iPrev = m_iValue;
			m_iValue += iValue;
			return iPrev;
		}

		inline TLONG Sub(TLONG iValue) NO_THREAD_SAFETY_ANALYSIS
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			TLONG iPrev = m_iValue;
			m_iValue -= iValue;
			return iPrev;
		}

		void SetValue(TLONG iValue) NO_THREAD_SAFETY_ANALYSIS
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			m_iValue = iValue;
		}

		inline TLONG CAS(TLONG iOldVal, TLONG iNewVal)
		{
			CSphScopedLock<CSphMutex> tLock(m_tLock);
			TLONG iRes = m_iValue;
			if (m_iValue == iOldVal)
				m_iValue = iNewVal;
			return iRes;
		}

#endif
	};

	typedef CSphAtomic_T<long> CSphAtomic;
	typedef CSphAtomic_T<int64_t> CSphAtomicL;

}