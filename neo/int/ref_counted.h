#pragma once
#include "neo/platform/atomic.h"
#include "neo/int/non_copyable.h"

namespace NEO {

	/// refcounted base
	/// WARNING, FOR SINGLE-THREADED USE ONLY
	struct ISphRefcounted : public ISphNoncopyable
	{
	protected:
		ISphRefcounted() : m_iRefCount(1) {}
		virtual			~ISphRefcounted() {}

	public:
		void			AddRef() const { m_iRefCount++; }
		void			Release() const { --m_iRefCount; assert(m_iRefCount >= 0); if (m_iRefCount == 0) delete this; }

	protected:
		mutable int		m_iRefCount;
	};


	/// automatic pointer wrapper for refcounted objects
	/// construction from or assignment of a raw pointer takes over (!) the ownership
	template < typename T >
	class CSphRefcountedPtr
	{
	public:
		explicit		CSphRefcountedPtr() { m_pPtr = NULL; }	///< default NULL wrapper construction (for vectors)
		explicit		CSphRefcountedPtr(T* pPtr) { m_pPtr = pPtr; }	///< construction from raw pointer, takes over ownership!

		CSphRefcountedPtr(const CSphRefcountedPtr& rhs)
		{
			if (rhs.m_pPtr)
				rhs.m_pPtr->AddRef();
			m_pPtr = rhs.m_pPtr;
		}

		CSphRefcountedPtr(CSphRefcountedPtr&& rhs)
			: m_pPtr(std::move(rhs.m_pPtr))
		{
			rhs.m_pPtr = nullptr;
		}
		~CSphRefcountedPtr() { SafeRelease(m_pPtr); }

		T* Ptr() const { return m_pPtr; }
		T* operator -> () const { return m_pPtr; }
		bool			operator ! () const { return m_pPtr == NULL; }

	public:
		/// assignment of a raw pointer, takes over ownership!
		CSphRefcountedPtr<T>& operator = (T* pPtr)
		{
			if (m_pPtr != pPtr)
				SafeRelease(m_pPtr);
			m_pPtr = pPtr;
			return *this;
		}

		/// wrapper assignment, does automated reference tracking
		CSphRefcountedPtr<T>& operator = (const CSphRefcountedPtr<T>& rhs)
		{
			if (rhs.m_pPtr)
				rhs.m_pPtr->AddRef();
			SafeRelease(m_pPtr);
			m_pPtr = rhs.m_pPtr;
			return *this;
		}

		CSphRefcountedPtr<T>& operator= (CSphRefcountedPtr<T>&& rhs)
		{
			SafeRelease(m_pPtr);
			m_pPtr = std::move(rhs.m_pPtr);
			rhs.m_pPtr = nullptr;
			return *this;
		}

	protected:
		T* m_pPtr;
	};


	/// MT-aware refcounted base (might be a mutex protected and slow)
	struct ISphRefcountedMT : public ISphNoncopyable
	{
	protected:
		ISphRefcountedMT()
			: m_iRefCount(1)
		{}

		virtual ~ISphRefcountedMT()
		{}

	public:
		void AddRef() const
		{
			++m_iRefCount;
		}

		void Release() const
		{
			long uRefs = --m_iRefCount;
			assert(uRefs >= 0);
			if (uRefs == 0)
				delete this;
		}

		int GetRefcount() const
		{
			return m_iRefCount;
		}

	protected:
		mutable CSphAtomic	m_iRefCount;
	};



}