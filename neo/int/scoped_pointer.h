#pragma once
#include "neo/int/types.h"

namespace NEO {

	/// pointer with automatic safe deletion when going out of scope
	template < typename T >
	class CSphScopedPtr : public ISphNoncopyable
	{
	public:
		explicit		CSphScopedPtr(T* pPtr) { m_pPtr = pPtr; }
		~CSphScopedPtr() { SafeDelete(m_pPtr); }
		T* operator -> () const { return m_pPtr; }
		T* Ptr() const { return m_pPtr; }
		CSphScopedPtr& operator = (T* pPtr) { SafeDelete(m_pPtr); m_pPtr = pPtr; return *this; }
		T* LeakPtr() { T* pPtr = m_pPtr; m_pPtr = NULL; return pPtr; }
		void			ReplacePtr(T* pPtr) { m_pPtr = pPtr; }
		void			Reset() { SafeDelete(m_pPtr); }

	protected:
		T* m_pPtr;
	};


}