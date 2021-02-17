#pragma once
#include "neo/core/globals.h"

namespace NEO {

	// this could be just DWORD[] but it's methods are very handy
	// used to store field information e.g. which fields do we need to search in
	struct FieldMask_t
	{
		static const int SIZE = SPH_MAX_FIELDS / 32;
		STATIC_ASSERT((SPH_MAX_FIELDS % 32) == 0, ASSUME_MAX_FIELDS_ARE_REPRESENTABLE_BY_DWORD);
		DWORD m_dMask[SIZE];

		// no custom cstr and d-tor - to be usable from inside unions
		// deep copy for it is ok - so, no explicit copying constructor and operator=

		// old-fashion layer to work with DWORD (32-bit) mask.
		// all bits above 32 assumed to be unset.
		void Assign32(DWORD uMask)
		{
			UnsetAll();
			m_dMask[0] = uMask;
		}

		DWORD GetMask32() const
		{
			return m_dMask[0];
		}

		DWORD operator[] (int iIdx) const
		{
			assert(0 <= iIdx && iIdx < SIZE);
			return m_dMask[iIdx];
		}

		DWORD& operator[] (int iIdx)
		{
			assert(0 <= iIdx && iIdx < SIZE);
			return m_dMask[iIdx];
		}

		// set n-th bit
		void Set(int iIdx)
		{
			assert(0 <= iIdx && iIdx < (int)sizeof(m_dMask) * 8);
			m_dMask[iIdx / 32] |= 1 << (iIdx % 32);
		}

		// set all bits
		void SetAll()
		{
			memset(m_dMask, 0xff, sizeof(m_dMask));
		}

		// unset n-th bit, or all
		void Unset(int iIdx)
		{
			assert(0 <= iIdx && iIdx < (int)sizeof(m_dMask) * 8);
			m_dMask[iIdx / 32] &= ~(1 << (iIdx % 32));
		}

		void UnsetAll()
		{
			memset(m_dMask, 0, sizeof(m_dMask));
		}

		// test if n-th bit is set
		bool Test(int iIdx) const
		{
			assert(iIdx >= 0 && iIdx < (int)sizeof(m_dMask) * 8);
			return (m_dMask[iIdx / 32] & (1 << (iIdx % 32))) != 0;
		}

		// test if all bits are set or unset
		bool TestAll(bool bSet) const
		{
			DWORD uTest = bSet ? 0xffffffff : 0;
			for (int i = 0; i < SIZE; i++)
				if (m_dMask[i] != uTest)
					return false;
			return true;
		}

		void Negate()
		{
			for (int i = 0; i < SIZE; i++)
				m_dMask[i] = ~m_dMask[i];
		}
	};


}