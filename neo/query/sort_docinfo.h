#pragma once
#include "neo/int/types.h"
#include "neo/core/generic.h"
#include "neo/tools/docinfo_transformer.h"

namespace NEO {

	/// sort baked docinfos by document ID
	struct DocinfoSort_fn
	{
		typedef SphDocID_t MEDIAN_TYPE;

		int m_iStride;

		explicit DocinfoSort_fn(int iStride)
			: m_iStride(iStride)
		{}

		SphDocID_t Key(DWORD* pData) const
		{
			return DOCINFO2ID(pData);
		}

		void CopyKey(SphDocID_t* pMed, DWORD* pVal) const
		{
			*pMed = Key(pVal);
		}

		bool IsLess(SphDocID_t a, SphDocID_t b) const
		{
			return a < b;
		}

		void Swap(DWORD* a, DWORD* b) const;

		DWORD* Add(DWORD* p, int i) const
		{
			return p + i * m_iStride;
		}

		int Sub(DWORD* b, DWORD* a) const
		{
			return (int)((b - a) / m_iStride);
		}
		
	};


	void sphSortDocinfos(DWORD* pBuf, int iCount, int iStride);



}