#include "neo/query/sort_docinfo.h"

	void NEO::DocinfoSort_fn::Swap(DWORD* a, DWORD* b) const
	{
		for (int i = 0; i < m_iStride; i++)
			NEO::Swap(a[i], b[i]);
	}

	inline void NEO::sphSortDocinfos(DWORD* pBuf, int iCount, int iStride)
	{
		DocinfoSort_fn fnSort(iStride);
		sphSort(pBuf, iCount, fnSort, fnSort);
	}