#include "neo/query/uniqounter.h"
#include "neo/query/grouper.h"


#include <cassert>


namespace NEO {

	int CSphUniqounter::CountStart(SphGroupKey_t* pOutGroup)
	{
		m_iCountPos = 0;
		return CountNext(pOutGroup);
	}


	int CSphUniqounter::CountNext(SphGroupKey_t* pOutGroup)
	{
		assert(m_bSorted);
		if (m_iCountPos >= m_iLength)
			return 0;

		SphGroupKey_t uGroup = m_pData[m_iCountPos].m_uGroup;
		SphAttr_t uValue = m_pData[m_iCountPos].m_uValue;
		int iCount = m_pData[m_iCountPos].m_iCount;
		*pOutGroup = uGroup;

		while (m_iCountPos < m_iLength && m_pData[m_iCountPos].m_uGroup == uGroup)
		{
			if (m_pData[m_iCountPos].m_uValue != uValue)
				iCount += m_pData[m_iCountPos].m_iCount;
			uValue = m_pData[m_iCountPos].m_uValue;
			m_iCountPos++;
		}
		return iCount;
	}


	void CSphUniqounter::Compact(SphGroupKey_t* pRemoveGroups, int iRemoveGroups)
	{
		assert(m_bSorted);
		if (!m_iLength)
			return;

		sphSort(pRemoveGroups, iRemoveGroups);

		SphGroupedValue_t* pSrc = m_pData;
		SphGroupedValue_t* pDst = m_pData;
		SphGroupedValue_t* pEnd = m_pData + m_iLength;

		// skip remove-groups which are not in my list
		while (iRemoveGroups && (*pRemoveGroups) < pSrc->m_uGroup)
		{
			pRemoveGroups++;
			iRemoveGroups--;
		}

		for (; pSrc < pEnd; pSrc++)
		{
			// check if this entry needs to be removed
			while (iRemoveGroups && (*pRemoveGroups) < pSrc->m_uGroup)
			{
				pRemoveGroups++;
				iRemoveGroups--;
			}
			if (iRemoveGroups && pSrc->m_uGroup == *pRemoveGroups)
				continue;

			// check if it's a dupe
			if (pDst > m_pData && pDst[-1] == pSrc[0])
				continue;

			*pDst++ = *pSrc;
		}

		assert(pDst - m_pData <= m_iLength);
		m_iLength = pDst - m_pData;
	}



}