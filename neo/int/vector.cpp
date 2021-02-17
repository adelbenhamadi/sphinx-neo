#include "neo/int/vector.h"

/// find a value-enclosing span in a sorted vector (aka an index at which vec[i] <= val < vec[i+1])
template<typename T, typename U>
int NEO::FindSpan(const CSphVector<T>& dVec, U tRef, int iSmallTreshold)
{
	// empty vector
	if (!dVec.GetLength())
		return -1;

	// check last semi-span
	if (dVec.Last() < tRef || dVec.Last() == tRef)
		return dVec.GetLength() - 1;

	// linear search for small vectors
	if (dVec.GetLength() <= iSmallTreshold)
	{
		for (int i = 0; i < dVec.GetLength() - 1; i++)
			if ((dVec[i] < tRef || dVec[i] == tRef) && tRef < dVec[i + 1])
				return i;
		return -1;
	}

	// binary search for longer vectors
	const T* pStart = dVec.Begin();
	const T* pEnd = &dVec.Last();

	if ((pStart[0] < tRef || pStart[0] == tRef) && tRef < pStart[1])
		return 0;

	if ((pEnd[-1] < tRef || pEnd[-1] == tRef) && tRef < pEnd[0])
		return pEnd - dVec.Begin() - 1;

	while (pEnd - pStart > 1)
	{
		if (tRef < *pStart || *pEnd < tRef)
			break;
		assert(*pStart < tRef);
		assert(tRef < *pEnd);

		const T* pMid = pStart + (pEnd - pStart) / 2;
		assert(pMid + 1 < &dVec.Last());

		if ((pMid[0] < tRef || pMid[0] == tRef) && tRef < pMid[1])
			return pMid - dVec.Begin();

		if (tRef < pMid[0])
			pEnd = pMid;
		else
			pStart = pMid;
	}

	return -1;
}

/// generic sort
template<typename T, typename POLICY>
template<typename F>
void NEO::CSphVector<T, POLICY>::Sort(F COMP, int iStart, int iEnd)
{
	if (m_iLength < 2) return;
	if (iStart < 0) iStart = m_iLength + iStart;
	if (iEnd < 0) iEnd = m_iLength + iEnd;
	assert(iStart <= iEnd);

	sphSort(m_pData + iStart, iEnd - iStart + 1, COMP);
}
/// default reverse sort
template<typename T, typename POLICY>
void NEO::CSphVector<T, POLICY>::RSort(int iStart, int iEnd)
{
	Sort(SphGreater_T<T>(), iStart, iEnd);
}