#include "neo/core/match.h"
#include "neo/core/arena.h"


namespace NEO {

	//extern DWORD* g_pMvaArena;



	// OPTIMIZE! try to inline or otherwise simplify maybe
	const DWORD* CSphMatch::GetAttrMVA(const CSphAttrLocator& tLoc, const DWORD* pPool, bool bArenaProhibit) const
	{
		DWORD uIndex = MVA_DOWNSIZE(GetAttr(tLoc));
		if (!uIndex)
			return NULL;

		if (!bArenaProhibit && (uIndex & MVA_ARENA_FLAG))
			return g_pMvaArena + (uIndex & MVA_OFFSET_MASK);

		assert(pPool);
		return pPool + uIndex;
	}
	inline void Swap(CSphMatch& a, CSphMatch& b)
	{
		Swap(a.m_uDocID, b.m_uDocID);
		Swap(a.m_pStatic, b.m_pStatic);
		Swap(a.m_pDynamic, b.m_pDynamic);
		Swap(a.m_iWeight, b.m_iWeight);
		Swap(a.m_iTag, b.m_iTag);
	}
}