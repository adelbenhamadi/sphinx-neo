#include "neo/core/iwordlist.h"

namespace NEO {

	ISphWordlist::Args_t::Args_t(bool bPayload, int iExpansionLimit, bool bHasMorphology, ESphHitless eHitless, const void* pIndexData)
		: m_bPayload(bPayload)
		, m_iExpansionLimit(iExpansionLimit)
		, m_bHasMorphology(bHasMorphology)
		, m_eHitless(eHitless)
		, m_pIndexData(pIndexData)
	{
		m_sBuf.Reserve(2048 * SPH_MAX_WORD_LEN * 3);
		m_dExpanded.Reserve(2048);
		m_pPayload = NULL;
		m_iTotalDocs = 0;
		m_iTotalHits = 0;
	}


	ISphWordlist::Args_t::~Args_t()
	{
		SafeDelete(m_pPayload);
	}


	void ISphWordlist::Args_t::AddExpanded(const BYTE* sName, int iLen, int iDocs, int iHits)
	{
		SphExpanded_t& tExpanded = m_dExpanded.Add();
		tExpanded.m_iDocs = iDocs;
		tExpanded.m_iHits = iHits;
		int iOff = m_sBuf.GetLength();
		tExpanded.m_iNameOff = iOff;

		m_sBuf.Resize(iOff + iLen + 1);
		memcpy(m_sBuf.Begin() + iOff, sName, iLen);
		m_sBuf[iOff + iLen] = '\0';
	}


	const char* ISphWordlist::Args_t::GetWordExpanded(int iIndex) const
	{
		assert(m_dExpanded[iIndex].m_iNameOff < m_sBuf.GetLength());
		return (const char*)m_sBuf.Begin() + m_dExpanded[iIndex].m_iNameOff;
	}


}