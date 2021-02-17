#include "query_stats.h"


namespace NEO {

	CSphQueryStats::CSphQueryStats()
		: m_pNanoBudget(NULL)
		, m_iFetchedDocs(0)
		, m_iFetchedHits(0)
		, m_iSkips(0)
	{
	}

	void CSphQueryStats::Add(const CSphQueryStats& tStats)
	{
		m_iFetchedDocs += tStats.m_iFetchedDocs;
		m_iFetchedHits += tStats.m_iFetchedHits;
		m_iSkips += tStats.m_iSkips;
	}

}