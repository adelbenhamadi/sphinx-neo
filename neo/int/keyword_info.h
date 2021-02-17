#pragma once 

namespace NEO {

	// keyword info
	struct CSphKeywordInfo
	{
		CSphString		m_sTokenized;
		CSphString		m_sNormalized;
		int				m_iDocs;
		int				m_iHits;
		int				m_iQpos;
	};

	inline void Swap(CSphKeywordInfo& v1, CSphKeywordInfo& v2)
	{
		v1.m_sTokenized.SwapWith(v2.m_sTokenized);
		v1.m_sNormalized.SwapWith(v2.m_sNormalized);
		Swap(v1.m_iDocs, v2.m_iDocs);
		Swap(v1.m_iHits, v2.m_iHits);
		Swap(v1.m_iQpos, v2.m_iQpos);
	}

}