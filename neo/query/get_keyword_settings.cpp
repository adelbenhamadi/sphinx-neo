#include "neo/query/get_keyword_settings.h"

namespace NEO {

	GetKeywordsSettings_t::GetKeywordsSettings_t()
	{
		m_bStats = true;
		m_bFoldLemmas = false;
		m_bFoldBlended = false;
		m_bFoldWildcards = false;
		m_iExpansionLimit = 0;
	}


}