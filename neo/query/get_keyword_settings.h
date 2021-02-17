#pragma once

namespace NEO {
	struct GetKeywordsSettings_t
	{
		bool	m_bStats;
		bool	m_bFoldLemmas;
		bool	m_bFoldBlended;
		bool	m_bFoldWildcards;
		int		m_iExpansionLimit;

		GetKeywordsSettings_t();
	};


}