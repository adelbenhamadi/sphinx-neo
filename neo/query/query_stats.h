#pragma once
#include "neo/int/types.h"


namespace NEO {
	/// low-level query stats
	struct CSphQueryStats
	{
		int64_t* m_pNanoBudget;		///< pointer to max_predicted_time budget (counted in nanosec)
		DWORD		m_iFetchedDocs;		///< processed documents
		DWORD		m_iFetchedHits;		///< processed hits (aka positions)
		DWORD		m_iSkips;			///< number of Skip() calls

		CSphQueryStats();

		void		Add(const CSphQueryStats& tStats);
	};


}