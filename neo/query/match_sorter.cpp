#include "neo/query/match_sorter.h"

namespace NEO {

	void ISphMatchSorter::SetState(const CSphMatchComparatorState& tState)
	{
		m_tState = tState;
		m_tState.m_iNow = (DWORD)time(NULL);
	}

}
