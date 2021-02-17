#pragma once
#include "neo/int/types.h"
#include "neo/dict/dict_header.h"
#include "neo/source/source_stats.h"
#include "neo/int/throttle_state.h"

namespace NEO {

	struct BuildHeader_t : public CSphSourceStats, public DictHeader_t
	{
		explicit BuildHeader_t(const CSphSourceStats& tStat)
			: m_sHeaderExtension(NULL)
			, m_pThrottle(NULL)
			, m_pMinRow(NULL)
			, m_uMinDocid(0)
			, m_uKillListSize(0)
			, m_iMinMaxIndex(0)
			, m_iTotalDups(0)
		{
			m_iTotalDocuments = tStat.m_iTotalDocuments;
			m_iTotalBytes = tStat.m_iTotalBytes;
		}

		const char* m_sHeaderExtension;
		ThrottleState_t* m_pThrottle;
		const CSphRowitem* m_pMinRow;
		SphDocID_t			m_uMinDocid;
		DWORD				m_uKillListSize;
		int64_t				m_iMinMaxIndex;
		int					m_iTotalDups;
	};

}