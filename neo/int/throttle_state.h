#pragma once
#include "neo/int/types.h"

namespace NEO {

	struct ThrottleState_t
	{
		int64_t	m_tmLastIOTime;
		int		m_iMaxIOps;
		int		m_iMaxIOSize;


		ThrottleState_t()
			: m_tmLastIOTime(0)
			, m_iMaxIOps(0)
			, m_iMaxIOSize(0)
		{}
	};

}

