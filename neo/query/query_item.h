#pragma once
#include "neo/int/types.h"
#include "neo/source/enums.h"

namespace NEO {

	/// query selection item
	struct CSphQueryItem
	{
		CSphString		m_sExpr;		///< expression to compute
		CSphString		m_sAlias;		///< alias to return
		ESphAggrFunc	m_eAggrFunc;

		CSphQueryItem() : m_eAggrFunc(SPH_AGGR_NONE) {}
	};

}