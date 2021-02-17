#pragma once
#include "neo/int/types.h"

namespace NEO {

	struct KillListTrait_t
	{
		const SphDocID_t* m_pBegin;
		int					m_iLen;
	};

	typedef CSphVector<KillListTrait_t> KillListVector;

}
