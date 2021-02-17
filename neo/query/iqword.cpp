#include "neo/query/iqword.h"

namespace NEO {

	void ISphQword::CollectHitMask()
	{
		if (m_bAllFieldsKnown)
			return;
		SeekHitlist(m_iHitlistPos);
		for (Hitpos_t uHit = GetNextHit(); uHit != EMPTY_HIT; uHit = GetNextHit())
			m_dQwordFields.Set(HITMAN::GetField(uHit));
		m_bAllFieldsKnown = true;
	}

}