#pragma once
#include "neo/int/types.h"

namespace NEO {
	/// decoder state saved at a certain offset
	struct SkiplistEntry_t
	{
		SphDocID_t		m_iBaseDocid;		///< delta decoder docid base (aka docid infinum)
		int64_t			m_iOffset;			///< offset in the doclist file (relative to the doclist start)
		int64_t			m_iBaseHitlistPos;	///< delta decoder hitlist offset base
	};


	bool operator < (const SkiplistEntry_t& a, SphDocID_t b);
	bool operator == (const SkiplistEntry_t& a, SphDocID_t b);
	bool operator < (SphDocID_t a, const SkiplistEntry_t& b);

}