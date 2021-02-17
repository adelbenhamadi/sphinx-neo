#include "neo/core/skip_list.h"

namespace NEO {

	bool operator < (const SkiplistEntry_t& a, SphDocID_t b) { return a.m_iBaseDocid < b; }
	bool operator == (const SkiplistEntry_t& a, SphDocID_t b) { return a.m_iBaseDocid == b; }
	bool operator < (SphDocID_t a, const SkiplistEntry_t& b) { return a < b.m_iBaseDocid; }

}

