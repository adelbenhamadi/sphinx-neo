#pragma once
#include "neo/int/types.h"

namespace NEO {

	/// hit info
	struct CSphWordHit
	{
		SphDocID_t		m_uDocID;		///< document ID
		SphWordID_t		m_uWordID;		///< word ID in current dictionary
		Hitpos_t		m_uWordPos;		///< word position in current document
	};

}