#pragma once
#include "neo/int/types.h"
#include "neo/query/field_mask.h"
#include "neo/source/hitman.h"

namespace NEO {
	/// aggregated hit info
	struct CSphAggregateHit
	{
		SphDocID_t		m_uDocID;		//document ID
		SphWordID_t		m_uWordID;		//word ID in current dictionary
		const BYTE* m_sKeyword;		//word itself (in keywords dictionary case only)
		Hitpos_t		m_iWordPos;		//word position in current document, or hit count in case of aggregate hit
		FieldMask_t	m_dFieldMask;	//mask of fields containing this word, 0 for regular hits, non-0 for aggregate hits

		CSphAggregateHit()
			: m_uDocID(0)
			, m_uWordID(0)
			, m_sKeyword(NULL)
		{}

		int GetAggrCount() const
		{
			assert(!m_dFieldMask.TestAll(false));
			return m_iWordPos;
		}

		void SetAggrCount(int iVal)
		{
			m_iWordPos = iVal;
		}
	};


	// OPTIMIZE?
	inline bool SPH_CMPAGGRHIT_LESS(const CSphAggregateHit& a, const CSphAggregateHit& b)
	{
		if (a.m_uWordID < b.m_uWordID)
			return true;

		if (a.m_uWordID > b.m_uWordID)
			return false;

		if (a.m_sKeyword)
		{
			int iCmp = strcmp((char*)a.m_sKeyword, (char*)b.m_sKeyword); // OPTIMIZE?
			if (iCmp != 0)
				return (iCmp < 0);
		}

		return
			(a.m_uDocID < b.m_uDocID) ||
			(a.m_uDocID == b.m_uDocID && HITMAN::GetPosWithField(a.m_iWordPos) < HITMAN::GetPosWithField(b.m_iWordPos));
	}

}