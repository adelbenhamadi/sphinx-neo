#pragma once
#include "neo/int/types.h"
#include "neo/source/attrib_locator.h"
#include "neo/source/hitman.h"
#include "neo/query/extra.h"
#include "neo/core/match.h"


namespace NEO {

	//fwd dec
	class CSphSchema;

	enum SphZoneHit_e
	{
		SPH_ZONE_FOUND,
		SPH_ZONE_NO_SPAN,
		SPH_ZONE_NO_DOCUMENT
	};

	class ISphZoneCheck
	{
	public:
		virtual ~ISphZoneCheck() {}
		virtual SphZoneHit_e IsInZone(int iZone, const ExtHit_t* pHit, int* pLastSpan) = 0;
	};


	struct SphFactorHashEntry_t
	{
		SphDocID_t				m_iId;
		int						m_iRefCount;
		BYTE* m_pData;
		SphFactorHashEntry_t* m_pPrev;
		SphFactorHashEntry_t* m_pNext;
	};

	typedef CSphFixedVector<SphFactorHashEntry_t*> SphFactorHash_t;


	struct SphExtraDataRankerState_t
	{
		const CSphSchema* m_pSchema;
		const int64_t* m_pFieldLens;
		CSphAttrLocator		m_tFieldLensLoc;
		int64_t				m_iTotalDocuments;
		int					m_iFields;
		int					m_iMaxQpos;
		SphExtraDataRankerState_t()
			: m_pSchema(NULL)
			, m_pFieldLens(NULL)
			, m_iTotalDocuments(0)
			, m_iFields(0)
			, m_iMaxQpos(0)
		{ }
	};


	struct MatchSortAccessor_t
	{
		typedef CSphMatch T;
		typedef CSphMatch* MEDIAN_TYPE;

		CSphMatch m_tMedian;

		MatchSortAccessor_t() {}
		MatchSortAccessor_t(const MatchSortAccessor_t&) {}

		virtual ~MatchSortAccessor_t()
		{
			m_tMedian.m_pDynamic = NULL; // not yours
		}

		MEDIAN_TYPE Key(CSphMatch* a) const
		{
			return a;
		}

		void CopyKey(MEDIAN_TYPE* pMed, CSphMatch* pVal)
		{
			*pMed = &m_tMedian;
			m_tMedian.m_uDocID = pVal->m_uDocID;
			m_tMedian.m_iWeight = pVal->m_iWeight;
			m_tMedian.m_pStatic = pVal->m_pStatic;
			m_tMedian.m_pDynamic = pVal->m_pDynamic;
			m_tMedian.m_iTag = pVal->m_iTag;
		}

		void Swap(T* a, T* b) const;

		T* Add(T* p, int i) const
		{
			return p + i;
		}

		int Sub(T* b, T* a) const
		{
			return (int)(b - a);
		}
	};


}