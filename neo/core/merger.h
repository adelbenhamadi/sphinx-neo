#pragma once
#include "neo/index/index_VLN.h"
#include "neo/tools/docinfo_transformer.h"
#include "neo/core/hit_builder.h"
#include "neo/dict/dict_reader.h"

namespace NEO {

	class CSphMerger
	{
	private:
		CSphFixedVector<CSphRowitem> m_dInlineRow;
		CSphHitBuilder* m_pHitBuilder;
		SphDocID_t			m_uMinID;

	public:
		explicit CSphMerger(CSphHitBuilder* pHitBuilder, int iInlineCount, SphDocID_t uMinID)
			: m_dInlineRow(iInlineCount)
			, m_pHitBuilder(pHitBuilder)
			, m_uMinID(uMinID)
		{
		}

		template < typename QWORD >
		static inline void PrepareQword(QWORD& tQword, const CSphDictReader& tReader, SphDocID_t iMinID, bool bWordDict) //NOLINT
		{
			tQword.m_iMinID = iMinID;
			tQword.m_tDoc.m_uDocID = iMinID;

			tQword.m_iDocs = tReader.m_iDocs;
			tQword.m_iHits = tReader.m_iHits;
			tQword.m_bHasHitlist = tReader.m_bHasHitlist;

			tQword.m_uHitPosition = 0;
			tQword.m_iHitlistPos = 0;

			if (bWordDict)
				tQword.m_rdDoclist.SeekTo(tReader.m_iDoclistOffset, tReader.m_iHint);
		}

		template < typename QWORD >
		inline bool NextDocument(QWORD& tQword, const CSphIndex_VLN* pSourceIndex, const ISphFilter* pFilter, const CSphVector<SphDocID_t>& dKillList)
		{
			for (;; )
			{
				tQword.GetNextDoc(m_dInlineRow.Begin());
				if (tQword.m_tDoc.m_uDocID)
				{
					tQword.SeekHitlist(tQword.m_iHitlistPos);

					if (dKillList.BinarySearch(tQword.m_tDoc.m_uDocID)) // optimize this somehow?
					{
						while (tQword.m_bHasHitlist && tQword.GetNextHit() != EMPTY_HIT);
						continue;
					}
					if (pFilter)
					{
						CSphMatch tMatch;
						tMatch.m_uDocID = tQword.m_tDoc.m_uDocID;
						if (pFilter->UsesAttrs())
						{
							if (m_dInlineRow.GetLength())
								tMatch.m_pDynamic = m_dInlineRow.Begin();
							else
							{
								const DWORD* pInfo = pSourceIndex->FindDocinfo(tQword.m_tDoc.m_uDocID);
								tMatch.m_pStatic = pInfo ? DOCINFO2ATTRS(pInfo) : NULL;
							}
						}
						bool bResult = pFilter->Eval(tMatch);
						tMatch.m_pDynamic = NULL;
						if (!bResult)
						{
							while (tQword.m_bHasHitlist && tQword.GetNextHit() != EMPTY_HIT);
							continue;
						}
					}
					return true;
				}
				else
					return false;
			}
		}

		template < typename QWORD >
		inline void TransferData(QWORD& tQword, SphWordID_t iWordID, const BYTE* sWord,
			const CSphIndex_VLN* pSourceIndex, const ISphFilter* pFilter,
			const CSphVector<SphDocID_t>& dKillList, volatile bool* pGlobalStop, volatile bool* pLocalStop)
		{
			CSphAggregateHit tHit;
			tHit.m_uWordID = iWordID;
			tHit.m_sKeyword = sWord;
			tHit.m_dFieldMask.UnsetAll();

			while (CSphMerger::NextDocument(tQword, pSourceIndex, pFilter, dKillList) && !*pGlobalStop && !*pLocalStop)
			{
				if (tQword.m_bHasHitlist)
					TransferHits(tQword, tHit);
				else
				{
					// convert to aggregate if there is no hit-list
					tHit.m_uDocID = tQword.m_tDoc.m_uDocID - m_uMinID;
					tHit.m_dFieldMask = tQword.m_dQwordFields;
					tHit.SetAggrCount(tQword.m_uMatchHits);
					m_pHitBuilder->cidxHit(&tHit, m_dInlineRow.Begin());
				}
			}
		}

		template < typename QWORD >
		inline void TransferHits(QWORD& tQword, CSphAggregateHit& tHit)
		{
			assert(tQword.m_bHasHitlist);
			tHit.m_uDocID = tQword.m_tDoc.m_uDocID - m_uMinID;
			for (Hitpos_t uHit = tQword.GetNextHit(); uHit != EMPTY_HIT; uHit = tQword.GetNextHit())
			{
				tHit.m_iWordPos = uHit;
				m_pHitBuilder->cidxHit(&tHit, m_dInlineRow.Begin());
			}
		}

		template < typename QWORD >
		static inline void ConfigureQword(QWORD& tQword, const CSphAutofile& tHits, const CSphAutofile& tDocs,
			int iDynamic, int iInline, const CSphRowitem* pMin, ThrottleState_t* pThrottle)
		{
			tQword.m_iInlineAttrs = iInline;
			tQword.m_pInlineFixup = iInline ? pMin : NULL;

			tQword.m_rdHitlist.SetThrottle(pThrottle);
			tQword.m_rdHitlist.SetFile(tHits);
			tQword.m_rdHitlist.GetByte();

			tQword.m_rdDoclist.SetThrottle(pThrottle);
			tQword.m_rdDoclist.SetFile(tDocs);
			tQword.m_rdDoclist.GetByte();

			tQword.m_tDoc.Reset(iDynamic);
		}

		const CSphRowitem* GetInline() const { return m_dInlineRow.Begin(); }
		CSphRowitem* AcquireInline() const { return m_dInlineRow.Begin(); }
	};

}