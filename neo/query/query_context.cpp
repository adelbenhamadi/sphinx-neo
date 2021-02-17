#include "neo/int/types.h"
#include "neo/query/query.h"
#include "neo/query/query_context.h"
#include "neo/query/query_result.h"
#include "neo/query/filter_settings.h"
#include "neo/query/match_processor.h"
#include "neo/index/enums.h"
#include "neo/core/kill_list_trait.h"
#include "neo/core/match.h"
#include "neo/core/attrib_index_builder.h"
#include "neo/core/ranker.h"
#include "neo/core/iextra.h"


#include "neo/sphinx/xfilter.h"
#include "neo/sphinxint.h"


namespace NEO {

	static inline void CalcContextItems(CSphMatch& tMatch, const CSphVector<CSphQueryContext::CalcItem_t>& dItems)
	{
		ARRAY_FOREACH(i, dItems)
		{
			const CSphQueryContext::CalcItem_t& tCalc = dItems[i];
			switch (tCalc.m_eType)
			{
			case ESphAttr::SPH_ATTR_INTEGER:
				tMatch.SetAttr(tCalc.m_tLoc, tCalc.m_pExpr->IntEval(tMatch));
				break;

			case ESphAttr::SPH_ATTR_BIGINT:
			case ESphAttr::SPH_ATTR_JSON_FIELD:
				tMatch.SetAttr(tCalc.m_tLoc, tCalc.m_pExpr->Int64Eval(tMatch));
				break;

			case ESphAttr::SPH_ATTR_STRINGPTR:
			{
				const BYTE* pStr = NULL;
				tCalc.m_pExpr->StringEval(tMatch, &pStr);
				tMatch.SetAttr(tCalc.m_tLoc, (SphAttr_t)pStr); // FIXME! a potential leak of *previous* value?
			}
			break;

			case ESphAttr::SPH_ATTR_FACTORS:
			case ESphAttr::SPH_ATTR_FACTORS_JSON:
				tMatch.SetAttr(tCalc.m_tLoc, (SphAttr_t)tCalc.m_pExpr->FactorEval(tMatch));
				break;

			case ESphAttr::SPH_ATTR_INT64SET:
			case ESphAttr::SPH_ATTR_UINT32SET:
				tMatch.SetAttr(tCalc.m_tLoc, (SphAttr_t)tCalc.m_pExpr->IntEval(tMatch));
				break;

			default:
				tMatch.SetAttrFloat(tCalc.m_tLoc, tCalc.m_pExpr->Eval(tMatch));
			}
		}
	}



	void CSphQueryContext::CalcFilter(CSphMatch& tMatch) const
	{
		CalcContextItems(tMatch, m_dCalcFilter);
	}


	void CSphQueryContext::CalcSort(CSphMatch& tMatch) const
	{
		CalcContextItems(tMatch, m_dCalcSort);
	}


	void CSphQueryContext::CalcFinal(CSphMatch& tMatch) const
	{
		CalcContextItems(tMatch, m_dCalcFinal);
	}

	static inline void FreeStrItems(CSphMatch& tMatch, const CSphVector<CSphQueryContext::CalcItem_t>& dItems)
	{
		if (!tMatch.m_pDynamic)
			return;

		ARRAY_FOREACH(i, dItems)
		{
			const CSphQueryContext::CalcItem_t& tCalc = dItems[i];
			switch (tCalc.m_eType)
			{
			case ESphAttr::SPH_ATTR_STRINGPTR:
			{
				CSphString sStr;
				sStr.Adopt((char**)(tMatch.m_pDynamic + tCalc.m_tLoc.m_iBitOffset / ROWITEM_BITS));
			}
			break;

			case ESphAttr::SPH_ATTR_FACTORS:
			case ESphAttr::SPH_ATTR_FACTORS_JSON:
			{
				BYTE* pData = (BYTE*)tMatch.GetAttr(tCalc.m_tLoc);
				delete[] pData;
				tMatch.SetAttr(tCalc.m_tLoc, 0);
			}
			break;
			default:
				break;
			}
		}
	}

	void CSphQueryContext::FreeStrFilter(CSphMatch& tMatch) const
	{
		FreeStrItems(tMatch, m_dCalcFilter);
	}


	void CSphQueryContext::FreeStrSort(CSphMatch& tMatch) const
	{
		FreeStrItems(tMatch, m_dCalcSort);
	}

	void CSphQueryContext::ExprCommand(ESphExprCommand eCmd, void* pArg)
	{
		ARRAY_FOREACH(i, m_dCalcFilter)
			m_dCalcFilter[i].m_pExpr->Command(eCmd, pArg);
		ARRAY_FOREACH(i, m_dCalcSort)
			m_dCalcSort[i].m_pExpr->Command(eCmd, pArg);
		ARRAY_FOREACH(i, m_dCalcFinal)
			m_dCalcFinal[i].m_pExpr->Command(eCmd, pArg);
	}


	void CSphQueryContext::SetStringPool(const BYTE* pStrings)
	{
		ExprCommand(SPH_EXPR_SET_STRING_POOL, (void*)pStrings);
		if (m_pFilter)
			m_pFilter->SetStringStorage(pStrings);
		if (m_pWeightFilter)
			m_pWeightFilter->SetStringStorage(pStrings);
	}


	void CSphQueryContext::SetMVAPool(const DWORD* pMva, bool bArenaProhibit)
	{
		PoolPtrs_t tMva;
		tMva.m_pMva = pMva;
		tMva.m_bArenaProhibit = bArenaProhibit;
		ExprCommand(SPH_EXPR_SET_MVA_POOL, &tMva);
		if (m_pFilter)
			m_pFilter->SetMVAStorage(pMva, bArenaProhibit);
		if (m_pWeightFilter)
			m_pWeightFilter->SetMVAStorage(pMva, bArenaProhibit);
	}


	/// FIXME, perhaps
	/// this rather crappy helper class really serves exactly 1 (one) simple purpose
	///
	/// it passes a sorting queue internals (namely, weight and float sortkey, if any,
	/// of the current-worst queue element) to the MIN_TOP_WORST() and MIN_TOP_SORTVAL()
	/// expression classes that expose those to the cruel outside world
	///
	/// all the COM-like EXTRA_xxx message back and forth is needed because expressions
	/// are currently parsed and created earlier than the sorting queue
	///
	/// that also is the reason why we mischievously return 0 instead of clearly failing
	/// with an error when the sortval is not a dynamic float; by the time we are parsing
	/// expressions, we do not *yet* know that; but by the time we create a sorting queue,
	/// we do not *want* to leak select expression checks into it
	///
	/// alternatively, we probably want to refactor this and introduce Bind(), to parse
	/// expressions once, then bind them to actual searching contexts (aka index or segment,
	/// and ranker, and sorter, and whatever else might be referenced by the expressions)
	struct ContextExtra : public ISphExtra
	{
		ISphRanker* m_pRanker;
		ISphMatchSorter* m_pSorter;

		virtual bool ExtraDataImpl(ExtraData_e eData, void** ppArg)
		{
			if (eData == EXTRA_GET_QUEUE_WORST || eData == EXTRA_GET_QUEUE_SORTVAL)
			{
				if (!m_pSorter)
					return false;
				const CSphMatch* pWorst = m_pSorter->GetWorst();
				if (!pWorst)
					return false;
				if (eData == EXTRA_GET_QUEUE_WORST)
				{
					*ppArg = (void*)pWorst;
					return true;
				}
				else
				{
					assert(eData == EXTRA_GET_QUEUE_SORTVAL);
					const CSphMatchComparatorState& tCmp = m_pSorter->GetState();
					if (tCmp.m_eKeypart[0] == SPH_KEYPART_FLOAT && tCmp.m_tLocator[0].m_bDynamic
						&& tCmp.m_tLocator[0].m_iBitCount == 32 && (tCmp.m_tLocator[0].m_iBitOffset % 32 == 0)
						&& tCmp.m_eKeypart[1] == SPH_KEYPART_ID && tCmp.m_dAttrs[1] == -1)
					{
						*(int*)ppArg = tCmp.m_tLocator[0].m_iBitOffset / 32;
						return true;
					}
					else
					{
						// min_top_sortval() only works with order by float_expr for now
						return false;
					}
				}
			}
			return m_pRanker->ExtraData(eData, ppArg);
		}
	};


	void CSphQueryContext::SetupExtraData(ISphRanker* pRanker, ISphMatchSorter* pSorter)
	{
		ContextExtra tExtra;
		tExtra.m_pRanker = pRanker;
		tExtra.m_pSorter = pSorter;
		ExprCommand(SPH_EXPR_SET_EXTRA_DATA, &tExtra);
	}


	CSphQueryContext::CSphQueryContext(const CSphQuery& q)
		: m_tQuery(q)
	{
		m_iWeights = 0;
		m_bLookupFilter = false;
		m_bLookupSort = false;
		m_uPackedFactorFlags = SPH_FACTOR_DISABLE;
		m_pFilter = NULL;
		m_pWeightFilter = NULL;
		m_pIndexData = NULL;
		m_pProfile = NULL;
		m_pLocalDocs = NULL;
		m_iTotalDocs = 0;
		m_iBadRows = 0;
	}

	CSphQueryContext::~CSphQueryContext()
	{
		SafeDelete(m_pFilter);
		SafeDelete(m_pWeightFilter);

		ARRAY_FOREACH(i, m_dUserVals)
			m_dUserVals[i]->Release();
	}

	void CSphQueryContext::BindWeights(const CSphQuery* pQuery, const CSphSchema& tSchema, CSphString& sWarning)
	{
		const int MIN_WEIGHT = 1;
		// const int HEAVY_FIELDS = 32;
		const int HEAVY_FIELDS = SPH_MAX_FIELDS;

		// defaults
		m_iWeights = Min(tSchema.m_dFields.GetLength(), HEAVY_FIELDS);
		for (int i = 0; i < m_iWeights; i++)
			m_dWeights[i] = MIN_WEIGHT;

		// name-bound weights
		CSphString sFieldsNotFound;
		if (pQuery->m_dFieldWeights.GetLength())
		{
			ARRAY_FOREACH(i, pQuery->m_dFieldWeights)
			{
				int j = tSchema.GetFieldIndex(pQuery->m_dFieldWeights[i].m_sName.cstr());
				if (j < 0)
				{
					if (sFieldsNotFound.IsEmpty())
						sFieldsNotFound = pQuery->m_dFieldWeights[i].m_sName;
					else
						sFieldsNotFound.SetSprintf("%s %s", sFieldsNotFound.cstr(), pQuery->m_dFieldWeights[i].m_sName.cstr());
				}

				if (j >= 0 && j < HEAVY_FIELDS)
					m_dWeights[j] = Max(MIN_WEIGHT, pQuery->m_dFieldWeights[i].m_iValue);
			}

			if (!sFieldsNotFound.IsEmpty())
				sWarning.SetSprintf("Fields specified in field_weights option not found: [%s]", sFieldsNotFound.cstr());

			return;
		}

		// order-bound weights
		if (pQuery->m_dWeights.GetLength())
		{
			for (int i = 0; i < Min(m_iWeights,(int) pQuery->m_dWeights.GetLength()); i++)
				m_dWeights[i] = Max(MIN_WEIGHT, (int)pQuery->m_dWeights[i]);
		}
	}

	bool CSphQueryContext::SetupCalc(CSphQueryResult* pResult, const ISphSchema& tInSchema,
		const CSphSchema& tSchema, const DWORD* pMvaPool, bool bArenaProhibit)
	{
		m_dCalcFilter.Resize(0);
		m_dCalcSort.Resize(0);
		m_dCalcFinal.Resize(0);

		// quickly verify that all my real attributes can be stashed there
		if (tInSchema.GetAttrsCount() < tSchema.GetAttrsCount())
		{
			pResult->m_sError.SetSprintf("INTERNAL ERROR: incoming-schema mismatch (incount=%d, mycount=%d)",
				tInSchema.GetAttrsCount(), tSchema.GetAttrsCount());
			return false;
		}

		bool bGotAggregate = false;

		// now match everyone
		for (int iIn = 0; iIn < tInSchema.GetAttrsCount(); iIn++)
		{
			const CSphColumnInfo& tIn = tInSchema.GetAttr(iIn);
			bGotAggregate |= (tIn.m_eAggrFunc != SPH_AGGR_NONE);

			switch (tIn.m_eStage)
			{
			case SPH_EVAL_STATIC:
			case SPH_EVAL_OVERRIDE:
			{
				// this check may significantly slow down queries with huge schema attribute count
#ifndef NDEBUG
				const CSphColumnInfo* pMy = tSchema.GetAttr(tIn.m_sName.cstr());
				if (!pMy)
				{
					pResult->m_sError.SetSprintf("INTERNAL ERROR: incoming-schema attr missing from index-schema (in=%s)",
						sphDumpAttr(tIn).cstr());
					return false;
				}

				if (tIn.m_eStage == SPH_EVAL_OVERRIDE)
				{
					// override; check for type/size match and dynamic part
					if (tIn.m_eAttrType != pMy->m_eAttrType
						|| tIn.m_tLocator.m_iBitCount != pMy->m_tLocator.m_iBitCount
						|| !tIn.m_tLocator.m_bDynamic)
					{
						pResult->m_sError.SetSprintf("INTERNAL ERROR: incoming-schema override mismatch (in=%s, my=%s)",
							sphDumpAttr(tIn).cstr(), sphDumpAttr(*pMy).cstr());
						return false;
					}
				}
				else
				{
					// static; check for full match
					if (!(tIn == *pMy))
					{
						pResult->m_sError.SetSprintf("INTERNAL ERROR: incoming-schema mismatch (in=%s, my=%s)",
							sphDumpAttr(tIn).cstr(), sphDumpAttr(*pMy).cstr());
						return false;
					}
				}
#endif
				break;
			}

			case SPH_EVAL_PREFILTER:
			case SPH_EVAL_PRESORT:
			case SPH_EVAL_FINAL:
			{
				ISphExpr* pExpr = tIn.m_pExpr.Ptr();
				if (!pExpr)
				{
					pResult->m_sError.SetSprintf("INTERNAL ERROR: incoming-schema expression missing evaluator (stage=%d, in=%s)",
						(int)tIn.m_eStage, sphDumpAttr(tIn).cstr());
					return false;
				}

				// an expression that index/searcher should compute
				CalcItem_t tCalc;
				tCalc.m_eType = tIn.m_eAttrType;
				tCalc.m_tLoc = tIn.m_tLocator;
				tCalc.m_pExpr = pExpr;
				PoolPtrs_t tMva;
				tMva.m_pMva = pMvaPool;
				tMva.m_bArenaProhibit = bArenaProhibit;
				tCalc.m_pExpr->Command(SPH_EXPR_SET_MVA_POOL, &tMva);

				switch (tIn.m_eStage)
				{
				case SPH_EVAL_PREFILTER:	m_dCalcFilter.Add(tCalc); break;
				case SPH_EVAL_PRESORT:		m_dCalcSort.Add(tCalc); break;
				case SPH_EVAL_FINAL:		m_dCalcFinal.Add(tCalc); break;

				default:					break;
				}
				break;
			}

			case SPH_EVAL_SORTER:
				// sorter tells it will compute itself; so just skip it
			case SPH_EVAL_POSTLIMIT:
				break;

			default:
				pResult->m_sError.SetSprintf("INTERNAL ERROR: unhandled eval stage=%d", (int)tIn.m_eStage);
				return false;
			}
		}

		// ok, we can emit matches in this schema (incoming for sorter, outgoing for index/searcher)
		return true;
	}


	static bool IsWeightColumn(const CSphString& sAttr, const ISphSchema& tSchema)
	{
		if (sAttr == "@weight")
			return true;

		const CSphColumnInfo* pCol = tSchema.GetAttr(sAttr.cstr());
		return (pCol && pCol->m_bWeight);
	}

	bool CSphQueryContext::CreateFilters(bool bFullscan,
		const CSphVector<CSphFilterSettings>* pdFilters, const ISphSchema& tSchema,
		const DWORD* pMvaPool, const BYTE* pStrings, CSphString& sError, CSphString& sWarning, ESphCollation eCollation, bool bArenaProhibit,
		const KillListVector& dKillList)
	{
		if (!pdFilters && !dKillList.GetLength())
			return true;

		if (pdFilters)
		{
			ARRAY_FOREACH(i, (*pdFilters))
			{
				const CSphFilterSettings* pFilterSettings = pdFilters->Begin() + i;
				if (pFilterSettings->m_sAttrName.IsEmpty())
					continue;

				bool bWeight = IsWeightColumn(pFilterSettings->m_sAttrName, tSchema);

				if (bFullscan && bWeight)
					continue; // @weight is not available in fullscan mode

				// bind user variable local to that daemon
				CSphFilterSettings tUservar;
				if (pFilterSettings->m_eType == SPH_FILTER_USERVAR)
				{
					const CSphString* sVar = pFilterSettings->m_dStrings.GetLength() == 1 ? pFilterSettings->m_dStrings.Begin() : NULL;
					if (!g_pUservarsHook || !sVar)
					{
						sError = "no global variables found";
						return false;
					}

					const UservarIntSet_c* pUservar = g_pUservarsHook(*sVar);
					if (!pUservar)
					{
						sError.SetSprintf("undefined global variable '%s'", sVar->cstr());
						return false;
					}

					m_dUserVals.Add(pUservar);
					tUservar = *pFilterSettings;
					tUservar.m_eType = SPH_FILTER_VALUES;
					tUservar.SetExternalValues(pUservar->Begin(), pUservar->GetLength());
					pFilterSettings = &tUservar;
				}

				ISphFilter* pFilter = sphCreateFilter(*pFilterSettings, tSchema, pMvaPool, pStrings, sError, sWarning, eCollation, bArenaProhibit);
				if (!pFilter)
					return false;

				ISphFilter** pGroup = bWeight ? &m_pWeightFilter : &m_pFilter;
				*pGroup = sphJoinFilters(*pGroup, pFilter);
			}
		}

		if (dKillList.GetLength())
		{
			ISphFilter* pFilter = sphCreateFilter(dKillList);
			if (!pFilter)
				return false;

			m_pFilter = sphJoinFilters(m_pFilter, pFilter);
		}

		if (m_pFilter)
			m_pFilter = m_pFilter->Optimize();

		return true;
	}


	bool CSphQueryContext::SetupOverrides(const CSphQuery* pQuery, CSphQueryResult* pResult, const CSphSchema& tIndexSchema, const ISphSchema& tOutgoingSchema)
	{
		m_pOverrides = NULL;
		m_dOverrideIn.Resize(pQuery->m_dOverrides.GetLength());
		m_dOverrideOut.Resize(pQuery->m_dOverrides.GetLength());

		ARRAY_FOREACH(i, pQuery->m_dOverrides)
		{
			const char* sAttr = pQuery->m_dOverrides[i].m_sAttr.cstr(); // shortcut
			const CSphColumnInfo* pCol = tIndexSchema.GetAttr(sAttr);
			if (!pCol)
			{
				pResult->m_sError.SetSprintf("attribute override: unknown attribute name '%s'", sAttr);
				return false;
			}

			if (pCol->m_eAttrType != pQuery->m_dOverrides[i].m_eAttrType)
			{
				pResult->m_sError.SetSprintf("attribute override: attribute '%s' type mismatch (index=%d, query=%d)",
					sAttr, pCol->m_eAttrType, pQuery->m_dOverrides[i].m_eAttrType);
				return false;
			}

			const CSphColumnInfo* pOutCol = tOutgoingSchema.GetAttr(pQuery->m_dOverrides[i].m_sAttr.cstr());
			if (!pOutCol)
			{
				pResult->m_sError.SetSprintf("attribute override: unknown attribute name '%s' in outgoing schema", sAttr);
				return false;
			}

			m_dOverrideIn[i] = pCol->m_tLocator;
			m_dOverrideOut[i] = pOutCol->m_tLocator;

#ifndef NDEBUG
			// check that the values are actually sorted
			const CSphVector<CSphAttrOverride::IdValuePair_t>& dValues = pQuery->m_dOverrides[i].m_dValues;
			for (int j = 1; j < dValues.GetLength(); j++)
				assert(dValues[j - 1] < dValues[j]);
#endif
		}

		if (pQuery->m_dOverrides.GetLength())
			m_pOverrides = &pQuery->m_dOverrides;
		return true;
	}

}