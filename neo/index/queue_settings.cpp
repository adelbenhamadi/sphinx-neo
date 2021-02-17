#include "neo/index/queue_settings.h"
#include "neo/index/ft_index.h"
#include "neo/query/query.h"
#include "neo/query/match_sorter.h"
#include "neo/query/match_queue.h"
#include "neo/query/query_result.h"
#include "neo/source/schema.h"
#include "neo/io/fnv64.h"
#include "neo/core/geo_dist.h"
#include "neo/query/group_sorter_settings.h"
#include "neo/query/group_sorter.h"
#include "neo/query/grouper.h"
#include "neo/query/attr_update.h"
#include "neo/query/imatch_comparator.h"
#include "neo/tools/utf8_tools.h"
#include "neo/platform/random.h"
#include "neo/tools/docinfo_transformer.h"

#include "neo/sphinx/xjson.h"
#include "neo/sphinx/xfilter.h"


namespace NEO {

//////////////////////////////////////////////////////////////////////////
// PLAIN SORTING FUNCTORS
//////////////////////////////////////////////////////////////////////////

/// match sorter
struct MatchRelevanceLt_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState&)
	{
		if (a.m_iWeight != b.m_iWeight)
			return a.m_iWeight < b.m_iWeight;

		return a.m_uDocID > b.m_uDocID;
	}
};


/// match sorter
struct MatchAttrLt_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		if (t.m_eKeypart[0] != SPH_KEYPART_STRING)
		{
			SphAttr_t aa = a.GetAttr(t.m_tLocator[0]);
			SphAttr_t bb = b.GetAttr(t.m_tLocator[0]);
			if (aa != bb)
				return aa < bb;
		}
		else
		{
			int iCmp = t.CmpStrings(a, b, 0);
			if (iCmp != 0)
				return iCmp < 0;
		}

		if (a.m_iWeight != b.m_iWeight)
			return a.m_iWeight < b.m_iWeight;

		return a.m_uDocID > b.m_uDocID;
	}
};


/// match sorter
struct MatchAttrGt_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		if (t.m_eKeypart[0] != SPH_KEYPART_STRING)
		{
			SphAttr_t aa = a.GetAttr(t.m_tLocator[0]);
			SphAttr_t bb = b.GetAttr(t.m_tLocator[0]);
			if (aa != bb)
				return aa > bb;
		}
		else
		{
			int iCmp = t.CmpStrings(a, b, 0);
			if (iCmp != 0)
				return iCmp > 0;
		}

		if (a.m_iWeight != b.m_iWeight)
			return a.m_iWeight < b.m_iWeight;

		return a.m_uDocID > b.m_uDocID;
	}
};


/// match sorter
struct MatchTimeSegments_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		SphAttr_t aa = a.GetAttr(t.m_tLocator[0]);
		SphAttr_t bb = b.GetAttr(t.m_tLocator[0]);
		int iA = GetSegment(aa, t.m_iNow);
		int iB = GetSegment(bb, t.m_iNow);
		if (iA != iB)
			return iA > iB;

		if (a.m_iWeight != b.m_iWeight)
			return a.m_iWeight < b.m_iWeight;

		if (aa != bb)
			return aa < bb;

		return a.m_uDocID > b.m_uDocID;
	}

protected:
	static inline int GetSegment(SphAttr_t iStamp, SphAttr_t iNow)
	{
		if (iStamp >= iNow - 3600) return 0; // last hour
		if (iStamp >= iNow - 24 * 3600) return 1; // last day
		if (iStamp >= iNow - 7 * 24 * 3600) return 2; // last week
		if (iStamp >= iNow - 30 * 24 * 3600) return 3; // last month
		if (iStamp >= iNow - 90 * 24 * 3600) return 4; // last 3 months
		return 5; // everything else
	}
};


/// match sorter
struct MatchExpr_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		float aa = a.GetAttrFloat(t.m_tLocator[0]); // FIXME! OPTIMIZE!!! simplified (dword-granular) getter could be used here
		float bb = b.GetAttrFloat(t.m_tLocator[0]);
		if (aa != bb)
			return aa < bb;
		return a.m_uDocID > b.m_uDocID;
	}
};

/////////////////////////////////////////////////////////////////////////////

#define SPH_TEST_PAIR(_aa,_bb,_idx ) \
	if ( (_aa)!=(_bb) ) \
		return ( (t.m_uAttrDesc >> (_idx)) & 1 ) ^ ( (_aa) > (_bb) );


#define SPH_TEST_KEYPART(_idx) \
	switch ( t.m_eKeypart[_idx] ) \
	{ \
		case SPH_KEYPART_ID:		SPH_TEST_PAIR ( a.m_uDocID, b.m_uDocID, _idx ); break; \
		case SPH_KEYPART_WEIGHT:	SPH_TEST_PAIR ( a.m_iWeight, b.m_iWeight, _idx ); break; \
		case SPH_KEYPART_INT: \
		{ \
			register SphAttr_t aa = a.GetAttr ( t.m_tLocator[_idx] ); \
			register SphAttr_t bb = b.GetAttr ( t.m_tLocator[_idx] ); \
			SPH_TEST_PAIR ( aa, bb, _idx ); \
			break; \
		} \
		case SPH_KEYPART_FLOAT: \
		{ \
			register float aa = a.GetAttrFloat ( t.m_tLocator[_idx] ); \
			register float bb = b.GetAttrFloat ( t.m_tLocator[_idx] ); \
			SPH_TEST_PAIR ( aa, bb, _idx ) \
			break; \
		} \
		case SPH_KEYPART_STRINGPTR: \
		case SPH_KEYPART_STRING: \
		{ \
			int iCmp = t.CmpStrings ( a, b, _idx ); \
			if ( iCmp!=0 ) \
				return ( ( t.m_uAttrDesc >> (_idx) ) & 1 ) ^ ( iCmp>0 ); \
			break; \
		} \
	}


struct MatchGeneric2_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		SPH_TEST_KEYPART(0);
		SPH_TEST_KEYPART(1);
		return a.m_uDocID > b.m_uDocID;
	}
};


struct MatchGeneric3_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		SPH_TEST_KEYPART(0);
		SPH_TEST_KEYPART(1);
		SPH_TEST_KEYPART(2);
		return a.m_uDocID > b.m_uDocID;
	}
};


struct MatchGeneric4_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		SPH_TEST_KEYPART(0);
		SPH_TEST_KEYPART(1);
		SPH_TEST_KEYPART(2);
		SPH_TEST_KEYPART(3);
		return a.m_uDocID > b.m_uDocID;
	}
};


struct MatchGeneric5_fn : public ISphMatchComparator
{
	virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t) const
	{
		return IsLess(a, b, t);
	}

	static inline bool IsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& t)
	{
		SPH_TEST_KEYPART(0);
		SPH_TEST_KEYPART(1);
		SPH_TEST_KEYPART(2);
		SPH_TEST_KEYPART(3);
		SPH_TEST_KEYPART(4);
		return a.m_uDocID > b.m_uDocID;
	}
};




//////////////////////////////////////////////////////////////////////////
// SORT CLAUSE PARSER
//////////////////////////////////////////////////////////////////////////

static const int MAX_SORT_FIELDS = 5; // MUST be in sync with CSphMatchComparatorState::m_iAttr


class SortClauseTokenizer_t
{
protected:
	const char* m_pCur;
	const char* m_pMax;
	char* m_pBuf;

protected:
	char ToLower(char c)
	{
		// 0..9, A..Z->a..z, _, a..z, @, .
		if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || c == '_' || c == '@' || c == '.' || c == '[' || c == ']' || c == '\'' || c == '\"' || c == '(' || c == ')' || c == '*')
			return c;
		if (c >= 'A' && c <= 'Z')
			return c - 'A' + 'a';
		return 0;
	}

public:
	explicit SortClauseTokenizer_t(const char* sBuffer)
	{
		int iLen = strlen(sBuffer);
		m_pBuf = new char[iLen + 1];
		m_pMax = m_pBuf + iLen;
		m_pCur = m_pBuf;

		// make string lowercase but keep case of JSON.field
		bool bJson = false;
		for (int i = 0; i <= iLen; i++)
		{
			char cSrc = sBuffer[i];
			char cDst = ToLower(cSrc);
			bJson = (cSrc == '.' || cSrc == '[' || (bJson && cDst > 0)); // keep case of valid char sequence after '.' and '[' symbols
			m_pBuf[i] = bJson ? cSrc : cDst;
		}
	}

	~SortClauseTokenizer_t()
	{
		SafeDeleteArray(m_pBuf);
	}

	const char* GetToken()
	{
		// skip spaces
		while (m_pCur < m_pMax && !*m_pCur)
			m_pCur++;
		if (m_pCur >= m_pMax)
			return NULL;

		// memorize token start, and move pointer forward
		const char* sRes = m_pCur;
		while (*m_pCur)
			m_pCur++;
		return sRes;
	}

	bool IsSparseCount(const char* sTok)
	{
		const char* sSeq = "(*)";
		for (; sTok < m_pMax && *sSeq; sTok++)
		{
			bool bGotSeq = (*sSeq == *sTok);
			if (bGotSeq)
				sSeq++;

			// stop checking on any non space char outside sequence or sequence end
			if ((!bGotSeq && !sphIsSpace(*sTok) && *sTok != '\0') || !*sSeq)
				break;
		}

		if (!*sSeq && sTok + 1 < m_pMax && !sTok[1])
		{
			// advance token iterator after composite count(*) token
			m_pCur = sTok + 1;
			return true;
		}
		else
		{
			return false;
		}
	}
};


static inline ESphSortKeyPart Attr2Keypart(ESphAttr eType)
{
	switch (eType)
	{
	case ESphAttr::SPH_ATTR_FLOAT:	return SPH_KEYPART_FLOAT;
	case ESphAttr::SPH_ATTR_STRING:	return SPH_KEYPART_STRING;
	case ESphAttr::SPH_ATTR_JSON:
	case ESphAttr::SPH_ATTR_JSON_FIELD:
	case ESphAttr::SPH_ATTR_STRINGPTR: return SPH_KEYPART_STRINGPTR;
	default:				return SPH_KEYPART_INT;
	}
}



ESortClauseParseResult sphParseSortClause(const CSphQuery* pQuery, const char* sClause, const ISphSchema& tSchema,
	ESphSortFunc& eFunc, CSphMatchComparatorState& tState, CSphString& sError)
{
	for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
		tState.m_dAttrs[i] = -1;

	// mini parser
	SortClauseTokenizer_t tTok(sClause);

	bool bField = false; // whether i'm expecting field name or sort order
	int iField = 0;

	for (const char* pTok = tTok.GetToken(); pTok; pTok = tTok.GetToken())
	{
		bField = !bField;

		// special case, sort by random
		if (iField == 0 && bField && strcmp(pTok, "@random") == 0)
			return SORT_CLAUSE_RANDOM;

		// handle sort order
		if (!bField)
		{
			// check
			if (strcmp(pTok, "desc") && strcmp(pTok, "asc"))
			{
				sError.SetSprintf("invalid sorting order '%s'", pTok);
				return SORT_CLAUSE_ERROR;
			}

			// set
			if (!strcmp(pTok, "desc"))
				tState.m_uAttrDesc |= (1 << iField);

			iField++;
			continue;
		}

		// handle attribute name
		if (iField == MAX_SORT_FIELDS)
		{
			sError.SetSprintf("too many sort-by attributes; maximum count is %d", MAX_SORT_FIELDS);
			return SORT_CLAUSE_ERROR;
		}

		if (!strcasecmp(pTok, "@relevance")
			|| !strcasecmp(pTok, "@rank")
			|| !strcasecmp(pTok, "@weight")
			|| !strcasecmp(pTok, "weight()"))
		{
			tState.m_eKeypart[iField] = SPH_KEYPART_WEIGHT;

		}
		else if (!strcasecmp(pTok, "@id") || !strcasecmp(pTok, "id"))
		{
			tState.m_eKeypart[iField] = SPH_KEYPART_ID;

		}
		else
		{
			ESphAttr eAttrType = ESphAttr::SPH_ATTR_NONE;

			if (!strcasecmp(pTok, "@group"))
				pTok = "@groupby";
			else if (!strcasecmp(pTok, "count(*)"))
				pTok = "@count";
			else if (!strcasecmp(pTok, "facet()"))
				pTok = "@groupby"; // facet() is essentially a @groupby alias
			else if (strcasecmp(pTok, "count") >= 0 && tTok.IsSparseCount(pTok + sizeof("count") - 1)) // epression count(*) with various spaces
				pTok = "@count";


			// try to lookup plain attr in sorter schema
			int iAttr = tSchema.GetAttrIndex(pTok);

			// do not order by mva (the result is undefined)
			if (iAttr >= 0 && (tSchema.GetAttr(iAttr).m_eAttrType == ESphAttr::SPH_ATTR_UINT32SET
				|| tSchema.GetAttr(iAttr).m_eAttrType == ESphAttr::SPH_ATTR_INT64SET))
			{
				sError.SetSprintf("order by MVA is undefined");
				return SORT_CLAUSE_ERROR;
			}

			// try to lookup aliased count(*) and aliased groupby() in select items
			if (iAttr < 0)
			{
				ARRAY_FOREACH(i, pQuery->m_dItems)
				{
					const CSphQueryItem& tItem = pQuery->m_dItems[i];
					if (!tItem.m_sAlias.cstr() || strcasecmp(tItem.m_sAlias.cstr(), pTok))
						continue;
					if (tItem.m_sExpr.Begins("@"))
						iAttr = tSchema.GetAttrIndex(tItem.m_sExpr.cstr());
					else if (tItem.m_sExpr == "count(*)")
						iAttr = tSchema.GetAttrIndex("@count");
					else if (tItem.m_sExpr == "groupby()")
					{
						iAttr = tSchema.GetAttrIndex("@groupbystr");
						// try numeric group by
						if (iAttr < 0)
							iAttr = tSchema.GetAttrIndex("@groupby");
					}
					break; // break in any case; because we did match the alias
				}
			}

			// try JSON attribute and use JSON attribute instead of JSON field
			if (iAttr < 0 || (iAttr >= 0 && (tSchema.GetAttr(iAttr).m_eAttrType == ESphAttr::SPH_ATTR_JSON_FIELD
				|| tSchema.GetAttr(iAttr).m_eAttrType == ESphAttr::SPH_ATTR_JSON)))
			{
				if (iAttr >= 0)
				{
					// aliased ESphAttr::SPH_ATTR_JSON_FIELD, reuse existing expression
					const CSphColumnInfo* pAttr = &tSchema.GetAttr(iAttr);
					if (pAttr->m_pExpr.Ptr())
						pAttr->m_pExpr->AddRef(); // SetupSortRemap uses refcounted pointer, but does not AddRef() itself, so help it
					tState.m_tSubExpr[iField] = pAttr->m_pExpr.Ptr();
					tState.m_tSubKeys[iField] = JsonKey_t(pTok, strlen(pTok));

				}
				else
				{
					CSphString sJsonCol, sJsonKey;
					if (sphJsonNameSplit(pTok, &sJsonCol, &sJsonKey))
					{
						iAttr = tSchema.GetAttrIndex(sJsonCol.cstr());
						if (iAttr >= 0)
						{
							tState.m_tSubExpr[iField] = sphExprParse(pTok, tSchema, NULL, NULL, sError, NULL);
							tState.m_tSubKeys[iField] = JsonKey_t(pTok, strlen(pTok));
						}
					}
				}
			}

			// try json conversion functions (integer()/double()/bigint() in the order by clause)
			if (iAttr < 0)
			{
				ISphExpr* pExpr = sphExprParse(pTok, tSchema, &eAttrType, NULL, sError, NULL);
				if (pExpr)
				{
					tState.m_tSubExpr[iField] = pExpr;
					tState.m_tSubKeys[iField] = JsonKey_t(pTok, strlen(pTok));
					tState.m_tSubKeys[iField].m_uMask = 0;
					tState.m_tSubType[iField] = eAttrType;
					iAttr = 0; // will be remapped in SetupSortRemap
				}
			}

			// try precalculated json fields received from agents (prefixed with @int_*)
			if (iAttr < 0)
			{
				CSphString sName;
				sName.SetSprintf("%s%s", g_sIntAttrPrefix, pTok);
				iAttr = tSchema.GetAttrIndex(sName.cstr());
			}

			// epic fail
			if (iAttr < 0)
			{
				sError.SetSprintf("sort-by attribute '%s' not found", pTok);
				return SORT_CLAUSE_ERROR;
			}

			const CSphColumnInfo& tCol = tSchema.GetAttr(iAttr);
			tState.m_eKeypart[iField] = Attr2Keypart(eAttrType != ESphAttr::SPH_ATTR_NONE ? eAttrType : tCol.m_eAttrType);
			tState.m_tLocator[iField] = tCol.m_tLocator;
			tState.m_dAttrs[iField] = iAttr;
		}
	}

	if (iField == 0)
	{
		sError.SetSprintf("no sort order defined");
		return SORT_CLAUSE_ERROR;
	}

	if (iField == 1)
		tState.m_eKeypart[iField++] = SPH_KEYPART_ID; // add "id ASC"

	switch (iField)
	{
	case 2:		eFunc = FUNC_GENERIC2; break;
	case 3:		eFunc = FUNC_GENERIC3; break;
	case 4:		eFunc = FUNC_GENERIC4; break;
	case 5:		eFunc = FUNC_GENERIC5; break;
	default:	sError.SetSprintf("INTERNAL ERROR: %d fields in sphParseSortClause()", iField); return SORT_CLAUSE_ERROR;
	}
	return SORT_CLAUSE_OK;
}

//////////////////////////////////////////////////////////////////////////
// SORTING+GROUPING INSTANTIATION
//////////////////////////////////////////////////////////////////////////

template < typename COMPGROUP >
static ISphMatchSorter* sphCreateSorter3rd(const ISphMatchComparator* pComp, const CSphQuery* pQuery,
	const CSphGroupSorterSettings& tSettings, bool bHasPackedFactors)
{
	BYTE uSelector = (bHasPackedFactors ? 1 : 0)
		+ (tSettings.m_bDistinct ? 2 : 0)
		+ (tSettings.m_bMVA ? 4 : 0)
		+ (tSettings.m_bImplicit ? 8 : 0)
		+ ((pQuery->m_iGroupbyLimit > 1) ? 16 : 0)
		+ (tSettings.m_bJson ? 32 : 0);
	switch (uSelector)
	{
	case 0:
		return new CSphKBufferGroupSorter < COMPGROUP, false, false >(pComp, pQuery, tSettings);
	case 1:
		return new CSphKBufferGroupSorter < COMPGROUP, false, true >(pComp, pQuery, tSettings);
	case 2:
		return new CSphKBufferGroupSorter < COMPGROUP, true, false >(pComp, pQuery, tSettings);
	case 3:
		return new CSphKBufferGroupSorter < COMPGROUP, true, true >(pComp, pQuery, tSettings);
	case 4:
		return new CSphKBufferMVAGroupSorter < COMPGROUP, false, false >(pComp, pQuery, tSettings);
	case 5:
		return new CSphKBufferMVAGroupSorter < COMPGROUP, false, true >(pComp, pQuery, tSettings);
	case 6:
		return new CSphKBufferMVAGroupSorter < COMPGROUP, true, false >(pComp, pQuery, tSettings);
	case 7:
		return new CSphKBufferMVAGroupSorter < COMPGROUP, true, true >(pComp, pQuery, tSettings);
	case 8:
		return new CSphImplicitGroupSorter < COMPGROUP, false, false >(pComp, pQuery, tSettings);
	case 9:
		return new CSphImplicitGroupSorter < COMPGROUP, false, true >(pComp, pQuery, tSettings);
	case 10:
		return new CSphImplicitGroupSorter < COMPGROUP, true, false >(pComp, pQuery, tSettings);
	case 11:
		return new CSphImplicitGroupSorter < COMPGROUP, true, true >(pComp, pQuery, tSettings);
	case 16:
		return new CSphKBufferNGroupSorter < COMPGROUP, false, false >(pComp, pQuery, tSettings);
	case 17:
		return new CSphKBufferNGroupSorter < COMPGROUP, false, true >(pComp, pQuery, tSettings);
	case 18:
		return new CSphKBufferNGroupSorter < COMPGROUP, true, false >(pComp, pQuery, tSettings);
	case 19:
		return new CSphKBufferNGroupSorter < COMPGROUP, true, true >(pComp, pQuery, tSettings);
	case 32:
		return new CSphKBufferJsonGroupSorter < COMPGROUP, false, false >(pComp, pQuery, tSettings);
	case 33:
		return new CSphKBufferJsonGroupSorter < COMPGROUP, false, true >(pComp, pQuery, tSettings);
	case 34:
		return new CSphKBufferJsonGroupSorter < COMPGROUP, true, false >(pComp, pQuery, tSettings);
	case 35:
		return new CSphKBufferJsonGroupSorter < COMPGROUP, true, true >(pComp, pQuery, tSettings);
	}
	assert(0);
	return NULL;
}


static ISphMatchSorter* sphCreateSorter2nd(ESphSortFunc eGroupFunc, const ISphMatchComparator* pComp,
	const CSphQuery* pQuery, const CSphGroupSorterSettings& tSettings, bool bHasPackedFactors)
{
	switch (eGroupFunc)
	{
	case FUNC_GENERIC2:		return sphCreateSorter3rd<MatchGeneric2_fn>(pComp, pQuery, tSettings, bHasPackedFactors); break;
	case FUNC_GENERIC3:		return sphCreateSorter3rd<MatchGeneric3_fn>(pComp, pQuery, tSettings, bHasPackedFactors); break;
	case FUNC_GENERIC4:		return sphCreateSorter3rd<MatchGeneric4_fn>(pComp, pQuery, tSettings, bHasPackedFactors); break;
	case FUNC_GENERIC5:		return sphCreateSorter3rd<MatchGeneric5_fn>(pComp, pQuery, tSettings, bHasPackedFactors); break;
	case FUNC_EXPR:			return sphCreateSorter3rd<MatchExpr_fn>(pComp, pQuery, tSettings, bHasPackedFactors); break;
	default:				return NULL;
	}
}


static ISphMatchSorter* sphCreateSorter1st(ESphSortFunc eMatchFunc, ESphSortFunc eGroupFunc,
	const CSphQuery* pQuery, const CSphGroupSorterSettings& tSettings, bool bHasPackedFactors)
{
	if (tSettings.m_bImplicit)
		return sphCreateSorter2nd(eGroupFunc, NULL, pQuery, tSettings, bHasPackedFactors);

	ISphMatchComparator* pComp = NULL;
	switch (eMatchFunc)
	{
	case FUNC_REL_DESC:		pComp = new MatchRelevanceLt_fn(); break;
	case FUNC_ATTR_DESC:	pComp = new MatchAttrLt_fn(); break;
	case FUNC_ATTR_ASC:		pComp = new MatchAttrGt_fn(); break;
	case FUNC_TIMESEGS:		pComp = new MatchTimeSegments_fn(); break;
	case FUNC_GENERIC2:		pComp = new MatchGeneric2_fn(); break;
	case FUNC_GENERIC3:		pComp = new MatchGeneric3_fn(); break;
	case FUNC_GENERIC4:		pComp = new MatchGeneric4_fn(); break;
	case FUNC_GENERIC5:		pComp = new MatchGeneric5_fn(); break;
	case FUNC_EXPR:			pComp = new MatchExpr_fn(); break; // only for non-bitfields, obviously
	}

	assert(pComp);
	return sphCreateSorter2nd(eGroupFunc, pComp, pQuery, tSettings, bHasPackedFactors);
}



class Utf8CIHash_fn
{
public:
	uint64_t Hash(const BYTE* pStr, int iLen, uint64_t uPrev = SPH_FNV64_SEED) const
	{
		assert(pStr && iLen);

		uint64_t uAcc = uPrev;
		while (iLen--)
		{
			const BYTE* pCur = pStr++;
			int iCode = sphUTF8Decode(pCur);
			iCode = CollateUTF8CI(iCode);
			uAcc = sphFNV64(&iCode, 4, uAcc);
		}

		return uAcc;
	}
};


class LibcCSHash_fn
{
public:
	mutable CSphTightVector<BYTE> m_dBuf;
	static const int LOCALE_SAFE_GAP = 16;

	LibcCSHash_fn()
	{
		m_dBuf.Resize(COLLATE_STACK_BUFFER);
	}

	uint64_t Hash(const BYTE* pStr, int iLen, uint64_t uPrev = SPH_FNV64_SEED) const
	{
		assert(pStr && iLen);

		int iCompositeLen = iLen + 1 + (int)(3.0f * iLen) + LOCALE_SAFE_GAP;
		if (m_dBuf.GetLength() < iCompositeLen)
			m_dBuf.Resize(iCompositeLen);

		memcpy(m_dBuf.Begin(), pStr, iLen);
		m_dBuf[iLen] = '\0';

		BYTE* pDst = m_dBuf.Begin() + iLen + 1;
		int iDstAvailable = m_dBuf.GetLength() - iLen - LOCALE_SAFE_GAP;

		int iDstLen = strxfrm((char*)pDst, (const char*)m_dBuf.Begin(), iDstAvailable);
		assert(iDstLen < iDstAvailable + LOCALE_SAFE_GAP);

		uint64_t uAcc = sphFNV64(pDst, iDstLen, uPrev);

		return uAcc;
	}
};


class LibcCIHash_fn
{
public:
	uint64_t Hash(const BYTE* pStr, int iLen, uint64_t uPrev = SPH_FNV64_SEED) const
	{
		assert(pStr && iLen);

		uint64_t uAcc = uPrev;
		while (iLen--)
		{
			int iChar = tolower(*pStr++);
			uAcc = sphFNV64(&iChar, 4, uAcc);
		}

		return uAcc;
	}
};

/////////////////////////
// SORTING QUEUE FACTORY
/////////////////////////



template < typename COMP >
static ISphMatchSorter* CreatePlainSorter(bool bKbuffer, int iMaxMatches, bool bUsesAttrs, bool bFactors)
{
	if (bKbuffer)
	{
		if (bFactors)
			return new CSphKbufferMatchQueue<COMP, true>(iMaxMatches, bUsesAttrs);
		else
			return new CSphKbufferMatchQueue<COMP, false>(iMaxMatches, bUsesAttrs);
	}
	else
	{
		if (bFactors)
			return new CSphMatchQueue<COMP, true>(iMaxMatches, bUsesAttrs);
		else
			return new CSphMatchQueue<COMP, false>(iMaxMatches, bUsesAttrs);
	}
}


int sphFlattenQueue(ISphMatchSorter* pQueue, CSphQueryResult* pResult, int iTag)
{
	if (!pQueue || !pQueue->GetLength())
		return 0;

	int iOffset = pResult->m_dMatches.GetLength();
	pResult->m_dMatches.Resize(iOffset + pQueue->GetLength());
	int iCopied = pQueue->Flatten(pResult->m_dMatches.Begin() + iOffset, iTag);
	pResult->m_dMatches.Resize(iOffset + iCopied);
	return iCopied;
}


bool sphHasExpressions(const CSphQuery& tQuery, const CSphSchema& tSchema)
{
	ARRAY_FOREACH(i, tQuery.m_dItems)
	{
		const CSphQueryItem& tItem = tQuery.m_dItems[i];
		const CSphString& sExpr = tItem.m_sExpr;

		// all expressions that come from parser are automatically aliased
		assert(!tItem.m_sAlias.IsEmpty());

		if (!(sExpr == "*"
			|| (tSchema.GetAttrIndex(sExpr.cstr()) >= 0 && tItem.m_eAggrFunc == SPH_AGGR_NONE && tItem.m_sAlias == sExpr)
			|| IsGroupbyMagic(sExpr)))
			return true;
	}

	return false;
}


static ISphMatchSorter* CreatePlainSorter(ESphSortFunc eMatchFunc, bool bKbuffer, int iMaxMatches, bool bUsesAttrs, bool bFactors)
{
	switch (eMatchFunc)
	{
	case FUNC_REL_DESC:		return CreatePlainSorter<MatchRelevanceLt_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_ATTR_DESC:	return CreatePlainSorter<MatchAttrLt_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_ATTR_ASC:		return CreatePlainSorter<MatchAttrGt_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_TIMESEGS:		return CreatePlainSorter<MatchTimeSegments_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_GENERIC2:		return CreatePlainSorter<MatchGeneric2_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_GENERIC3:		return CreatePlainSorter<MatchGeneric3_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_GENERIC4:		return CreatePlainSorter<MatchGeneric4_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_GENERIC5:		return CreatePlainSorter<MatchGeneric5_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	case FUNC_EXPR:			return CreatePlainSorter<MatchExpr_fn>(bKbuffer, iMaxMatches, bUsesAttrs, bFactors); break;
	default:				return NULL;
	}
}

///////////////////////////////////




static bool SetupGroupbySettings(const CSphQuery* pQuery, const ISphSchema& tSchema,
	CSphGroupSorterSettings& tSettings, CSphVector<int>& dGroupColumns, CSphString& sError, bool bImplicit)
{
	tSettings.m_tDistinctLoc.m_iBitOffset = -1;

	if (pQuery->m_sGroupBy.IsEmpty() && !bImplicit)
		return true;

	if (pQuery->m_eGroupFunc == SPH_GROUPBY_ATTRPAIR)
	{
		sError.SetSprintf("SPH_GROUPBY_ATTRPAIR is not supported any more (just group on 'bigint' attribute)");
		return false;
	}

	CSphString sJsonColumn;
	CSphString sJsonKey;
	if (pQuery->m_eGroupFunc == SPH_GROUPBY_MULTIPLE)
	{
		CSphVector<CSphAttrLocator> dLocators;
		CSphVector<ESphAttr> dAttrTypes;
		CSphVector<ISphExpr*> dJsonKeys;

		CSphVector<CSphString> dGroupBy;
		const char* a = pQuery->m_sGroupBy.cstr();
		const char* b = a;
		while (*a)
		{
			while (*b && *b != ',')
				b++;
			CSphString& sNew = dGroupBy.Add();
			sNew.SetBinary(a, b - a);
			if (*b == ',')
				b += 2;
			a = b;
		}
		dGroupBy.Uniq();

		ARRAY_FOREACH(i, dGroupBy)
		{
			CSphString sJsonExpr;
			dJsonKeys.Add(NULL);
			if (sphJsonNameSplit(dGroupBy[i].cstr(), &sJsonColumn, &sJsonKey))
			{
				sJsonExpr = dGroupBy[i];
				dGroupBy[i] = sJsonColumn;
			}

			const int iAttr = tSchema.GetAttrIndex(dGroupBy[i].cstr());
			if (iAttr < 0)
			{
				sError.SetSprintf("group-by attribute '%s' not found", dGroupBy[i].cstr());
				return false;
			}

			ESphAttr eType = tSchema.GetAttr(iAttr).m_eAttrType;
			if (eType == ESphAttr::SPH_ATTR_UINT32SET || eType == ESphAttr::SPH_ATTR_INT64SET)
			{
				sError.SetSprintf("MVA values can't be used in multiple group-by");
				return false;
			}
			else if (sJsonExpr.IsEmpty() && eType == ESphAttr::SPH_ATTR_JSON)
			{
				sError.SetSprintf("JSON blob can't be used in multiple group-by");
				return false;
			}

			dLocators.Add(tSchema.GetAttr(iAttr).m_tLocator);
			dAttrTypes.Add(eType);
			dGroupColumns.Add(iAttr);

			if (!sJsonExpr.IsEmpty())
				dJsonKeys.Last() = sphExprParse(sJsonExpr.cstr(), tSchema, NULL, NULL, sError, NULL);
		}

		tSettings.m_pGrouper = sphCreateGrouperMulti(dLocators, dAttrTypes, dJsonKeys, pQuery->m_eCollation);

	}
	else if (sphJsonNameSplit(pQuery->m_sGroupBy.cstr(), &sJsonColumn, &sJsonKey))
	{
		const int iAttr = tSchema.GetAttrIndex(sJsonColumn.cstr());
		if (iAttr < 0)
		{
			sError.SetSprintf("groupby: no such attribute '%s'", sJsonColumn.cstr());
			return false;
		}

		if (tSchema.GetAttr(iAttr).m_eAttrType != ESphAttr::SPH_ATTR_JSON)
		{
			sError.SetSprintf("groupby: attribute '%s' does not have subfields (must be sql_attr_json)", sJsonColumn.cstr());
			return false;
		}

		if (pQuery->m_eGroupFunc != SPH_GROUPBY_ATTR)
		{
			sError.SetSprintf("groupby: legacy groupby modes are not supported on JSON attributes");
			return false;
		}

		dGroupColumns.Add(iAttr);

		ISphExpr* pExpr = sphExprParse(pQuery->m_sGroupBy.cstr(), tSchema, NULL, NULL, sError, NULL, pQuery->m_eCollation);
		tSettings.m_pGrouper = new CSphGrouperJsonField(tSchema.GetAttr(iAttr).m_tLocator, pExpr);
		tSettings.m_bJson = true;

	}
	else if (bImplicit)
	{
		tSettings.m_bImplicit = true;

	}
	else
	{
		// setup groupby attr
		int iGroupBy = tSchema.GetAttrIndex(pQuery->m_sGroupBy.cstr());

		if (iGroupBy < 0)
		{
			// try aliased groupby attr (facets)
			ARRAY_FOREACH(i, pQuery->m_dItems)
				if (pQuery->m_sGroupBy == pQuery->m_dItems[i].m_sExpr)
				{
					iGroupBy = tSchema.GetAttrIndex(pQuery->m_dItems[i].m_sAlias.cstr());
					break;

				}
				else if (pQuery->m_sGroupBy == pQuery->m_dItems[i].m_sAlias)
				{
					iGroupBy = tSchema.GetAttrIndex(pQuery->m_dItems[i].m_sExpr.cstr());
					break;
				}
		}

		if (iGroupBy < 0)
		{
			sError.SetSprintf("group-by attribute '%s' not found", pQuery->m_sGroupBy.cstr());
			return false;
		}

		ESphAttr eType = tSchema.GetAttr(iGroupBy).m_eAttrType;
		CSphAttrLocator tLoc = tSchema.GetAttr(iGroupBy).m_tLocator;
		switch (pQuery->m_eGroupFunc)
		{
		case SPH_GROUPBY_DAY:		tSettings.m_pGrouper = new CSphGrouperDay(tLoc); break;
		case SPH_GROUPBY_WEEK:		tSettings.m_pGrouper = new CSphGrouperWeek(tLoc); break;
		case SPH_GROUPBY_MONTH:		tSettings.m_pGrouper = new CSphGrouperMonth(tLoc); break;
		case SPH_GROUPBY_YEAR:		tSettings.m_pGrouper = new CSphGrouperYear(tLoc); break;
		case SPH_GROUPBY_ATTR:
			if (eType == ESphAttr::SPH_ATTR_JSON || eType == ESphAttr::SPH_ATTR_JSON_FIELD)
			{
				ISphExpr* pExpr = sphExprParse(pQuery->m_sGroupBy.cstr(), tSchema, NULL, NULL, sError, NULL, pQuery->m_eCollation);
				tSettings.m_pGrouper = new CSphGrouperJsonField(tLoc, pExpr);
				tSettings.m_bJson = true;

			}
			else if (eType == ESphAttr::SPH_ATTR_STRING)
				tSettings.m_pGrouper = sphCreateGrouperString(tLoc, pQuery->m_eCollation);
			else
				tSettings.m_pGrouper = new CSphGrouperAttr(tLoc);
			break;
		default:
			sError.SetSprintf("invalid group-by mode (mode=%d)", pQuery->m_eGroupFunc);
			return false;
		}

		tSettings.m_bMVA = (eType == ESphAttr::SPH_ATTR_UINT32SET || eType == ESphAttr::SPH_ATTR_INT64SET);
		tSettings.m_bMva64 = (eType == ESphAttr::SPH_ATTR_INT64SET);
		dGroupColumns.Add(iGroupBy);
	}

	// setup distinct attr
	if (!pQuery->m_sGroupDistinct.IsEmpty())
	{
		int iDistinct = tSchema.GetAttrIndex(pQuery->m_sGroupDistinct.cstr());
		if (iDistinct < 0)
		{
			sError.SetSprintf("group-count-distinct attribute '%s' not found", pQuery->m_sGroupDistinct.cstr());
			return false;
		}

		const CSphColumnInfo& tDistinctAttr = tSchema.GetAttr(iDistinct);
		tSettings.m_tDistinctLoc = tDistinctAttr.m_tLocator;
		tSettings.m_eDistinctAttr = tDistinctAttr.m_eAttrType;
	}

	return true;
}


// expression that transform string pool base + offset -> ptr
struct ExprSortStringAttrFixup_c : public ISphExpr
{
	const BYTE* m_pStrings; ///< string pool; base for offset of string attributes
	const CSphAttrLocator	m_tLocator; ///< string attribute to fix

	explicit ExprSortStringAttrFixup_c(const CSphAttrLocator& tLocator)
		: m_pStrings(NULL)
		, m_tLocator(tLocator)
	{
	}

	virtual float Eval(const CSphMatch&) const { assert(0); return 0.0f; }

	virtual int64_t Int64Eval(const CSphMatch& tMatch) const
	{
		SphAttr_t uOff = tMatch.GetAttr(m_tLocator);
		return (int64_t)(m_pStrings && uOff ? m_pStrings + uOff : NULL);
	}

	virtual void Command(ESphExprCommand eCmd, void* pArg)
	{
		if (eCmd == SPH_EXPR_SET_STRING_POOL)
			m_pStrings = (const BYTE*)pArg;
	}

	virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
	{
		assert(0 && "remap expressions in filters");
		return 0;
	}
};


// expression that transform string pool base + offset -> ptr
struct ExprSortJson2StringPtr_c : public ISphExpr
{
	const BYTE* m_pStrings; ///< string pool; base for offset of string attributes
	const CSphAttrLocator	m_tJsonCol; ///< JSON attribute to fix
	CSphRefcountedPtr<ISphExpr>	m_pExpr;

	ExprSortJson2StringPtr_c(const CSphAttrLocator& tLocator, ISphExpr* pExpr)
		: m_pStrings(NULL)
		, m_tJsonCol(tLocator)
		, m_pExpr(pExpr)
	{}

	virtual bool IsStringPtr() const { return true; }

	virtual float Eval(const CSphMatch&) const { assert(0); return 0.0f; }

	virtual int StringEval(const CSphMatch& tMatch, const BYTE** ppStr) const
	{
		if (!m_pStrings || !m_pExpr)
		{
			*ppStr = NULL;
			return 0;
		}

		uint64_t uValue = m_pExpr->Int64Eval(tMatch);
		const BYTE* pVal = m_pStrings + (uValue & 0xffffffff);
		ESphJsonType eJson = (ESphJsonType)(uValue >> 32);
		CSphString sVal;

		// FIXME!!! make string length configurable for STRING and STRING_VECTOR to compare and allocate only Min(String.Length, CMP_LENGTH)
		switch (eJson)
		{
		case JSON_INT32:
			sVal.SetSprintf("%d", sphJsonLoadInt(&pVal));
			break;
		case JSON_INT64:
			sVal.SetSprintf(INT64_FMT, sphJsonLoadBigint(&pVal));
			break;
		case JSON_DOUBLE:
			sVal.SetSprintf("%f", sphQW2D(sphJsonLoadBigint(&pVal)));
			break;
		case JSON_STRING:
		{
			int iLen = sphJsonUnpackInt(&pVal);
			sVal.SetBinary((const char*)pVal, iLen);
			break;
		}
		case JSON_STRING_VECTOR:
		{
			int iTotalLen = sphJsonUnpackInt(&pVal);
			int iCount = sphJsonUnpackInt(&pVal);

			CSphFixedVector<BYTE> dBuf(iTotalLen + 4); // data and tail GAP
			BYTE* pDst = dBuf.Begin();

			// head element
			if (iCount)
			{
				int iElemLen = sphJsonUnpackInt(&pVal);
				memcpy(pDst, pVal, iElemLen);
				pDst += iElemLen;
			}

			// tail elements separated by space
			for (int i = 1; i < iCount; i++)
			{
				*pDst++ = ' ';
				int iElemLen = sphJsonUnpackInt(&pVal);
				memcpy(pDst, pVal, iElemLen);
				pDst += iElemLen;
			}

			int iStrLen = pDst - dBuf.Begin();
			// filling junk space
			while (pDst < dBuf.Begin() + dBuf.GetLength())
				*pDst++ = '\0';

			*ppStr = dBuf.LeakData();
			return iStrLen;
		}
		default:
			break;
		}

		int iStriLen = sVal.Length();
		*ppStr = (const BYTE*)sVal.Leak();
		return iStriLen;
	}

	virtual void Command(ESphExprCommand eCmd, void* pArg)
	{
		if (eCmd == SPH_EXPR_SET_STRING_POOL)
		{
			m_pStrings = (const BYTE*)pArg;
			if (m_pExpr.Ptr())
				m_pExpr->Command(eCmd, pArg);
		}
	}

	virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
	{
		assert(0 && "remap expression in filters");
		return 0;
	}
};




bool sphSortGetStringRemap(const ISphSchema& tSorterSchema, const ISphSchema& tIndexSchema,
	CSphVector<SphStringSorterRemap_t>& dAttrs)
{
	dAttrs.Resize(0);
	for (int i = 0; i < tSorterSchema.GetAttrsCount(); i++)
	{
		const CSphColumnInfo& tDst = tSorterSchema.GetAttr(i);
		// remap only static strings
		if (!tDst.m_sName.Begins(g_sIntAttrPrefix) || tDst.m_eAttrType == ESphAttr::SPH_ATTR_STRINGPTR)
			continue;

		const CSphColumnInfo* pSrcCol = tIndexSchema.GetAttr(tDst.m_sName.cstr() + sizeof(g_sIntAttrPrefix) - 1);
		if (!pSrcCol) // skip internal attributes received from agents
			continue;

		SphStringSorterRemap_t& tRemap = dAttrs.Add();
		tRemap.m_tSrc = pSrcCol->m_tLocator;
		tRemap.m_tDst = tDst.m_tLocator;
	}
	return (dAttrs.GetLength() > 0);
}

///////////////////////////////////


static CSphGrouper* sphCreateGrouperString(const CSphAttrLocator& tLoc, ESphCollation eCollation)
{
	if (eCollation == SPH_COLLATION_UTF8_GENERAL_CI)
		return new CSphGrouperString<Utf8CIHash_fn>(tLoc);
	else if (eCollation == SPH_COLLATION_LIBC_CI)
		return new CSphGrouperString<LibcCIHash_fn>(tLoc);
	else if (eCollation == SPH_COLLATION_LIBC_CS)
		return new CSphGrouperString<LibcCSHash_fn>(tLoc);
	else
		return new CSphGrouperString<BinaryHash_fn>(tLoc);
}

static CSphGrouper* sphCreateGrouperMulti(const CSphVector<CSphAttrLocator>& dLocators, const CSphVector<ESphAttr>& dAttrTypes,
	const CSphVector<ISphExpr*>& dJsonKeys, ESphCollation eCollation)
{
	if (eCollation == SPH_COLLATION_UTF8_GENERAL_CI)
		return new CSphGrouperMulti<Utf8CIHash_fn>(dLocators, dAttrTypes, dJsonKeys);
	else if (eCollation == SPH_COLLATION_LIBC_CI)
		return new CSphGrouperMulti<LibcCIHash_fn>(dLocators, dAttrTypes, dJsonKeys);
	else if (eCollation == SPH_COLLATION_LIBC_CS)
		return new CSphGrouperMulti<LibcCSHash_fn>(dLocators, dAttrTypes, dJsonKeys);
	else
		return new CSphGrouperMulti<BinaryHash_fn>(dLocators, dAttrTypes, dJsonKeys);
}


static void ExtraAddSortkeys(CSphSchema* pExtra, const ISphSchema& tSorterSchema, const int* dAttrs)
{
	if (pExtra)
		for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
			if (dAttrs[i] >= 0)
				pExtra->AddAttr(tSorterSchema.GetAttr(dAttrs[i]), true);
}

// move expressions used in ORDER BY or WITHIN GROUP ORDER BY to presort phase
static bool FixupDependency(ISphSchema& tSchema, const int* pAttrs, int iAttrCount)
{
	assert(pAttrs);

	CSphVector<int> dCur;

	// add valid attributes to processing list
	for (int i = 0; i < iAttrCount; i++)
		if (pAttrs[i] >= 0)
			dCur.Add(pAttrs[i]);

	int iInitialAttrs = dCur.GetLength();

	// collect columns which affect current expressions
	for (int i = 0; i < dCur.GetLength(); i++)
	{
		const CSphColumnInfo& tCol = tSchema.GetAttr(dCur[i]);
		if (tCol.m_eStage > SPH_EVAL_PRESORT && tCol.m_pExpr.Ptr() != NULL)
			tCol.m_pExpr->Command(SPH_EXPR_GET_DEPENDENT_COLS, &dCur);
	}

	// get rid of dupes
	dCur.Uniq();

	// fix up of attributes stages
	ARRAY_FOREACH(i, dCur)
	{
		int iAttr = dCur[i];
		if (iAttr < 0)
			continue;

		CSphColumnInfo& tCol = const_cast <CSphColumnInfo&> (tSchema.GetAttr(iAttr));
		if (tCol.m_eStage == SPH_EVAL_FINAL)
			tCol.m_eStage = SPH_EVAL_PRESORT;
	}

	// it uses attributes if it has dependencies from other attributes
	return (iInitialAttrs > dCur.GetLength());
}


// only STRING ( static packed ) and JSON fields mush be remapped
static void SetupSortRemap(CSphRsetSchema& tSorterSchema, CSphMatchComparatorState& tState)
{
#ifndef NDEBUG
	int iColWasCount = tSorterSchema.GetAttrsCount();
#endif
	for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
	{
		if (!(tState.m_eKeypart[i] == SPH_KEYPART_STRING || tState.m_tSubKeys[i].m_sKey.cstr()))
			continue;

		assert(tState.m_dAttrs[i] >= 0 && tState.m_dAttrs[i] < iColWasCount);

		bool bIsJson = !tState.m_tSubKeys[i].m_sKey.IsEmpty();
		bool bIsFunc = bIsJson && tState.m_tSubKeys[i].m_uMask == 0;

		CSphString sRemapCol;
		sRemapCol.SetSprintf("%s%s", g_sIntAttrPrefix, bIsJson
			? tState.m_tSubKeys[i].m_sKey.cstr()
			: tSorterSchema.GetAttr(tState.m_dAttrs[i]).m_sName.cstr());

		int iRemap = tSorterSchema.GetAttrIndex(sRemapCol.cstr());
		if (iRemap == -1 && bIsJson)
		{
			CSphString sRemapLowercase = sRemapCol;
			sRemapLowercase.ToLower();
			iRemap = tSorterSchema.GetAttrIndex(sRemapLowercase.cstr());
		}

		if (iRemap == -1)
		{
			CSphColumnInfo tRemapCol(sRemapCol.cstr(), bIsJson ? ESphAttr::SPH_ATTR_STRINGPTR : ESphAttr::SPH_ATTR_BIGINT);
			tRemapCol.m_eStage = SPH_EVAL_PRESORT;
			if (bIsJson)
				tRemapCol.m_pExpr = bIsFunc ? tState.m_tSubExpr[i] : new ExprSortJson2StringPtr_c(tState.m_tLocator[i], tState.m_tSubExpr[i]);
			else
				tRemapCol.m_pExpr = new ExprSortStringAttrFixup_c(tState.m_tLocator[i]);

			if (bIsFunc)
			{
				tRemapCol.m_eAttrType = tState.m_tSubType[i];
				tState.m_eKeypart[i] = Attr2Keypart(tState.m_tSubType[i]);
			}

			iRemap = tSorterSchema.GetAttrsCount();
			tSorterSchema.AddDynamicAttr(tRemapCol);
		}
		tState.m_tLocator[i] = tSorterSchema.GetAttr(iRemap).m_tLocator;
	}
}

/// collector for UPDATE statement
class CSphUpdateQueue : public CSphMatchQueueTraits
{
	CSphAttrUpdate		m_tWorkSet;
	CSphIndex* m_pIndex;
	CSphString* m_pError;
	CSphString* m_pWarning;
	int* m_pAffected;

private:
	void DoUpdate()
	{
		if (!m_iUsed)
			return;

		m_tWorkSet.m_dRowOffset.Resize(m_iUsed);
		m_tWorkSet.m_dDocids.Resize(m_iUsed);
		m_tWorkSet.m_dRows.Resize(m_iUsed);

		ARRAY_FOREACH(i, m_tWorkSet.m_dDocids)
		{
			m_tWorkSet.m_dRowOffset[i] = 0;
			m_tWorkSet.m_dDocids[i] = 0;
			m_tWorkSet.m_dRows[i] = NULL;
			if (!DOCINFO2ID(STATIC2DOCINFO(m_pData[i].m_pStatic))) // if static attributes were copied, so, they actually dynamic
			{
				m_tWorkSet.m_dDocids[i] = m_pData[i].m_uDocID;
			}
			else // static attributes points to the active indexes - so, no lookup, 5 times faster update.
			{
				m_tWorkSet.m_dRows[i] = m_pData[i].m_pStatic - (sizeof(SphDocID_t) / sizeof(CSphRowitem));
			}
		}

		*m_pAffected += m_pIndex->UpdateAttributes(m_tWorkSet, -1, *m_pError, *m_pWarning);
		m_iUsed = 0;
	}
public:
	/// ctor
	CSphUpdateQueue(int iSize, CSphAttrUpdateEx* pUpdate, bool bIgnoreNonexistent, bool bStrict)
		: CSphMatchQueueTraits(iSize, true)
	{
		m_tWorkSet.m_dRowOffset.Reserve(m_iSize);
		m_tWorkSet.m_dDocids.Reserve(m_iSize);
		m_tWorkSet.m_dRows.Reserve(m_iSize);

		m_tWorkSet.m_bIgnoreNonexistent = bIgnoreNonexistent;
		m_tWorkSet.m_bStrict = bStrict;
		m_tWorkSet.m_dTypes = pUpdate->m_pUpdate->m_dTypes;
		m_tWorkSet.m_dPool = pUpdate->m_pUpdate->m_dPool;
		m_tWorkSet.m_dAttrs.Resize(pUpdate->m_pUpdate->m_dAttrs.GetLength());
		ARRAY_FOREACH(i, m_tWorkSet.m_dAttrs)
		{
			CSphString sTmp;
			sTmp = pUpdate->m_pUpdate->m_dAttrs[i];
			m_tWorkSet.m_dAttrs[i] = sTmp.Leak();
		}

		m_pIndex = pUpdate->m_pIndex;
		m_pError = pUpdate->m_pError;
		m_pWarning = pUpdate->m_pWarning;
		m_pAffected = &pUpdate->m_iAffected;
	}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return false;
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tEntry)
	{
		m_iTotal++;

		if (m_iUsed == m_iSize)
			DoUpdate();

		// do add
		m_tSchema.CloneMatch(&m_pData[m_iUsed++], tEntry);
		return true;
	}

	/// add grouped entry (must not happen)
	virtual bool PushGrouped(const CSphMatch&, bool)
	{
		assert(0);
		return false;
	}

	/// store all entries into specified location in sorted order, and remove them from queue
	int Flatten(CSphMatch*, int)
	{
		assert(m_iUsed >= 0);
		DoUpdate();
		m_iTotal = 0;
		return 0;
	}

	virtual void Finalize(ISphMatchProcessor& tProcessor, bool)
	{
		if (!GetLength())
			return;

		// just evaluate in heap order
		CSphMatch* pCur = m_pData;
		const CSphMatch* pEnd = m_pData + m_iUsed;
		while (pCur < pEnd)
		{
			tProcessor.Process(pCur++);
		}
	}
};

//////////////////////////////////////////////////////////////////////////

/// collector for DELETE statement
class CSphDeleteQueue : public CSphMatchQueueTraits
{
	CSphVector<SphDocID_t>* m_pValues;
public:
	/// ctor
	CSphDeleteQueue(int iSize, CSphVector<SphDocID_t>* pDeletes)
		: CSphMatchQueueTraits(1, false)
		, m_pValues(pDeletes)
	{
		m_pValues->Reserve(iSize);
	}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return false;
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tEntry)
	{
		m_iTotal++;
		m_pValues->Add(tEntry.m_uDocID);
		return true;
	}

	/// add grouped entry (must not happen)
	virtual bool PushGrouped(const CSphMatch&, bool)
	{
		assert(0);
		return false;
	}

	/// store all entries into specified location in sorted order, and remove them from queue
	int Flatten(CSphMatch*, int)
	{
		m_iTotal = 0;
		return 0;
	}

	virtual void Finalize(ISphMatchProcessor&, bool)
	{}
};


ISphMatchSorter* sphCreateQueue(SphQueueSettings_t& tQueue)
{
	// prepare for descent
	ISphMatchSorter* pTop = NULL;
	CSphMatchComparatorState tStateMatch, tStateGroup;

	// short-cuts
	const CSphQuery* pQuery = &tQueue.m_tQuery;
	const ISphSchema& tSchema = tQueue.m_tSchema;
	CSphString& sError = tQueue.m_sError;
	CSphQueryProfile* pProfiler = tQueue.m_pProfiler;
	CSphSchema* pExtra = tQueue.m_pExtra;

	sError = "";
	bool bHasZonespanlist = false;
	bool bNeedZonespanlist = false;
	DWORD uPackedFactorFlags = SPH_FACTOR_DISABLE;

	///////////////////////////////////////
	// build incoming and outgoing schemas
	///////////////////////////////////////

	// sorter schema
	// adds computed expressions and groupby stuff on top of the original index schema
	CSphRsetSchema tSorterSchema;
	tSorterSchema = tSchema;

	CSphVector<uint64_t> dQueryAttrs;

	// we need this to perform a sanity check
	bool bHasGroupByExpr = false;

	// setup overrides, detach them into dynamic part
	ARRAY_FOREACH(i, pQuery->m_dOverrides)
	{
		const char* sAttr = pQuery->m_dOverrides[i].m_sAttr.cstr();

		int iIndex = tSorterSchema.GetAttrIndex(sAttr);
		if (iIndex < 0)
		{
			sError.SetSprintf("override attribute '%s' not found", sAttr);
			return NULL;
		}

		CSphColumnInfo tCol;
		tCol = tSorterSchema.GetAttr(iIndex);
		tCol.m_eStage = SPH_EVAL_OVERRIDE;
		tSorterSchema.AddDynamicAttr(tCol);
		if (pExtra)
			pExtra->AddAttr(tCol, true);
		tSorterSchema.RemoveStaticAttr(iIndex);

		dQueryAttrs.Add(sphFNV64(tCol.m_sName.cstr()));
	}

	// setup @geodist
	if (pQuery->m_bGeoAnchor && tSorterSchema.GetAttrIndex("@geodist") < 0)
	{
		ExprGeodist_t* pExpr = new ExprGeodist_t();
		if (!pExpr->Setup(pQuery, tSorterSchema, sError))
		{
			pExpr->Release();
			return NULL;
		}
		CSphColumnInfo tCol("@geodist", ESphAttr::SPH_ATTR_FLOAT);
		tCol.m_pExpr = pExpr; // takes ownership, no need to for explicit pExpr release
		tCol.m_eStage = SPH_EVAL_PREFILTER; // OPTIMIZE? actual stage depends on usage
		tSorterSchema.AddDynamicAttr(tCol);
		if (pExtra)
			pExtra->AddAttr(tCol, true);

		dQueryAttrs.Add(sphFNV64(tCol.m_sName.cstr()));
	}

	// setup @expr
	if (pQuery->m_eSort == SPH_SORT_EXPR && tSorterSchema.GetAttrIndex("@expr") < 0)
	{
		CSphColumnInfo tCol("@expr", ESphAttr::SPH_ATTR_FLOAT); // enforce float type for backwards compatibility (ie. too lazy to fix those tests right now)
		tCol.m_pExpr = sphExprParse(pQuery->m_sSortBy.cstr(), tSorterSchema, NULL, NULL, sError, pProfiler, pQuery->m_eCollation, NULL, &bHasZonespanlist);
		bNeedZonespanlist |= bHasZonespanlist;
		if (!tCol.m_pExpr)
			return NULL;
		tCol.m_eStage = SPH_EVAL_PRESORT;
		tSorterSchema.AddDynamicAttr(tCol);

		dQueryAttrs.Add(sphFNV64(tCol.m_sName.cstr()));
	}

	// expressions from select items
	bool bHasCount = false;

	if (tQueue.m_bComputeItems)
		ARRAY_FOREACH(iItem, pQuery->m_dItems)
	{
		const CSphQueryItem& tItem = pQuery->m_dItems[iItem];
		const CSphString& sExpr = tItem.m_sExpr;
		bool bIsCount = IsCount(sExpr);
		bHasCount |= bIsCount;

		if (sExpr == "*")
		{
			for (int i = 0; i < tSchema.GetAttrsCount(); i++)
				dQueryAttrs.Add(sphFNV64(tSchema.GetAttr(i).m_sName.cstr()));
		}

		// for now, just always pass "plain" attrs from index to sorter; they will be filtered on searchd level
		int iAttrIdx = tSchema.GetAttrIndex(sExpr.cstr());
		bool bPlainAttr = ((sExpr == "*" || (iAttrIdx >= 0 && tItem.m_eAggrFunc == SPH_AGGR_NONE)) &&
			(tItem.m_sAlias.IsEmpty() || tItem.m_sAlias == tItem.m_sExpr));
		if (iAttrIdx >= 0)
		{
			ESphAttr eAttr = tSchema.GetAttr(iAttrIdx).m_eAttrType;
			if (eAttr == ESphAttr::SPH_ATTR_STRING || eAttr == ESphAttr::SPH_ATTR_UINT32SET || eAttr == ESphAttr::SPH_ATTR_INT64SET)
			{
				if (tItem.m_eAggrFunc != SPH_AGGR_NONE)
				{
					sError.SetSprintf("can not aggregate non-scalar attribute '%s'", tItem.m_sExpr.cstr());
					return NULL;
				}

				if (!bPlainAttr && eAttr == ESphAttr::SPH_ATTR_STRING)
				{
					bPlainAttr = true;
					for (int i = 0; i < iItem && bPlainAttr; i++)
						if (sExpr == pQuery->m_dItems[i].m_sAlias)
							bPlainAttr = false;
				}
			}
		}

		if (bPlainAttr || IsGroupby(sExpr) || bIsCount)
		{
			bHasGroupByExpr = IsGroupby(sExpr);
			continue;
		}

		// not an attribute? must be an expression, and must be aliased by query parser
		assert(!tItem.m_sAlias.IsEmpty());

		// tricky part
		// we might be fed with precomputed matches, but it's all or nothing
		// the incoming match either does not have anything computed, or it has everything
		int iSorterAttr = tSorterSchema.GetAttrIndex(tItem.m_sAlias.cstr());
		if (iSorterAttr >= 0)
		{
			if (dQueryAttrs.Contains(sphFNV64(tItem.m_sAlias.cstr())))
			{
				sError.SetSprintf("alias '%s' must be unique (conflicts with another alias)", tItem.m_sAlias.cstr());
				return NULL;
			}
			else
			{
				tSorterSchema.RemoveStaticAttr(iSorterAttr);
			}
		}

		// a new and shiny expression, lets parse
		CSphColumnInfo tExprCol(tItem.m_sAlias.cstr(), ESphAttr::SPH_ATTR_NONE);
		DWORD uQueryPackedFactorFlags = SPH_FACTOR_DISABLE;

		// tricky bit
		// GROUP_CONCAT() adds an implicit TO_STRING() conversion on top of its argument
		// and then the aggregate operation simply concatenates strings as matches arrive
		// ideally, we would instead pass ownership of the expression to G_C() implementation
		// and also the original expression type, and let the string conversion happen in G_C() itself
		// but that ideal route seems somewhat more complicated in the current architecture
		if (tItem.m_eAggrFunc == SPH_AGGR_CAT)
		{
			CSphString sExpr2;
			sExpr2.SetSprintf("TO_STRING(%s)", sExpr.cstr());
			tExprCol.m_pExpr = sphExprParse(sExpr2.cstr(), tSorterSchema, &tExprCol.m_eAttrType,
				&tExprCol.m_bWeight, sError, pProfiler, pQuery->m_eCollation, tQueue.m_pHook, &bHasZonespanlist, &uQueryPackedFactorFlags, &tExprCol.m_eStage);
		}
		else
		{
			tExprCol.m_pExpr = sphExprParse(sExpr.cstr(), tSorterSchema, &tExprCol.m_eAttrType,
				&tExprCol.m_bWeight, sError, pProfiler, pQuery->m_eCollation, tQueue.m_pHook, &bHasZonespanlist, &uQueryPackedFactorFlags, &tExprCol.m_eStage);
		}

		uPackedFactorFlags |= uQueryPackedFactorFlags;
		bNeedZonespanlist |= bHasZonespanlist;
		tExprCol.m_eAggrFunc = tItem.m_eAggrFunc;
		if (!tExprCol.m_pExpr)
		{
			sError.SetSprintf("parse error: %s", sError.cstr());
			return NULL;
		}

		// force AVG() to be computed in floats
		if (tExprCol.m_eAggrFunc == SPH_AGGR_AVG)
		{
			tExprCol.m_eAttrType = ESphAttr::SPH_ATTR_FLOAT;
			tExprCol.m_tLocator.m_iBitCount = 32;
		}

		// force explicit type conversion for JSON attributes
		if (tExprCol.m_eAggrFunc != SPH_AGGR_NONE && tExprCol.m_eAttrType == ESphAttr::SPH_ATTR_JSON_FIELD)
		{
			sError.SetSprintf("ambiguous attribute type '%s', use INTEGER(), BIGINT() or DOUBLE() conversion functions", tItem.m_sExpr.cstr());
			return NULL;
		}

		if (uQueryPackedFactorFlags & SPH_FACTOR_JSON_OUT)
			tExprCol.m_eAttrType = ESphAttr::SPH_ATTR_FACTORS_JSON;

		// force GROUP_CONCAT() to be computed as strings
		if (tExprCol.m_eAggrFunc == SPH_AGGR_CAT)
		{
			tExprCol.m_eAttrType = ESphAttr::SPH_ATTR_STRINGPTR;
			tExprCol.m_tLocator.m_iBitCount = ROWITEMPTR_BITS;
		}

		// postpone aggregates, add non-aggregates
		if (tExprCol.m_eAggrFunc == SPH_AGGR_NONE)
		{
			// is this expression used in filter?
			// OPTIMIZE? hash filters and do hash lookups?
			if (tExprCol.m_eAttrType != ESphAttr::SPH_ATTR_JSON_FIELD)
				ARRAY_FOREACH(i, pQuery->m_dFilters)
				if (pQuery->m_dFilters[i].m_sAttrName == tExprCol.m_sName)
				{
					// is this a hack?
					// m_bWeight is computed after EarlyReject() get called
					// that means we can't evaluate expressions with WEIGHT() in prefilter phase
					if (tExprCol.m_bWeight)
					{
						tExprCol.m_eStage = SPH_EVAL_PRESORT; // special, weight filter ( short cut )
						break;
					}

					// so we are about to add a filter condition
					// but it might depend on some preceding columns (e.g. SELECT 1+attr f1 ... WHERE f1>5)
					// lets detect those and move them to prefilter \ presort phase too
					CSphVector<int> dCur;
					tExprCol.m_pExpr->Command(SPH_EXPR_GET_DEPENDENT_COLS, &dCur);

					// usual filter
					tExprCol.m_eStage = SPH_EVAL_PREFILTER;
					ARRAY_FOREACH(j, dCur)
					{
						const CSphColumnInfo& tCol = tSorterSchema.GetAttr(dCur[j]);
						if (tCol.m_bWeight)
						{
							tExprCol.m_eStage = SPH_EVAL_PRESORT;
							tExprCol.m_bWeight = true;
						}
						// handle chains of dependencies (e.g. SELECT 1+attr f1, f1-1 f2 ... WHERE f2>5)
						if (tCol.m_pExpr.Ptr())
						{
							tCol.m_pExpr->Command(SPH_EXPR_GET_DEPENDENT_COLS, &dCur);
						}
					}
					dCur.Uniq();

					ARRAY_FOREACH(j, dCur)
					{
						CSphColumnInfo& tDep = const_cast <CSphColumnInfo&> (tSorterSchema.GetAttr(dCur[j]));
						if (tDep.m_eStage > tExprCol.m_eStage)
							tDep.m_eStage = tExprCol.m_eStage;
					}
					break;
				}

			// add it!
			// NOTE, "final" stage might need to be fixed up later
			// we'll do that when parsing sorting clause
			tSorterSchema.AddDynamicAttr(tExprCol);
		}
		else // some aggregate
		{
			tExprCol.m_eStage = SPH_EVAL_PRESORT; // sorter expects computed expression
			tSorterSchema.AddDynamicAttr(tExprCol);
			if (pExtra)
				pExtra->AddAttr(tExprCol, true);

			/// update aggregate dependencies (e.g. SELECT 1+attr f1, min(f1), ...)
			CSphVector<int> dCur;
			tExprCol.m_pExpr->Command(SPH_EXPR_GET_DEPENDENT_COLS, &dCur);

			ARRAY_FOREACH(j, dCur)
			{
				const CSphColumnInfo& tCol = tSorterSchema.GetAttr(dCur[j]);
				if (tCol.m_pExpr.Ptr())
					tCol.m_pExpr->Command(SPH_EXPR_GET_DEPENDENT_COLS, &dCur);
			}
			dCur.Uniq();

			ARRAY_FOREACH(j, dCur)
			{
				CSphColumnInfo& tDep = const_cast <CSphColumnInfo&> (tSorterSchema.GetAttr(dCur[j]));
				if (tDep.m_eStage > tExprCol.m_eStage)
					tDep.m_eStage = tExprCol.m_eStage;
			}
		}
		dQueryAttrs.Add(sphFNV64((const BYTE*)tExprCol.m_sName.cstr()));
	}

	////////////////////////////////////////////
	// setup groupby settings, create shortcuts
	////////////////////////////////////////////

	CSphVector<int> dGroupColumns;
	CSphGroupSorterSettings tSettings;
	bool bImplicit = false;

	if (pQuery->m_sGroupBy.IsEmpty())
		ARRAY_FOREACH_COND(i, pQuery->m_dItems, !bImplicit)
	{
		const CSphQueryItem& t = pQuery->m_dItems[i];
		bImplicit = (t.m_eAggrFunc != SPH_AGGR_NONE) || t.m_sExpr == "count(*)" || t.m_sExpr == "@distinct";
	}

	if (!SetupGroupbySettings(pQuery, tSorterSchema, tSettings, dGroupColumns, sError, bImplicit))
		return NULL;

	const bool bGotGroupby = !pQuery->m_sGroupBy.IsEmpty() || tSettings.m_bImplicit; // or else, check in SetupGroupbySettings() would already fail
	const bool bGotDistinct = (tSettings.m_tDistinctLoc.m_iBitOffset >= 0);

	if (bHasGroupByExpr && !bGotGroupby)
	{
		sError = "GROUPBY() is allowed only in GROUP BY queries";
		return NULL;
	}

	// check for HAVING constrains
	if (tQueue.m_pAggrFilter && !tQueue.m_pAggrFilter->m_sAttrName.IsEmpty())
	{
		if (!bGotGroupby)
		{
			sError.SetSprintf("can not use HAVING without GROUP BY");
			return NULL;
		}

		// should be column named at group by or it's alias or aggregate
		const CSphString& sHaving = tQueue.m_pAggrFilter->m_sAttrName;
		if (!IsGroupbyMagic(sHaving))
		{
			bool bValidHaving = false;
			ARRAY_FOREACH(i, pQuery->m_dItems)
			{
				const CSphQueryItem& tItem = pQuery->m_dItems[i];
				if (tItem.m_sAlias != sHaving)
					continue;

				bValidHaving = (IsGroupbyMagic(tItem.m_sExpr) || tItem.m_eAggrFunc != SPH_AGGR_NONE);
				break;
			}

			if (!bValidHaving)
			{
				sError.SetSprintf("can not use HAVING with attribute not related to GROUP BY");
				return NULL;
			}
		}
	}

	// now lets add @groupby etc if needed
	if (bGotGroupby && tSorterSchema.GetAttrIndex("@groupby") < 0)
	{
		ESphAttr eGroupByResult = (!tSettings.m_bImplicit) ? tSettings.m_pGrouper->GetResultType() : ESphAttr::SPH_ATTR_INTEGER; // implicit do not have grouper
		if (tSettings.m_bMva64)
			eGroupByResult = ESphAttr::SPH_ATTR_BIGINT;

		CSphColumnInfo tGroupby("@groupby", eGroupByResult);
		CSphColumnInfo tCount("@count", ESphAttr::SPH_ATTR_INTEGER);
		CSphColumnInfo tDistinct("@distinct", ESphAttr::SPH_ATTR_INTEGER);

		tGroupby.m_eStage = SPH_EVAL_SORTER;
		tCount.m_eStage = SPH_EVAL_SORTER;
		tDistinct.m_eStage = SPH_EVAL_SORTER;

		tSorterSchema.AddDynamicAttr(tGroupby);
		tSorterSchema.AddDynamicAttr(tCount);
		if (pExtra)
		{
			pExtra->AddAttr(tGroupby, true);
			pExtra->AddAttr(tCount, true);
		}
		if (bGotDistinct)
		{
			tSorterSchema.AddDynamicAttr(tDistinct);
			if (pExtra)
				pExtra->AddAttr(tDistinct, true);
		}

		// add @groupbystr last in case we need to skip it on sending (like @int_str2ptr_*)
		if (tSettings.m_bJson)
		{
			CSphColumnInfo tGroupbyStr("@groupbystr", ESphAttr::SPH_ATTR_JSON_FIELD);
			tGroupbyStr.m_eStage = SPH_EVAL_SORTER;
			tSorterSchema.AddDynamicAttr(tGroupbyStr);
		}
	}

#define LOC_CHECK(_cond,_msg) if (!(_cond)) { sError = "invalid schema: " _msg; return NULL; }

	int iGroupby = tSorterSchema.GetAttrIndex("@groupby");
	if (iGroupby >= 0)
	{
		tSettings.m_bDistinct = bGotDistinct;
		tSettings.m_tLocGroupby = tSorterSchema.GetAttr(iGroupby).m_tLocator;
		LOC_CHECK(tSettings.m_tLocGroupby.m_bDynamic, "@groupby must be dynamic");

		int iCount = tSorterSchema.GetAttrIndex("@count");
		LOC_CHECK(iCount >= 0, "missing @count");

		tSettings.m_tLocCount = tSorterSchema.GetAttr(iCount).m_tLocator;
		LOC_CHECK(tSettings.m_tLocCount.m_bDynamic, "@count must be dynamic");

		int iDistinct = tSorterSchema.GetAttrIndex("@distinct");
		if (bGotDistinct)
		{
			LOC_CHECK(iDistinct >= 0, "missing @distinct");
			tSettings.m_tLocDistinct = tSorterSchema.GetAttr(iDistinct).m_tLocator;
			LOC_CHECK(tSettings.m_tLocDistinct.m_bDynamic, "@distinct must be dynamic");
		}
		else
		{
			LOC_CHECK(iDistinct <= 0, "unexpected @distinct");
		}

		int iGroupbyStr = tSorterSchema.GetAttrIndex("@groupbystr");
		if (iGroupbyStr >= 0)
			tSettings.m_tLocGroupbyStr = tSorterSchema.GetAttr(iGroupbyStr).m_tLocator;
	}

	if (bHasCount)
	{
		LOC_CHECK(tSorterSchema.GetAttrIndex("@count") >= 0, "Count(*) or @count is queried, but not available in the schema");
	}

#undef LOC_CHECK

	////////////////////////////////////
	// choose and setup sorting functor
	////////////////////////////////////

	ESphSortFunc eMatchFunc = FUNC_REL_DESC;
	ESphSortFunc eGroupFunc = FUNC_REL_DESC;
	bool bUsesAttrs = false;
	bool bRandomize = false;

	// matches sorting function
	if (pQuery->m_eSort == SPH_SORT_EXTENDED)
	{
		ESortClauseParseResult eRes = sphParseSortClause(pQuery, pQuery->m_sSortBy.cstr(),
			tSorterSchema, eMatchFunc, tStateMatch, sError);

		if (eRes == SORT_CLAUSE_ERROR)
			return NULL;

		if (eRes == SORT_CLAUSE_RANDOM)
			bRandomize = true;

		ExtraAddSortkeys(pExtra, tSorterSchema, tStateMatch.m_dAttrs);

		bUsesAttrs = FixupDependency(tSorterSchema, tStateMatch.m_dAttrs, CSphMatchComparatorState::MAX_ATTRS);
		if (!bUsesAttrs)
		{
			for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
			{
				ESphSortKeyPart ePart = tStateMatch.m_eKeypart[i];
				if (ePart == SPH_KEYPART_INT || ePart == SPH_KEYPART_FLOAT || ePart == SPH_KEYPART_STRING || ePart == SPH_KEYPART_STRINGPTR)
					bUsesAttrs = true;
			}
		}
		SetupSortRemap(tSorterSchema, tStateMatch);

	}
	else if (pQuery->m_eSort == SPH_SORT_EXPR)
	{
		tStateMatch.m_eKeypart[0] = SPH_KEYPART_INT;
		tStateMatch.m_tLocator[0] = tSorterSchema.GetAttr(tSorterSchema.GetAttrIndex("@expr")).m_tLocator;
		tStateMatch.m_eKeypart[1] = SPH_KEYPART_ID;
		tStateMatch.m_uAttrDesc = 1;
		eMatchFunc = FUNC_EXPR;
		bUsesAttrs = true;

	}
	else
	{
		// check sort-by attribute
		if (pQuery->m_eSort != SPH_SORT_RELEVANCE)
		{
			int iSortAttr = tSorterSchema.GetAttrIndex(pQuery->m_sSortBy.cstr());
			if (iSortAttr < 0)
			{
				sError.SetSprintf("sort-by attribute '%s' not found", pQuery->m_sSortBy.cstr());
				return NULL;
			}
			const CSphColumnInfo& tAttr = tSorterSchema.GetAttr(iSortAttr);
			tStateMatch.m_eKeypart[0] = Attr2Keypart(tAttr.m_eAttrType);
			tStateMatch.m_tLocator[0] = tAttr.m_tLocator;
			tStateMatch.m_dAttrs[0] = iSortAttr;
			SetupSortRemap(tSorterSchema, tStateMatch);
		}

		// find out what function to use and whether it needs attributes
		bUsesAttrs = true;
		switch (pQuery->m_eSort)
		{
		case SPH_SORT_ATTR_DESC:		eMatchFunc = FUNC_ATTR_DESC; break;
		case SPH_SORT_ATTR_ASC:			eMatchFunc = FUNC_ATTR_ASC; break;
		case SPH_SORT_TIME_SEGMENTS:	eMatchFunc = FUNC_TIMESEGS; break;
		case SPH_SORT_RELEVANCE:		eMatchFunc = FUNC_REL_DESC; bUsesAttrs = false; break;
		default:
			sError.SetSprintf("unknown sorting mode %d", pQuery->m_eSort);
			return NULL;
		}
	}

	// groups sorting function
	if (bGotGroupby)
	{
		ESortClauseParseResult eRes = sphParseSortClause(pQuery, pQuery->m_sGroupSortBy.cstr(),
			tSorterSchema, eGroupFunc, tStateGroup, sError);

		if (eRes == SORT_CLAUSE_ERROR || eRes == SORT_CLAUSE_RANDOM)
		{
			if (eRes == SORT_CLAUSE_RANDOM)
				sError.SetSprintf("groups can not be sorted by @random");
			return NULL;
		}

		ExtraAddSortkeys(pExtra, tSorterSchema, tStateGroup.m_dAttrs);

		assert(dGroupColumns.GetLength() || tSettings.m_bImplicit);
		if (pExtra && !tSettings.m_bImplicit)
		{
			ARRAY_FOREACH(i, dGroupColumns)
				pExtra->AddAttr(tSorterSchema.GetAttr(dGroupColumns[i]), true);
		}

		if (bGotDistinct)
		{
			dGroupColumns.Add(tSorterSchema.GetAttrIndex(pQuery->m_sGroupDistinct.cstr()));
			assert(dGroupColumns.Last() >= 0);
			if (pExtra)
				pExtra->AddAttr(tSorterSchema.GetAttr(dGroupColumns.Last()), true);
		}

		if (dGroupColumns.GetLength()) // implicit case
		{
			FixupDependency(tSorterSchema, dGroupColumns.Begin(), dGroupColumns.GetLength());
		}
		FixupDependency(tSorterSchema, tStateGroup.m_dAttrs, CSphMatchComparatorState::MAX_ATTRS);

		// GroupSortBy str attributes setup
		SetupSortRemap(tSorterSchema, tStateGroup);
	}

	// set up aggregate filter for grouper
	if (bGotGroupby && tQueue.m_pAggrFilter && !tQueue.m_pAggrFilter->m_sAttrName.IsEmpty())
	{
		if (tSorterSchema.GetAttr(tQueue.m_pAggrFilter->m_sAttrName.cstr()))
		{
			tSettings.m_pAggrFilterTrait = sphCreateAggrFilter(tQueue.m_pAggrFilter, tQueue.m_pAggrFilter->m_sAttrName, tSorterSchema, sError);
		}
		else
		{
			// having might reference aliased attributes but @* attributes got stored without alias in sorter schema
			CSphString sHaving;
			ARRAY_FOREACH(i, pQuery->m_dItems)
			{
				const CSphQueryItem& tItem = pQuery->m_dItems[i];
				if (tItem.m_sAlias == tQueue.m_pAggrFilter->m_sAttrName)
				{
					sHaving = tItem.m_sExpr;
					break;
				}
			}

			if (sHaving == "groupby()")
				sHaving = "@groupby";
			else if (sHaving == "count(*)")
				sHaving = "@count";

			tSettings.m_pAggrFilterTrait = sphCreateAggrFilter(tQueue.m_pAggrFilter, sHaving, tSorterSchema, sError);
		}

		if (!tSettings.m_pAggrFilterTrait)
			return NULL;
	}

	///////////////////
	// spawn the queue
	///////////////////

	if (!bGotGroupby)
	{
		if (tQueue.m_pUpdate)
			pTop = new CSphUpdateQueue(pQuery->m_iMaxMatches, tQueue.m_pUpdate, pQuery->m_bIgnoreNonexistent, pQuery->m_bStrict);
		else if (tQueue.m_pDeletes)
			pTop = new CSphDeleteQueue(pQuery->m_iMaxMatches, tQueue.m_pDeletes);
		else
			pTop = CreatePlainSorter(eMatchFunc, pQuery->m_bSortKbuffer, pQuery->m_iMaxMatches, bUsesAttrs, uPackedFactorFlags & SPH_FACTOR_ENABLE);
	}
	else
	{
		pTop = sphCreateSorter1st(eMatchFunc, eGroupFunc, pQuery, tSettings, uPackedFactorFlags & SPH_FACTOR_ENABLE);
	}

	if (!pTop)
	{
		sError.SetSprintf("internal error: unhandled sorting mode (match-sort=%d, group=%d, group-sort=%d)",
			eMatchFunc, bGotGroupby, eGroupFunc);
		return NULL;
	}

	switch (pQuery->m_eCollation)
	{
	case SPH_COLLATION_LIBC_CI:
		tStateMatch.m_fnStrCmp = sphCollateLibcCI;
		tStateGroup.m_fnStrCmp = sphCollateLibcCI;
		break;
	case SPH_COLLATION_LIBC_CS:
		tStateMatch.m_fnStrCmp = sphCollateLibcCS;
		tStateGroup.m_fnStrCmp = sphCollateLibcCS;
		break;
	case SPH_COLLATION_UTF8_GENERAL_CI:
		tStateMatch.m_fnStrCmp = sphCollateUtf8GeneralCI;
		tStateGroup.m_fnStrCmp = sphCollateUtf8GeneralCI;
		break;
	case SPH_COLLATION_BINARY:
		tStateMatch.m_fnStrCmp = sphCollateBinary;
		tStateGroup.m_fnStrCmp = sphCollateBinary;
		break;
	}

	assert(pTop);
	pTop->SetState(tStateMatch);
	pTop->SetGroupState(tStateGroup);
	pTop->SetSchema(tSorterSchema);
	pTop->m_bRandomize = bRandomize;

	if (bRandomize)
	{
		if (pQuery->m_iRandSeed >= 0)
			sphSrand((DWORD)pQuery->m_iRandSeed);
		else
			sphAutoSrand();
	}

	tQueue.m_bZonespanlist = bNeedZonespanlist;
	tQueue.m_uPackedFactorFlags = uPackedFactorFlags;

	return pTop;
}

}