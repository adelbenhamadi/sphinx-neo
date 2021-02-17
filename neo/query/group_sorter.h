#pragma once
#include "neo/int/types.h"
#include "neo/index/enums.h"
#include "neo/query/group_sorter_settings.h"
#include "neo/query/grouper.h"
#include "neo/query/match_sorter.h"
#include "neo/query/imatch_comparator.h"
#include "neo/core/attrib_index_builder.h"
#include "neo/core/match_engine.h"
#include "neo/core/match.h"
#include "neo/query/uniqounter.h"
#include "neo/tools/utf8_tools.h"
#include "neo/utility/fixed_hash.h"
#include "neo/utility/hash.h"

namespace NEO {

/*static*/ const char g_sIntAttrPrefix[] = "@int_str2ptr_";

//fwd dec
class CSphGrouper; 
class CSphQuery;


bool			sphIsSortStringInternal(const char* sColumnName);

/// attribute magic
enum
{
	SPH_VATTR_ID = -1,	///< tells match sorter to use doc id
	SPH_VATTR_RELEVANCE = -2,	///< tells match sorter to use match weight
	SPH_VATTR_FLOAT = 10000	///< tells match sorter to compare floats
};


/// aggregate function interface
class IAggrFunc
{
public:
	virtual			~IAggrFunc() {}
	virtual void	Ungroup(CSphMatch*) {}
	virtual void	Update(CSphMatch* pDst, const CSphMatch* pSrc, bool bGrouped) = 0;
	virtual void	Finalize(CSphMatch*) {}
};


/// aggregate traits for different attribute types
template < typename T >
class IAggrFuncTraits : public IAggrFunc
{
public:
	explicit		IAggrFuncTraits(const CSphAttrLocator& tLocator) : m_tLocator(tLocator) {}
	inline T		GetValue(const CSphMatch* pRow);
	inline void		SetValue(CSphMatch* pRow, T val);

protected:
	CSphAttrLocator	m_tLocator;
};



/// SUM() implementation
template < typename T >
class AggrSum_t : public IAggrFuncTraits<T>
{
public:
	explicit AggrSum_t(const CSphAttrLocator& tLoc) : IAggrFuncTraits<T>(tLoc)
	{}

	virtual void Update(CSphMatch* pDst, const CSphMatch* pSrc, bool)
	{
		this->SetValue(pDst, this->GetValue(pDst) + this->GetValue(pSrc));
	}
};


/// AVG() implementation
template < typename T >
class AggrAvg_t : public IAggrFuncTraits<T>
{
protected:
	CSphAttrLocator m_tCountLoc;
public:
	AggrAvg_t(const CSphAttrLocator& tLoc, const CSphAttrLocator& tCountLoc) : IAggrFuncTraits<T>(tLoc), m_tCountLoc(tCountLoc)
	{}

	virtual void Ungroup(CSphMatch* pDst)
	{
		this->SetValue(pDst, T(this->GetValue(pDst) * pDst->GetAttr(m_tCountLoc)));
	}

	virtual void Update(CSphMatch* pDst, const CSphMatch* pSrc, bool bGrouped)
	{
		if (bGrouped)
			this->SetValue(pDst, T(this->GetValue(pDst) + this->GetValue(pSrc) * pSrc->GetAttr(m_tCountLoc)));
		else
			this->SetValue(pDst, this->GetValue(pDst) + this->GetValue(pSrc));
	}

	virtual void Finalize(CSphMatch* pDst)
	{
		this->SetValue(pDst, T(this->GetValue(pDst) / pDst->GetAttr(m_tCountLoc)));
	}
};


/// MAX() implementation
template < typename T >
class AggrMax_t : public IAggrFuncTraits<T>
{
public:
	explicit AggrMax_t(const CSphAttrLocator& tLoc) : IAggrFuncTraits<T>(tLoc)
	{}

	virtual void Update(CSphMatch* pDst, const CSphMatch* pSrc, bool)
	{
		this->SetValue(pDst, Max(this->GetValue(pDst), this->GetValue(pSrc)));
	}
};


/// MIN() implementation
template < typename T >
class AggrMin_t : public IAggrFuncTraits<T>
{
public:
	explicit AggrMin_t(const CSphAttrLocator& tLoc) : IAggrFuncTraits<T>(tLoc)
	{}

	virtual void Update(CSphMatch* pDst, const CSphMatch* pSrc, bool)
	{
		this->SetValue(pDst, Min(this->GetValue(pDst), this->GetValue(pSrc)));
	}
};


/// GROUP_CONCAT() implementation
class AggrConcat_t : public IAggrFunc
{
protected:
	CSphAttrLocator	m_tLoc;

public:
	explicit AggrConcat_t(const CSphColumnInfo& tCol)
		: m_tLoc(tCol.m_tLocator)
	{}

	void Ungroup(CSphMatch*) {}
	void Finalize(CSphMatch*) {}

	void Update(CSphMatch* pDst, const CSphMatch* pSrc, bool)
	{
		const char* sDst = (const char*)pDst->GetAttr(m_tLoc);
		const char* sSrc = (const char*)pSrc->GetAttr(m_tLoc);
		assert(!sDst || *sDst); // ensure the string is either NULL, or has data
		assert(!sSrc || *sSrc);

		// empty source? kinda weird, but done!
		if (!sSrc)
			return;

		// empty destination? just clone the source
		if (!sDst)
		{
			if (sSrc)
				pDst->SetAttr(m_tLoc, (SphAttr_t)strdup(sSrc));
			return;
		}

		// both source and destination present? append source to destination
		// note that it gotta be manual copying here, as SetSprintf (currently) comes with a 1K limit
		assert(sDst && sSrc);
		int iSrc = strlen(sSrc);
		int iDst = strlen(sDst);
		char* sNew = new char[iSrc + iDst + 2]; // OPTIMIZE? careful pre-reserve and/or realloc would be even faster
		memcpy(sNew, sDst, iDst);
		sNew[iDst] = ',';
		memcpy(sNew + iDst + 1, sSrc, iSrc);
		sNew[iSrc + iDst + 1] = '\0';
		pDst->SetAttr(m_tLoc, (SphAttr_t)sNew);
		SafeDeleteArray(sDst);
	}
};


/// group sorting functor
template < typename COMPGROUP >
struct GroupSorter_fn : public CSphMatchComparatorState, public MatchSortAccessor_t
{
	bool IsLess(const MEDIAN_TYPE a, const MEDIAN_TYPE b) const
	{
		return COMPGROUP::IsLess(*b, *a, *this);
	}
};


struct MatchCloner_t
{
	CSphFixedVector<CSphRowitem>	m_dRowBuf;
	CSphVector<CSphAttrLocator>		m_dAttrsRaw;
	CSphVector<CSphAttrLocator>		m_dAttrsPtr;
	const CSphRsetSchema* m_pSchema;

	MatchCloner_t()
		: m_dRowBuf(0)
		, m_pSchema(NULL)
	{ }

	void SetSchema(const CSphRsetSchema* pSchema)
	{
		m_pSchema = pSchema;
		m_dRowBuf.Reset(m_pSchema->GetDynamicSize());
	}

	void Combine(CSphMatch* pDst, const CSphMatch* pSrc, const CSphMatch* pGroup)
	{
		assert(m_pSchema && pDst && pSrc && pGroup);
		assert(pDst != pGroup);
		m_pSchema->CloneMatch(pDst, *pSrc);

		ARRAY_FOREACH(i, m_dAttrsRaw)
		{
			pDst->SetAttr(m_dAttrsRaw[i], pGroup->GetAttr(m_dAttrsRaw[i]));
		}

		ARRAY_FOREACH(i, m_dAttrsPtr)
		{
			assert(!pDst->GetAttr(m_dAttrsPtr[i]));
			const char* sSrc = (const char*)pGroup->GetAttr(m_dAttrsPtr[i]);
			const char* sDst = NULL;
			if (sSrc && *sSrc)
				sDst = strdup(sSrc);

			pDst->SetAttr(m_dAttrsPtr[i], (SphAttr_t)sDst);
		}
	}

	void Clone(CSphMatch* pOld, const CSphMatch* pNew)
	{
		assert(m_pSchema && pOld && pNew);
		if (pOld->m_pDynamic == NULL) // no old match has no data to copy, just a fresh but old match
		{
			m_pSchema->CloneMatch(pOld, *pNew);
			return;
		}

		memcpy(m_dRowBuf.Begin(), pOld->m_pDynamic, sizeof(m_dRowBuf[0]) * m_dRowBuf.GetLength());

		// don't let cloning operation to free old string data
		// as it will be copied back
		ARRAY_FOREACH(i, m_dAttrsPtr)
			pOld->SetAttr(m_dAttrsPtr[i], 0);

		m_pSchema->CloneMatch(pOld, *pNew);

		ARRAY_FOREACH(i, m_dAttrsRaw)
			pOld->SetAttr(m_dAttrsRaw[i], sphGetRowAttr(m_dRowBuf.Begin(), m_dAttrsRaw[i]));
		ARRAY_FOREACH(i, m_dAttrsPtr)
			pOld->SetAttr(m_dAttrsPtr[i], sphGetRowAttr(m_dRowBuf.Begin(), m_dAttrsPtr[i]));
	}
};


static void ExtractAggregates(const CSphRsetSchema& tSchema, const CSphAttrLocator& tLocCount, const ESphSortKeyPart* m_pGroupSorterKeyparts, const CSphAttrLocator* m_pGroupSorterLocator,
	CSphVector<IAggrFunc*>& dAggregates, CSphVector<IAggrFunc*>& dAvgs, MatchCloner_t& tCloner)
{
	for (int i = 0; i < tSchema.GetAttrsCount(); i++)
	{
		const CSphColumnInfo& tAttr = tSchema.GetAttr(i);
		bool bMagicAggr = IsGroupbyMagic(tAttr.m_sName) || sphIsSortStringInternal(tAttr.m_sName.cstr()); // magic legacy aggregates

		if (tAttr.m_eAggrFunc == SPH_AGGR_NONE || bMagicAggr)
			continue;

		switch (tAttr.m_eAggrFunc)
		{
		case SPH_AGGR_SUM:
			switch (tAttr.m_eAttrType)
			{
			case ESphAttr::SPH_ATTR_INTEGER:	dAggregates.Add(new AggrSum_t<DWORD>(tAttr.m_tLocator)); break;
			case ESphAttr::SPH_ATTR_BIGINT:	dAggregates.Add(new AggrSum_t<int64_t>(tAttr.m_tLocator)); break;
			case ESphAttr::SPH_ATTR_FLOAT:	dAggregates.Add(new AggrSum_t<float>(tAttr.m_tLocator)); break;
			default:				assert(0 && "internal error: unhandled aggregate type"); break;
			}
			break;

		case SPH_AGGR_AVG:
			switch (tAttr.m_eAttrType)
			{
			case ESphAttr::SPH_ATTR_INTEGER:	dAggregates.Add(new AggrAvg_t<DWORD>(tAttr.m_tLocator, tLocCount)); break;
			case ESphAttr::SPH_ATTR_BIGINT:	dAggregates.Add(new AggrAvg_t<int64_t>(tAttr.m_tLocator, tLocCount)); break;
			case ESphAttr::SPH_ATTR_FLOAT:	dAggregates.Add(new AggrAvg_t<float>(tAttr.m_tLocator, tLocCount)); break;
			default:				assert(0 && "internal error: unhandled aggregate type"); break;
			}
			// store avg to calculate these attributes prior to groups sort
			for (int iState = 0; iState < CSphMatchComparatorState::MAX_ATTRS; iState++)
			{
				ESphSortKeyPart eKeypart = m_pGroupSorterKeyparts[iState];
				CSphAttrLocator tLoc = m_pGroupSorterLocator[iState];
				if ((eKeypart == SPH_KEYPART_INT || eKeypart == SPH_KEYPART_FLOAT)
					&& tLoc.m_bDynamic == tAttr.m_tLocator.m_bDynamic && tLoc.m_iBitOffset == tAttr.m_tLocator.m_iBitOffset
					&& tLoc.m_iBitCount == tAttr.m_tLocator.m_iBitCount)
				{
					dAvgs.Add(dAggregates.Last());
					break;
				}
			}
			break;

		case SPH_AGGR_MIN:
			switch (tAttr.m_eAttrType)
			{
			case ESphAttr::SPH_ATTR_INTEGER:	dAggregates.Add(new AggrMin_t<DWORD>(tAttr.m_tLocator)); break;
			case ESphAttr::SPH_ATTR_BIGINT:	dAggregates.Add(new AggrMin_t<int64_t>(tAttr.m_tLocator)); break;
			case ESphAttr::SPH_ATTR_FLOAT:	dAggregates.Add(new AggrMin_t<float>(tAttr.m_tLocator)); break;
			default:				assert(0 && "internal error: unhandled aggregate type"); break;
			}
			break;

		case SPH_AGGR_MAX:
			switch (tAttr.m_eAttrType)
			{
			case ESphAttr::SPH_ATTR_INTEGER:	dAggregates.Add(new AggrMax_t<DWORD>(tAttr.m_tLocator)); break;
			case ESphAttr::SPH_ATTR_BIGINT:	dAggregates.Add(new AggrMax_t<int64_t>(tAttr.m_tLocator)); break;
			case ESphAttr::SPH_ATTR_FLOAT:	dAggregates.Add(new AggrMax_t<float>(tAttr.m_tLocator)); break;
			default:				assert(0 && "internal error: unhandled aggregate type"); break;
			}
			break;

		case SPH_AGGR_CAT:
			dAggregates.Add(new AggrConcat_t(tAttr));
			break;

		default:
			assert(0 && "internal error: unhandled aggregate function");
			break;
		}

		if (tAttr.m_eAggrFunc == SPH_AGGR_CAT)
			tCloner.m_dAttrsPtr.Add(tAttr.m_tLocator);
		else
			tCloner.m_dAttrsRaw.Add(tAttr.m_tLocator);
	}
}


static SphAttr_t GetDistinctKey(const CSphMatch& tEntry, const CSphAttrLocator& tDistinctLoc, ESphAttr eDistinctAttr, const BYTE* pStringBase)
{
	SphAttr_t tRes = tEntry.GetAttr(tDistinctLoc);
	if (eDistinctAttr == ESphAttr::SPH_ATTR_STRING)
	{
		const BYTE* pStr = NULL;
		int iLen = sphUnpackStr(pStringBase + tRes, &pStr);
		tRes = pStr && iLen ? sphFNV64(pStr, iLen) : 0;
	}

	return tRes;
}


/// match sorter with k-buffering and group-by
template < typename COMPGROUP, bool DISTINCT, bool NOTIFICATIONS >
class CSphKBufferGroupSorter : public CSphMatchQueueTraits, protected CSphGroupSorterSettings
{
protected:
	ESphGroupBy		m_eGroupBy;			///< group-by function
	CSphGrouper* m_pGrouper;

	CSphFixedHash < CSphMatch*, SphGroupKey_t, IdentityHash_fn >	m_hGroup2Match;

protected:
	int				m_iLimit;		///< max matches to be retrieved

	CSphUniqounter	m_tUniq;
	bool			m_bSortByDistinct;

	GroupSorter_fn<COMPGROUP>	m_tGroupSorter;
	const ISphMatchComparator* m_pComp;

	CSphVector<IAggrFunc*>		m_dAggregates;
	CSphVector<IAggrFunc*>		m_dAvgs;
	const ISphFilter* m_pAggrFilter; ///< aggregate filter for matches on flatten
	MatchCloner_t				m_tPregroup;
	const BYTE* m_pStringBase;

	static const int			GROUPBY_FACTOR = 4;	///< allocate this times more storage when doing group-by (k, as in k-buffer)

public:
	/// ctor
	CSphKBufferGroupSorter(const ISphMatchComparator* pComp, const CSphQuery* pQuery, const CSphGroupSorterSettings& tSettings) // FIXME! make k configurable
		: CSphMatchQueueTraits(pQuery->m_iMaxMatches* GROUPBY_FACTOR, true)
		, CSphGroupSorterSettings(tSettings)
		, m_eGroupBy(pQuery->m_eGroupFunc)
		, m_pGrouper(tSettings.m_pGrouper)
		, m_hGroup2Match(pQuery->m_iMaxMatches* GROUPBY_FACTOR)
		, m_iLimit(pQuery->m_iMaxMatches)
		, m_bSortByDistinct(false)
		, m_pComp(pComp)
		, m_pAggrFilter(tSettings.m_pAggrFilterTrait)
		, m_pStringBase(NULL)
	{
		assert(GROUPBY_FACTOR > 1);
		assert(DISTINCT == false || tSettings.m_tDistinctLoc.m_iBitOffset >= 0);

		if_const(NOTIFICATIONS)
			m_dJustPopped.Reserve(m_iSize);
	}

	/// schema setup
	virtual void SetSchema(CSphRsetSchema& tSchema)
	{
		m_tSchema = tSchema;
		m_tPregroup.SetSchema(&m_tSchema);
		m_tPregroup.m_dAttrsRaw.Add(m_tLocGroupby);
		m_tPregroup.m_dAttrsRaw.Add(m_tLocCount);
		if_const(DISTINCT)
		{
			m_tPregroup.m_dAttrsRaw.Add(m_tLocDistinct);
		}
		ExtractAggregates(m_tSchema, m_tLocCount, m_tGroupSorter.m_eKeypart, m_tGroupSorter.m_tLocator, m_dAggregates, m_dAvgs, m_tPregroup);
	}

	/// dtor
	~CSphKBufferGroupSorter()
	{
		SafeDelete(m_pComp);
		SafeDelete(m_pGrouper);
		SafeDelete(m_pAggrFilter);
		ARRAY_FOREACH(i, m_dAggregates)
			SafeDelete(m_dAggregates[i]);
	}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return true;
	}

	virtual bool CanMulti() const
	{
		if (m_pGrouper && !m_pGrouper->CanMulti())
			return false;

		if (HasString(&m_tState))
			return false;

		if (HasString(&m_tGroupSorter))
			return false;

		return true;
	}

	/// set string pool pointer (for string+groupby sorters)
	void SetStringPool(const BYTE* pStrings)
	{
		m_pStringBase = pStrings;
		m_pGrouper->SetStringPool(pStrings);
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tEntry)
	{
		SphGroupKey_t uGroupKey = m_pGrouper->KeyFromMatch(tEntry);
		return PushEx(tEntry, uGroupKey, false, false);
	}

	/// add grouped entry to the queue
	virtual bool PushGrouped(const CSphMatch& tEntry, bool)
	{
		return PushEx(tEntry, tEntry.GetAttr(m_tLocGroupby), true, false);
	}

	/// add entry to the queue
	virtual bool PushEx(const CSphMatch& tEntry, const SphGroupKey_t uGroupKey, bool bGrouped, bool, SphAttr_t* pAttr = NULL)
	{
		if_const(NOTIFICATIONS)
		{
			m_iJustPushed = 0;
			m_dJustPopped.Resize(0);
		}

		// if this group is already hashed, we only need to update the corresponding match
		CSphMatch** ppMatch = m_hGroup2Match(uGroupKey);
		if (ppMatch)
		{
			CSphMatch* pMatch = (*ppMatch);
			assert(pMatch);
			assert(pMatch->GetAttr(m_tLocGroupby) == uGroupKey);
			assert(pMatch->m_pDynamic[-1] == tEntry.m_pDynamic[-1]);

			if (bGrouped)
			{
				// it's already grouped match
				// sum grouped matches count
				pMatch->SetAttr(m_tLocCount, pMatch->GetAttr(m_tLocCount) + tEntry.GetAttr(m_tLocCount)); // OPTIMIZE! AddAttr()?
			}
			else
			{
				// it's a simple match
				// increase grouped matches count
				pMatch->SetAttr(m_tLocCount, 1 + pMatch->GetAttr(m_tLocCount)); // OPTIMIZE! IncAttr()?
			}

			// update aggregates
			ARRAY_FOREACH(i, m_dAggregates)
				m_dAggregates[i]->Update(pMatch, &tEntry, bGrouped);

			// if new entry is more relevant, update from it
			if (m_pComp->VirtualIsLess(*pMatch, tEntry, m_tState))
			{
				if_const(NOTIFICATIONS)
				{
					m_iJustPushed = tEntry.m_uDocID;
					m_dJustPopped.Add(pMatch->m_uDocID);
				}

				// clone the low part of the match
				m_tPregroup.Clone(pMatch, &tEntry);

				// update @groupbystr value, if available
				if (pAttr && m_tLocGroupbyStr.m_bDynamic)
					pMatch->SetAttr(m_tLocGroupbyStr, *pAttr);
			}
		}

		// submit actual distinct value in all cases
		if_const(DISTINCT)
		{
			int iCount = 1;
			if (bGrouped)
				iCount = (int)tEntry.GetAttr(m_tLocDistinct);

			SphAttr_t tAttr = GetDistinctKey(tEntry, m_tDistinctLoc, m_eDistinctAttr, m_pStringBase);
			m_tUniq.Add(SphGroupedValue_t(uGroupKey, tAttr, iCount)); // OPTIMIZE! use simpler locator here?
		}

		// it's a dupe anyway, so we shouldn't update total matches count
		if (ppMatch)
			return false;

		// if we're full, let's cut off some worst groups
		if (m_iUsed == m_iSize)
			CutWorst(m_iLimit * (int)(GROUPBY_FACTOR / 2));

		// do add
		assert(m_iUsed < m_iSize);
		CSphMatch& tNew = m_pData[m_iUsed++];
		m_tSchema.CloneMatch(&tNew, tEntry);

		if_const(NOTIFICATIONS)
			m_iJustPushed = tNew.m_uDocID;

		if (!bGrouped)
		{
			tNew.SetAttr(m_tLocGroupby, uGroupKey);
			tNew.SetAttr(m_tLocCount, 1);
			if_const(DISTINCT)
				tNew.SetAttr(m_tLocDistinct, 0);

			// set @groupbystr value if available
			if (pAttr && m_tLocGroupbyStr.m_bDynamic)
				tNew.SetAttr(m_tLocGroupbyStr, *pAttr);
		}
		else
		{
			ARRAY_FOREACH(i, m_dAggregates)
				m_dAggregates[i]->Ungroup(&tNew);
		}

		m_hGroup2Match.Add(&tNew, uGroupKey);
		m_iTotal++;
		return true;
	}

	void CalcAvg(bool bGroup)
	{
		if (!m_dAvgs.GetLength())
			return;

		CSphMatch* pMatch = m_pData;
		CSphMatch* pEnd = pMatch + m_iUsed;
		while (pMatch < pEnd)
		{
			ARRAY_FOREACH(j, m_dAvgs)
			{
				if (bGroup)
					m_dAvgs[j]->Finalize(pMatch);
				else
					m_dAvgs[j]->Ungroup(pMatch);
			}
			++pMatch;
		}
	}

	/// store all entries into specified location in sorted order, and remove them from queue
	int Flatten(CSphMatch* pTo, int iTag)
	{
		CountDistinct();

		CalcAvg(true);
		SortGroups();

		CSphVector<IAggrFunc*> dAggrs;
		if (m_dAggregates.GetLength() != m_dAvgs.GetLength())
		{
			dAggrs = m_dAggregates;
			ARRAY_FOREACH(i, m_dAvgs)
				dAggrs.RemoveValue(m_dAvgs[i]);
		}

		// FIXME!!! we should provide up-to max_matches to output buffer
		const CSphMatch* pBegin = pTo;
		int iLen = GetLength();
		for (int i = 0; i < iLen; ++i)
		{
			CSphMatch& tMatch = m_pData[i];
			ARRAY_FOREACH(j, dAggrs)
				dAggrs[j]->Finalize(&tMatch);

			// HAVING filtering
			if (m_pAggrFilter && !m_pAggrFilter->Eval(tMatch))
				continue;

			m_tSchema.CloneMatch(pTo, tMatch);
			if (iTag >= 0)
				pTo->m_iTag = iTag;

			pTo++;
		}

		m_iUsed = 0;
		m_iTotal = 0;

		m_hGroup2Match.Reset();
		if_const(DISTINCT)
			m_tUniq.Resize(0);

		return (pTo - pBegin);
	}

	/// get entries count
	int GetLength() const
	{
		return Min(m_iUsed, m_iLimit);
	}

	/// set group comparator state
	void SetGroupState(const CSphMatchComparatorState& tState)
	{
		m_tGroupSorter.m_fnStrCmp = tState.m_fnStrCmp;

		// FIXME! manual bitwise copying.. yuck
		for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
		{
			m_tGroupSorter.m_eKeypart[i] = tState.m_eKeypart[i];
			m_tGroupSorter.m_tLocator[i] = tState.m_tLocator[i];
		}
		m_tGroupSorter.m_uAttrDesc = tState.m_uAttrDesc;
		m_tGroupSorter.m_iNow = tState.m_iNow;

		// check whether we sort by distinct
		if_const(DISTINCT && m_tDistinctLoc.m_iBitOffset >= 0)
			for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
				if (m_tGroupSorter.m_tLocator[i].m_iBitOffset == m_tDistinctLoc.m_iBitOffset)
				{
					m_bSortByDistinct = true;
					break;
				}
	}

protected:
	/// count distinct values if necessary
	void CountDistinct()
	{
		if_const(DISTINCT)
		{
			m_tUniq.Sort();
			SphGroupKey_t uGroup;
			for (int iCount = m_tUniq.CountStart(&uGroup); iCount; iCount = m_tUniq.CountNext(&uGroup))
			{
				CSphMatch** ppMatch = m_hGroup2Match(uGroup);
				if (ppMatch)
					(*ppMatch)->SetAttr(m_tLocDistinct, iCount);
			}
		}
	}

	/// cut worst N groups off the buffer tail
	void CutWorst(int iBound)
	{
		// sort groups
		if (m_bSortByDistinct)
			CountDistinct();

		CalcAvg(true);
		SortGroups();
		CalcAvg(false);

		if_const(NOTIFICATIONS)
		{
			for (int i = iBound; i < m_iUsed; ++i)
				m_dJustPopped.Add(m_pData[i].m_uDocID);
		}

		// cleanup unused distinct stuff
		if_const(DISTINCT)
		{
			// build kill-list
			CSphVector<SphGroupKey_t> dRemove;
			dRemove.Resize(m_iUsed - iBound);
			ARRAY_FOREACH(i, dRemove)
				dRemove[i] = m_pData[iBound + i].GetAttr(m_tLocGroupby);

			// sort and compact
			if (!m_bSortByDistinct)
				m_tUniq.Sort();
			m_tUniq.Compact(&dRemove[0], m_iUsed - iBound);
		}

		// rehash
		m_hGroup2Match.Reset();
		for (int i = 0; i < iBound; i++)
			m_hGroup2Match.Add(m_pData + i, m_pData[i].GetAttr(m_tLocGroupby));

		// cut groups
		m_iUsed = iBound;
	}

	/// sort groups buffer
	void SortGroups()
	{
		sphSort(m_pData, m_iUsed, m_tGroupSorter, m_tGroupSorter);
	}

	virtual void Finalize(ISphMatchProcessor& tProcessor, bool)
	{
		if (!GetLength())
			return;

		if (m_iUsed > m_iLimit)
			CutWorst(m_iLimit);

		// just evaluate in heap order
		CSphMatch* pCur = m_pData;
		const CSphMatch* pEnd = m_pData + m_iUsed;
		while (pCur < pEnd)
			tProcessor.Process(pCur++);
	}
};


/// match sorter with k-buffering and N-best group-by
template < typename COMPGROUP, bool DISTINCT, bool NOTIFICATIONS >
class CSphKBufferNGroupSorter : public CSphMatchQueueTraits, protected CSphGroupSorterSettings
{
protected:
	ESphGroupBy		m_eGroupBy;			///< group-by function
	CSphGrouper* m_pGrouper;

	CSphFixedHash < CSphMatch*, SphGroupKey_t, IdentityHash_fn >	m_hGroup2Match;

protected:
	int				m_iLimit;		///< max matches to be retrieved
	int				m_iGLimit;		///< limit per one group
	CSphFixedVector<int>	m_dGroupByList;	///< chains of equal matches from groups
	CSphFixedVector<int>	m_dGroupsLen;	///< lengths of chains of equal matches from groups
	int				m_iFreeHeads;		///< insertion point for head matches.
	CSphFreeList	m_dFreeTails;		///< where to put tails of the subgroups
	SphGroupKey_t	m_uLastGroupKey;	///< helps to determine in pushEx whether the new subgroup started
#ifndef NDEBUG
	int				m_iruns;		///< helpers for conditional breakpoints on debug
	int				m_ipushed;
#endif
	CSphUniqounter	m_tUniq;
	bool			m_bSortByDistinct;

	GroupSorter_fn<COMPGROUP>	m_tGroupSorter;
	const ISphMatchComparator* m_pComp;

	CSphVector<IAggrFunc*>		m_dAggregates;
	CSphVector<IAggrFunc*>		m_dAvgs;
	const ISphFilter* m_pAggrFilter; ///< aggregate filter for matches on flatten
	MatchCloner_t				m_tPregroup;
	const BYTE* m_pStringBase;

	static const int			GROUPBY_FACTOR = 4;	///< allocate this times more storage when doing group-by (k, as in k-buffer)

protected:
	inline int GetFreePos(bool bTailPos)
	{
		// if we're full, let's cut off some worst groups
		if (m_iUsed == m_iSize)
		{
			CutWorst(m_iLimit * (int)(GROUPBY_FACTOR / 2));
			// don't return true value for tail this case,
			// since the context might be changed by the CutWorst!
			if (bTailPos)
				return -1;
		}

		// do add
		assert(m_iUsed < m_iSize);
		++m_iUsed;
		if (bTailPos)
		{
			int iRes = m_dFreeTails.Get() + m_iSize;
			assert(iRes < CSphMatchQueueTraits::GetDataLength());
			return iRes;
		}
		else
		{
			assert(m_iFreeHeads < m_iSize);
			return m_iFreeHeads++;
		}
	}

public:
	/// ctor
	CSphKBufferNGroupSorter(const ISphMatchComparator* pComp, const CSphQuery* pQuery, const CSphGroupSorterSettings& tSettings) // FIXME! make k configurable
		: CSphMatchQueueTraits((pQuery->m_iGroupbyLimit > 1 ? 2 : 1)* pQuery->m_iMaxMatches* GROUPBY_FACTOR, true)
		, CSphGroupSorterSettings(tSettings)
		, m_eGroupBy(pQuery->m_eGroupFunc)
		, m_pGrouper(tSettings.m_pGrouper)
		, m_hGroup2Match(pQuery->m_iMaxMatches* GROUPBY_FACTOR * 2)
		, m_iLimit(pQuery->m_iMaxMatches)
		, m_iGLimit(pQuery->m_iGroupbyLimit)
		, m_dGroupByList(0)
		, m_dGroupsLen(0)
		, m_iFreeHeads(0)
		, m_uLastGroupKey(-1)
		, m_bSortByDistinct(false)
		, m_pComp(pComp)
		, m_pAggrFilter(tSettings.m_pAggrFilterTrait)
		, m_pStringBase(NULL)
	{
		assert(GROUPBY_FACTOR > 1);
		assert(DISTINCT == false || tSettings.m_tDistinctLoc.m_iBitOffset >= 0);
		assert(m_iGLimit > 1);

		if_const(NOTIFICATIONS)
			m_dJustPopped.Reserve(m_iSize);

		// trick! This case we allocated 2*m_iSize mem.
		// range 0..m_iSize used for 1-st matches of each subgroup (i.e., for heads)
		// range m_iSize+1..2*m_iSize used for the tails.
		m_dGroupByList.Reset(m_iSize);
		m_dGroupsLen.Reset(m_iSize);
		m_iSize >>= 1;
		ARRAY_FOREACH(i, m_dGroupByList)
		{
			m_dGroupByList[i] = -1;
			m_dGroupsLen[i] = 0;
		}
		m_dFreeTails.Reset(m_iSize);

#ifndef NDEBUG
		m_iruns = 0;
		m_ipushed = 0;
#endif
	}

	// only for n-group
	virtual int GetDataLength() const
	{
		return CSphMatchQueueTraits::GetDataLength() / 2;
	}

	/// schema setup
	virtual void SetSchema(CSphRsetSchema& tSchema)
	{
		m_tSchema = tSchema;

		m_tPregroup.SetSchema(&m_tSchema);
		m_tPregroup.m_dAttrsRaw.Add(m_tLocGroupby);
		m_tPregroup.m_dAttrsRaw.Add(m_tLocCount);
		if_const(DISTINCT)
		{
			m_tPregroup.m_dAttrsRaw.Add(m_tLocDistinct);
		}
		ExtractAggregates(m_tSchema, m_tLocCount, m_tGroupSorter.m_eKeypart, m_tGroupSorter.m_tLocator, m_dAggregates, m_dAvgs, m_tPregroup);
	}

	/// dtor
	virtual ~CSphKBufferNGroupSorter()
	{
		SafeDelete(m_pComp);
		SafeDelete(m_pGrouper);
		SafeDelete(m_pAggrFilter);
		ARRAY_FOREACH(i, m_dAggregates)
			SafeDelete(m_dAggregates[i]);
	}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return true;
	}

	virtual bool CanMulti() const
	{
		if (m_pGrouper && !m_pGrouper->CanMulti())
			return false;

		if (HasString(&m_tState))
			return false;

		if (HasString(&m_tGroupSorter))
			return false;

		return true;
	}

	/// set string pool pointer (for string+groupby sorters)
	void SetStringPool(const BYTE* pStrings)
	{
		m_pStringBase = pStrings;
		m_pGrouper->SetStringPool(pStrings);
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tEntry)
	{
		SphGroupKey_t uGroupKey = m_pGrouper->KeyFromMatch(tEntry);
		return PushEx(tEntry, uGroupKey, false, false);
	}

	/// add grouped entry to the queue
	virtual bool PushGrouped(const CSphMatch& tEntry, bool bNewSet)
	{
		return PushEx(tEntry, tEntry.GetAttr(m_tLocGroupby), true, bNewSet);
	}

	void FreeMatchChain(int iPos)
	{
		while (iPos >= 0)
		{
			if_const(NOTIFICATIONS)
				m_dJustPopped.Add(m_pData[iPos].m_uDocID);

			int iLastPos = iPos;
			m_tSchema.FreeStringPtrs(m_pData + iPos);
			iPos = m_dGroupByList[iPos];
			m_dGroupByList[iLastPos] = -1;
			if (iLastPos >= m_iSize)
				m_dFreeTails.Free(iLastPos - m_iSize);
		}
	}

	// insert a match into subgroup, or discard it
	// returns 0 if no place now (need to flush)
	// returns 1 if value was discarded or replaced other existing value
	// returns 2 if value was added.
	enum InsertRes_e
	{
		INSERT_NONE,
		INSERT_REPLACED,
		INSERT_ADDED
	};

	InsertRes_e InsertMatch(int iPos, const CSphMatch& tEntry)
	{
		const int iHead = iPos;
		int iPrev = -1;
		const bool bDoAdd = (m_dGroupsLen[iHead] < m_iGLimit);
		while (iPos >= 0)
		{
			CSphMatch* pMatch = m_pData + iPos;
			if (m_pComp->VirtualIsLess(*pMatch, tEntry, m_tState)) // the tEntry is better than current *pMatch
			{
				int iNew = iPos;
				if (bDoAdd)
				{
					iNew = GetFreePos(true); // add to the tails (2-nd subrange)
					if (iNew < 0)
						return INSERT_NONE;
				}
				else
				{
					int iPreLast = iPrev;
					while (m_dGroupByList[iNew] >= 0)
					{
						iPreLast = iNew;
						iNew = m_dGroupByList[iNew];
					}

					m_tSchema.FreeStringPtrs(m_pData + iNew);
					m_dGroupByList[iPreLast] = -1;

					if (iPos == iNew) // avoid cycle link to itself
						iPos = -1;
				}

				CSphMatch& tNew = m_pData[iNew];

				if_const(NOTIFICATIONS)
				{
					m_iJustPushed = tEntry.m_uDocID;
					if (tNew.m_uDocID != 0)
						m_dJustPopped.Add(tNew.m_uDocID);
				}
				if (bDoAdd)
					++m_dGroupsLen[iHead];

				if (iPos == iHead) // this is the first elem, need to copy groupby staff from it
				{
					// trick point! The first elem MUST live in the low half of the pool.
					// So, replacing the first is actually moving existing one to the last half,
					// then overwriting the one in the low half with the new value and link them
					m_tPregroup.Clone(&tNew, pMatch);
					m_tPregroup.Clone(pMatch, &tEntry);
					m_dGroupByList[iNew] = m_dGroupByList[iPos];
					m_dGroupByList[iPos] = iNew;
				}
				else // this is elem somewhere in the chain, just shift it.
				{
					m_tPregroup.Clone(&tNew, &tEntry);
					m_dGroupByList[iPrev] = iNew;
					m_dGroupByList[iNew] = iPos;
				}
				break;
			}
			iPrev = iPos;
			iPos = m_dGroupByList[iPos];
		}

		if (iPos < 0 && bDoAdd) // this item is less than everything, but still appropriate
		{
			int iNew = GetFreePos(true); // add to the tails (2-nd subrange)
			if (iNew < 0)
				return INSERT_NONE;

			CSphMatch& tNew = m_pData[iNew];
			m_tPregroup.Clone(&tNew, &tEntry);
			m_dGroupByList[iPrev] = iNew;
			m_dGroupByList[iNew] = iPos;

			if_const(NOTIFICATIONS)
				m_iJustPushed = tEntry.m_uDocID;
			if (bDoAdd)
				++m_dGroupsLen[iHead];
		}

		return bDoAdd ? INSERT_ADDED : INSERT_REPLACED;
	}

#ifndef NDEBUG
	void CheckIntegrity()
	{
		int iTotalLen = 0;
		for (int i = 0; i < m_iFreeHeads; ++i)
		{
			int iLen = 0;
			int iCur = i;
			while (iCur >= 0)
			{
				int iNext = m_dGroupByList[iCur];
				assert(iNext == -1 || m_pData[iCur].GetAttr(m_tLocGroupby) == 0 || m_pData[iNext].GetAttr(m_tLocGroupby) == 0
					|| m_pData[iCur].GetAttr(m_tLocGroupby) == m_pData[iNext].GetAttr(m_tLocGroupby));
				++iLen;
				iCur = iNext;
			}
			assert(m_dGroupsLen[i] == iLen);
			iTotalLen += iLen;
		}
		assert(iTotalLen == m_iUsed);
	}
#define CHECKINTEGRITY() CheckIntegrity()
#else
#define CHECKINTEGRITY()
#endif

	/// add entry to the queue
	virtual bool PushEx(const CSphMatch& tEntry, const SphGroupKey_t uGroupKey, bool bGrouped, bool bNewSet)
	{
#ifndef NDEBUG
		++m_ipushed;
#endif
		CHECKINTEGRITY();
		if_const(NOTIFICATIONS)
		{
			m_iJustPushed = 0;
			m_dJustPopped.Resize(0);
		}

		// if this group is already hashed, we only need to update the corresponding match
		CSphMatch** ppMatch = m_hGroup2Match(uGroupKey);
		if (ppMatch)
		{
			CSphMatch* pMatch = (*ppMatch);
			assert(pMatch);
			assert(pMatch->GetAttr(m_tLocGroupby) == uGroupKey);
			assert(pMatch->m_pDynamic[-1] == tEntry.m_pDynamic[-1]);

			if (bGrouped)
			{
				// it's already grouped match
				// sum grouped matches count
				if (bNewSet || uGroupKey != m_uLastGroupKey)
				{
					pMatch->SetAttr(m_tLocCount, pMatch->GetAttr(m_tLocCount) + tEntry.GetAttr(m_tLocCount)); // OPTIMIZE! AddAttr()?
					m_uLastGroupKey = uGroupKey;
					bNewSet = true;
				}
			}
			else
			{
				// it's a simple match
				// increase grouped matches count
				pMatch->SetAttr(m_tLocCount, 1 + pMatch->GetAttr(m_tLocCount)); // OPTIMIZE! IncAttr()?
			}

			bNewSet |= !bGrouped;

			// update aggregates
			if (bNewSet)
				ARRAY_FOREACH(i, m_dAggregates)
				m_dAggregates[i]->Update(pMatch, &tEntry, bGrouped);

			// if new entry is more relevant, update from it
			InsertRes_e eRes = InsertMatch(pMatch - m_pData, tEntry);
			if (eRes == INSERT_NONE)
			{
				// need to keep all poped values
				CSphTightVector<SphDocID_t> dJustPopped;
				dJustPopped.SwapData(m_dJustPopped);

				// was no insertion because cache cleaning. Recall myself
				PushEx(tEntry, uGroupKey, bGrouped, bNewSet);

				// post Push work
				ARRAY_FOREACH(i, dJustPopped)
					m_dJustPopped.Add(dJustPopped[i]);
				CSphMatch** ppDec = m_hGroup2Match(uGroupKey);
				assert(ppDec);
				(*ppDec)->SetAttr(m_tLocCount, (*ppDec)->GetAttr(m_tLocCount) - 1);

			}
			else if (eRes == INSERT_ADDED)
			{
				if (bGrouped)
					return true;
				++m_iTotal;
			}
		}

		// submit actual distinct value in all cases
		if_const(DISTINCT)
		{
			int iCount = 1;
			if (bGrouped)
				iCount = (int)tEntry.GetAttr(m_tLocDistinct);

			SphAttr_t tAttr = GetDistinctKey(tEntry, m_tDistinctLoc, m_eDistinctAttr, m_pStringBase);
			m_tUniq.Add(SphGroupedValue_t(uGroupKey, tAttr, iCount)); // OPTIMIZE! use simpler locator here?
		}
		CHECKINTEGRITY();
		// it's a dupe anyway, so we shouldn't update total matches count
		if (ppMatch)
			return false;

		// do add
		int iNew = GetFreePos(false);
		assert(iNew >= 0 && iNew < m_iSize);

		CSphMatch& tNew = m_pData[iNew];
		m_tSchema.CloneMatch(&tNew, tEntry);

		m_dGroupByList[iNew] = -1;
		m_dGroupsLen[iNew] = 1;

		if_const(NOTIFICATIONS)
			m_iJustPushed = tNew.m_uDocID;

		if (!bGrouped)
		{
			tNew.SetAttr(m_tLocGroupby, uGroupKey);
			tNew.SetAttr(m_tLocCount, 1);
			if_const(DISTINCT)
				tNew.SetAttr(m_tLocDistinct, 0);
		}
		else
		{
			m_uLastGroupKey = uGroupKey;
			ARRAY_FOREACH(i, m_dAggregates)
				m_dAggregates[i]->Ungroup(&tNew);
		}

		m_hGroup2Match.Add(&tNew, uGroupKey);
		m_iTotal++;
		CHECKINTEGRITY();
		return true;
	}

	void CalcAvg(bool bGroup)
	{
		if (!m_dAvgs.GetLength())
			return;

		int iHeadMatch;
		int iMatch = iHeadMatch = 0;
		for (int i = 0; i < m_iUsed; ++i)
		{
			CSphMatch* pMatch = m_pData + iMatch;
			ARRAY_FOREACH(j, m_dAvgs)
			{
				if (bGroup)
					m_dAvgs[j]->Finalize(pMatch);
				else
					m_dAvgs[j]->Ungroup(pMatch);
			}
			iMatch = m_dGroupByList[iMatch];
			if (iMatch < 0)
				iMatch = ++iHeadMatch;
		}
	}

	// rebuild m_hGroup2Match to point to the subgroups (2-nd elem and further)
	// returns true if any such subgroup found
	void Hash2nd()
	{
		// let the hash points to the chains from 2-nd elem
		m_hGroup2Match.Reset();
		int iHeads = m_iUsed;
		for (int i = 0; i < iHeads; i++)
		{
			if (m_dGroupsLen[i] > 1)
			{
				int iCount = m_dGroupsLen[i];
				int iTail = m_dGroupByList[i];

				assert(iTail >= 0);
				assert(m_pData[iTail].GetAttr(m_tLocGroupby) == 0 ||
					m_pData[i].GetAttr(m_tLocGroupby) == m_pData[iTail].GetAttr(m_tLocGroupby));

				m_hGroup2Match.Add(m_pData + iTail, m_pData[i].GetAttr(m_tLocGroupby));
				iHeads -= iCount - 1;
				m_dGroupsLen[iTail] = iCount;
			}
		}
	}


	/// store all entries into specified location in sorted order, and remove them from queue
	int Flatten(CSphMatch* pTo, int iTag)
	{
		CountDistinct();
		CalcAvg(true);

		Hash2nd();

		SortGroups();
		CSphVector<IAggrFunc*> dAggrs;
		if (m_dAggregates.GetLength() != m_dAvgs.GetLength())
		{
			dAggrs = m_dAggregates;
			ARRAY_FOREACH(i, m_dAvgs)
				dAggrs.RemoveValue(m_dAvgs[i]);
		}

		const CSphMatch* pBegin = pTo;
		int iTotal = GetLength();
		int iTopGroupMatch = 0;
		int iEntry = 0;

		while (iEntry < iTotal)
		{
			CSphMatch* pMatch = m_pData + iTopGroupMatch;
			ARRAY_FOREACH(j, dAggrs)
				dAggrs[j]->Finalize(pMatch);

			bool bTopPassed = (!m_pAggrFilter || m_pAggrFilter->Eval(*pMatch));

			// copy top group match
			if (bTopPassed)
			{
				m_tSchema.CloneMatch(pTo, *pMatch);
				if (iTag >= 0)
					pTo->m_iTag = iTag;
				pTo++;
			}
			iEntry++;
			iTopGroupMatch++;

			// now look for the next match.
			// In this specific case (2-nd, just after the head)
			// we have to look it in the hash, not in the linked list!
			CSphMatch** ppMatch = m_hGroup2Match(pMatch->GetAttr(m_tLocGroupby));
			int iNext = (ppMatch ? *ppMatch - m_pData : -1);
			while (iNext >= 0)
			{
				// copy rest group matches
				if (bTopPassed)
				{
					m_tPregroup.Combine(pTo, m_pData + iNext, pMatch);
					if (iTag >= 0)
						pTo->m_iTag = iTag;
					pTo++;
				}
				iEntry++;
				iNext = m_dGroupByList[iNext];
			}
		}

		m_iFreeHeads = m_iUsed = 0;
		m_iTotal = 0;
		ARRAY_FOREACH(i, m_dGroupByList)
		{
			m_dGroupByList[i] = -1;
			m_dGroupsLen[i] = 0;
		}

		m_hGroup2Match.Reset();
		if_const(DISTINCT)
			m_tUniq.Resize(0);

		return (pTo - pBegin);
	}

	/// get entries count
	int GetLength() const
	{
		return Min(m_iUsed, m_iLimit);
	}

	/// set group comparator state
	void SetGroupState(const CSphMatchComparatorState& tState)
	{
		m_tGroupSorter.m_fnStrCmp = tState.m_fnStrCmp;

		// FIXME! manual bitwise copying.. yuck
		for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
		{
			m_tGroupSorter.m_eKeypart[i] = tState.m_eKeypart[i];
			m_tGroupSorter.m_tLocator[i] = tState.m_tLocator[i];
		}
		m_tGroupSorter.m_uAttrDesc = tState.m_uAttrDesc;
		m_tGroupSorter.m_iNow = tState.m_iNow;

		// check whether we sort by distinct
		if_const(DISTINCT && m_tDistinctLoc.m_iBitOffset >= 0)
			for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
				if (m_tGroupSorter.m_tLocator[i].m_iBitOffset == m_tDistinctLoc.m_iBitOffset)
				{
					m_bSortByDistinct = true;
					break;
				}
	}

protected:
	/// count distinct values if necessary
	void CountDistinct()
	{
		if_const(DISTINCT)
		{
			m_tUniq.Sort();
			SphGroupKey_t uGroup;
			for (int iCount = m_tUniq.CountStart(&uGroup); iCount; iCount = m_tUniq.CountNext(&uGroup))
			{
				CSphMatch** ppMatch = m_hGroup2Match(uGroup);
				if (ppMatch)
					(*ppMatch)->SetAttr(m_tLocDistinct, iCount);
			}
		}
	}

	/// cut worst N groups off the buffer tail
	void CutWorst(int iBound)
	{
#ifndef NDEBUG
		++m_iruns;
#endif
		CHECKINTEGRITY();

		// sort groups
		if (m_bSortByDistinct)
			CountDistinct();

		Hash2nd();

		CalcAvg(true);
		SortGroups();
		CalcAvg(false);

		int iHead = 0;
		for (int iLimit = 0; iLimit < iBound; iHead++)
		{
			const CSphMatch* pMatch = m_pData + iHead;
			CSphMatch** ppTailMatch = m_hGroup2Match(pMatch->GetAttr(m_tLocGroupby));
			int iCount = 1;
			int iTail = -1;
			if (ppTailMatch)
			{
				assert((*ppTailMatch)->GetAttr(m_tLocGroupby) == 0 ||
					pMatch->GetAttr(m_tLocGroupby) == (*ppTailMatch)->GetAttr(m_tLocGroupby));
				iTail = (*ppTailMatch) - m_pData;
				iCount = m_dGroupsLen[iTail];
				assert(iCount > 0);
			}

			// whole chain fits limit
			if ((iLimit + iCount) <= iBound)
			{
				m_dGroupByList[iHead] = iTail;
				m_dGroupsLen[iHead] = iCount;
				iLimit += iCount;
				continue;
			}

			// only head match fits limit but not tail match(es)
			if ((iLimit + 1) == iBound)
			{
				m_dGroupByList[iHead] = -1;
				m_dGroupsLen[iHead] = 1;

				FreeMatchChain(iTail);
				iHead++;
				break;
			}

			// part of tail fits limit - it our last chain
			// fix-up chain to fits limit
			assert(iBound - iLimit <= iCount);
			iCount = iBound - iLimit;
			m_dGroupByList[iHead] = iTail;
			m_dGroupsLen[iHead] = iCount;

			iCount--;
			int iPrev = iTail;
			while (iCount > 0)
			{
				iPrev = iTail;
				iTail = m_dGroupByList[iTail];
				iCount--;
				assert(!iCount || iTail >= 0);
			}
			m_dGroupByList[iPrev] = -1;

			iHead++;
			FreeMatchChain(iTail);
			break;
		}

		if_const(DISTINCT)
		{
			int iCount = m_iUsed - iHead;
			CSphFixedVector<SphGroupKey_t> dRemove(iCount);
			for (int i = 0; i < iCount; i++)
				dRemove[i] = m_pData[i + iHead].GetAttr(m_tLocGroupby);

			if (!m_bSortByDistinct)
				m_tUniq.Sort();

			m_tUniq.Compact(&dRemove[0], iCount);
		}

		// cleanup chains after limit
		for (int i = iHead; i < m_iFreeHeads; ++i)
		{
			CSphMatch* pMatch = m_pData + i;
			CSphMatch** ppTailMatch = m_hGroup2Match(pMatch->GetAttr(m_tLocGroupby));
			if (ppTailMatch)
			{
				assert((*ppTailMatch)->GetAttr(m_tLocGroupby) == 0 || pMatch->GetAttr(m_tLocGroupby) == (*ppTailMatch)->GetAttr(m_tLocGroupby));
				int iTail = (*ppTailMatch) - m_pData;
				assert(m_dGroupsLen[iTail] > 0);
				FreeMatchChain(iTail);
			}

			if_const(NOTIFICATIONS)
				m_dJustPopped.Add(pMatch->m_uDocID);
			m_tSchema.FreeStringPtrs(pMatch);
			m_dGroupByList[i] = -1;
			m_dGroupsLen[i] = 0;
		}

		// cleanup chain lengths after hash2nd
		for (int i = m_iSize; i < m_dGroupsLen.GetLength(); i++)
			m_dGroupsLen[i] = 0;

		// rehash
		m_hGroup2Match.Reset();
		for (int i = 0; i < iHead; i++)
			m_hGroup2Match.Add(m_pData + i, m_pData[i].GetAttr(m_tLocGroupby));

		// cut groups
		m_iUsed = iBound;
		m_iFreeHeads = iHead;
		CHECKINTEGRITY();
	}

	/// sort groups buffer
	void SortGroups()
	{
		sphSort(m_pData, m_iFreeHeads, m_tGroupSorter, m_tGroupSorter);
	}

	virtual void Finalize(ISphMatchProcessor& tProcessor, bool)
	{
		if (!GetLength())
			return;

		if (m_iUsed > m_iLimit)
			CutWorst(m_iLimit);

		for (int iMatch = 0; iMatch < m_iFreeHeads; iMatch++)
		{
			// this is head match
			CSphMatch* pMatch = m_pData + iMatch;
			tProcessor.Process(pMatch);

			int iNext = m_dGroupByList[iMatch];
			int iCount = m_dGroupsLen[iMatch] - 1;
			while (iCount > 0)
			{
				tProcessor.Process(m_pData + iNext);
				iNext = m_dGroupByList[iNext];
				iCount--;
			}
		}
	}
};


/// match sorter with k-buffering and group-by for MVAs
template < typename COMPGROUP, bool DISTINCT, bool NOTIFICATIONS >
class CSphKBufferMVAGroupSorter : public CSphKBufferGroupSorter < COMPGROUP, DISTINCT, NOTIFICATIONS >
{
protected:
	const DWORD* m_pMva;		///< pointer to MVA pool for incoming matches
	bool				m_bArenaProhibit;
	CSphAttrLocator		m_tMvaLocator;
	bool				m_bMva64;

public:
	/// ctor
	CSphKBufferMVAGroupSorter(const ISphMatchComparator* pComp, const CSphQuery* pQuery, const CSphGroupSorterSettings& tSettings)
		: CSphKBufferGroupSorter < COMPGROUP, DISTINCT, NOTIFICATIONS >(pComp, pQuery, tSettings)
		, m_pMva(NULL)
		, m_bArenaProhibit(false)
		, m_bMva64(tSettings.m_bMva64)
	{
		this->m_pGrouper->GetLocator(m_tMvaLocator);
	}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return true;
	}

	/// set MVA pool for subsequent matches
	void SetMVAPool(const DWORD* pMva, bool bArenaProhibit)
	{
		m_pMva = pMva;
		m_bArenaProhibit = bArenaProhibit;
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tEntry)
	{
		assert(m_pMva);
		if (!m_pMva)
			return false;

		// get that list
		// FIXME! OPTIMIZE! use simpler locator than full bits/count here
		// FIXME! hardcoded MVA type, so here's MVA_DOWNSIZE marker for searching
		const DWORD* pValues = tEntry.GetAttrMVA(this->m_tMvaLocator, m_pMva, m_bArenaProhibit); // (this pointer is for gcc; it doesn't work otherwise)
		if (!pValues)
			return false;

		DWORD iValues = *pValues++;

		bool bRes = false;
		if (m_bMva64)
		{
			assert((iValues % 2) == 0);
			for (; iValues > 0; iValues -= 2, pValues += 2)
			{
				int64_t iMva = MVA_UPSIZE(pValues);
				SphGroupKey_t uGroupkey = this->m_pGrouper->KeyFromValue(iMva);
				bRes |= this->PushEx(tEntry, uGroupkey, false, false);
			}

		}
		else
		{
			while (iValues--)
			{
				SphGroupKey_t uGroupkey = this->m_pGrouper->KeyFromValue(*pValues++);
				bRes |= this->PushEx(tEntry, uGroupkey, false, false);
			}
		}
		return bRes;
	}

	/// add pre-grouped entry to the queue
	virtual bool PushGrouped(const CSphMatch& tEntry, bool bNewSet)
	{
		// re-group it based on the group key
		// (first 'this' is for icc; second 'this' is for gcc)
		return this->PushEx(tEntry, tEntry.GetAttr(this->m_tLocGroupby), true, bNewSet);
	}
};


/// match sorter with k-buffering and group-by for JSON arrays
template < typename COMPGROUP, bool DISTINCT, bool NOTIFICATIONS >
class CSphKBufferJsonGroupSorter : public CSphKBufferGroupSorter < COMPGROUP, DISTINCT, NOTIFICATIONS >
{
public:
	/// ctor
	CSphKBufferJsonGroupSorter(const ISphMatchComparator* pComp, const CSphQuery* pQuery, const CSphGroupSorterSettings& tSettings)
		: CSphKBufferGroupSorter < COMPGROUP, DISTINCT, NOTIFICATIONS >(pComp, pQuery, tSettings)
	{}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return true;
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tMatch)
	{
		bool bRes = false;

		int iLen;
		char sBuf[32];

		SphGroupKey_t uGroupkey = this->m_pGrouper->KeyFromMatch(tMatch);

		int64_t iValue = (int64_t)uGroupkey;
		const BYTE* pStrings = ((CSphGrouperJsonField*)this->m_pGrouper)->m_pStrings;
		const BYTE* pValue = pStrings + (iValue & 0xffffffff);
		ESphJsonType eRes = (ESphJsonType)(iValue >> 32);

		switch (eRes)
		{
		case JSON_ROOT:
		{
			iLen = sphJsonNodeSize(JSON_ROOT, pValue);
			bool bEmpty = iLen == 5; // mask and JSON_EOF
			uGroupkey = bEmpty ? 0 : sphFNV64(pValue, iLen);
			return this->PushEx(tMatch, uGroupkey, false, false, bEmpty ? NULL : &iValue);
		}
		case JSON_STRING:
		case JSON_OBJECT:
		case JSON_MIXED_VECTOR:
			iLen = sphJsonUnpackInt(&pValue);
			uGroupkey = (iLen == 1 && eRes != JSON_STRING) ? 0 : sphFNV64(pValue, iLen);
			return this->PushEx(tMatch, uGroupkey, false, false, (iLen == 1 && eRes != JSON_STRING) ? 0 : &iValue);
		case JSON_STRING_VECTOR:
		{
			sphJsonUnpackInt(&pValue);
			iLen = sphJsonUnpackInt(&pValue);
			for (int i = 0; i < iLen; i++)
			{
				DWORD uOff = pValue - pStrings;
				int64_t iValue = (((int64_t)uOff) | (((int64_t)JSON_STRING) << 32));
				int iStrLen = sphJsonUnpackInt(&pValue);
				uGroupkey = sphFNV64(pValue, iStrLen);
				bRes |= this->PushEx(tMatch, uGroupkey, false, false, &iValue);
				pValue += iStrLen;
			}
			return bRes;
		}
		case JSON_INT32:
			uGroupkey = sphFNV64((BYTE*)FormatInt(sBuf, (int)sphGetDword(pValue)));
			break;
		case JSON_INT64:
			uGroupkey = sphFNV64((BYTE*)FormatInt(sBuf, (int)sphJsonLoadBigint(&pValue)));
			break;
		case JSON_DOUBLE:
			snprintf(sBuf, sizeof(sBuf), "%f", sphQW2D(sphJsonLoadBigint(&pValue)));
			uGroupkey = sphFNV64((const BYTE*)sBuf);
			break;
		case JSON_INT32_VECTOR:
		{
			iLen = sphJsonUnpackInt(&pValue);
			int* p = (int*)pValue;
			DWORD uOff = pValue - pStrings;
			for (int i = 0; i < iLen; i++)
			{
				int64_t iPacked = (((int64_t)uOff) | (((int64_t)JSON_INT32) << 32));
				uGroupkey = *p++;
				bRes |= this->PushEx(tMatch, uGroupkey, false, false, &iPacked);
				uOff += 4;
			}
			return bRes;
		}
		break;
		case JSON_INT64_VECTOR:
		case JSON_DOUBLE_VECTOR:
		{
			iLen = sphJsonUnpackInt(&pValue);
			int64_t* p = (int64_t*)pValue;
			DWORD uOff = pValue - pStrings;
			ESphJsonType eType = eRes == JSON_INT64_VECTOR ? JSON_INT64 : JSON_DOUBLE;
			for (int i = 0; i < iLen; i++)
			{
				int64_t iPacked = (((int64_t)uOff) | (((int64_t)eType) << 32));
				uGroupkey = *p++;
				bRes |= this->PushEx(tMatch, uGroupkey, false, false, &iPacked);
				uOff += 8;
			}
			return bRes;
		}
		break;
		default:
			uGroupkey = 0;
			iValue = 0;
			break;
		}

		bRes |= this->PushEx(tMatch, uGroupkey, false, false, &iValue);
		return bRes;
	}

	/// add pre-grouped entry to the queue
	virtual bool PushGrouped(const CSphMatch& tEntry, bool bNewSet)
	{
		// re-group it based on the group key
		// (first 'this' is for icc; second 'this' is for gcc)
		return this->PushEx(tEntry, tEntry.GetAttr(this->m_tLocGroupby), true, bNewSet);
	}
};


/// implicit group-by sorter
template < typename COMPGROUP, bool DISTINCT, bool NOTIFICATIONS >
class CSphImplicitGroupSorter : public ISphMatchSorter, ISphNoncopyable, protected CSphGroupSorterSettings
{
protected:
	CSphMatch		m_tData;
	bool			m_bDataInitialized;

	CSphVector<SphUngroupedValue_t>	m_dUniq;

	CSphVector<IAggrFunc*>		m_dAggregates;
	const ISphFilter* m_pAggrFilter;				///< aggregate filter for matches on flatten
	MatchCloner_t				m_tPregroup;
	const BYTE* m_pStringBase;

public:
	/// ctor
	CSphImplicitGroupSorter(const ISphMatchComparator* DEBUGARG(pComp), const CSphQuery*, const CSphGroupSorterSettings& tSettings)
		: CSphGroupSorterSettings(tSettings)
		, m_bDataInitialized(false)
		, m_pAggrFilter(tSettings.m_pAggrFilterTrait)
		, m_pStringBase(NULL)
	{
		assert(DISTINCT == false || tSettings.m_tDistinctLoc.m_iBitOffset >= 0);
		assert(!pComp);

		if_const(NOTIFICATIONS)
			m_dJustPopped.Reserve(1);

		m_dUniq.Reserve(16384);
		m_iMatchCapacity = 1;
	}

	/// dtor
	~CSphImplicitGroupSorter()
	{
		SafeDelete(m_pAggrFilter);
		ARRAY_FOREACH(i, m_dAggregates)
			SafeDelete(m_dAggregates[i]);
	}

	/// schema setup
	virtual void SetSchema(CSphRsetSchema& tSchema)
	{
		m_tSchema = tSchema;
		m_tPregroup.SetSchema(&m_tSchema);
		m_tPregroup.m_dAttrsRaw.Add(m_tLocGroupby);
		m_tPregroup.m_dAttrsRaw.Add(m_tLocCount);
		if_const(DISTINCT)
		{
			m_tPregroup.m_dAttrsRaw.Add(m_tLocDistinct);
		}

		CSphVector<IAggrFunc*> dTmp;
		ESphSortKeyPart dTmpKeypart[CSphMatchComparatorState::MAX_ATTRS];
		CSphAttrLocator dTmpLocator[CSphMatchComparatorState::MAX_ATTRS];
		ExtractAggregates(m_tSchema, m_tLocCount, dTmpKeypart, dTmpLocator, m_dAggregates, dTmp, m_tPregroup);
		assert(!dTmp.GetLength());
	}

	int GetDataLength() const
	{
		return 1;
	}

	bool UsesAttrs() const
	{
		return true;
	}

	/// check if this sorter does groupby
	virtual bool IsGroupby() const
	{
		return true;
	}

	virtual bool CanMulti() const
	{
		return true;
	}

	/// set string pool pointer (for string+groupby sorters)
	void SetStringPool(const BYTE* pStringBase)
	{
		m_pStringBase = pStringBase;
	}

	/// add entry to the queue
	virtual bool Push(const CSphMatch& tEntry)
	{
		return PushEx(tEntry, false);
	}

	/// add grouped entry to the queue. bNewSet indicates the beginning of resultset returned by an agent.
	virtual bool PushGrouped(const CSphMatch& tEntry, bool)
	{
		return PushEx(tEntry, true);
	}

	/// store all entries into specified location in sorted order, and remove them from queue
	virtual int Flatten(CSphMatch* pTo, int iTag)
	{
		assert(m_bDataInitialized);

		CountDistinct();

		ARRAY_FOREACH(j, m_dAggregates)
			m_dAggregates[j]->Finalize(&m_tData);

		int iCopied = 0;
		if (!m_pAggrFilter || m_pAggrFilter->Eval(m_tData))
		{
			iCopied = 1;
			m_tSchema.CloneMatch(pTo, m_tData);
			m_tSchema.FreeStringPtrs(&m_tData);
			if (iTag >= 0)
				pTo->m_iTag = iTag;
		}

		m_iTotal = 0;
		m_bDataInitialized = false;

		if_const(DISTINCT)
			m_dUniq.Resize(0);

		return iCopied;
	}

	/// finalize, perform final sort/cut as needed
	void Finalize(ISphMatchProcessor& tProcessor, bool)
	{
		if (!GetLength())
			return;

		tProcessor.Process(&m_tData);
	}

	/// get entries count
	int GetLength() const
	{
		return m_bDataInitialized ? 1 : 0;
	}

protected:
	/// add entry to the queue
	bool PushEx(const CSphMatch& tEntry, bool bGrouped)
	{
		if_const(NOTIFICATIONS)
		{
			m_iJustPushed = 0;
			m_dJustPopped.Resize(0);
		}

		if (m_bDataInitialized)
		{
			assert(m_tData.m_pDynamic[-1] == tEntry.m_pDynamic[-1]);

			if (bGrouped)
			{
				// it's already grouped match
				// sum grouped matches count
				m_tData.SetAttr(m_tLocCount, m_tData.GetAttr(m_tLocCount) + tEntry.GetAttr(m_tLocCount)); // OPTIMIZE! AddAttr()?
			}
			else
			{
				// it's a simple match
				// increase grouped matches count
				m_tData.SetAttr(m_tLocCount, 1 + m_tData.GetAttr(m_tLocCount)); // OPTIMIZE! IncAttr()?
			}

			// update aggregates
			ARRAY_FOREACH(i, m_dAggregates)
				m_dAggregates[i]->Update(&m_tData, &tEntry, bGrouped);

			// if new entry is more relevant, update from it
			if (tEntry.m_uDocID < m_tData.m_uDocID)
			{
				if_const(NOTIFICATIONS)
				{
					m_iJustPushed = tEntry.m_uDocID;
					m_dJustPopped.Add(m_tData.m_uDocID);
				}

				m_tPregroup.Clone(&m_tData, &tEntry);
			}
		}

		// submit actual distinct value in all cases
		if_const(DISTINCT)
		{
			int iCount = 1;
			if (bGrouped)
				iCount = (int)tEntry.GetAttr(m_tLocDistinct);

			SphAttr_t tAttr = GetDistinctKey(tEntry, m_tDistinctLoc, m_eDistinctAttr, m_pStringBase);
			m_dUniq.Add(SphUngroupedValue_t(tAttr, iCount)); // OPTIMIZE! use simpler locator here?
		}

		// it's a dupe anyway, so we shouldn't update total matches count
		if (m_bDataInitialized)
			return false;

		// add first
		m_tSchema.CloneMatch(&m_tData, tEntry);

		if_const(NOTIFICATIONS)
			m_iJustPushed = m_tData.m_uDocID;

		if (!bGrouped)
		{
			m_tData.SetAttr(m_tLocGroupby, 1); // fake group number
			m_tData.SetAttr(m_tLocCount, 1);
			if_const(DISTINCT)
				m_tData.SetAttr(m_tLocDistinct, 0);
		}
		else
		{
			ARRAY_FOREACH(i, m_dAggregates)
				m_dAggregates[i]->Ungroup(&m_tData);
		}

		m_bDataInitialized = true;
		m_iTotal++;
		return true;
	}

	/// count distinct values if necessary
	void CountDistinct()
	{
		if_const(DISTINCT)
		{
			assert(m_bDataInitialized);

			m_dUniq.Sort();
			int iCount = 0;
			ARRAY_FOREACH(i, m_dUniq)
			{
				if (i > 0 && m_dUniq[i - 1] == m_dUniq[i])
					continue;
				iCount += m_dUniq[i].m_iCount;
			}

			m_tData.SetAttr(m_tLocDistinct, iCount);
		}
	}
};


}