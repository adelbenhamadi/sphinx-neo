#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/query/extra.h"
#include "neo/query/query_context.h"
#include "neo/query/query_stats.h"
#include "neo/query/query_debug.h"
#include "neo/query/field_mask.h"
#include "neo/io/fnv64.h"
#include "neo/query/iqword.h"

namespace NEO {



//fwd dec
class CSphDict;
class CSphIndex;
class ISphZoneCheck;


/// extended query node
/// plain nodes are just an atom
/// non-plain nodes are a logical function over children nodes
struct XQNode_t : public ISphNoncopyable
{
	XQNode_t* m_pParent;		///< my parent node (NULL for root ones)

private:
	XQOperator_e			m_eOp;			///< operation over childen
	int						m_iOrder;
	int						m_iCounter;

private:
	mutable uint64_t		m_iMagicHash;
	mutable uint64_t		m_iFuzzyHash;

public:
	CSphVector<XQNode_t*>	m_dChildren;	///< non-plain node children
	XQLimitSpec_t			m_dSpec;		///< specification by field, zone(s), etc.

	CSphVector<XQKeyword_t>	m_dWords;		///< query words (plain node)
	int						m_iOpArg;		///< operator argument (proximity distance, quorum count)
	int						m_iAtomPos;		///< atom position override (currently only used within expanded nodes)
	int						m_iUser;
	bool					m_bVirtuallyPlain;	///< "virtually plain" flag (currently only used by expanded nodes)
	bool					m_bNotWeighted;	///< this our expanded but empty word's node
	bool					m_bPercentOp;

public:
	/// ctor
	explicit XQNode_t(const XQLimitSpec_t& dSpec);

	/// dtor
	~XQNode_t();

	/// check if i'm empty
	bool IsEmpty() const
	{
		assert(m_dWords.GetLength() == 0 || m_dChildren.GetLength() == 0);
		return m_dWords.GetLength() == 0 && m_dChildren.GetLength() == 0;
	}

	/// setup field limits
	void SetFieldSpec(const FieldMask_t& uMask, int iMaxPos);

	/// setup zone limits
	void SetZoneSpec(const CSphVector<int>& dZones, bool bZoneSpan = false);

	/// copy field/zone limits from another node
	void CopySpecs(const XQNode_t* pSpecs);

	/// unconditionally clear field mask
	void ClearFieldMask();

public:
	/// get my operator
	XQOperator_e GetOp() const
	{
		return m_eOp;
	}

	/// get my cache order
	DWORD GetOrder() const
	{
		return m_iOrder;
	}

	/// get my cache counter
	int GetCount() const
	{
		return m_iCounter;
	}

	/// setup common nodes for caching
	void TagAsCommon(int iOrder, int iCounter)
	{
		m_iCounter = iCounter;
		m_iOrder = iOrder;
	}

	/// hash me
	uint64_t GetHash() const;

	/// fuzzy hash ( a hash value is equal for proximity and phrase nodes
	/// with similar keywords )
	uint64_t GetFuzzyHash() const;

	/// setup new operator and args
	void SetOp(XQOperator_e eOp, XQNode_t* pArg1, XQNode_t* pArg2 = NULL);

	/// setup new operator and args
	void SetOp(XQOperator_e eOp, CSphVector<XQNode_t*>& dArgs)
	{
		m_eOp = eOp;
		m_dChildren.SwapData(dArgs);
		ARRAY_FOREACH(i, m_dChildren)
			m_dChildren[i]->m_pParent = this;
	}

	/// setup new operator (careful parser/transform use only)
	void SetOp(XQOperator_e eOp)
	{
		m_eOp = eOp;
	}

	/// fixup atom positions in case of proximity queries and blended chars
	/// we need to handle tokens with blended characters as simple tokens in this case
	/// and we need to remove possible gaps in atom positions
	int FixupAtomPos();

	/// return node like current
	inline XQNode_t* Clone();

	/// force resetting magic hash value ( that changed after transformation )
	inline bool ResetHash();

#ifndef NDEBUG
	/// consistency check
	void Check(bool bRoot)
	{
		assert(bRoot || !IsEmpty()); // empty leaves must be removed from the final tree; empty root is allowed
		assert(!(m_dWords.GetLength() && m_eOp != SPH_QUERY_AND && m_eOp != SPH_QUERY_OR && m_eOp != SPH_QUERY_PHRASE
			&& m_eOp != SPH_QUERY_PROXIMITY && m_eOp != SPH_QUERY_QUORUM)); // words are only allowed in these node types
		assert((m_dWords.GetLength() == 1 && (m_eOp == SPH_QUERY_AND || m_eOp == SPH_QUERY_OR)) ||
			m_dWords.GetLength() != 1); // 1-word leaves must be of AND | OR types

		ARRAY_FOREACH(i, m_dChildren)
		{
			assert(m_dChildren[i]->m_pParent == this);
			m_dChildren[i]->Check(false);
		}
	}
#else
	void Check(bool) {}
#endif
};

//////////////////////////////////////////////////////////////////////////
// INTRA-BATCH CACHING
//////////////////////////////////////////////////////////////////////////

class ExtNode_i;

/// container that does intra-batch query-sub-tree caching
/// actually carries the cached data, NOT to be recreated frequently (see thin wrapper below)
class NodeCacheContainer_t
{
private:
	friend class ExtNodeCached_t;
	friend class CSphQueryNodeCache;

private:
	int								m_iRefCount;
	bool							m_StateOk;
	const ISphQwordSetup* m_pSetup;

	CSphVector<ExtDoc_t>			m_Docs;
	CSphVector<ExtHit_t>			m_Hits;
	CSphVector<CSphRowitem>			m_InlineAttrs;
	int								m_iAtomPos; // minimal position from original donor, used for shifting

	CSphQueryNodeCache* m_pNodeCache;

public:
	NodeCacheContainer_t()
		: m_iRefCount(1)
		, m_StateOk(true)
		, m_pSetup(NULL)
		, m_iAtomPos(0)
		, m_pNodeCache(NULL)
	{}

public:
	void Release()
	{
		if (--m_iRefCount <= 0)
			Invalidate();
	}

	ExtNode_i* CreateCachedWrapper(ExtNode_i* pChild, const XQNode_t* pRawChild, const ISphQwordSetup& tSetup);

private:
	bool							WarmupCache(ExtNode_i* pChild, int iQWords);
	void							Invalidate();
};



/// intra-batch node cache
class CSphQueryNodeCache
{
	friend class NodeCacheContainer_t;

protected:
	class NodeCacheContainer_t* m_pPool;
	int								m_iMaxCachedDocs;
	int								m_iMaxCachedHits;

public:
	CSphQueryNodeCache(int iCells, int MaxCachedDocs, int MaxCachedHits);
	~CSphQueryNodeCache();

	ExtNode_i* CreateProxy(ExtNode_i* pChild, const XQNode_t* pRawChild, const ISphQwordSetup& tSetup);
};



/// generic match streamer
class ExtNode_i
{
public:
	ExtNode_i();
	virtual						~ExtNode_i() { SafeDeleteArray(m_pDocinfo); }

	static ExtNode_i* Create(const XQNode_t* pNode, const ISphQwordSetup& tSetup);
	static ExtNode_i* Create(const XQKeyword_t& tWord, const XQNode_t* pNode, const ISphQwordSetup& tSetup);
	static ExtNode_i* Create(ISphQword* pQword, const XQNode_t* pNode, const ISphQwordSetup& tSetup);

	virtual void				Reset(const ISphQwordSetup& tSetup) = 0;
	virtual void				HintDocid(SphDocID_t uMinID) = 0;
	virtual const ExtDoc_t* GetDocsChunk() = 0;
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs) = 0;

	virtual int					GetQwords(ExtQwordsHash_t& hQwords) = 0;
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords) = 0;
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const = 0;
	virtual bool				GotHitless() = 0;
	virtual int					GetDocsCount() { return INT_MAX; }
	virtual int					GetHitsCount() { return 0; }
	virtual uint64_t			GetWordID() const = 0;			///< for now, only used for duplicate keyword checks in quorum operator

	void DebugIndent(int iLevel)
	{
		while (iLevel--)
			printf("    ");
	}

	virtual void DebugDump(int iLevel)
	{
		DebugIndent(iLevel);
		printf("ExtNode\n");
	}

	// return specific extra data may be associated with the node
	// intended to be used a bit similar to QueryInterface() in COM technology
	// but simpler due to enum instead of 128-bit GUID, and no ref. counting
	inline bool GetExtraData(ExtraData_e eNode, void** ppData)
	{
		return ExtraDataImpl(eNode, ppData);
	}
private:
	virtual bool ExtraDataImpl(ExtraData_e, void**)
	{
		return false;
	}

public:
	static const int			MAX_DOCS = 512;
	static const int			MAX_HITS = 512;

	int							m_iAtomPos;		///< we now need it on this level for tricks like expanded keywords within phrases

protected:
	ExtDoc_t					m_dDocs[MAX_DOCS];
	ExtHit_t					m_dHits[MAX_HITS];

public:
	int							m_iStride;		///< docinfo stride (for inline mode only)

protected:
	CSphRowitem* m_pDocinfo;		///< docinfo storage (for inline mode only)

	void AllocDocinfo(const ISphQwordSetup& tSetup)
	{
		if (tSetup.m_iInlineRowitems)
		{
			m_iStride = tSetup.m_iInlineRowitems;
			m_pDocinfo = new CSphRowitem[MAX_DOCS * m_iStride];
		}
	}

protected:
	inline const ExtDoc_t* ReturnDocsChunk(int iCount, const char* sNode)
	{
		assert(iCount >= 0 && iCount < MAX_DOCS);
		m_dDocs[iCount].m_uDocid = DOCID_MAX;

		PrintDocsChunk(iCount, m_iAtomPos, m_dDocs, sNode, this);
		return iCount ? m_dDocs : NULL;
	}

	inline const ExtHit_t* ReturnHitsChunk(int iCount, const char* sNode, bool bReverse)
	{
		assert(iCount >= 0 && iCount < MAX_HITS);
		m_dHits[iCount].m_uDocid = DOCID_MAX;

#ifndef NDEBUG
		for (int i = 1; i < iCount; i++)
		{
			bool bQPosPassed = ((bReverse && m_dHits[i - 1].m_uQuerypos >= m_dHits[i].m_uQuerypos) || (!bReverse && m_dHits[i - 1].m_uQuerypos <= m_dHits[i].m_uQuerypos));
			assert(m_dHits[i - 1].m_uDocid != m_dHits[i].m_uDocid ||
				(m_dHits[i - 1].m_uHitpos < m_dHits[i].m_uHitpos || (m_dHits[i - 1].m_uHitpos == m_dHits[i].m_uHitpos && bQPosPassed)));
		}
#endif

		PrintHitsChunk(iCount, m_iAtomPos, m_dHits, sNode, this);
		return iCount ? m_dHits : NULL;
	}
};



/// cached node wrapper to be injected into actual search trees
/// (special container actually carries all the data and does the work, see below)
class ExtNodeCached_t : public ExtNode_i
{
	friend class NodeCacheContainer_t;
	NodeCacheContainer_t* m_pNode;
	ExtDoc_t* m_pHitDoc;			///< points to entry in m_dDocs which GetHitsChunk() currently emits hits for
	SphDocID_t					m_uHitsOverFor;		///< there are no more hits for matches block starting with this ID
	CSphString* m_pWarning;
	int64_t						m_iMaxTimer;		///< work until this timestamp
	int							m_iHitIndex;		///< store the current position in m_Hits for GetHitsChunk()
	int							m_iDocIndex;		///< store the current position in m_Docs for GetDocsChunk()
	ExtNode_i* m_pChild;			///< pointer to donor for the sake of AtomPos procession
	int							m_iQwords;			///< number of tokens in parent query

	void StepForwardToHitsFor(SphDocID_t uDocId);

	// creation possible ONLY via NodeCacheContainer_t
	explicit ExtNodeCached_t(NodeCacheContainer_t* pNode, ExtNode_i* pChild)
		: m_pNode(pNode)
		, m_pHitDoc(NULL)
		, m_uHitsOverFor(0)
		, m_pWarning(NULL)
		, m_iMaxTimer(0)
		, m_iHitIndex(0)
		, m_iDocIndex(0)
		, m_pChild(pChild)
		, m_iQwords(0)
	{
		m_iAtomPos = pChild->m_iAtomPos;
	}

public:
	virtual ~ExtNodeCached_t()
	{
		SafeDelete(m_pChild);
		SafeRelease(m_pNode);
	}

	virtual void Reset(const ISphQwordSetup& tSetup)
	{
		if (m_pChild)
			m_pChild->Reset(tSetup);

		m_iHitIndex = 0;
		m_iDocIndex = 0;
		m_uHitsOverFor = 0;
		m_pHitDoc = NULL;
		m_iMaxTimer = 0;
		m_iMaxTimer = tSetup.m_iMaxTimer;
		m_pWarning = tSetup.m_pWarning;
	}

	virtual void HintDocid(SphDocID_t) {}

	virtual const ExtDoc_t* GetDocsChunk();

	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pMatched);

	virtual int GetQwords(ExtQwordsHash_t& hQwords)
	{
		if (!m_pChild)
			return -1;

		int iChildAtom = m_pChild->GetQwords(hQwords);
		if (iChildAtom < 0)
			return -1;

		return m_iAtomPos + iChildAtom;
	}

	virtual void SetQwordsIDF(const ExtQwordsHash_t& hQwords)
	{
		m_iQwords = hQwords.GetLength();
		if (m_pNode->m_pSetup)
		{
			if (m_pChild)
				m_pChild->SetQwordsIDF(hQwords);

			m_pNode->WarmupCache(m_pChild, m_iQwords);
		}
	}

	virtual void GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const
	{
		if (m_pChild)
			m_pChild->GetTerms(hQwords, dTermDupes);
	}

	virtual bool GotHitless()
	{
		return (m_pChild)
			? m_pChild->GotHitless()
			: false;
	}

	virtual uint64_t GetWordID() const
	{
		if (m_pChild)
			return m_pChild->GetWordID();
		else
			return 0;
	}
};

///////////////////////



/// single keyword streamer
class ExtTerm_c : public ExtNode_i, ISphNoncopyable
{
public:
	ExtTerm_c(ISphQword* pQword, const FieldMask_t& uFields, const ISphQwordSetup& tSetup, bool bNotWeighted);
	ExtTerm_c(ISphQword* pQword, const ISphQwordSetup& tSetup);
	ExtTerm_c() {} ///< to be used in pair with Init()
	~ExtTerm_c()
	{
		SafeDelete(m_pQword);
	}

	void						Init(ISphQword* pQword, const FieldMask_t& uFields, const ISphQwordSetup& tSetup, bool bNotWeighted);
	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);

	virtual int					GetQwords(ExtQwordsHash_t& hQwords);
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const;
	virtual bool				GotHitless() { return false; }
	virtual int					GetDocsCount() { return m_pQword->m_iDocs; }
	virtual int					GetHitsCount() { return m_pQword->m_iHits; }
	virtual uint64_t			GetWordID() const
	{
		if (m_pQword->m_uWordID)
			return m_pQword->m_uWordID;
		else
			return sphFNV64(m_pQword->m_sDictWord.cstr());
	}

	virtual void HintDocid(SphDocID_t uMinID)
	{
		m_pQword->HintDocid(uMinID);
		if (m_pStats)
			m_pStats->m_iSkips++;
		if (m_pNanoBudget)
			*m_pNanoBudget -= g_iPredictorCostSkip;
	}

	virtual void DebugDump(int iLevel)
	{
		DebugIndent(iLevel);
		printf("ExtTerm: %s at: %d ", m_pQword->m_sWord.cstr(), m_pQword->m_iAtomPos);
		if (m_dQueriedFields.TestAll(true))
		{
			printf("(all)\n");
		}
		else
		{
			bool bFirst = true;
			printf("in: ");
			for (int iField = 0; iField < SPH_MAX_FIELDS; iField++)
			{
				if (m_dQueriedFields.Test(iField))
				{
					if (!bFirst)
						printf(", ");
					printf("%d", iField);
					bFirst = false;
				}
			}
			printf("\n");
		}
	}

protected:
	ISphQword* m_pQword;
	FieldMask_t				m_dQueriedFields;	///< accepted fields mask
	bool						m_bHasWideFields;	///< whether fields mask for this term refer to fields 32+
	float						m_fIDF;				///< IDF for this term (might be 0.0f for non-1st occurences in query)
	int64_t						m_iMaxTimer;		///< work until this timestamp
	CSphString* m_pWarning;
	bool						m_bNotWeighted;
	CSphQueryStats* m_pStats;
	int64_t* m_pNanoBudget;

	ExtDoc_t* m_pLastChecked;		///< points to entry in m_dDocs which GetHitsChunk() currently emits hits for
	SphDocID_t					m_uMatchChecked;	///< there are no more hits for matches block starting with this ID
	bool						m_bTail;			///< should we emit more hits for current docid or proceed furthwer

public:
	static volatile bool		m_bInterruptNow; ///< may be set from outside to indicate the globally received sigterm
};



/// single keyword streamer with artificial hitlist
class ExtTermHitless_c : public ExtTerm_c
{
public:
	ExtTermHitless_c(ISphQword* pQword, const FieldMask_t& uFields, const ISphQwordSetup& tSetup, bool bNotWeighted)
		: ExtTerm_c(pQword, uFields, tSetup, bNotWeighted)
		, m_uFieldPos(0)
	{}

	virtual void				Reset(const ISphQwordSetup&) { m_uFieldPos = 0; }
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	virtual bool				GotHitless() { return true; }

protected:
	DWORD	m_uFieldPos;
};


/// immediately interrupt current query
void			sphInterruptNow();

/// check if we got interrupted
bool			sphInterrupted();



//////////////////////

/// position filter policy
template < TermPosFilter_e T >
class TermAcceptor_c
{
public:
	TermAcceptor_c(ISphQword*, const XQNode_t*, const ISphQwordSetup&) {}
protected:
	inline bool					IsAcceptableHit(const ExtHit_t*) const { return true; }
	inline void					Reset() {}
};

template<>
class TermAcceptor_c<TERM_POS_FIELD_LIMIT> : public ISphNoncopyable
{
public:
	TermAcceptor_c(ISphQword*, const XQNode_t* pNode, const ISphQwordSetup&)
		: m_iMaxFieldPos(pNode->m_dSpec.m_iFieldMaxPos)
	{}
protected:
	inline bool					IsAcceptableHit(const ExtHit_t*) const;
	inline void					Reset() {}
private:
	const int					m_iMaxFieldPos;
};

template<>
class TermAcceptor_c<TERM_POS_ZONES> : public ISphNoncopyable
{
public:
	TermAcceptor_c(ISphQword*, const XQNode_t* pNode, const ISphQwordSetup& tSetup)
		: m_pZoneChecker(tSetup.m_pZoneChecker)
		, m_dZones(pNode->m_dSpec.m_dZones)
		, m_uLastZonedId(0)
		, m_iCheckFrom(0)
	{}
protected:
	inline bool					IsAcceptableHit(const ExtHit_t* pHit) const;
	inline void					Reset()
	{
		m_uLastZonedId = 0;
		m_iCheckFrom = 0;
	}
protected:
	ISphZoneCheck* m_pZoneChecker;				///< zone-limited searches query ranker about zones
	mutable CSphVector<int>		m_dZones;					///< zone ids for this particular term
	mutable SphDocID_t			m_uLastZonedId;
	mutable int					m_iCheckFrom;
};

///
class BufferedNode_c
{
protected:
	BufferedNode_c()
		: m_pRawDocs(NULL)
		, m_pRawDoc(NULL)
		, m_pRawHit(NULL)
		, m_uLastID(0)
		, m_eState(COPY_DONE)
		, m_uDoneFor(0)
		, m_uHitStartDocid(0)
	{
		m_dMyDocs[0].m_uDocid = DOCID_MAX;
		m_dMyHits[0].m_uDocid = DOCID_MAX;
		m_dFilteredHits[0].m_uDocid = DOCID_MAX;
	}

	void Reset()
	{
		m_pRawDocs = NULL;
		m_pRawDoc = NULL;
		m_pRawHit = NULL;
		m_uLastID = 0;
		m_eState = COPY_DONE;
		m_uDoneFor = 0;
		m_uHitStartDocid = 0;
		m_dMyDocs[0].m_uDocid = DOCID_MAX;
		m_dMyHits[0].m_uDocid = DOCID_MAX;
		m_dFilteredHits[0].m_uDocid = DOCID_MAX;
	}

protected:
	const ExtDoc_t* m_pRawDocs;					///< chunk start as returned by raw GetDocsChunk() (need to store it for raw GetHitsChunk() calls)
	const ExtDoc_t* m_pRawDoc;					///< current position in raw docs chunk
	const ExtHit_t* m_pRawHit;					///< current position in raw hits chunk
	SphDocID_t					m_uLastID;
	enum
	{
		COPY_FILTERED,
		COPY_TRAILING,
		COPY_DONE
	}							m_eState;					///< internal GetHitsChunk() state (are we copying from my hits, or passing trailing raw hits, or done)
	ExtDoc_t					m_dMyDocs[ExtNode_i::MAX_DOCS];		///< all documents within the required pos range
	ExtHit_t					m_dMyHits[ExtNode_i::MAX_HITS];		///< all hits within the required pos range
	ExtHit_t					m_dFilteredHits[ExtNode_i::MAX_HITS];	///< hits from requested subset of the documents (for GetHitsChunk())
	SphDocID_t					m_uDoneFor;
	SphDocID_t					m_uHitStartDocid;
};

/// single keyword streamer, with term position filtering
template < TermPosFilter_e T, class ExtBase = ExtTerm_c >
class ExtConditional : public BufferedNode_c, public ExtBase, protected TermAcceptor_c<T>
{
	typedef TermAcceptor_c<T>	t_Acceptor;
protected:
	ExtConditional(ISphQword* pQword, const XQNode_t* pNode, const ISphQwordSetup& tSetup);
public:
	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	virtual bool				GotHitless() { return false; }

private:
	virtual bool				ExtraDataImpl(ExtraData_e eData, void** ppResult);
};

/// single keyword streamer, with term position filtering
template < TermPosFilter_e T >
class ExtTermPos_c : public ExtConditional<T, ExtTerm_c>
{
public:
	ExtTermPos_c(ISphQword* pQword, const XQNode_t* pNode, const ISphQwordSetup& tSetup)
		: ExtConditional<T, ExtTerm_c>(pQword, pNode, tSetup)
	{
		ExtTerm_c::Init(pQword, pNode->m_dSpec.m_dFieldMask, tSetup, pNode->m_bNotWeighted);
	}
};


template<TermPosFilter_e T, class ExtBase>
bool ExtConditional<T, ExtBase>::ExtraDataImpl(ExtraData_e, void**)
{
	return false;
}

/// multi-node binary-operation streamer traits
class ExtTwofer_c : public ExtNode_i
{
public:
	ExtTwofer_c(ExtNode_i* pLeft, ExtNode_i* pRight, const ISphQwordSetup& tSetup);
	ExtTwofer_c() {} ///< to be used in pair with Init();
	~ExtTwofer_c();

	void				Init(ExtNode_i* pFirst, ExtNode_i* pSecond, const ISphQwordSetup& tSetup);
	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual int					GetQwords(ExtQwordsHash_t& hQwords);
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const;

	virtual bool				GotHitless() { return m_pLeft->GotHitless() || m_pRight->GotHitless(); }

	void DebugDumpT(const char* sName, int iLevel)
	{
		DebugIndent(iLevel);
		printf("%s:\n", sName);
		m_pLeft->DebugDump(iLevel + 1);
		m_pRight->DebugDump(iLevel + 1);
	}

	void SetNodePos(WORD uPosLeft, WORD uPosRight)
	{
		m_uNodePosL = uPosLeft;
		m_uNodePosR = uPosRight;
	}

	virtual void HintDocid(SphDocID_t uMinID)
	{
		m_pLeft->HintDocid(uMinID);
		m_pRight->HintDocid(uMinID);
	}

	virtual uint64_t GetWordID() const
	{
		uint64_t dHash[2];
		dHash[0] = m_pLeft->GetWordID();
		dHash[1] = m_pRight->GetWordID();
		return sphFNV64(dHash, sizeof(dHash));
	}

protected:
	ExtNode_i* m_pLeft;
	ExtNode_i* m_pRight;
	const ExtDoc_t* m_pCurDocL;
	const ExtDoc_t* m_pCurDocR;
	const ExtHit_t* m_pCurHitL;
	const ExtHit_t* m_pCurHitR;
	WORD						m_uNodePosL;
	WORD						m_uNodePosR;
	SphDocID_t					m_uMatchedDocid;
};

/// A-and-B streamer
class ExtAnd_c : public ExtTwofer_c
{
public:
	ExtAnd_c(ExtNode_i* pLeft, ExtNode_i* pRight, const ISphQwordSetup& tSetup) : ExtTwofer_c(pLeft, pRight, tSetup), m_bQPosReverse(false) {}
	ExtAnd_c() : m_bQPosReverse(false) {} ///< to be used with Init()
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);

	void DebugDump(int iLevel) { DebugDumpT("ExtAnd", iLevel); }

	void SetQPosReverse()
	{
		m_bQPosReverse = true;
	}

protected:
	bool m_bQPosReverse;
};

class ExtAndZonespanned_c : public ExtAnd_c
{
public:
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	void DebugDump(int iLevel) { DebugDumpT("ExtAndZonespan", iLevel); }

protected:
	bool IsSameZonespan(const ExtHit_t* pHit1, const ExtHit_t* pHit2) const;

	ISphZoneCheck* m_pZoneChecker;
	CSphVector<int> m_dZones;
};

class ExtAndZonespan_c : public ExtConditional < TERM_POS_NONE, ExtAndZonespanned_c >
{
public:
	ExtAndZonespan_c(ExtNode_i* pFirst, ExtNode_i* pSecond, const ISphQwordSetup& tSetup, const XQNode_t* pNode)
		: ExtConditional<TERM_POS_NONE, ExtAndZonespanned_c>(NULL, pNode, tSetup)
	{
		Init(pFirst, pSecond, tSetup);
		m_pZoneChecker = tSetup.m_pZoneChecker;
		m_dZones = pNode->m_dSpec.m_dZones;
	}
};

/// A-or-B streamer
class ExtOr_c : public ExtTwofer_c
{
public:
	ExtOr_c(ExtNode_i* pLeft, ExtNode_i* pRight, const ISphQwordSetup& tSetup) : ExtTwofer_c(pLeft, pRight, tSetup) {}
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);

	void DebugDump(int iLevel) { DebugDumpT("ExtOr", iLevel); }
};


/// A-maybe-B streamer
class ExtMaybe_c : public ExtOr_c
{
public:
	ExtMaybe_c(ExtNode_i* pLeft, ExtNode_i* pRight, const ISphQwordSetup& tSetup) : ExtOr_c(pLeft, pRight, tSetup) {}
	virtual const ExtDoc_t* GetDocsChunk();

	void DebugDump(int iLevel) { DebugDumpT("ExtMaybe", iLevel); }
};


/// A-and-not-B streamer
class ExtAndNot_c : public ExtTwofer_c
{
public:
	ExtAndNot_c(ExtNode_i* pLeft, ExtNode_i* pRight, const ISphQwordSetup& tSetup);
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	virtual void				Reset(const ISphQwordSetup& tSetup);

	void DebugDump(int iLevel) { DebugDumpT("ExtAndNot", iLevel); }

protected:
	bool						m_bPassthrough;
};


/// generic operator over N nodes
class ExtNWayT : public ExtNode_i
{
public:
	ExtNWayT(const CSphVector<ExtNode_i*>& dNodes, const ISphQwordSetup& tSetup);
	~ExtNWayT();
	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual int					GetQwords(ExtQwordsHash_t& hQwords);
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const;
	virtual bool				GotHitless() { return false; }
	virtual void				HintDocid(SphDocID_t uMinID) { m_pNode->HintDocid(uMinID); }
	virtual uint64_t			GetWordID() const;

protected:
	ExtNode_i* m_pNode;				///< my and-node for all the terms
	const ExtDoc_t* m_pDocs;				///< current docs chunk from and-node
	const ExtHit_t* m_pHits;				///< current hits chunk from and-node
	const ExtDoc_t* m_pDoc;					///< current doc from and-node
	const ExtHit_t* m_pHit;					///< current hit from and-node
	const ExtDoc_t* m_pMyDoc;				///< current doc for hits getter
	const ExtHit_t* m_pMyHit;				///< current hit for hits getter
	SphDocID_t					m_uLastDocID;			///< last emitted hit
	ExtHit_t					m_dMyHits[MAX_HITS];	///< buffer for all my phrase hits; inherited m_dHits will receive filtered results
	SphDocID_t					m_uMatchedDocid;		///< doc currently in process
	SphDocID_t					m_uHitsOverFor;			///< there are no more hits for matches block starting with this ID

protected:
	inline void ConstructNode(const CSphVector<ExtNode_i*>& dNodes, const CSphVector<WORD>& dPositions, const ISphQwordSetup& tSetup)
	{
		assert(m_pNode == NULL);
		WORD uLPos = dPositions[0];
		ExtNode_i* pCur = dNodes[uLPos++]; // ++ for zero-based to 1-based
		ExtAnd_c* pCurEx = NULL;
		DWORD uLeaves = dNodes.GetLength();
		WORD uRPos;
		for (DWORD i = 1; i < uLeaves; i++)
		{
			uRPos = dPositions[i];
			pCur = pCurEx = new ExtAnd_c(pCur, dNodes[uRPos++], tSetup); // ++ for zero-based to 1-based
			pCurEx->SetNodePos(uLPos, uRPos);
			uLPos = 0;
		}
		if (pCurEx)
			pCurEx->SetQPosReverse();
		m_pNode = pCur;
	}
};

struct ExtNodeTF_fn
{
	bool IsLess(ExtNode_i* pA, ExtNode_i* pB) const
	{
		return pA->GetDocsCount() < pB->GetDocsCount();
	}
};

struct ExtNodeTFExt_fn
{
	const CSphVector<ExtNode_i*>& m_dNodes;

	explicit ExtNodeTFExt_fn(const CSphVector<ExtNode_i*>& dNodes)
		: m_dNodes(dNodes)
	{}

	ExtNodeTFExt_fn(const ExtNodeTFExt_fn& rhs)
		: m_dNodes(rhs.m_dNodes)
	{}

	bool IsLess(WORD uA, WORD uB) const
	{
		return m_dNodes[uA]->GetDocsCount() < m_dNodes[uB]->GetDocsCount();
	}

private:
	const ExtNodeTFExt_fn& operator = (const ExtNodeTFExt_fn&)
	{
		return *this;
	}
};

/// FSM is Finite State Machine
template < class FSM >
class ExtNWay_c : public ExtNWayT, private FSM
{
public:
	ExtNWay_c(const CSphVector<ExtNode_i*>& dNodes, const XQNode_t& tNode, const ISphQwordSetup& tSetup)
		: ExtNWayT(dNodes, tSetup)
		, FSM(dNodes, tNode, tSetup)
	{
		CSphVector<WORD> dPositions(dNodes.GetLength());
		ARRAY_FOREACH(i, dPositions)
			dPositions[i] = (WORD)i;
		dPositions.Sort(ExtNodeTFExt_fn(dNodes));
		ConstructNode(dNodes, dPositions, tSetup);
	}

public:
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	virtual void DebugDump(int iLevel)
	{
		DebugIndent(iLevel);
		printf("%s\n", FSM::GetName());
		m_pNode->DebugDump(iLevel + 1);
	}
private:
	bool						EmitTail(int& iHit);	///< the "trickiest part" extracted in order to process the proximity also
};

class FSMphrase
{
protected:
	struct State_t
	{
		int m_iTagQword;
		DWORD m_uExpHitposWithField;
	};

protected:
	FSMphrase(const CSphVector<ExtNode_i*>& dQwords, const XQNode_t& tNode, const ISphQwordSetup& tSetup);
	bool						HitFSM(const ExtHit_t* pHit, ExtHit_t* dTarget);

	inline static const char* GetName() { return "ExtPhrase"; }
	inline void ResetFSM()
	{
		m_dStates.Resize(0);
	}

protected:
	CSphVector<int>				m_dQposDelta;			///< next expected qpos delta for each existing qpos (for skipped stopwords case)
	CSphVector<int>				m_dAtomPos;				///< lets use it as finite automata states and keep references on it
	CSphVector<State_t>			m_dStates;				///< pointers to states of finite automata
	DWORD						m_uQposMask;
};
/// exact phrase streamer
typedef ExtNWay_c < FSMphrase > ExtPhrase_c;

/// proximity streamer
class FSMproximity
{
protected:
	FSMproximity(const CSphVector<ExtNode_i*>& dQwords, const XQNode_t& tNode, const ISphQwordSetup& tSetup);
	bool						HitFSM(const ExtHit_t* pHit, ExtHit_t* dTarget);

	inline static const char* GetName() { return "ExtProximity"; }
	inline void ResetFSM()
	{
		m_uExpPos = 0;
		m_uWords = 0;
		m_iMinQindex = -1;
		ARRAY_FOREACH(i, m_dProx)
			m_dProx[i] = UINT_MAX;
	}

protected:
	int							m_iMaxDistance;
	DWORD						m_uWordsExpected;
	DWORD						m_uMinQpos;
	DWORD						m_uQLen;
	DWORD						m_uExpPos;
	CSphVector<DWORD>			m_dProx; // proximity hit position for i-th word
	CSphVector<int> 			m_dDeltas; // used for weight calculation
	DWORD						m_uWords;
	int							m_iMinQindex;
	DWORD						m_uQposMask;
};
/// exact phrase streamer
typedef ExtNWay_c<FSMproximity> ExtProximity_c;

/// proximity streamer
class FSMmultinear
{
protected:
	FSMmultinear(const CSphVector<ExtNode_i*>& dNodes, const XQNode_t& tNode, const ISphQwordSetup& tSetup);
	bool						HitFSM(const ExtHit_t* pHit, ExtHit_t* dTarget);

	inline static const char* GetName() { return "ExtMultinear"; }
	inline void ResetFSM()
	{
		m_iRing = m_uLastP = m_uPrelastP = 0;
	}

protected:
	int							m_iNear;			///< the NEAR distance
	DWORD						m_uPrelastP;
	DWORD						m_uPrelastML;
	DWORD						m_uPrelastSL;
	DWORD						m_uPrelastW;
	DWORD						m_uLastP;			///< position of the last hit
	DWORD						m_uLastML;			///< the length of the previous hit
	DWORD						m_uLastSL;			///< the length of the previous hit in Query
	DWORD						m_uLastW;			///< last weight
	DWORD						m_uWordsExpected;	///< now many hits we're expect
	DWORD						m_uWeight;			///< weight accum
	DWORD						m_uFirstHit;		///< hitpos of the beginning of the match chain
	WORD						m_uFirstNpos;		///< N-position of the head of the chain
	WORD						m_uFirstQpos;		///< Q-position of the head of the chain (for twofers)
	CSphVector<WORD>			m_dNpos;			///< query positions for multinear
	CSphVector<ExtHit_t>		m_dRing;			///< ring buffer for multihit data
	int							m_iRing;			///< the head of the ring
	bool						m_bTwofer;			///< if we have 2- or N-way NEAR
	bool						m_bQposMask;
private:
	inline int RingTail() const
	{
		return (m_iRing + m_dNpos.GetLength() - 1) % m_uWordsExpected;
	}
	inline void Add2Ring(const ExtHit_t* pHit)
	{
		if (!m_bTwofer)
			m_dRing[RingTail()] = *pHit;
	}
	inline void ShiftRing()
	{
		if (++m_iRing == (int)m_uWordsExpected)
			m_iRing = 0;
	}
};
/// exact phrase streamer
typedef ExtNWay_c<FSMmultinear> ExtMultinear_c;

/// quorum streamer
class ExtQuorum_c : public ExtNode_i
{
public:
	ExtQuorum_c(CSphVector<ExtNode_i*>& dQwords, const XQNode_t& tNode, const ISphQwordSetup& tSetup);
	virtual						~ExtQuorum_c();

	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);

	virtual int					GetQwords(ExtQwordsHash_t& hQwords);
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const;
	virtual uint64_t			GetWordID() const;

	virtual bool				GotHitless() { return false; }

	virtual void				HintDocid(SphDocID_t uMinID)
	{
		ARRAY_FOREACH(i, m_dChildren)
			if (m_dChildren[i].m_pTerm)
				m_dChildren[i].m_pTerm->HintDocid(uMinID);
	}

	static int					GetThreshold(const XQNode_t& tNode, int iQwords);

	struct TermTuple_t
	{
		ExtNode_i* m_pTerm;		///< my children nodes (simply ExtTerm_c for now, not true anymore)
		const ExtDoc_t* m_pCurDoc;		///< current positions into children doclists
		const ExtHit_t* m_pCurHit;		///< current positions into children hitlists
		int					m_iCount;		///< terms count in case of dupes
		bool				m_bStandStill;	///< should we emit hits to proceed further
	};

private:

	ExtHit_t					m_dQuorumHits[MAX_HITS];	///< buffer for all my quorum hits; inherited m_dHits will receive filtered results
	int							m_iMyHitCount;				///< hits collected so far
	int							m_iMyLast;					///< hits processed so far
	CSphVector<TermTuple_t>		m_dInitialChildren;			///< my children nodes (simply ExtTerm_c for now)
	CSphVector<TermTuple_t>		m_dChildren;
	SphDocID_t					m_uMatchedDocid;			///< tail docid for hitlist emission
	int							m_iThresh;					///< keyword count threshold
	// FIXME!!! also skip hits processing for children w\o constrains ( zones or field limit )
	bool						m_bHasDupes;				///< should we analyze hits on docs collecting

	// check for hits that matches and return flag that docs might be advanced
	bool						CollectMatchingHits(SphDocID_t uDocid, int iQuorum);
	const ExtHit_t* GetHitsChunkDupes(const ExtDoc_t* pDocs);
	const ExtHit_t* GetHitsChunkDupesTail();
	const ExtHit_t* GetHitsChunkSimple(const ExtDoc_t* pDocs);

	int							CountQuorum(bool bFixDupes)
	{
		if (!m_bHasDupes)
			return m_dChildren.GetLength();

		int iSum = 0;
		bool bHasDupes = false;
		ARRAY_FOREACH(i, m_dChildren)
		{
			iSum += m_dChildren[i].m_iCount;
			bHasDupes |= (m_dChildren[i].m_iCount > 1);
		}

#if QDEBUG
		if (bFixDupes && bHasDupes != m_bHasDupes)
			printf("quorum dupes %d -> %d\n", m_bHasDupes, bHasDupes);
#endif

		m_bHasDupes = bFixDupes ? bHasDupes : m_bHasDupes;
		return iSum;
	}
};


/// A-B-C-in-this-order streamer
class ExtOrder_c : public ExtNode_i
{
public:
	ExtOrder_c(const CSphVector<ExtNode_i*>& dChildren, const ISphQwordSetup& tSetup);
	~ExtOrder_c();

	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	virtual int					GetQwords(ExtQwordsHash_t& hQwords);
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const;
	virtual bool				GotHitless() { return false; }
	virtual uint64_t			GetWordID() const;

	virtual void HintDocid(SphDocID_t uMinID)
	{
		ARRAY_FOREACH(i, m_dChildren)
			m_dChildren[i]->HintDocid(uMinID);
	}

protected:
	CSphVector<ExtNode_i*>		m_dChildren;
	CSphVector<const ExtDoc_t*>	m_pDocsChunk;	///< last document chunk (for hit fetching)
	CSphVector<const ExtDoc_t*>	m_pDocs;		///< current position in document chunk
	CSphVector<const ExtHit_t*>	m_pHits;		///< current position in hits chunk
	ExtHit_t					m_dMyHits[MAX_HITS];	///< buffer for all my phrase hits; inherited m_dHits will receive filtered results
	bool						m_bDone;
	SphDocID_t					m_uHitsOverFor;

protected:
	int							GetChildIdWithNextHit(SphDocID_t uDocid);							///< get next hit within given document, and return its child-id
	int							GetMatchingHits(SphDocID_t uDocid, ExtHit_t* pHitbuf, int iLimit);	///< process candidate hits and stores actual matches while we can
};


/// same-text-unit streamer
/// (aka, A and B within same sentence, or same paragraph)
class ExtUnit_c : public ExtNode_i
{
public:
	ExtUnit_c(ExtNode_i* pFirst, ExtNode_i* pSecond, const FieldMask_t& dFields, const ISphQwordSetup& tSetup, const char* sUnit);
	~ExtUnit_c();

	virtual const ExtDoc_t* GetDocsChunk();
	virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);
	virtual void				Reset(const ISphQwordSetup& tSetup);
	virtual int					GetQwords(ExtQwordsHash_t& hQwords);
	virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
	virtual void				GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const;
	virtual uint64_t			GetWordID() const;

public:
	virtual bool GotHitless()
	{
		return false;
	}

	virtual void HintDocid(SphDocID_t uMinID)
	{
		m_pArg1->HintDocid(uMinID);
		m_pArg2->HintDocid(uMinID);
	}

	virtual void DebugDump(int iLevel)
	{
		DebugIndent(iLevel);
		printf("ExtSentence\n");
		m_pArg1->DebugDump(iLevel + 1);
		m_pArg2->DebugDump(iLevel + 1);
	}

protected:
	inline const ExtDoc_t* ReturnDocsChunk(int iDocs, int iMyHit)
	{
		assert(iMyHit < MAX_HITS);
		m_dMyHits[iMyHit].m_uDocid = DOCID_MAX;
		m_uHitsOverFor = 0;
		return ExtNode_i::ReturnDocsChunk(iDocs, "unit");
	}

protected:
	int					FilterHits(int iMyHit, DWORD uSentenceEnd, SphDocID_t uDocid, int* pDoc);
	void				SkipTailHits();

private:
	ExtNode_i* m_pArg1;				///< left arg
	ExtNode_i* m_pArg2;				///< right arg
	ExtTerm_c* m_pDot;					///< dot positions

	ExtHit_t			m_dMyHits[MAX_HITS];	///< matching hits buffer (inherited m_dHits will receive filtered results)
	SphDocID_t			m_uHitsOverFor;			///< no more hits for matches block starting with this ID
	SphDocID_t			m_uTailDocid;			///< trailing docid
	DWORD				m_uTailSentenceEnd;		///< trailing hits filtering state

	const ExtDoc_t* m_pDocs1;		///< last chunk start
	const ExtDoc_t* m_pDocs2;		///< last chunk start
	const ExtDoc_t* m_pDotDocs;		///< last chunk start
	const ExtDoc_t* m_pDoc1;		///< current in-chunk ptr
	const ExtDoc_t* m_pDoc2;		///< current in-chunk ptr
	const ExtDoc_t* m_pDotDoc;		///< current in-chunk ptr
	const ExtHit_t* m_pHit1;		///< current in-chunk ptr
	const ExtHit_t* m_pHit2;		///< current in-chunk ptr
	const ExtHit_t* m_pDotHit;		///< current in-chunk ptr
};

 ISphQword* CreateQueryWord(const XQKeyword_t& tWord, const ISphQwordSetup& tSetup, CSphDict* pZonesDict = NULL);

 }