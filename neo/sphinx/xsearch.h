#pragma once


#include "neo/sphinx/xquery.h"
#include "neo/sphinx/xudf.h"
#include "neo/sphinx/xqcache.h"
#include "neo/sphinx/xplugin.h"

#include "neo/core/iextra.h"
#include "neo/core/skip_list.h"
#include "neo/core/match.h"
#include "neo/core/match_engine.h"
#include "neo/core/ranker.h"
#include "neo/source/hitman.h"
#include "neo/query/query_stats.h"
#include "neo/query/extra.h"
#include "neo/query/enums.h"
#include "neo/source/hitman.h"

namespace NEO {

	/// term setup, searcher view
	class CSphQueryNodeCache;
	struct CSphQueryStats;
	class CSphQueryResult;
	class ExtNode_i;
	class ISphQwordSetup;


	//////////////////////////////////////////////////////////////////////////

	/// hit mark, used for snippets generation
	struct SphHitMark_t
	{
		DWORD	m_uPosition;
		DWORD	m_uSpan;

		bool operator == (const SphHitMark_t& rhs) const
		{
			return m_uPosition == rhs.m_uPosition && m_uSpan == rhs.m_uSpan;
		}
	};

	/// hit marker, used for snippets generation
	class CSphHitMarker
	{
	public:
		class ExtNode_i* m_pRoot;

	public:
		CSphHitMarker() : m_pRoot(NULL) {}
		~CSphHitMarker();

		void					Mark(CSphVector<SphHitMark_t>&);
		static CSphHitMarker* Create(const XQNode_t* pRoot, const ISphQwordSetup& tSetup);
	};

	//////////////////////////////////////////////////////////////////////////

	struct ExtPayloadEntry_t
	{
		SphDocID_t	m_uDocid;
		Hitpos_t	m_uHitpos;

		bool operator < (const ExtPayloadEntry_t& rhs) const
		{
			if (m_uDocid != rhs.m_uDocid)
				return (m_uDocid < rhs.m_uDocid);
			return (m_uHitpos < rhs.m_uHitpos);
		}
	};

	struct ExtPayloadKeyword_t : public XQKeyword_t
	{
		CSphString	m_sDictWord;
		SphWordID_t	m_uWordID;
		float		m_fIDF;
		int			m_iDocs;
		int			m_iHits;
	};

	/// simple in-memory multi-term cache
	class ExtPayload_c : public ExtNode_i
	{
	private:
		CSphVector<ExtPayloadEntry_t>	m_dCache;
		ExtPayloadKeyword_t				m_tWord;
		FieldMask_t						m_dFieldMask;

		int								m_iCurDocsEnd;		///< end of the last docs chunk returned, exclusive, ie [begin,end)
		int								m_iCurHit;			///< end of the last hits chunk (within the last docs chunk) returned, exclusive

		int64_t							m_iMaxTimer;		///< work until this timestamp
		CSphString* m_pWarning;

	public:
		explicit						ExtPayload_c(const XQNode_t* pNode, const ISphQwordSetup& tSetup);
		virtual void					Reset(const ISphQwordSetup& tSetup);
		virtual void					HintDocid(SphDocID_t) {} // FIXME!!! implement with tree
		virtual const ExtDoc_t* GetDocsChunk();
		virtual const ExtHit_t* GetHitsChunk(const ExtDoc_t* pDocs);

		virtual int						GetQwords(ExtQwordsHash_t& hQwords);
		virtual void					SetQwordsIDF(const ExtQwordsHash_t& hQwords);
		virtual void					GetTerms(const ExtQwordsHash_t&, CSphVector<TermPos_t>&) const;
		virtual bool					GotHitless() { return false; }
		virtual int						GetDocsCount() { return m_tWord.m_iDocs; }
		virtual uint64_t				GetWordID() const { return m_tWord.m_uWordID; }

	private:
		void							PopulateCache(const ISphQwordSetup& tSetup, bool bFillStat);
	};

//////////////////////////////////////////////////////////////////////
// RANKER
//////////////////////////////////////////////////////////////////////

	typedef CSphFixedVector < CSphVector < ZoneInfo_t > > ZoneVVector_t;

	/// ranker interface
	/// ranker folds incoming hitstream into simple match chunks, and computes relevance rank
	class ExtRanker_c : public ISphRanker, public ISphZoneCheck
	{
	public:
		ExtRanker_c(const XQQuery_t& tXQ, const ISphQwordSetup& tSetup);
		virtual						~ExtRanker_c();
		virtual void				Reset(const ISphQwordSetup& tSetup);

		virtual CSphMatch* GetMatchesBuffer() { return m_dMatches; }
		virtual const ExtDoc_t* GetFilteredDocs();

		int							GetQwords(ExtQwordsHash_t& hQwords) { return m_pRoot ? m_pRoot->GetQwords(hQwords) : -1; }
		virtual void				SetQwordsIDF(const ExtQwordsHash_t& hQwords);
		virtual void				SetTermDupes(const ExtQwordsHash_t&, int) {}
		virtual bool				InitState(const CSphQueryContext&, CSphString&) { return true; }

		virtual void				FinalizeCache(const ISphSchema& tSorterSchema);

	public:
		// FIXME? hide and friend?
		virtual SphZoneHit_e		IsInZone(int iZone, const ExtHit_t* pHit, int* pLastSpan);
		virtual const CSphIndex* GetIndex() { return m_pIndex; }

	public:
		CSphMatch					m_dMatches[ExtNode_i::MAX_DOCS];	///< exposed for caller
		DWORD						m_uPayloadMask;						///< exposed for ranker state functors
		int							m_iQwords;							///< exposed for ranker state functors
		int							m_iMaxQpos;							///< max in-query pos among all keywords, including dupes; for ranker state functors

	protected:
		void						CleanupZones(SphDocID_t uMaxDocid);
		int							m_iInlineRowitems;
		ExtNode_i* m_pRoot;
		const ExtDoc_t* m_pDoclist;
		const ExtHit_t* m_pHitlist;
		ExtDoc_t					m_dMyDocs[ExtNode_i::MAX_DOCS];		///< my local documents pool; for filtering
		CSphMatch					m_dMyMatches[ExtNode_i::MAX_DOCS];	///< my local matches pool; for filtering
		CSphMatch					m_tTestMatch;
		const CSphIndex* m_pIndex;							///< this is he who'll do my filtering!
		CSphQueryContext* m_pCtx;
		int64_t* m_pNanoBudget;
		QcacheEntry_c* m_pQcacheEntry;						///< data to cache if we decide that the current query is worth caching

	protected:
		CSphVector<CSphString>		m_dZones;
		CSphVector<ExtTerm_c*>		m_dZoneStartTerm;
		CSphVector<ExtTerm_c*>		m_dZoneEndTerm;
		CSphVector<const ExtDoc_t*>	m_dZoneStart;
		CSphVector<const ExtDoc_t*>	m_dZoneEnd;
		CSphVector<SphDocID_t>		m_dZoneMax;				///< last docid we (tried) to cache
		CSphVector<SphDocID_t>		m_dZoneMin;				///< first docid we (tried) to cache
		ZoneVVector_t				m_dZoneInfo;
		bool						m_bZSlist;

	protected:
		void						UpdateQcache(int iMatches);
	};


	STATIC_ASSERT((8 * 8 * sizeof(DWORD)) >= SPH_MAX_FIELDS, PAYLOAD_MASK_OVERFLOW);

	static const bool WITH_BM25 = true;

	template < bool USE_BM25 = false >
	class ExtRanker_WeightSum_c : public ExtRanker_c
	{
	protected:
		int				m_iWeights;
		const int* m_pWeights;

	public:
		ExtRanker_WeightSum_c(const XQQuery_t& tXQ, const ISphQwordSetup& tSetup) : ExtRanker_c(tXQ, tSetup) {}
		virtual int		GetMatches();

		virtual bool InitState(const CSphQueryContext& tCtx, CSphString&)
		{
			m_iWeights = tCtx.m_iWeights;
			m_pWeights = tCtx.m_dWeights;
			return true;
		}
	};


	class ExtRanker_None_c : public ExtRanker_c
	{
	public:
		ExtRanker_None_c(const XQQuery_t& tXQ, const ISphQwordSetup& tSetup) : ExtRanker_c(tXQ, tSetup) {}
		virtual int		GetMatches();
	};


	template < typename STATE >
	class ExtRanker_T : public ExtRanker_c
	{
	protected:
		STATE			m_tState;
		const ExtHit_t* m_pHitBase;
		CSphVector<int>		m_dZonespans; // zonespanlists for my matches

	public:
		ExtRanker_T(const XQQuery_t& tXQ, const ISphQwordSetup& tSetup);
		virtual int		GetMatches();

		virtual bool InitState(const CSphQueryContext& tCtx, CSphString& sError)
		{
			return m_tState.Init(tCtx.m_iWeights, &tCtx.m_dWeights[0], this, sError, tCtx.m_uPackedFactorFlags);
		}
	private:
		virtual bool ExtraDataImpl(ExtraData_e eType, void** ppResult)
		{
			switch (eType)
			{
			case EXTRA_GET_DATA_ZONESPANS:
				assert(ppResult);
				*ppResult = &m_dZonespans;
				return true;
			default:
				return m_tState.ExtraData(eType, ppResult);
			}
		}
	};

	//////////////////////////////////////////////////////////////////////////

	struct QuorumDupeNodeHash_t
	{
		uint64_t m_uWordID;
		int m_iIndex;

		bool operator < (const QuorumDupeNodeHash_t& b) const
		{
			if (m_uWordID == b.m_uWordID)
				return m_iIndex < b.m_iIndex;
			else
				return m_uWordID < b.m_uWordID;
		}
	};

	struct QuorumNodeAtomPos_fn
	{
		inline bool IsLess(const ExtQuorum_c::TermTuple_t& a, const ExtQuorum_c::TermTuple_t& b) const
		{
			return a.m_pTerm->m_iAtomPos < b.m_pTerm->m_iAtomPos;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	template < bool USE_BM25, bool HANDLE_DUPES >
	struct RankerState_Proximity_fn : public ISphExtra
	{
		BYTE m_uLCS[SPH_MAX_FIELDS];
		BYTE m_uCurLCS;
		int m_iExpDelta;
		int m_iLastHitPosWithField;
		int m_iFields;
		const int* m_pWeights;

		DWORD m_uLcsTailPos;
		DWORD m_uLcsTailQposMask;
		DWORD m_uCurQposMask;
		DWORD m_uCurPos;

		bool Init(int iFields, const int* pWeights, ExtRanker_c*, CSphString&, DWORD)
		{
			memset(m_uLCS, 0, sizeof(m_uLCS));
			m_uCurLCS = 0;
			m_iExpDelta = -INT_MAX;
			m_iLastHitPosWithField = -INT_MAX;
			m_iFields = iFields;
			m_pWeights = pWeights;

			m_uLcsTailPos = 0;
			m_uLcsTailQposMask = 0;
			m_uCurQposMask = 0;
			m_uCurPos = 0;

			return true;
		}

		void Update(const ExtHit_t* pHlist)
		{
			if_const(!HANDLE_DUPES)
			{
				// all query keywords are unique
				// simpler path (just do the delta)
				const int iPosWithField = HITMAN::GetPosWithField(pHlist->m_uHitpos);
				int iDelta = iPosWithField - pHlist->m_uQuerypos;
				if (iPosWithField > m_iLastHitPosWithField)
					m_uCurLCS = ((iDelta == m_iExpDelta) ? m_uCurLCS : 0) + BYTE(pHlist->m_uWeight);

				DWORD uField = HITMAN::GetField(pHlist->m_uHitpos);
				if (m_uCurLCS > m_uLCS[uField])
					m_uLCS[uField] = m_uCurLCS;

				m_iLastHitPosWithField = iPosWithField;
				m_iExpDelta = iDelta + pHlist->m_uSpanlen - 1; // !COMMIT why spanlen??
			}
 else
		{
		// keywords are duplicated in the query
		// so there might be multiple qpos entries sharing the same hitpos
		DWORD uPos = HITMAN::GetPosWithField(pHlist->m_uHitpos);
		DWORD uField = HITMAN::GetField(pHlist->m_uHitpos);

		// reset accumulated data from previous field
		if ((DWORD)HITMAN::GetField(m_uCurPos) != uField)
			m_uCurQposMask = 0;

		if (uPos != m_uCurPos)
		{
			// next new and shiny hitpos in line
			// FIXME!? what do we do with longer spans? keep looking? reset?
			if (m_uCurLCS < 2)
			{
				m_uLcsTailPos = m_uCurPos;
				m_uLcsTailQposMask = m_uCurQposMask;
				m_uCurLCS = 1; // FIXME!? can this ever be different? ("a b" c) maybe..
			}
			m_uCurQposMask = 0;
			m_uCurPos = uPos;
			if (m_uLCS[uField] < pHlist->m_uWeight)
				m_uLCS[uField] = BYTE(pHlist->m_uWeight);
		}

		// add that qpos to current qpos mask (for the current hitpos)
		m_uCurQposMask |= (1UL << pHlist->m_uQuerypos);

		// and check if that results in a better lcs match now
		int iDelta = m_uCurPos - m_uLcsTailPos;
		if (iDelta && iDelta < 32 && (m_uCurQposMask >> iDelta) & m_uLcsTailQposMask)
		{
			// cool, it matched!
			m_uLcsTailQposMask = (1UL << pHlist->m_uQuerypos); // our lcs span now ends with a specific qpos
			m_uLcsTailPos = m_uCurPos; // and in a specific position
			m_uCurLCS = BYTE(m_uCurLCS + pHlist->m_uWeight); // and it's longer
			m_uCurQposMask = 0; // and we should avoid matching subsequent hits on the same hitpos

			// update per-field vector
			if (m_uCurLCS > m_uLCS[uField])
				m_uLCS[uField] = m_uCurLCS;
		}
		}
		}

		DWORD Finalize(const CSphMatch& tMatch)
		{
			m_uCurLCS = 0;
			m_iExpDelta = -1;
			m_iLastHitPosWithField = -1;

			if_const(HANDLE_DUPES)
			{
				m_uLcsTailPos = 0;
				m_uLcsTailQposMask = 0;
				m_uCurQposMask = 0;
				m_uCurPos = 0;
			}

			DWORD uRank = 0;
			for (int i = 0; i < m_iFields; i++)
			{
				uRank += m_uLCS[i] * m_pWeights[i];
				m_uLCS[i] = 0;
			}

			return USE_BM25 ? tMatch.m_iWeight + uRank * SPH_BM25_SCALE : uRank;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	// sph04, proximity + exact boost
	struct RankerState_ProximityBM25Exact_fn : public ISphExtra
	{
		BYTE m_uLCS[SPH_MAX_FIELDS];
		BYTE m_uCurLCS;
		int m_iExpDelta;
		int m_iLastHitPos;
		DWORD m_uMinExpPos;
		int m_iFields;
		const int* m_pWeights;
		DWORD m_uHeadHit;
		DWORD m_uExactHit;
		int m_iMaxQuerypos;

		bool Init(int iFields, const int* pWeights, ExtRanker_c* pRanker, CSphString&, DWORD)
		{
			memset(m_uLCS, 0, sizeof(m_uLCS));
			m_uCurLCS = 0;
			m_iExpDelta = -INT_MAX;
			m_iLastHitPos = -1;
			m_uMinExpPos = 0;
			m_iFields = iFields;
			m_pWeights = pWeights;
			m_uHeadHit = 0;
			m_uExactHit = 0;

			// tricky bit
			// in expr and export rankers, this gets handled by the overridden (!) SetQwordsIDF()
			// but in all the other ones, we need this, because SetQwordsIDF() won't touch the state by default
			// FIXME? this is actually MaxUniqueQpos, queries like [foo|foo|foo] might break
			m_iMaxQuerypos = pRanker->m_iMaxQpos;
			return true;
		}

		void Update(const ExtHit_t* pHlist)
		{
			// upd LCS
			DWORD uField = HITMAN::GetField(pHlist->m_uHitpos);
			int iPosWithField = HITMAN::GetPosWithField(pHlist->m_uHitpos);
			int iDelta = iPosWithField - pHlist->m_uQuerypos;

			if (iDelta == m_iExpDelta && HITMAN::GetPosWithField(pHlist->m_uHitpos) >= m_uMinExpPos)
			{
				if (iPosWithField > m_iLastHitPos)
					m_uCurLCS = (BYTE)(m_uCurLCS + pHlist->m_uWeight);
				if (HITMAN::IsEnd(pHlist->m_uHitpos)
					&& (int)pHlist->m_uQuerypos == m_iMaxQuerypos
					&& HITMAN::GetPos(pHlist->m_uHitpos) == m_iMaxQuerypos)
				{
					m_uExactHit |= (1UL << HITMAN::GetField(pHlist->m_uHitpos));
				}
			}
			else
			{
				if (iPosWithField > m_iLastHitPos)
					m_uCurLCS = BYTE(pHlist->m_uWeight);
				if (HITMAN::GetPos(pHlist->m_uHitpos) == 1)
				{
					m_uHeadHit |= (1UL << HITMAN::GetField(pHlist->m_uHitpos));
					if (HITMAN::IsEnd(pHlist->m_uHitpos) && m_iMaxQuerypos == 1)
						m_uExactHit |= (1UL << HITMAN::GetField(pHlist->m_uHitpos));
				}
			}

			if (m_uCurLCS > m_uLCS[uField])
				m_uLCS[uField] = m_uCurLCS;

			m_iExpDelta = iDelta + pHlist->m_uSpanlen - 1;
			m_iLastHitPos = iPosWithField;
			m_uMinExpPos = HITMAN::GetPosWithField(pHlist->m_uHitpos) + 1;
		}

		DWORD Finalize(const CSphMatch& tMatch)
		{
			m_uCurLCS = 0;
			m_iExpDelta = -1;
			m_iLastHitPos = -1;

			DWORD uRank = 0;
			for (int i = 0; i < m_iFields; i++)
			{
				uRank += (4 * m_uLCS[i] + 2 * ((m_uHeadHit >> i) & 1) + ((m_uExactHit >> i) & 1)) * m_pWeights[i];
				m_uLCS[i] = 0;
			}
			m_uHeadHit = 0;
			m_uExactHit = 0;

			return tMatch.m_iWeight + uRank * SPH_BM25_SCALE;
		}
	};


	template < bool USE_BM25 >
	struct RankerState_ProximityPayload_fn : public RankerState_Proximity_fn<USE_BM25, false>
	{
		DWORD m_uPayloadRank;
		DWORD m_uPayloadMask;

		bool Init(int iFields, const int* pWeights, ExtRanker_c* pRanker, CSphString& sError, DWORD)
		{
			RankerState_Proximity_fn<USE_BM25, false>::Init(iFields, pWeights, pRanker, sError, false);
			m_uPayloadRank = 0;
			m_uPayloadMask = pRanker->m_uPayloadMask;
			return true;
		}

		void Update(const ExtHit_t* pHlist)
		{
			DWORD uField = HITMAN::GetField(pHlist->m_uHitpos);
			if ((1 << uField) & m_uPayloadMask)
				this->m_uPayloadRank += HITMAN::GetPos(pHlist->m_uHitpos) * this->m_pWeights[uField];
			else
				RankerState_Proximity_fn<USE_BM25, false>::Update(pHlist);
		}

		DWORD Finalize(const CSphMatch& tMatch)
		{
			// as usual, redundant 'this' is just because gcc is stupid
			this->m_uCurLCS = 0;
			this->m_iExpDelta = -1;
			this->m_iLastHitPosWithField = -1;

			DWORD uRank = m_uPayloadRank;
			for (int i = 0; i < this->m_iFields; i++)
			{
				// no special care for payload fields as their LCS will be 0 anyway
				uRank += this->m_uLCS[i] * this->m_pWeights[i];
				this->m_uLCS[i] = 0;
			}

			m_uPayloadRank = 0;
			return USE_BM25 ? tMatch.m_iWeight + uRank * SPH_BM25_SCALE : uRank;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	struct RankerState_MatchAny_fn : public RankerState_Proximity_fn<false, false>
	{
		int m_iPhraseK;
		BYTE m_uMatchMask[SPH_MAX_FIELDS];

		bool Init(int iFields, const int* pWeights, ExtRanker_c* pRanker, CSphString& sError, DWORD)
		{
			RankerState_Proximity_fn<false, false>::Init(iFields, pWeights, pRanker, sError, false);
			m_iPhraseK = 0;
			for (int i = 0; i < iFields; i++)
				m_iPhraseK += pWeights[i] * pRanker->m_iQwords;
			memset(m_uMatchMask, 0, sizeof(m_uMatchMask));
			return true;
		}

		void Update(const ExtHit_t* pHlist)
		{
			RankerState_Proximity_fn<false, false>::Update(pHlist);
			m_uMatchMask[HITMAN::GetField(pHlist->m_uHitpos)] |= (1 << (pHlist->m_uQuerypos - 1));
		}

		DWORD Finalize(const CSphMatch&)
		{
			m_uCurLCS = 0;
			m_iExpDelta = -1;
			m_iLastHitPosWithField = -1;

			DWORD uRank = 0;
			for (int i = 0; i < m_iFields; i++)
			{
				if (m_uMatchMask[i])
					uRank += (sphBitCount(m_uMatchMask[i]) + (m_uLCS[i] - 1) * m_iPhraseK) * m_pWeights[i];
				m_uMatchMask[i] = 0;
				m_uLCS[i] = 0;
			}

			return uRank;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	struct RankerState_Wordcount_fn : public ISphExtra
	{
		DWORD m_uRank;
		int m_iFields;
		const int* m_pWeights;

		bool Init(int iFields, const int* pWeights, ExtRanker_c*, CSphString&, DWORD)
		{
			m_uRank = 0;
			m_iFields = iFields;
			m_pWeights = pWeights;
			return true;
		}

		void Update(const ExtHit_t* pHlist)
		{
			m_uRank += m_pWeights[HITMAN::GetField(pHlist->m_uHitpos)];
		}

		DWORD Finalize(const CSphMatch&)
		{
			DWORD uRes = m_uRank;
			m_uRank = 0;
			return uRes;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	struct RankerState_Fieldmask_fn : public ISphExtra
	{
		DWORD m_uRank;

		bool Init(int, const int*, ExtRanker_c*, CSphString&, DWORD)
		{
			m_uRank = 0;
			return true;
		}

		void Update(const ExtHit_t* pHlist)
		{
			m_uRank |= 1UL << HITMAN::GetField(pHlist->m_uHitpos);
		}

		DWORD Finalize(const CSphMatch&)
		{
			DWORD uRes = m_uRank;
			m_uRank = 0;
			return uRes;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	struct RankerState_Plugin_fn : public ISphExtra
	{
		RankerState_Plugin_fn()
			: m_pData(NULL)
			, m_pPlugin(NULL)
		{}

		~RankerState_Plugin_fn()
		{
			assert(m_pPlugin);
			if (m_pPlugin->m_fnDeinit)
				m_pPlugin->m_fnDeinit(m_pData);
			m_pPlugin->Release();
		}

		bool Init(int iFields, const int* pWeights, ExtRanker_c* pRanker, CSphString& sError, DWORD)
		{
			if (!m_pPlugin->m_fnInit)
				return true;

			SPH_RANKER_INIT r;
			r.num_field_weights = iFields;
			r.field_weights = const_cast<int*>(pWeights);
			r.options = m_sOptions.cstr();
			r.payload_mask = pRanker->m_uPayloadMask;
			r.num_query_words = pRanker->m_iQwords;
			r.max_qpos = pRanker->m_iMaxQpos;

			char sErrorBuf[SPH_UDF_ERROR_LEN];
			if (m_pPlugin->m_fnInit(&m_pData, &r, sErrorBuf) == 0)
				return true;
			sError = sErrorBuf;
			return false;
		}

		void Update(const ExtHit_t* p)
		{
			if (!m_pPlugin->m_fnUpdate)
				return;

			SPH_RANKER_HIT h;
			h.doc_id = p->m_uDocid;
			h.hit_pos = p->m_uHitpos;
			h.query_pos = p->m_uQuerypos;
			h.node_pos = p->m_uNodepos;
			h.span_length = p->m_uSpanlen;
			h.match_length = p->m_uMatchlen;
			h.weight = p->m_uWeight;
			h.query_pos_mask = p->m_uQposMask;
			m_pPlugin->m_fnUpdate(m_pData, &h);
		}

		DWORD Finalize(const CSphMatch& tMatch)
		{
			// at some point in the future, we might start passing the entire match,
			// with blackjack, hookers, attributes, and their schema; but at this point,
			// the only sort-of useful part of a match that we are able to push down to
			// the ranker plugin is the match weight
			return m_pPlugin->m_fnFinalize(m_pData, tMatch.m_iWeight);
		}

	private:
		void* m_pData;
		const PluginRanker_c* m_pPlugin;
		CSphString				m_sOptions;

		virtual bool ExtraDataImpl(ExtraData_e eType, void** ppResult)
		{
			switch (eType)
			{
			case EXTRA_SET_RANKER_PLUGIN:		m_pPlugin = (const PluginRanker_c*)ppResult; break;
			case EXTRA_SET_RANKER_PLUGIN_OPTS:	m_sOptions = (char*)ppResult; break;
			default:							return false;
			}
			return true;
		}
	};

	//////////////////////////////////////////////////////////////////////////

	class FactorPool_c
	{
	public:
		FactorPool_c();

		void			Prealloc(int iElementSize, int nElements);
		BYTE* Alloc();
		void			Free(BYTE* pPtr);
		int				GetElementSize() const;
		int				GetIntElementSize() const;
		void			AddToHash(SphDocID_t uId, BYTE* pPacked);
		void			AddRef(SphDocID_t uId);
		void			Release(SphDocID_t uId);
		void			Flush();

		bool			IsInitialized() const;
		SphFactorHash_t* GetHashPtr();

	private:
		int				m_iElementSize;

		CSphFixedVector<BYTE>	m_dPool;
		SphFactorHash_t			m_dHash;
		CSphFreeList			m_dFree;
		SphFactorHashEntry_t* Find(SphDocID_t uId) const;
		inline DWORD	HashFunc(SphDocID_t uId) const;
		bool			FlushEntry(SphFactorHashEntry_t* pEntry);
	};


// EXPRESSION RANKER
//////////////////////////////////////////////////////////////////////////

/// lean hit
/// only stores keyword id and hit position
	struct LeanHit_t
	{
		WORD		m_uQuerypos;
		Hitpos_t	m_uHitpos;

		LeanHit_t& operator = (const ExtHit_t& rhs)
		{
			m_uQuerypos = rhs.m_uQuerypos;
			m_uHitpos = rhs.m_uHitpos;
			return *this;
		}
	};

	/// ranker state that computes weight dynamically based on user supplied expression (formula)
	template < bool NEED_PACKEDFACTORS = false, bool HANDLE_DUPES = false >
	struct RankerState_Expr_fn : public ISphExtra
	{
	public:
		// per-field and per-document stuff
		BYTE				m_uLCS[SPH_MAX_FIELDS];
		BYTE				m_uCurLCS;
		DWORD				m_uCurPos;
		DWORD				m_uLcsTailPos;
		DWORD				m_uLcsTailQposMask;
		DWORD				m_uCurQposMask;
		int					m_iExpDelta;
		int					m_iLastHitPos;
		int					m_iFields;
		const int* m_pWeights;
		DWORD				m_uDocBM25;
		CSphBitvec			m_tMatchedFields;
		int					m_iCurrentField;
		DWORD				m_uHitCount[SPH_MAX_FIELDS];
		DWORD				m_uWordCount[SPH_MAX_FIELDS];
		CSphVector<float>	m_dIDF;
		float				m_dTFIDF[SPH_MAX_FIELDS];
		float				m_dMinIDF[SPH_MAX_FIELDS];
		float				m_dMaxIDF[SPH_MAX_FIELDS];
		float				m_dSumIDF[SPH_MAX_FIELDS];
		int					m_iMinHitPos[SPH_MAX_FIELDS];
		int					m_iMinBestSpanPos[SPH_MAX_FIELDS];
		CSphBitvec			m_tExactHit;
		CSphBitvec			m_tExactOrder;
		CSphBitvec			m_tKeywords;
		DWORD				m_uDocWordCount;
		int					m_iMaxWindowHits[SPH_MAX_FIELDS];
		CSphVector<int>		m_dTF;			///< for bm25a
		float				m_fDocBM25A;	///< for bm25a
		CSphVector<int>		m_dFieldTF;		///< for bm25f, per-field layout (ie all field0 tfs, then all field1 tfs, etc)
		int					m_iMinGaps[SPH_MAX_FIELDS];		///< number of gaps in the minimum matching window

		const char* m_sExpr;
		ISphExpr* m_pExpr;
		ESphAttr			m_eExprType;
		const CSphSchema* m_pSchema;
		CSphAttrLocator		m_tFieldLensLoc;
		float				m_fAvgDocLen;
		const int64_t* m_pFieldLens;
		int64_t				m_iTotalDocuments;
		float				m_fParamK1;
		float				m_fParamB;
		int					m_iMaxQpos;			///< among all words, including dupes
		CSphVector<WORD>	m_dTermDupes;
		CSphVector<Hitpos_t>	m_dTermsHit;
		CSphBitvec			m_tHasMultiQpos;
		int					m_uLastSpanStart;

		FactorPool_c 		m_tFactorPool;
		int					m_iPoolMatchCapacity;

		// per-query stuff
		int					m_iMaxLCS;
		int					m_iQueryWordCount;

	public:
		// internal state, and factor settings
		// max_window_hits(n)
		CSphVector<DWORD>	m_dWindow;
		int					m_iWindowSize;

		// min_gaps
		int						m_iHaveMinWindow;			///< whether to compute minimum matching window, and over how many query words
		int						m_iMinWindowWords;			///< how many unique words have we seen so far
		CSphVector<LeanHit_t>	m_dMinWindowHits;			///< current minimum matching window candidate hits
		CSphVector<int>			m_dMinWindowCounts;			///< maps querypos indexes to number of occurrencess in m_dMinWindowHits

		// exact_order
		int					m_iLastField;
		int					m_iLastQuerypos;
		int					m_iExactOrderWords;

		// LCCS and Weighted LCCS
		BYTE				m_dLCCS[SPH_MAX_FIELDS];
		float				m_dWLCCS[SPH_MAX_FIELDS];
		CSphVector<WORD>	m_dNextQueryPos;				///< words positions might have gaps due to stop-words
		WORD				m_iQueryPosLCCS;
		int					m_iHitPosLCCS;
		BYTE				m_iLenLCCS;
		float				m_fWeightLCCS;

		// ATC
#define XRANK_ATC_WINDOW_LEN 10
#define XRANK_ATC_BUFFER_LEN 30
#define XRANK_ATC_DUP_DIV 0.25f
#define XRANK_ATC_EXP 1.75f
		struct AtcHit_t
		{
			int			m_iHitpos;
			WORD		m_uQuerypos;
		};
		AtcHit_t			m_dAtcHits[XRANK_ATC_BUFFER_LEN];	///< ATC hits ring buffer
		int					m_iAtcHitStart;						///< hits start at ring buffer
		int					m_iAtcHitCount;						///< hits amount in buffer
		CSphVector<float>	m_dAtcTerms;						///< per-word ATC
		CSphBitvec			m_dAtcProcessedTerms;				///< temporary processed mask
		DWORD				m_uAtcField;						///< currently processed field
		float				m_dAtc[SPH_MAX_FIELDS];				///< ATC per-field values
		bool				m_bAtcHeadProcessed;				///< flag for hits from buffer start to window start
		bool				m_bHaveAtc;							///< calculate ATC?
		bool				m_bWantAtc;

		void				UpdateATC(bool bFlushField);
		float				TermTC(int iTerm, bool bLeft);

	public:
		RankerState_Expr_fn();
		~RankerState_Expr_fn();

		bool				Init(int iFields, const int* pWeights, ExtRanker_c* pRanker, CSphString& sError, DWORD uFactorFlags);
		void				Update(const ExtHit_t* pHlist);
		DWORD				Finalize(const CSphMatch& tMatch);
		bool				IsTermSkipped(int iTerm);

	public:
		/// setup per-keyword data needed to compute the factors
		/// (namely IDFs, counts, masks etc)
		/// WARNING, CALLED EVEN BEFORE INIT()!
		void SetQwords(const ExtQwordsHash_t& hQwords)
		{
			m_dIDF.Resize(m_iMaxQpos + 1); // [MaxUniqQpos, MaxQpos] range will be all 0, but anyway
			m_dIDF.Fill(0.0f);

			m_dTF.Resize(m_iMaxQpos + 1);
			m_dTF.Fill(0);

			m_dMinWindowCounts.Resize(m_iMaxQpos + 1);
			m_dMinWindowCounts.Fill(0);

			m_iQueryWordCount = 0;
			m_tKeywords.Init(m_iMaxQpos + 1); // will not be tracking dupes
			bool bGotExpanded = false;
			CSphVector<WORD> dQueryPos;
			dQueryPos.Reserve(m_iMaxQpos + 1);

			hQwords.IterateStart();
			while (hQwords.IterateNext())
			{
				// tricky bit
				// for query_word_count, we only want to count keywords that are not (!) excluded by the query
				// that is, in (aa NOT bb) case, we want a value of 1, not 2
				// there might be tail excluded terms these not affected MaxQpos
				ExtQword_t& dCur = hQwords.IterateGet();
				const int iQueryPos = dCur.m_iQueryPos;
				if (dCur.m_bExcluded)
					continue;

				bool bQposUsed = m_tKeywords.BitGet(iQueryPos);
				bGotExpanded |= bQposUsed;
				m_iQueryWordCount += (bQposUsed ? 0 : 1); // count only one term at that position
				m_tKeywords.BitSet(iQueryPos); // just to assert at early stage!

				m_dIDF[iQueryPos] += dCur.m_fIDF;
				m_dTF[iQueryPos]++;
				if (!bQposUsed)
					dQueryPos.Add((WORD)iQueryPos);
			}

			// FIXME!!! average IDF for expanded terms (aot morphology or dict=keywords)
			if (bGotExpanded)
				ARRAY_FOREACH(i, m_dTF)
			{
				if (m_dTF[i] > 1)
					m_dIDF[i] /= m_dTF[i];
			}

			m_dTF.Fill(0);

			// set next term position for current term in query (degenerates to +1 in the simplest case)
			dQueryPos.Sort();
			m_dNextQueryPos.Resize(m_iMaxQpos + 1);
			m_dNextQueryPos.Fill((WORD)-1); // WORD_MAX filler
			for (int i = 0; i < dQueryPos.GetLength() - 1; i++)
			{
				WORD iCutPos = dQueryPos[i];
				WORD iNextPos = dQueryPos[i + 1];
				m_dNextQueryPos[iCutPos] = iNextPos;
			}
		}

		void SetTermDupes(const ExtQwordsHash_t& hQwords, int iMaxQpos, const ExtNode_i* pRoot)
		{
			if (!pRoot)
				return;

			m_dTermsHit.Resize(iMaxQpos + 1);
			m_dTermsHit.Fill(EMPTY_HIT);
			m_tHasMultiQpos.Init(iMaxQpos + 1);
			m_dTermDupes.Resize(iMaxQpos + 1);
			m_dTermDupes.Fill((WORD)-1);

			CSphVector<TermPos_t> dTerms;
			dTerms.Reserve(iMaxQpos);
			pRoot->GetTerms(hQwords, dTerms);

			// reset excluded for all duplicates
			ARRAY_FOREACH(i, dTerms)
			{
				WORD uAtomPos = dTerms[i].m_uAtomPos;
				WORD uQpos = dTerms[i].m_uQueryPos;
				if (uAtomPos != uQpos)
				{
					m_tHasMultiQpos.BitSet(uAtomPos);
					m_tHasMultiQpos.BitSet(uQpos);
				}

				m_tKeywords.BitSet(uAtomPos);
				m_tKeywords.BitSet(uQpos);
				m_dTermDupes[uAtomPos] = uQpos;

				// fill missed idf for dups
				if (fabs(m_dIDF[uAtomPos]) <= 1e-6)
					m_dIDF[uAtomPos] = m_dIDF[uQpos];
			}
		}

		/// finalize per-document factors that, well, need finalization
		void FinalizeDocFactors(const CSphMatch& tMatch)
		{
			m_uDocBM25 = tMatch.m_iWeight;
			for (int i = 0; i < m_iFields; i++)
			{
				m_uWordCount[i] = sphBitCount(m_uWordCount[i]);
				if (m_dMinIDF[i] > m_dMaxIDF[i])
					m_dMinIDF[i] = m_dMaxIDF[i] = 0; // must be FLT_MAX vs -FLT_MAX, aka no hits
			}
			m_uDocWordCount = sphBitCount(m_uDocWordCount);

			// compute real BM25
			// with blackjack, and hookers, and field lengths, and parameters
			//
			// canonical idf = log ( (N-n+0.5) / (n+0.5) )
			// sphinx idf = log ( (N-n+1) / n )
			// and we also downscale our idf by 1/log(N+1) to map it into [-0.5, 0.5] range

			// compute document length
			float dl = 0; // OPTIMIZE? could precompute and store total dl in attrs, but at a storage cost
			CSphAttrLocator tLoc = m_tFieldLensLoc;
			if (tLoc.m_iBitOffset >= 0)
				for (int i = 0; i < m_iFields; i++)
				{
					dl += tMatch.GetAttr(tLoc);
					tLoc.m_iBitOffset += 32;
				}

			// compute BM25A (one value per document)
			m_fDocBM25A = 0.0f;
			for (int iWord = 1; iWord <= m_iMaxQpos; iWord++)
			{
				if (IsTermSkipped(iWord))
					continue;

				float tf = (float)m_dTF[iWord]; // OPTIMIZE? remove this vector, hook into m_uMatchHits somehow?
				float idf = m_dIDF[iWord];
				m_fDocBM25A += tf / (tf + m_fParamK1 * (1 - m_fParamB + m_fParamB * dl / m_fAvgDocLen)) * idf;
			}
			m_fDocBM25A += 0.5f; // map to [0..1] range
		}

		/// reset per-document factors, prepare for the next document
		void ResetDocFactors()
		{
			// OPTIMIZE? quick full wipe? (using dwords/sse/whatever)
			m_uCurLCS = 0;
			if_const(HANDLE_DUPES)
			{
				m_uCurPos = 0;
				m_uLcsTailPos = 0;
				m_uLcsTailQposMask = 0;
				m_uCurQposMask = 0;
				m_uLastSpanStart = 0;
			}
			m_iExpDelta = -1;
			m_iLastHitPos = -1;
			for (int i = 0; i < m_iFields; i++)
			{
				m_uLCS[i] = 0;
				m_uHitCount[i] = 0;
				m_uWordCount[i] = 0;
				m_dMinIDF[i] = FLT_MAX;
				m_dMaxIDF[i] = -FLT_MAX;
				m_dSumIDF[i] = 0;
				m_dTFIDF[i] = 0;
				m_iMinHitPos[i] = 0;
				m_iMinBestSpanPos[i] = 0;
				m_iMaxWindowHits[i] = 0;
				m_iMinGaps[i] = 0;
				m_dAtc[i] = 0.0f;
			}
			m_dTF.Fill(0);
			m_dFieldTF.Fill(0); // OPTIMIZE? make conditional?
			m_tMatchedFields.Clear();
			m_tExactHit.Clear();
			m_tExactOrder.Clear();
			m_uDocWordCount = 0;
			m_dWindow.Resize(0);
			m_fDocBM25A = 0;
			m_dMinWindowHits.Resize(0);
			m_dMinWindowCounts.Fill(0);
			m_iMinWindowWords = 0;
			m_iLastField = -1;
			m_iLastQuerypos = 0;
			m_iExactOrderWords = 0;

			m_dAtcTerms.Fill(0.0f);
			m_iAtcHitStart = 0;
			m_iAtcHitCount = 0;
			m_uAtcField = 0;

			if_const(HANDLE_DUPES)
				m_dTermsHit.Fill(EMPTY_HIT);
		}

		void FlushMatches()
		{
			m_tFactorPool.Flush();
		}

	protected:
		inline void UpdateGap(int iField, int iWords, int iGap)
		{
			if (m_iMinWindowWords < iWords || (m_iMinWindowWords == iWords && m_iMinGaps[iField] > iGap))
			{
				m_iMinGaps[iField] = iGap;
				m_iMinWindowWords = iWords;
			}
		}

		void			UpdateMinGaps(const ExtHit_t* pHlist);
		void			UpdateFreq(WORD uQpos, DWORD uField);

	private:
		virtual bool	ExtraDataImpl(ExtraData_e eType, void** ppResult);
		int				GetMaxPackedLength();
		BYTE* PackFactors();
	};

	/// extra expression ranker node types
	enum ExprRankerNode_e
	{
		// field level factors
		XRANK_LCS,
		XRANK_USER_WEIGHT,
		XRANK_HIT_COUNT,
		XRANK_WORD_COUNT,
		XRANK_TF_IDF,
		XRANK_MIN_IDF,
		XRANK_MAX_IDF,
		XRANK_SUM_IDF,
		XRANK_MIN_HIT_POS,
		XRANK_MIN_BEST_SPAN_POS,
		XRANK_EXACT_HIT,
		XRANK_EXACT_ORDER,
		XRANK_MAX_WINDOW_HITS,
		XRANK_MIN_GAPS,
		XRANK_LCCS,
		XRANK_WLCCS,
		XRANK_ATC,

		// document level factors
		XRANK_BM25,
		XRANK_MAX_LCS,
		XRANK_FIELD_MASK,
		XRANK_QUERY_WORD_COUNT,
		XRANK_DOC_WORD_COUNT,
		XRANK_BM25A,
		XRANK_BM25F,

		// field aggregation functions
		XRANK_SUM,
		XRANK_TOP
	};


	/// generic field factor
	template < typename T >
	struct Expr_FieldFactor_c : public ISphExpr
	{
		const int* m_pIndex;
		const T* m_pData;

		Expr_FieldFactor_c(const int* pIndex, const T* pData)
			: m_pIndex(pIndex)
			, m_pData(pData)
		{}

		float Eval(const CSphMatch&) const
		{
			return (float)m_pData[*m_pIndex];
		}

		int IntEval(const CSphMatch&) const
		{
			return (int)m_pData[*m_pIndex];
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// bitmask field factor specialization
	template<>
	struct Expr_FieldFactor_c<bool> : public ISphExpr
	{
		const int* m_pIndex;
		const DWORD* m_pData;

		Expr_FieldFactor_c(const int* pIndex, const DWORD* pData)
			: m_pIndex(pIndex)
			, m_pData(pData)
		{}

		float Eval(const CSphMatch&) const
		{
			return (float)((*m_pData) >> (*m_pIndex));
		}

		int IntEval(const CSphMatch&) const
		{
			return (int)((*m_pData) >> (*m_pIndex));
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// generic per-document int factor
	struct Expr_IntPtr_c : public ISphExpr
	{
		DWORD* m_pVal;

		explicit Expr_IntPtr_c(DWORD* pVal)
			: m_pVal(pVal)
		{}

		float Eval(const CSphMatch&) const
		{
			return (float)*m_pVal;
		}

		int IntEval(const CSphMatch&) const
		{
			return (int)*m_pVal;
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// per-document field mask factor
	struct Expr_FieldMask_c : public ISphExpr
	{
		const CSphBitvec& m_tFieldMask;

		explicit Expr_FieldMask_c(const CSphBitvec& tFieldMask)
			: m_tFieldMask(tFieldMask)
		{}

		float Eval(const CSphMatch&) const
		{
			return (float)*m_tFieldMask.Begin();
		}

		int IntEval(const CSphMatch&) const
		{
			return (int)*m_tFieldMask.Begin();
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// bitvec field factor specialization
	template<>
	struct Expr_FieldFactor_c<CSphBitvec> : public ISphExpr
	{
		const int* m_pIndex;
		const CSphBitvec& m_tField;

		Expr_FieldFactor_c(const int* pIndex, const CSphBitvec& tField)
			: m_pIndex(pIndex)
			, m_tField(tField)
		{}

		float Eval(const CSphMatch&) const
		{
			return (float)(m_tField.BitGet(*m_pIndex));
		}

		int IntEval(const CSphMatch&) const
		{
			return (int)(m_tField.BitGet(*m_pIndex));
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// generic per-document float factor
	struct Expr_FloatPtr_c : public ISphExpr
	{
		float* m_pVal;

		explicit Expr_FloatPtr_c(float* pVal)
			: m_pVal(pVal)
		{}

		float Eval(const CSphMatch&) const
		{
			return (float)*m_pVal;
		}

		int IntEval(const CSphMatch&) const
		{
			return (int)*m_pVal;
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};

	template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
	struct Expr_BM25F_T : public ISphExpr
	{
		RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* m_pState;
		float					m_fK1;
		float					m_fB;
		float					m_fWeightedAvgDocLen;
		CSphVector<int>			m_dWeights;		///< per field weights

		explicit Expr_BM25F_T(RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* pState, float k1, float b, ISphExpr* pFieldWeights)
		{
			// bind k1, b
			m_pState = pState;
			m_fK1 = k1;
			m_fB = b;

			// bind weights
			m_dWeights.Resize(pState->m_iFields);
			m_dWeights.Fill(1);
			if (pFieldWeights)
			{
				Expr_MapArg_c* pMapArg = (Expr_MapArg_c*)pFieldWeights;

				CSphVector<CSphNamedVariant>& dOpts = pMapArg->m_dValues;
				ARRAY_FOREACH(i, dOpts)
				{
					// FIXME? report errors if field was not found?
					if (!dOpts[i].m_sValue.IsEmpty())
						continue; // weights must be int, not string
					CSphString& sField = dOpts[i].m_sKey;
					int iField = pState->m_pSchema->GetFieldIndex(sField.cstr());
					if (iField >= 0)
						m_dWeights[iField] = dOpts[i].m_iValue;
				}
			}

			// compute weighted avgdl
			m_fWeightedAvgDocLen = 0;
			if (m_pState->m_pFieldLens)
				ARRAY_FOREACH(i, m_dWeights)
				m_fWeightedAvgDocLen += m_pState->m_pFieldLens[i] * m_dWeights[i];
			else
				m_fWeightedAvgDocLen = 1.0f;
			m_fWeightedAvgDocLen /= m_pState->m_iTotalDocuments;
		}

		float Eval(const CSphMatch& tMatch) const
		{
			// compute document length
			// OPTIMIZE? could precompute and store total dl in attrs, but at a storage cost
			// OPTIMIZE? could at least share between multiple BM25F instances, if there are many
			float dl = 0;
			CSphAttrLocator tLoc = m_pState->m_tFieldLensLoc;
			if (tLoc.m_iBitOffset >= 0)
				for (int i = 0; i < m_pState->m_iFields; i++)
				{
					dl += tMatch.GetAttr(tLoc) * m_dWeights[i];
					tLoc.m_iBitOffset += 32;
				}

			// compute (the current instance of) BM25F
			float fRes = 0.0f;
			for (int iWord = 1; iWord <= m_pState->m_iMaxQpos; iWord++)
			{
				if (m_pState->IsTermSkipped(iWord))
					continue;

				// compute weighted TF
				float tf = 0.0f;
				for (int i = 0; i < m_pState->m_iFields; i++)
					tf += m_pState->m_dFieldTF[iWord + i * (1 + m_pState->m_iMaxQpos)] * m_dWeights[i];
				float idf = m_pState->m_dIDF[iWord]; // FIXME? zeroed out for dupes!
				fRes += tf / (tf + m_fK1 * (1.0f - m_fB + m_fB * dl / m_fWeightedAvgDocLen)) * idf;
			}
			return fRes + 0.5f; // map to [0..1] range
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// function that sums sub-expressions over matched fields
	template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
	struct Expr_Sum_T : public ISphExpr
	{
		RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* m_pState;
		ISphExpr* m_pArg;

		Expr_Sum_T(RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* pState, ISphExpr* pArg)
			: m_pState(pState)
			, m_pArg(pArg)
		{}

		virtual ~Expr_Sum_T()
		{
			SafeRelease(m_pArg);
		}

		float Eval(const CSphMatch& tMatch) const
		{
			m_pState->m_iCurrentField = 0;
			float fRes = 0;
			const CSphBitvec& tFields = m_pState->m_tMatchedFields;
			int iBits = tFields.BitCount();
			while (iBits)
			{
				if (tFields.BitGet(m_pState->m_iCurrentField))
				{
					fRes += m_pArg->Eval(tMatch);
					iBits--;
				}
				m_pState->m_iCurrentField++;
			}
			return fRes;
		}

		int IntEval(const CSphMatch& tMatch) const
		{
			m_pState->m_iCurrentField = 0;
			int iRes = 0;
			const CSphBitvec& tFields = m_pState->m_tMatchedFields;
			int iBits = tFields.BitCount();
			while (iBits)
			{
				if (tFields.BitGet(m_pState->m_iCurrentField))
				{
					iRes += m_pArg->IntEval(tMatch);
					iBits--;
				}
				m_pState->m_iCurrentField++;
			}
			return iRes;
		}

		virtual void Command(ESphExprCommand eCmd, void* pArg)
		{
			assert(m_pArg);
			m_pArg->Command(eCmd, pArg);
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// aggregate max over matched fields
	template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
	struct Expr_Top_T : public ISphExpr
	{
		RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* m_pState;
		ISphExpr* m_pArg;

		Expr_Top_T(RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* pState, ISphExpr* pArg)
			: m_pState(pState)
			, m_pArg(pArg)
		{}

		virtual ~Expr_Top_T()
		{
			SafeRelease(m_pArg);
		}

		float Eval(const CSphMatch& tMatch) const
		{
			m_pState->m_iCurrentField = 0;
			float fRes = FLT_MIN;
			const CSphBitvec& tFields = m_pState->m_tMatchedFields;
			int iBits = tFields.BitCount();
			while (iBits)
			{
				if (tFields.BitGet(m_pState->m_iCurrentField))
				{
					fRes = Max(fRes, m_pArg->Eval(tMatch));
					iBits--;
				}
				m_pState->m_iCurrentField++;
			}
			return fRes;
		}

		int IntEval(const CSphMatch& tMatch) const
		{
			m_pState->m_iCurrentField = 0;
			int iRes = INT_MIN;
			const CSphBitvec& tFields = m_pState->m_tMatchedFields;
			int iBits = tFields.BitCount();
			while (iBits)
			{
				if (tFields.BitGet(m_pState->m_iCurrentField))
				{
					iRes = Max(iRes, m_pArg->IntEval(tMatch));
					iBits--;
				}
				m_pState->m_iCurrentField++;
			}
			return iRes;
		}

		virtual void Command(ESphExprCommand eCmd, void* pArg)
		{
			assert(m_pArg);
			m_pArg->Command(eCmd, pArg);
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	// FIXME! cut/pasted from sphinxexpr; remove dupe
	struct Expr_GetIntConst_Rank_c : public ISphExpr
	{
		int m_iValue;
		explicit Expr_GetIntConst_Rank_c(int iValue) : m_iValue(iValue) {}
		virtual float Eval(const CSphMatch&) const { return (float)m_iValue; } // no assert() here cause generic float Eval() needs to work even on int-evaluator tree
		virtual int IntEval(const CSphMatch&) const { return m_iValue; }
		virtual int64_t Int64Eval(const CSphMatch&) const { return m_iValue; }

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "ranker expressions in filters");
			return 0;
		}
	};


	/// hook that exposes field-level factors, document-level factors, and matched field SUM() function to generic expressions
	template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
	class ExprRankerHook_T : public ISphExprHook
	{
	public:
		RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* m_pState;
		const char* m_sCheckError;
		bool					m_bCheckInFieldAggr;

	public:
		explicit ExprRankerHook_T(RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>* pState)
			: m_pState(pState)
			, m_sCheckError(NULL)
			, m_bCheckInFieldAggr(false)
		{}

		int IsKnownIdent(const char* sIdent)
		{
			// OPTIMIZE? hash this some nice long winter night?
			if (!strcasecmp(sIdent, "lcs"))
				return XRANK_LCS;
			if (!strcasecmp(sIdent, "user_weight"))
				return XRANK_USER_WEIGHT;
			if (!strcasecmp(sIdent, "hit_count"))
				return XRANK_HIT_COUNT;
			if (!strcasecmp(sIdent, "word_count"))
				return XRANK_WORD_COUNT;
			if (!strcasecmp(sIdent, "tf_idf"))
				return XRANK_TF_IDF;
			if (!strcasecmp(sIdent, "min_idf"))
				return XRANK_MIN_IDF;
			if (!strcasecmp(sIdent, "max_idf"))
				return XRANK_MAX_IDF;
			if (!strcasecmp(sIdent, "sum_idf"))
				return XRANK_SUM_IDF;
			if (!strcasecmp(sIdent, "min_hit_pos"))
				return XRANK_MIN_HIT_POS;
			if (!strcasecmp(sIdent, "min_best_span_pos"))
				return XRANK_MIN_BEST_SPAN_POS;
			if (!strcasecmp(sIdent, "exact_hit"))
				return XRANK_EXACT_HIT;
			if (!strcasecmp(sIdent, "exact_order"))
				return XRANK_EXACT_ORDER;

			if (!strcasecmp(sIdent, "bm25"))
				return XRANK_BM25;
			if (!strcasecmp(sIdent, "max_lcs"))
				return XRANK_MAX_LCS;
			if (!strcasecmp(sIdent, "field_mask"))
				return XRANK_FIELD_MASK;
			if (!strcasecmp(sIdent, "query_word_count"))
				return XRANK_QUERY_WORD_COUNT;
			if (!strcasecmp(sIdent, "doc_word_count"))
				return XRANK_DOC_WORD_COUNT;

			if (!strcasecmp(sIdent, "min_gaps"))
				return XRANK_MIN_GAPS;

			if (!strcasecmp(sIdent, "lccs"))
				return XRANK_LCCS;
			if (!strcasecmp(sIdent, "wlccs"))
				return XRANK_WLCCS;
			if (!strcasecmp(sIdent, "atc"))
				return XRANK_ATC;

			return -1;
		}

		int IsKnownFunc(const char* sFunc)
		{
			if (!strcasecmp(sFunc, "sum"))
				return XRANK_SUM;
			if (!strcasecmp(sFunc, "top"))
				return XRANK_TOP;
			if (!strcasecmp(sFunc, "max_window_hits"))
				return XRANK_MAX_WINDOW_HITS;
			if (!strcasecmp(sFunc, "bm25a"))
				return XRANK_BM25A;
			if (!strcasecmp(sFunc, "bm25f"))
				return XRANK_BM25F;
			return -1;
		}

		ISphExpr* CreateNode(int iID, ISphExpr* pLeft, ESphEvalStage*, CSphString&)
		{
			int* pCF = &m_pState->m_iCurrentField; // just a shortcut
			switch (iID)
			{
			case XRANK_LCS:					return new Expr_FieldFactor_c<BYTE>(pCF, m_pState->m_uLCS);
			case XRANK_USER_WEIGHT:			return new Expr_FieldFactor_c<int>(pCF, m_pState->m_pWeights);
			case XRANK_HIT_COUNT:			return new Expr_FieldFactor_c<DWORD>(pCF, m_pState->m_uHitCount);
			case XRANK_WORD_COUNT:			return new Expr_FieldFactor_c<DWORD>(pCF, m_pState->m_uWordCount);
			case XRANK_TF_IDF:				return new Expr_FieldFactor_c<float>(pCF, m_pState->m_dTFIDF);
			case XRANK_MIN_IDF:				return new Expr_FieldFactor_c<float>(pCF, m_pState->m_dMinIDF);
			case XRANK_MAX_IDF:				return new Expr_FieldFactor_c<float>(pCF, m_pState->m_dMaxIDF);
			case XRANK_SUM_IDF:				return new Expr_FieldFactor_c<float>(pCF, m_pState->m_dSumIDF);
			case XRANK_MIN_HIT_POS:			return new Expr_FieldFactor_c<int>(pCF, m_pState->m_iMinHitPos);
			case XRANK_MIN_BEST_SPAN_POS:	return new Expr_FieldFactor_c<int>(pCF, m_pState->m_iMinBestSpanPos);
			case XRANK_EXACT_HIT:			return new Expr_FieldFactor_c<CSphBitvec>(pCF, m_pState->m_tExactHit);
			case XRANK_EXACT_ORDER:			return new Expr_FieldFactor_c<CSphBitvec>(pCF, m_pState->m_tExactOrder);
			case XRANK_MAX_WINDOW_HITS:
			{
				CSphMatch tDummy;
				m_pState->m_iWindowSize = pLeft->IntEval(tDummy); // must be constant; checked in GetReturnType()
				SafeRelease(pLeft);
				return new Expr_FieldFactor_c<int>(pCF, m_pState->m_iMaxWindowHits);
			}
			case XRANK_MIN_GAPS:			return new Expr_FieldFactor_c<int>(pCF, m_pState->m_iMinGaps);
			case XRANK_LCCS:				return new Expr_FieldFactor_c<BYTE>(pCF, m_pState->m_dLCCS);
			case XRANK_WLCCS:				return new Expr_FieldFactor_c<float>(pCF, m_pState->m_dWLCCS);
			case XRANK_ATC:
				m_pState->m_bWantAtc = true;
				return new Expr_FieldFactor_c<float>(pCF, m_pState->m_dAtc);

			case XRANK_BM25:				return new Expr_IntPtr_c(&m_pState->m_uDocBM25);
			case XRANK_MAX_LCS:				return new Expr_GetIntConst_Rank_c(m_pState->m_iMaxLCS);
			case XRANK_FIELD_MASK:			return new Expr_FieldMask_c(m_pState->m_tMatchedFields);
			case XRANK_QUERY_WORD_COUNT:	return new Expr_GetIntConst_Rank_c(m_pState->m_iQueryWordCount);
			case XRANK_DOC_WORD_COUNT:		return new Expr_IntPtr_c(&m_pState->m_uDocWordCount);
			case XRANK_BM25A:
			{
				// exprs we'll evaluate here must be constant; that is checked in GetReturnType()
				// so having a dummy match with no data work alright
				assert(pLeft->IsArglist());
				CSphMatch tDummy;
				m_pState->m_fParamK1 = pLeft->GetArg(0)->Eval(tDummy);
				m_pState->m_fParamB = pLeft->GetArg(1)->Eval(tDummy);
				m_pState->m_fParamK1 = Max(m_pState->m_fParamK1, 0.001f);
				m_pState->m_fParamB = Min(Max(m_pState->m_fParamB, 0.0f), 1.0f);
				SafeDelete(pLeft);
				return new Expr_FloatPtr_c(&m_pState->m_fDocBM25A);
			}
			case XRANK_BM25F:
			{
				assert(pLeft->IsArglist());
				CSphMatch tDummy;
				float fK1 = pLeft->GetArg(0)->Eval(tDummy);
				float fB = pLeft->GetArg(1)->Eval(tDummy);
				fK1 = Max(fK1, 0.001f);
				fB = Min(Max(fB, 0.0f), 1.0f);
				ISphExpr* pRes = new Expr_BM25F_T<NEED_PACKEDFACTORS, HANDLE_DUPES>(m_pState, fK1, fB, pLeft->GetArg(2));
				SafeDelete(pLeft);
				return pRes;
			}

			case XRANK_SUM:					return new Expr_Sum_T<NEED_PACKEDFACTORS, HANDLE_DUPES>(m_pState, pLeft);
			case XRANK_TOP:					return new Expr_Top_T<NEED_PACKEDFACTORS, HANDLE_DUPES>(m_pState, pLeft);
			default:						return NULL;
			}
		}

		ESphAttr GetIdentType(int iID)
		{
			switch (iID)
			{
			case XRANK_LCS: // field-level
			case XRANK_USER_WEIGHT:
			case XRANK_HIT_COUNT:
			case XRANK_WORD_COUNT:
			case XRANK_MIN_HIT_POS:
			case XRANK_MIN_BEST_SPAN_POS:
			case XRANK_EXACT_HIT:
			case XRANK_EXACT_ORDER:
			case XRANK_MAX_WINDOW_HITS:
			case XRANK_BM25: // doc-level
			case XRANK_MAX_LCS:
			case XRANK_FIELD_MASK:
			case XRANK_QUERY_WORD_COUNT:
			case XRANK_DOC_WORD_COUNT:
			case XRANK_MIN_GAPS:
			case XRANK_LCCS:
				return ESphAttr::SPH_ATTR_INTEGER;
			case XRANK_TF_IDF:
			case XRANK_MIN_IDF:
			case XRANK_MAX_IDF:
			case XRANK_SUM_IDF:
			case XRANK_WLCCS:
			case XRANK_ATC:
				return ESphAttr::SPH_ATTR_FLOAT;
			default:
				assert(0);
				return ESphAttr::SPH_ATTR_INTEGER;
			}
		}

		/// helper to check argument types by a signature string (passed in sArgs)
		/// every character in the signature describes a type
		/// ? = any type
		/// i = integer
		/// I = integer constant
		/// f = float
		/// s = scalar (int/float)
		/// h = hash
		/// signature can also be preceded by "c:" modifier than means that all arguments must be constant
		bool CheckArgtypes(const CSphVector<ESphAttr>& dArgs, const char* sFuncname, const char* sArgs, bool bAllConst, CSphString& sError)
		{
			if (sArgs[0] == 'c' && sArgs[1] == ':')
			{
				if (!bAllConst)
				{
					sError.SetSprintf("%s() requires constant arguments", sFuncname);
					return false;
				}
				sArgs += 2;
			}

			int iLen = strlen(sArgs);
			if (dArgs.GetLength() != iLen)
			{
				sError.SetSprintf("%s() requires %d argument(s), not %d", sFuncname, iLen, dArgs.GetLength());
				return false;
			}

			ARRAY_FOREACH(i, dArgs)
			{
				switch (*sArgs++)
				{
				case '?':
					break;
				case 'i':
					if (dArgs[i] != ESphAttr::SPH_ATTR_INTEGER)
					{
						sError.SetSprintf("argument %d to %s() must be integer", i, sFuncname);
						return false;
					}
					break;
				case 's':
					if (dArgs[i] != ESphAttr::SPH_ATTR_INTEGER && dArgs[i] != ESphAttr::SPH_ATTR_FLOAT)
					{
						sError.SetSprintf("argument %d to %s() must be scalar (integer or float)", i, sFuncname);
						return false;
					}
					break;
				case 'h':
					if (dArgs[i] != ESphAttr::SPH_ATTR_MAPARG)
					{
						sError.SetSprintf("argument %d to %s() must be a map of constants", i, sFuncname);
						return false;
					}
					break;
				default:
					assert(0 && "unknown signature code");
					break;
				}
			}

			// this is important!
			// other previous failed checks might have filled sError
			// and if anything else up the stack checks it, we need an empty message now
			sError = "";
			return true;
		}

		ESphAttr GetReturnType(int iID, const CSphVector<ESphAttr>& dArgs, bool bAllConst, CSphString& sError)
		{
			switch (iID)
			{
			case XRANK_SUM:
				if (!CheckArgtypes(dArgs, "SUM", "?", bAllConst, sError))
					return ESphAttr::SPH_ATTR_NONE;
				return dArgs[0];

			case XRANK_TOP:
				if (!CheckArgtypes(dArgs, "TOP", "?", bAllConst, sError))
					return ESphAttr::SPH_ATTR_NONE;
				return dArgs[0];

			case XRANK_MAX_WINDOW_HITS:
				if (!CheckArgtypes(dArgs, "MAX_WINDOW_HITS", "c:i", bAllConst, sError))
					return ESphAttr::SPH_ATTR_NONE;
				return ESphAttr::SPH_ATTR_INTEGER;

			case XRANK_BM25A:
				if (!CheckArgtypes(dArgs, "BM25A", "c:ss", bAllConst, sError))
					return ESphAttr::SPH_ATTR_NONE;
				return ESphAttr::SPH_ATTR_FLOAT;

			case XRANK_BM25F:
				if (!CheckArgtypes(dArgs, "BM25F", "c:ss", bAllConst, sError))
					if (!CheckArgtypes(dArgs, "BM25F", "c:ssh", bAllConst, sError))
						return ESphAttr::SPH_ATTR_NONE;
				return ESphAttr::SPH_ATTR_FLOAT;

			default:
				sError.SetSprintf("internal error: unknown hook function (id=%d)", iID);
			}
			return ESphAttr::SPH_ATTR_NONE;
		}

		void CheckEnter(int iID)
		{
			if (!m_sCheckError)
				switch (iID)
				{
				case XRANK_LCS:
				case XRANK_USER_WEIGHT:
				case XRANK_HIT_COUNT:
				case XRANK_WORD_COUNT:
				case XRANK_TF_IDF:
				case XRANK_MIN_IDF:
				case XRANK_MAX_IDF:
				case XRANK_SUM_IDF:
				case XRANK_MIN_HIT_POS:
				case XRANK_MIN_BEST_SPAN_POS:
				case XRANK_EXACT_HIT:
				case XRANK_MAX_WINDOW_HITS:
				case XRANK_LCCS:
				case XRANK_WLCCS:
					if (!m_bCheckInFieldAggr)
						m_sCheckError = "field factors must only occur withing field aggregates in ranking expression";
					break;

				case XRANK_SUM:
				case XRANK_TOP:
					if (m_bCheckInFieldAggr)
						m_sCheckError = "field aggregates can not be nested in ranking expression";
					else
						m_bCheckInFieldAggr = true;
					break;

				default:
					assert(iID >= 0);
					return;
				}
		}

		void CheckExit(int iID)
		{
			if (!m_sCheckError && (iID == XRANK_SUM || iID == XRANK_TOP))
			{
				assert(m_bCheckInFieldAggr);
				m_bCheckInFieldAggr = false;
			}
		}
	};


	///ranker factory
	ISphRanker* sphCreateRanker(const XQQuery_t& tXQ, const CSphQuery* pQuery, CSphQueryResult* pResult, const ISphQwordSetup& tTermSetup, const CSphQueryContext& tCtx, const ISphSchema& tSorterSchema);


}