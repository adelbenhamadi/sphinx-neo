#pragma once
#include "neo/int/types.h"
#include "neo/index/enums.h"
#include "neo/query/enums.h"
#include "neo/core/skip_list.h"
#include "neo/query/field_mask.h"
#include "neo/source/hitman.h"

namespace NEO {

	//fwd dec
	class CSphDict;
	class CSphIndex;
	class CSphQueryContext;
	class ISphZoneCheck;
	class CSphQueryNodeCache;
	struct CSphQueryStats;
	class CSphMatch;


	/// extended query word with attached position within atom
	struct XQKeyword_t
	{
		CSphString			m_sWord;
		int					m_iAtomPos;
		int					m_iSkippedBefore; ///< positions skipped before this token (because of blended chars)
		bool				m_bFieldStart;	///< must occur at very start
		bool				m_bFieldEnd;	///< must occur at very end
		float				m_fBoost;		///< keyword IDF will be multiplied by this
		bool				m_bExpanded;	///< added by prefix expansion
		bool				m_bExcluded;	///< excluded by query (rval to operator NOT)
		bool				m_bMorphed;		///< morphology processing (wordforms, stemming etc) already done
		void* m_pPayload;

		XQKeyword_t()
			: m_iAtomPos(-1)
			, m_iSkippedBefore(0)
			, m_bFieldStart(false)
			, m_bFieldEnd(false)
			, m_fBoost(1.0f)
			, m_bExpanded(false)
			, m_bExcluded(false)
			, m_bMorphed(false)
			, m_pPayload(NULL)
		{}

		XQKeyword_t(const char* sWord, int iPos)
			: m_sWord(sWord)
			, m_iAtomPos(iPos)
			, m_iSkippedBefore(0)
			, m_bFieldStart(false)
			, m_bFieldEnd(false)
			, m_fBoost(1.0f)
			, m_bExpanded(false)
			, m_bExcluded(false)
			, m_bMorphed(false)
			, m_pPayload(NULL)
		{}
	};



	// the limit of field or zone or zonespan
	struct XQLimitSpec_t
	{
		bool					m_bFieldSpec;	///< whether field spec was already explicitly set
		FieldMask_t			m_dFieldMask;	///< fields mask (spec part)
		int						m_iFieldMaxPos;	///< max position within field (spec part)
		CSphVector<int>			m_dZones;		///< zone indexes in per-query zones list
		bool					m_bZoneSpan;	///< if we need to hits within only one span

	public:
		XQLimitSpec_t()
		{
			Reset();
		}

		inline void Reset()
		{
			m_bFieldSpec = false;
			m_iFieldMaxPos = 0;
			m_bZoneSpan = false;
			m_dFieldMask.SetAll();
			m_dZones.Reset();
		}

		bool IsEmpty() const
		{
			return m_bFieldSpec == false && m_iFieldMaxPos == 0 && m_bZoneSpan == false && m_dZones.GetLength() == 0;
		}

		XQLimitSpec_t(const XQLimitSpec_t& dLimit)
		{
			if (this == &dLimit)
				return;
			Reset();
			*this = dLimit;
		}

		XQLimitSpec_t& operator = (const XQLimitSpec_t& dLimit)
		{
			if (this == &dLimit)
				return *this;

			if (dLimit.m_bFieldSpec)
				SetFieldSpec(dLimit.m_dFieldMask, dLimit.m_iFieldMaxPos);

			if (dLimit.m_dZones.GetLength())
				SetZoneSpec(dLimit.m_dZones, dLimit.m_bZoneSpan);

			return *this;
		}
	public:
		void SetZoneSpec(const CSphVector<int>& dZones, bool bZoneSpan);
		void SetFieldSpec(const FieldMask_t& uMask, int iMaxPos);
	};


	/// term, searcher view
	class ISphQword
	{
	public:
		// setup by query parser
		CSphString		m_sWord;		///< my copy of word
		CSphString		m_sDictWord;	///< word after being processed by dict (eg. stemmed)
		SphWordID_t		m_uWordID;		///< word ID, from dictionary
		TermPosFilter_e	m_iTermPos;
		int				m_iAtomPos;		///< word position, from query
		float			m_fBoost;		///< IDF keyword boost (multiplier)
		bool			m_bExpanded;	///< added by prefix expansion
		bool			m_bExcluded;	///< excluded by the query (rval to operator NOT)

		// setup by QwordSetup()
		int				m_iDocs;		///< document count, from wordlist
		int				m_iHits;		///< hit count, from wordlist
		bool			m_bHasHitlist;	///< hitlist presence flag
		CSphVector<SkiplistEntry_t>		m_dSkiplist;	///< skiplist for quicker document list seeks

		// iterator state
		FieldMask_t m_dQwordFields;	///< current match fields
		DWORD			m_uMatchHits;	///< current match hits count
		SphOffset_t		m_iHitlistPos;	///< current position in hitlist, from doclist

	protected:
		bool			m_bAllFieldsKnown; ///< whether the all match fields is known, or only low 32.

	public:
		ISphQword()
			: m_uWordID(0)
			, m_iTermPos(TERM_POS_NONE)
			, m_iAtomPos(0)
			, m_fBoost(1.0f)
			, m_bExpanded(false)
			, m_bExcluded(false)
			, m_iDocs(0)
			, m_iHits(0)
			, m_bHasHitlist(true)
			, m_uMatchHits(0)
			, m_iHitlistPos(0)
			, m_bAllFieldsKnown(false)
		{
			m_dQwordFields.UnsetAll();
		}
		virtual ~ISphQword() {}

		virtual void				HintDocid(SphDocID_t) {}
		virtual const CSphMatch& GetNextDoc(DWORD* pInlineDocinfo) = 0;
		virtual void				SeekHitlist(SphOffset_t uOff) = 0;
		virtual Hitpos_t			GetNextHit() = 0;
		virtual void				CollectHitMask();

		virtual void Reset()
		{
			m_iDocs = 0;
			m_iHits = 0;
			m_dQwordFields.UnsetAll();
			m_bAllFieldsKnown = false;
			m_uMatchHits = 0;
			m_iHitlistPos = 0;
		}
	};



	class ISphQwordSetup : ISphNoncopyable
	{
	public:
		CSphDict* m_pDict;
		const CSphIndex* m_pIndex;
		ESphDocinfo				m_eDocinfo;
		const CSphRowitem* m_pMinRow;
		SphDocID_t				m_uMinDocid;
		int						m_iInlineRowitems;		///< inline rowitems count
		int						m_iDynamicRowitems;		///< dynamic rowitems counts (including (!) inline)
		int64_t					m_iMaxTimer;
		CSphString* m_pWarning;
		CSphQueryContext* m_pCtx;
		CSphQueryNodeCache* m_pNodeCache;
		mutable ISphZoneCheck* m_pZoneChecker;
		CSphQueryStats* m_pStats;
		mutable bool			m_bSetQposMask;

		ISphQwordSetup()
			: m_pDict(NULL)
			, m_pIndex(NULL)
			, m_eDocinfo(SPH_DOCINFO_NONE)
			, m_pMinRow(NULL)
			, m_uMinDocid(0)
			, m_iInlineRowitems(0)
			, m_iDynamicRowitems(0)
			, m_iMaxTimer(0)
			, m_pWarning(NULL)
			, m_pCtx(NULL)
			, m_pNodeCache(NULL)
			, m_pZoneChecker(NULL)
			, m_pStats(NULL)
			, m_bSetQposMask(false)
		{}
		virtual ~ISphQwordSetup() {}

		virtual ISphQword* QwordSpawn(const XQKeyword_t& tWord) const = 0;
		virtual bool						QwordSetup(ISphQword* pQword) const = 0;
	};


}