#pragma once
#include "neo/query/enums.h"
#include "neo/core/match.h"
#include "neo/source/schema.h"

namespace NEO {

	//fwd dec
	struct CSphAttrLocator;
	struct ISphExpr;
	struct ISphMatchProcessor;
	struct CSphMatchComparatorState;

	/// JSON key lookup stuff
	struct JsonKey_t
	{
		CSphString		m_sKey;		///< name string
		DWORD			m_uMask;	///< Bloom mask for this key
		int				m_iLen;		///< name length, in bytes

		JsonKey_t();
		explicit JsonKey_t(const char* sKey, int iLen);
	};

	typedef int (*SphStringCmp_fn)(const BYTE* pStr1, const BYTE* pStr2, bool bPacked);


	/// match comparator state
	struct CSphMatchComparatorState
	{
		static const int	MAX_ATTRS = 5;

		ESphSortKeyPart		m_eKeypart[MAX_ATTRS];		///< sort-by key part type
		CSphAttrLocator		m_tLocator[MAX_ATTRS];		///< sort-by attr locator
		JsonKey_t			m_tSubKeys[MAX_ATTRS];		///< sort-by attr sub-locator
		ISphExpr* m_tSubExpr[MAX_ATTRS];		///< sort-by attr expression
		ESphAttr			m_tSubType[MAX_ATTRS];		///< sort-by expression type
		int					m_dAttrs[MAX_ATTRS];		///< sort-by attr index

		DWORD				m_uAttrDesc;				///< sort order mask (if i-th bit is set, i-th attr order is DESC)
		DWORD				m_iNow;						///< timestamp (for timesegments sorting mode)
		SphStringCmp_fn		m_fnStrCmp;					///< string comparator


		/// create default empty state
		CSphMatchComparatorState()
			: m_uAttrDesc(0)
			, m_iNow(0)
			, m_fnStrCmp(NULL)
		{
			for (int i = 0; i < MAX_ATTRS; i++)
			{
				m_eKeypart[i] = SPH_KEYPART_ID;
				m_dAttrs[i] = -1;
			}
		}

		/// check if any of my attrs are bitfields
		bool UsesBitfields()
		{
			for (int i = 0; i < MAX_ATTRS; i++)
				if (m_eKeypart[i] == SPH_KEYPART_INT && m_tLocator[i].IsBitfield())
					return true;
			return false;
		}

		inline int CmpStrings(const CSphMatch& a, const CSphMatch& b, int iAttr) const
		{
			assert(iAttr >= 0 && iAttr < MAX_ATTRS);
			assert(m_eKeypart[iAttr] == SPH_KEYPART_STRING || m_eKeypart[iAttr] == SPH_KEYPART_STRINGPTR);
			assert(m_fnStrCmp);

			const BYTE* aa = (const BYTE*)a.GetAttr(m_tLocator[iAttr]);
			const BYTE* bb = (const BYTE*)b.GetAttr(m_tLocator[iAttr]);
			if (aa == NULL || bb == NULL)
			{
				if (aa == bb)
					return 0;
				if (aa == NULL)
					return -1;
				return 1;
			}
			return m_fnStrCmp(aa, bb, (m_eKeypart[iAttr] == SPH_KEYPART_STRING));
		}
	};



	/// generic match sorter interface
	class ISphMatchSorter
	{
	public:
		bool				m_bRandomize;
		int64_t				m_iTotal;

		SphDocID_t			m_iJustPushed;
		int					m_iMatchCapacity;
		CSphTightVector<SphDocID_t> m_dJustPopped;

	protected:
		CSphRsetSchema				m_tSchema;		///< sorter schema (adds dynamic attributes on top of index schema)
		CSphMatchComparatorState	m_tState;		///< protected to set m_iNow automatically on SetState() calls

	public:
		/// ctor
		ISphMatchSorter() : m_bRandomize(false), m_iTotal(0), m_iJustPushed(0), m_iMatchCapacity(0) {}

		/// virtualizing dtor
		virtual				~ISphMatchSorter() {}

		/// check if this sorter needs attr values
		virtual bool		UsesAttrs() const = 0;

		// check if sorter might be used in multi-queue
		virtual bool		CanMulti() const = 0;

		/// check if this sorter does groupby
		virtual bool		IsGroupby() const = 0;

		/// set match comparator state
		virtual void		SetState(const CSphMatchComparatorState& tState);

		/// get match comparator stat
		virtual CSphMatchComparatorState& GetState() { return m_tState; }

		/// set group comparator state
		virtual void		SetGroupState(const CSphMatchComparatorState&) {}

		/// set MVA pool pointer (for MVA+groupby sorters)
		virtual void SetMVAPool(const DWORD*, bool) {}

		/// set string pool pointer (for string+groupby sorters)
		virtual void		SetStringPool(const BYTE*) {}

		/// set sorter schema by swapping in and (optionally) adjusting the argument
		virtual void		SetSchema(CSphRsetSchema& tSchema) { m_tSchema = tSchema; }

		/// get incoming schema
		virtual const CSphRsetSchema& GetSchema() const { return m_tSchema; }

		/// base push
		/// returns false if the entry was rejected as duplicate
		/// returns true otherwise (even if it was not actually inserted)
		virtual bool		Push(const CSphMatch& tEntry) = 0;

		/// submit pre-grouped match. bNewSet indicates that the match begins the bunch of matches got from one source
		virtual bool		PushGrouped(const CSphMatch& tEntry, bool bNewSet) = 0;

		/// get	rough entries count, due of aggregate filtering phase
		virtual int			GetLength() const = 0;

		/// get internal buffer length
		virtual int			GetDataLength() const = 0;

		/// get total count of non-duplicates Push()ed through this queue
		virtual int64_t		GetTotalCount() const { return m_iTotal; }

		/// process collected entries up to length count
		virtual void		Finalize(ISphMatchProcessor& tProcessor, bool bCallProcessInResultSetOrder) = 0;

		/// store all entries into specified location and remove them from the queue
		/// entries are stored in properly sorted order,
		/// if iTag is non-negative, entries are also tagged; otherwise, their tag's unchanged
		/// return sored entries count, might be less than length due of aggregate filtering phase
		virtual int			Flatten(CSphMatch* pTo, int iTag) = 0;

		/// get a pointer to the worst element, NULL if there is no fixed location
		virtual const CSphMatch* GetWorst() const { return NULL; }
	};

}