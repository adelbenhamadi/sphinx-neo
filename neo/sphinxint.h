//
// $Id$
//

//
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

#ifndef _sphinxint_
#define _sphinxint_

#include <sys/stat.h>
#include <fcntl.h>
#include <cfloat>


/*
#include "neo/sphinx/xfilter.h"
#include "neo/sphinx/xrt.h"
#include "neo/sphinx/xquery.h"
#include "neo/sphinxexcerpt.h"
#include "neo/sphinx/xudf.h"
*/

/////////////////////////////////////

#include "neo/core/globals.h"
#include "neo/query/query_state.h"
#include "neo/query/query_context.h"
#include "neo/query/query_profile.h"
#include "neo/query/query_result.h"
#include "neo/query/iqword.h"
#include "neo/index/queue_settings.h"
#include "neo/index/keyword_stat.h"
#include "neo/io/writer.h"
#include "neo/io/autofile.h"
#include "neo/io/reader.h"
#include "neo/core/iextra.h"
#include "neo/core/memory_tracker.h"
#include "neo/core/attrib_index_builder.h"
#include "neo/core/iwordlist.h"
#include "neo/source/attrib_locator.h"
#include "neo/dict/dict.h"
#include "neo/int/ref_counted.h"
#include "neo/utility/hash.h"
#include "neo/source/schema_int.h"

//#include "neo/sphinx.h"

namespace NEO {



	class ISphRanker;
	class ISphMatchSorter;
	class UservarIntSet_c;

	enum QueryDebug_e
	{
		QUERY_DEBUG_NO_PAYLOAD = 1 << 0
	};


	/// locator pair, for RT string dynamization
	struct LocatorPair_t
	{
		CSphAttrLocator m_tFrom;	///< source (static) locator
		CSphAttrLocator m_tTo;		///< destination (dynamized) locator
	};


/// value container for the intset uservar type
	class UservarIntSet_c : public CSphVector<SphAttr_t>, public ISphRefcountedMT
	{
	};

	extern UservarIntSet_c* (*g_pUservarsHook)(const CSphString& sUservar);


//////////////////////////////////////////////////////////////////////////
// MISC FUNCTION PROTOTYPES
//////////////////////////////////////////////////////////////////////////



	bool			sphCheckQueryHeight(const struct XQNode_t* pRoot, CSphString& sError);
	void			sphTransformExtendedQuery(XQNode_t** ppNode, const CSphIndexSettings& tSettings, bool bHasBooleanOptimization, const ISphKeywordsStat* pKeywords);
	void			TransformAotFilter(XQNode_t* pNode, const CSphWordforms* pWordforms, const CSphIndexSettings& tSettings);
	CSphString		sphReconstructNode(const XQNode_t* pNode, const CSphSchema* pSchema);


   // all indexes should produce same terms for same query
	struct SphWordStatChecker_t
	{
		SphWordStatChecker_t() {}
		void Set(const SmallStringHash_T<CSphQueryResultMeta::WordStat_t>& hStat);
		void DumpDiffer(const SmallStringHash_T<CSphQueryResultMeta::WordStat_t>& hStat, const char* sIndex, CSphString& sWarning) const;

		CSphVector<uint64_t> m_dSrcWords;
	};


	int sphDictCmp(const char* pStr1, int iLen1, const char* pStr2, int iLen2);
	int sphDictCmpStrictly(const char* pStr1, int iLen1, const char* pStr2, int iLen2);

	template <typename CP>
	int sphCheckpointCmp(const char* sWord, int iLen, SphWordID_t iWordID, bool bWordDict, const CP& tCP)
	{
		if (bWordDict)
			return sphDictCmp(sWord, iLen, tCP.m_sWord, strlen(tCP.m_sWord));

		int iRes = 0;
		iRes = iWordID < tCP.m_uWordID ? -1 : iRes;
		iRes = iWordID > tCP.m_uWordID ? 1 : iRes;
		return iRes;
	}

	template <typename CP>
	int sphCheckpointCmpStrictly(const char* sWord, int iLen, SphWordID_t iWordID, bool bWordDict, const CP& tCP)
	{
		if (bWordDict)
			return sphDictCmpStrictly(sWord, iLen, tCP.m_sWord, strlen(tCP.m_sWord));

		int iRes = 0;
		iRes = iWordID < tCP.m_uWordID ? -1 : iRes;
		iRes = iWordID > tCP.m_uWordID ? 1 : iRes;
		return iRes;
	}

	template < typename CP >
	struct SphCheckpointAccess_fn
	{
		const CP& operator () (const CP* pCheckpoint) const { return *pCheckpoint; }
	};

	template < typename CP, typename PRED >
	const CP* sphSearchCheckpoint(const char* sWord, int iWordLen, SphWordID_t iWordID
		, bool bStarMode, bool bWordDict
		, const CP* pFirstCP, const CP* pLastCP, const PRED& tPred)
	{
		assert(!bWordDict || iWordLen > 0);

		const CP* pStart = pFirstCP;
		const CP* pEnd = pLastCP;

		if (bStarMode && sphCheckpointCmp(sWord, iWordLen, iWordID, bWordDict, tPred(pStart)) < 0)
			return NULL;
		if (!bStarMode && sphCheckpointCmpStrictly(sWord, iWordLen, iWordID, bWordDict, tPred(pStart)) < 0)
			return NULL;

		if (sphCheckpointCmpStrictly(sWord, iWordLen, iWordID, bWordDict, tPred(pEnd)) >= 0)
			pStart = pEnd;
		else
		{
			while (pEnd - pStart > 1)
			{
				const CP* pMid = pStart + (pEnd - pStart) / 2;
				const int iCmpRes = sphCheckpointCmpStrictly(sWord, iWordLen, iWordID, bWordDict, tPred(pMid));

				if (iCmpRes == 0)
				{
					pStart = pMid;
					break;
				}
				else if (iCmpRes < 0)
					pEnd = pMid;
				else
					pStart = pMid;
			}

			assert(pStart >= pFirstCP);
			assert(pStart <= pLastCP);
			assert(sphCheckpointCmp(sWord, iWordLen, iWordID, bWordDict, tPred(pStart)) >= 0
				&& sphCheckpointCmpStrictly(sWord, iWordLen, iWordID, bWordDict, tPred(pEnd)) < 0);
		}

		return pStart;
	}

	template < typename CP >
	const CP* sphSearchCheckpoint(const char* sWord, int iWordLen, SphWordID_t iWordID, bool bStarMode, bool bWordDict, const CP* pFirstCP, const CP* pLastCP)
	{
		return sphSearchCheckpoint(sWord, iWordLen, iWordID, bStarMode, bWordDict, pFirstCP, pLastCP, SphCheckpointAccess_fn<CP>());
	}

	///////////

	class ISphRtDictWraper : public CSphDict
	{
	public:
		virtual const BYTE* GetPackedKeywords() = 0;
		virtual int				GetPackedLen() = 0;

		virtual void			ResetKeywords() = 0;

		virtual const char* GetLastWarning() const = 0;
		virtual void			ResetWarning() = 0;
	};


	void sphBuildNGrams(const char* sWord, int iLen, char cDelimiter, CSphVector<char>& dNgrams);

	// levenstein distance for words
	int sphLevenshtein(const char* sWord1, int iLen1, const char* sWord2, int iLen2);

	// levenstein distance for unicode codepoints
	int sphLevenshtein(const int* sWord1, int iLen1, const int* sWord2, int iLen2);

	struct Slice_t
	{
		DWORD				m_uOff;
		DWORD				m_uLen;
	};

	struct SuggestWord_t
	{
		int	m_iNameOff;
		int m_iLen;
		int m_iDistance;
		int m_iDocs;
		DWORD m_iNameHash;
	};

	struct SuggestArgs_t
	{
		int				m_iLimit;			// limit into result set
		int				m_iMaxEdits;		// levenstein distance threshold
		int				m_iDeltaLen;		// filter out words from dictionary these shorter \ longer then reference word
		int				m_iQueueLen;
		int				m_iRejectThr;
		bool			m_bQueryMode;

		bool			m_bResultOneline;
		bool			m_bResultStats;
		bool			m_bNonCharAllowed;

		SuggestArgs_t() : m_iLimit(5), m_iMaxEdits(4), m_iDeltaLen(3), m_iQueueLen(25), m_iRejectThr(4), m_bQueryMode(false), m_bResultOneline(false), m_bResultStats(true), m_bNonCharAllowed(false)
		{}
	};

	struct SuggestResult_t
	{
		// result set
		CSphVector<BYTE>			m_dBuf;
		CSphVector<SuggestWord_t>	m_dMatched;

		// state
		CSphVector<char>			m_dTrigrams;
		// payload
		void* m_pWordReader;
		void* m_pSegments;
		bool						m_bMergeWords;
		// word
		CSphString		m_sWord;
		int				m_iLen;
		int				m_dCodepoints[SPH_MAX_WORD_LEN];
		int				m_iCodepoints;
		bool			m_bUtf8;
		bool			m_bHasExactDict;

		SuggestResult_t() : m_pWordReader(NULL), m_pSegments(NULL), m_bMergeWords(false), m_iLen(0), m_iCodepoints(0), m_bUtf8(false), m_bHasExactDict(false)
		{
			m_dBuf.Reserve(8096);
			m_dMatched.Reserve(512);
		}

		~SuggestResult_t()
		{
			assert(!m_pWordReader);
			assert(!m_pSegments);
		}

		bool SetWord(const char* sWord, const ISphTokenizer* pTok, bool bUseLastWord);

		void Flattern(int iLimit);
	};

	class ISphWordlistSuggest
	{
	public:
		virtual ~ISphWordlistSuggest() {}

		virtual void SuffixGetChekpoints(const SuggestResult_t& tRes, const char* sSuffix, int iLen, CSphVector<DWORD>& dCheckpoints) const = 0;

		virtual void SetCheckpoint(SuggestResult_t& tRes, DWORD iCP) const = 0;

		struct DictWord_t
		{
			const char* m_sWord;
			int				m_iLen;
			int				m_iDocs;
		};

		virtual bool ReadNextWord(SuggestResult_t& tRes, DictWord_t& tWord) const = 0;
	};

	void sphGetSuggest(const ISphWordlistSuggest* pWordlist, int iInfixCodepointBytes, const SuggestArgs_t& tArgs, SuggestResult_t& tRes);


	class CSphScopedPayload
	{
	public:
		CSphScopedPayload() {}
		~CSphScopedPayload()
		{
			ARRAY_FOREACH(i, m_dPayloads)
				SafeDelete(m_dPayloads[i]);
		}
		void Add(ISphSubstringPayload* pPayload) { m_dPayloads.Add(pPayload); }

	private:
		CSphVector<ISphSubstringPayload*> m_dPayloads;
	};


	struct ExpansionContext_t
	{
		const ISphWordlist* m_pWordlist;
		BYTE* m_pBuf;
		CSphQueryResultMeta* m_pResult;
		int m_iMinPrefixLen;
		int m_iMinInfixLen;
		int m_iExpansionLimit;
		bool m_bHasMorphology;
		bool m_bMergeSingles;
		CSphScopedPayload* m_pPayloads;
		ESphHitless m_eHitless;
		const void* m_pIndexData;

		ExpansionContext_t();
	};


	XQNode_t* sphExpandXQNode(XQNode_t* pNode, ExpansionContext_t& tCtx);
	XQNode_t* sphQueryExpandKeywords(XQNode_t* pNode, const CSphIndexSettings& tSettings);


	bool sphHasExpandableWildcards(const char* sWord);
	bool sphExpandGetWords(const char* sWord, const ExpansionContext_t& tCtx, ISphWordlist::Args_t& tWordlist);


	template<typename T>
	struct ExpandedOrderDesc_T
	{
		bool IsLess(const T& a, const T& b)
		{
			return (sphGetExpansionMagic(a.m_iDocs, a.m_iHits) > sphGetExpansionMagic(b.m_iDocs, b.m_iHits));
		}
	};

	BYTE sphDoclistHintPack(SphOffset_t iDocs, SphOffset_t iLen);


   //////////////////////////////////////////////////////////////////////////


	uint64_t sphCalcLocatorHash(const CSphAttrLocator& tLoc, uint64_t uPrevHash);
	uint64_t sphCalcExprDepHash(const char* szTag, ISphExpr* pExpr, const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable);
	uint64_t sphCalcExprDepHash(ISphExpr* pExpr, const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable);



}
#endif // _sphinxint_



//
// $Id$
//
