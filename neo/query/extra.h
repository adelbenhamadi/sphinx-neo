#pragma once
#include "neo/int/types.h"
#include "neo/query/enums.h"
#include "neo/query/field_mask.h"
#include "neo/io/crc32.h"
#include "neo/core/skip_list.h"
#include "neo/core/match.h"


//this unit contains various structs for quering, need refactoring!

namespace NEO {

	//////////////////////////////////////////////////////////////////////////
	// EXTENDED MATCHING V2
	//////////////////////////////////////////////////////////////////////////

#define SPH_TREE_DUMP			0

#define SPH_BM25_K1				1.2f
#define SPH_BM25_SCALE			1000


	struct QwordsHash_fn
	{
		static inline int Hash(const CSphString& sKey)
		{
			return sphCRC32(sKey.cstr());
		}
	};
	////////////////////////////////////////

	/// hit in the stream
	/// combines posting info (docid and hitpos) with a few more matching/ranking bits
	///
	/// note that while in simple cases every hit would just represent a single keyword,
	/// this is NOT always the case; phrase, proximity, and NEAR operators (that already
	/// analyze keywords positions while matching the document) can emit a single folded
	/// hit representing the entire multi-keyword match, so that the ranker could avoid
	/// double work processing individual hits again. in such cases, m_uWeight, m_uSpanlen,
	/// and m_uMatchlen will differ from the "usual" value of 1.
	///
	/// thus, in folded hits:
	/// - m_uWeight is the match LCS value in all cases (phrase, proximity, near).
	/// - m_uSpanlen is the match span length, ie. a distance from the first to the last
	/// matching keyword. for phrase operators it natually equals m_uWeight, for other
	/// operators it might be very different.
	/// - m_uMatchlen is a piece of voodoo magic that only the near operator seems to use.
	struct ExtHit_t
	{
		SphDocID_t	m_uDocid;
		Hitpos_t	m_uHitpos;
		WORD		m_uQuerypos;
		WORD		m_uNodepos;
		WORD		m_uSpanlen;
		WORD		m_uMatchlen;
		DWORD		m_uWeight;		///< 1 for individual keywords, LCS value for folded phrase/proximity/near hits
		DWORD		m_uQposMask;
	};


	/// match in the stream
	struct ExtDoc_t
	{
		SphDocID_t		m_uDocid;
		CSphRowitem* m_pDocinfo;			///< for inline storage only
		SphOffset_t		m_uHitlistOffset;
		DWORD			m_uDocFields;
		float			m_fTFIDF;
	};


	/// word in the query
	struct ExtQword_t
	{
		CSphString	m_sWord;		///< word
		CSphString	m_sDictWord;	///< word as processed by dict
		int			m_iDocs;		///< matching documents
		int			m_iHits;		///< matching hits
		float		m_fIDF;			///< IDF value
		float		m_fBoost;		///< IDF multiplier
		int			m_iQueryPos;	///< position in the query
		bool		m_bExpanded;	///< added by prefix expansion
		bool		m_bExcluded;	///< excluded by the query (eg. bb in (aa AND NOT bb))
	};


	/// query words set
	typedef CSphOrderedHash < ExtQword_t, CSphString, QwordsHash_fn, 256 > ExtQwordsHash_t;

	struct ZoneHits_t
	{
		CSphVector<Hitpos_t>	m_dStarts;
		CSphVector<Hitpos_t>	m_dEnds;
	};

	/// per-document zone information (span start/end positions)
	struct ZoneInfo_t
	{
		SphDocID_t		m_uDocid;
		ZoneHits_t* m_pHits;
	};



	struct TermPos_t
	{
		WORD m_uQueryPos;
		WORD m_uAtomPos;
	};


	// FindSpan vector operators
	static bool operator < (const ZoneInfo_t& tZone, SphDocID_t uDocid)
	{
		return tZone.m_uDocid < uDocid;
	}

	static bool operator == (const ZoneInfo_t& tZone, SphDocID_t uDocid)
	{
		return tZone.m_uDocid == uDocid;
	}

	static bool operator < (SphDocID_t uDocid, const ZoneInfo_t& tZone)
	{
		return uDocid < tZone.m_uDocid;
	}

	/////////////////////

	static inline void CopyExtDocinfo(ExtDoc_t& tDst, const ExtDoc_t& tSrc, CSphRowitem** ppRow, int iStride)
	{
		if (tSrc.m_pDocinfo)
		{
			assert(ppRow && *ppRow);
			memcpy(*ppRow, tSrc.m_pDocinfo, iStride * sizeof(CSphRowitem));
			tDst.m_pDocinfo = *ppRow;
			*ppRow += iStride;
		}
		else
			tDst.m_pDocinfo = NULL;
	}

	static inline void CopyExtDoc(ExtDoc_t& tDst, const ExtDoc_t& tSrc, CSphRowitem** ppRow, int iStride)
	{
		tDst = tSrc;
		CopyExtDocinfo(tDst, tSrc, ppRow, iStride);
	}


}