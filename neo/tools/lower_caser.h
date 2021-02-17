#pragma once
#include "neo/int/types.h"

namespace NEO {

	enum ESphCodePoint
	{
		MASK_CODEPOINT = 0x00ffffffUL,	// mask off codepoint flags
		MASK_FLAGS = 0xff000000UL, // mask off codepoint value
		FLAG_CODEPOINT_SPECIAL = 0x01000000UL,	// this codepoint is special
		FLAG_CODEPOINT_DUAL = 0x02000000UL,	// this codepoint is special but also a valid word part
		FLAG_CODEPOINT_NGRAM = 0x04000000UL,	// this codepoint is n-gram indexed
		FLAG_CODEPOINT_BOUNDARY = 0x10000000UL,	// this codepoint is phrase boundary
		FLAG_CODEPOINT_IGNORE = 0x20000000UL,	// this codepoint is ignored
		FLAG_CODEPOINT_BLEND = 0x40000000UL	// this codepoint is "blended" (indexed both as a character, and as a separator)
	};

	/// lowercaser remap range
	struct CSphRemapRange
	{
		size_t			m_iStart;
		size_t			m_iEnd;
		size_t			m_iRemapStart;

		CSphRemapRange()
			: m_iStart(-1)
			, m_iEnd(-1)
			, m_iRemapStart(-1)
		{}

		CSphRemapRange(size_t iStart, size_t iEnd, size_t iRemapStart)
			: m_iStart(iStart)
			, m_iEnd(iEnd)
			, m_iRemapStart(iRemapStart)
		{}
	};


	inline bool operator < (const CSphRemapRange& a, const CSphRemapRange& b)
	{
		return a.m_iStart < b.m_iStart;
	}
	/// lowercaser
	class CSphLowercaser
	{
		friend class ISphTokenizer;
		friend class CSphTokenizerBase;
		friend class CSphTokenizer_UTF8_Base;
		friend class CSphTokenizerBase2;

	public:
		CSphLowercaser();
		~CSphLowercaser();

		void		Reset();
		void		SetRemap(const CSphLowercaser* pLC);
		void		AddRemaps(const CSphVector<CSphRemapRange>& dRemaps, DWORD uFlags);
		void		AddSpecials(const char* sSpecials);
		uint64_t	GetFNV() const;

	public:
		const CSphLowercaser& operator = (const CSphLowercaser& rhs);

	public:
		inline size_t	ToLower(size_t iCode) const
		{
			if (iCode < 0 || iCode >= MAX_CODE)
				return iCode;
			register auto* pChunk = m_pChunk[iCode >> CHUNK_BITS];
			if (pChunk)
				return pChunk[iCode & CHUNK_MASK];
			return 0;
		}

		int GetMaxCodepointLength() const;

	protected:
		static const size_t	CHUNK_COUNT = 0x300;
		static const size_t	CHUNK_BITS = 8;

		static const size_t	CHUNK_SIZE = 1 << CHUNK_BITS;
		static const size_t	CHUNK_MASK = CHUNK_SIZE - 1;
		static const size_t	MAX_CODE = CHUNK_COUNT * CHUNK_SIZE;

		size_t					m_iChunks;					///< how much chunks are actually allocated
		size_t* m_pData;					///< chunks themselves
		size_t* m_pChunk[CHUNK_COUNT];	///< pointers to non-empty chunks
	};

}