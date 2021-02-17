#include "neo/tools/lower_caser.h"
#include "neo/io/fnv64.h"



namespace NEO {

	CSphLowercaser::CSphLowercaser()
		: m_pData(NULL)
	{
	}


	void CSphLowercaser::Reset()
	{
		SafeDeleteArray(m_pData);
		m_pData = new size_t[CHUNK_SIZE];
		memset(m_pData, 0, CHUNK_SIZE * sizeof(size_t)); // NOLINT sizeof(int)
		m_iChunks = 1;
		m_pChunk[0] = m_pData; // chunk 0 must always be allocated, for utf-8 tokenizer shortcut to work
		for (size_t i = 1; i < CHUNK_COUNT; i++)
			m_pChunk[i] = NULL;
	}


	CSphLowercaser::~CSphLowercaser()
	{
		SafeDeleteArray(m_pData);
	}


	void CSphLowercaser::SetRemap(const CSphLowercaser* pLC)
	{
		if (!pLC)
			return;

		SafeDeleteArray(m_pData);

		m_iChunks = pLC->m_iChunks;
		m_pData = new size_t[m_iChunks * CHUNK_SIZE];
		memcpy(m_pData, pLC->m_pData, sizeof(size_t) * m_iChunks * CHUNK_SIZE); // NOLINT sizeof(int)

		for (size_t i = 0; i < CHUNK_COUNT; i++)
			m_pChunk[i] = pLC->m_pChunk[i]
			? pLC->m_pChunk[i] - pLC->m_pData + m_pData
			: NULL;
	}


	void CSphLowercaser::AddRemaps(const CSphVector<CSphRemapRange>& dRemaps, DWORD uFlags)
	{
		if (!dRemaps.GetLength())
			return;

		// build new chunks map
		// 0 means "was unused"
		// 1 means "was used"
		// 2 means "is used now"
		size_t dUsed[CHUNK_COUNT];
		for (size_t i = 0; i < CHUNK_COUNT; i++)
			dUsed[i] = m_pChunk[i] ? 1 : 0;

		auto iNewChunks = m_iChunks;

		ARRAY_FOREACH(i, dRemaps)
		{
			const CSphRemapRange& tRemap = dRemaps[i];

#define LOC_CHECK_RANGE(_a) assert ( (_a)>=0 && (_a)<MAX_CODE );
			LOC_CHECK_RANGE(tRemap.m_iStart);
			LOC_CHECK_RANGE(tRemap.m_iEnd);
			LOC_CHECK_RANGE(tRemap.m_iRemapStart);
			LOC_CHECK_RANGE(tRemap.m_iRemapStart + tRemap.m_iEnd - tRemap.m_iStart);
#undef LOC_CHECK_RANGE

			for (size_t iChunk = (tRemap.m_iStart >> CHUNK_BITS); iChunk <= (tRemap.m_iEnd >> CHUNK_BITS); iChunk++)
				if (dUsed[iChunk] == 0)
				{
					dUsed[iChunk] = 2;
					iNewChunks++;
				}
		}

		// alloc new tables and copy, if necessary
		if (iNewChunks > m_iChunks)
		{
			auto* pData = new size_t[iNewChunks * CHUNK_SIZE];
			memset(pData, 0, sizeof(size_t) * iNewChunks * CHUNK_SIZE); // NOLINT sizeof(int)

			auto* pChunk = pData;
			for (size_t i = 0; i < CHUNK_COUNT; i++)
			{
				auto* pOldChunk = m_pChunk[i];

				// build new ptr
				if (dUsed[i])
				{
					m_pChunk[i] = pChunk;
					pChunk += CHUNK_SIZE;
				}

				// copy old data
				if (dUsed[i] == 1)
					memcpy(m_pChunk[i], pOldChunk, sizeof(size_t) * CHUNK_SIZE); // NOLINT sizeof(int)
			}
			assert(pChunk - pData == iNewChunks * CHUNK_SIZE);

			SafeDeleteArray(m_pData);
			m_pData = pData;
			m_iChunks = iNewChunks;
		}

		// fill new stuff
		ARRAY_FOREACH(i, dRemaps)
		{
			const CSphRemapRange& tRemap = dRemaps[i];

			auto iRemapped = tRemap.m_iRemapStart;
			for (size_t j = tRemap.m_iStart; j <= tRemap.m_iEnd; j++, iRemapped++)
			{
				assert(m_pChunk[j >> CHUNK_BITS]);
				auto& iCodepoint = m_pChunk[j >> CHUNK_BITS][j & CHUNK_MASK];
				bool bWordPart = (iCodepoint & MASK_CODEPOINT) != 0;
				auto iNew = iRemapped | uFlags | (iCodepoint & MASK_FLAGS);
				if (bWordPart && (uFlags & FLAG_CODEPOINT_SPECIAL))
					iNew |= FLAG_CODEPOINT_DUAL;
				iCodepoint = iNew;
			}
		}
	}


	void CSphLowercaser::AddSpecials(const char* sSpecials)
	{
		assert(sSpecials);
		auto iSpecials = strlen(sSpecials);

		CSphVector<CSphRemapRange> dRemaps;
		dRemaps.Resize(iSpecials);
		ARRAY_FOREACH(i, dRemaps)
			dRemaps[i].m_iStart = dRemaps[i].m_iEnd = dRemaps[i].m_iRemapStart = sSpecials[i];

		AddRemaps(dRemaps, FLAG_CODEPOINT_SPECIAL);
	}

	const CSphLowercaser& CSphLowercaser::operator = (const CSphLowercaser& rhs)
	{
		SetRemap(&rhs);
		return *this;
	}

	uint64_t CSphLowercaser::GetFNV() const
	{
		auto iLen = (sizeof(size_t) * m_iChunks * CHUNK_SIZE) / sizeof(BYTE); // NOLINT
		return sphFNV64(m_pData, iLen);
	}

	int CSphLowercaser::GetMaxCodepointLength() const
	{
		size_t iMax = 0;
		for (size_t iChunk = 0; iChunk < CHUNK_COUNT; iChunk++)
		{
			auto* pChunk = m_pChunk[iChunk];
			if (!pChunk)
				continue;

			auto* pMax = pChunk + CHUNK_SIZE;
			while (pChunk < pMax)
			{
				auto iCode = *pChunk++ & MASK_CODEPOINT;
				iMax = Max(iMax, iCode);
			}
		}
		if (iMax < 0x80)
			return 1;
		if (iMax < 0x800)
			return 2;
		return 3; // actually, 4 once we hit 0x10000
	}

}