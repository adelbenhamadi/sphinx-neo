#include "neo/core/arena.h"
#include "neo/tools/convert.h"
#include "neo/core/generic.h"
#include "neo/utility/inline_misc.h"

namespace NEO {

	DWORD* g_pMvaArena = NULL;		//initialized by sphArenaInit()

								   // global mega-arena
	static CSphArena g_tMvaArena;

	const char* sphArenaInit(int iMaxBytes)
	{
		if (!g_pMvaArena)
			g_pMvaArena = g_tMvaArena.ReInit(iMaxBytes);

		const char* sError = g_tMvaArena.GetError();
		return sError;
	}


	CSphArena::CSphArena()
		: m_iPages(0)
	{
	}


	CSphArena::~CSphArena()
	{
		// notify callers that arena no longer exists
		g_pMvaArena = NULL;
	}

	DWORD* CSphArena::ReInit(int uMaxBytes)
	{
		if (m_iPages != 0)
		{
			m_pArena.Reset();
			m_iPages = 0;
		}
		return Init(uMaxBytes);
	}

	DWORD* CSphArena::Init(int uMaxBytes)
	{
		m_iPages = (uMaxBytes + PAGE_SIZE - 1) / PAGE_SIZE;

		int iData = m_iPages * PAGE_SIZE; // data size, bytes
		int iMyTaglist = sizeof(int) + MAX_TAGS * sizeof(TagDesc_t); // int length, TagDesc_t[] tags; NOLINT
		int iMy = m_iPages * sizeof(PageDesc_t) + NUM_SIZES * sizeof(int) + iMyTaglist; // my internal structures size, bytes; NOLINT
#if ARENADEBUG
		iMy += 2 * sizeof(int); // debugging counters; NOLINT
#endif

		assert(iData % sizeof(DWORD) == 0);
		assert(iMy % sizeof(DWORD) == 0);

		CSphString sError;
		if (!m_pArena.Alloc((iData + iMy) / sizeof(DWORD), sError))
		{
			m_iPages = 0;
			m_sError.SetSprintf("alloc, error='%s'", sError.cstr());
			return NULL;
		}

		// setup internal pointers
		DWORD* pCur = m_pArena.GetWritePtr();

		m_pPages = (PageDesc_t*)pCur;
		pCur += sizeof(PageDesc_t) * m_iPages / sizeof(DWORD);

		m_pFreelistHeads = (int*)pCur;
		pCur += NUM_SIZES; // one for each size, and one extra for zero

		m_pTagCount = (int*)pCur++;
		m_pTags = (TagDesc_t*)pCur;
		pCur += sizeof(TagDesc_t) * MAX_TAGS / sizeof(DWORD);

#if ARENADEBUG
		m_pTotalAllocs = (int*)pCur++;
		m_pTotalBytes = (int*)pCur++;
		*m_pTotalAllocs = 0;
		*m_pTotalBytes = 0;
#endif

		m_pBasePtr = m_pArena.GetWritePtr() + iMy / sizeof(DWORD);
		assert(m_pBasePtr == pCur);

		// setup initial state
		for (int i = 0; i < m_iPages; i++)
		{
			m_pPages[i].m_iSizeBits = 0; // fully empty
			m_pPages[i].m_iPrev = (i > 0) ? i - 1 : -1;
			m_pPages[i].m_iNext = (i < m_iPages - 1) ? i + 1 : -1;
		}

		m_pFreelistHeads[0] = 0;
		for (int i = 1; i < NUM_SIZES; i++)
			m_pFreelistHeads[i] = -1;

		*m_pTagCount = 0;

		return m_pBasePtr;
	}


	int CSphArena::RawAlloc(int iBytes)
	{
		CheckFreelists();

		if (iBytes <= 0 || iBytes > ((1 << MAX_BITS) - (int)sizeof(int)))
			return -1;

		int iSizeBits = sphLog2(iBytes + 2 * sizeof(int) - 1); // always reserve sizeof(int) for the tag and AllocsLogEntry_t backtrack; NOLINT
		iSizeBits = Max(iSizeBits, MIN_BITS);
		assert(iSizeBits >= MIN_BITS && iSizeBits <= MAX_BITS);

		int iSizeSlot = iSizeBits - MIN_BITS + 1;
		assert(iSizeSlot >= 1 && iSizeSlot < NUM_SIZES);

		// get semi-free page for this size
		PageDesc_t* pPage = NULL;
		if (m_pFreelistHeads[iSizeSlot] >= 0)
		{
			// got something in the free-list
			pPage = m_pPages + m_pFreelistHeads[iSizeSlot];

		}
		else
		{
			// nothing in free-list, alloc next empty one
			if (m_pFreelistHeads[0] < 0)
				return -1; // out of memory

			// update the page
			pPage = m_pPages + m_pFreelistHeads[0];
			assert(pPage->m_iPrev == -1);

			m_pFreelistHeads[iSizeSlot] = m_pFreelistHeads[0];
			m_pFreelistHeads[0] = pPage->m_iNext;
			if (pPage->m_iNext >= 0)
				m_pPages[pPage->m_iNext].m_iPrev = -1;

			pPage->m_iSizeBits = iSizeBits;
			pPage->m_iUsed = 0;
			pPage->m_iNext = -1;

			CheckFreelists();

			// setup bitmap
			int iUsedBits = (1 << (MAX_BITS - iSizeBits)); // max-used-bits = page-size/alloc-size = ( 1<<page-bitsize )/( 1<<alloc-bitsize )
			assert(iUsedBits > 0 && iUsedBits <= (PAGE_BITMAP << 5));

			for (int i = 0; i < PAGE_BITMAP; i++)
				pPage->m_uBitmap[i] = ((i << 5) >= iUsedBits) ? 0xffffffffUL : 0;

			if (iUsedBits < 32)
				pPage->m_uBitmap[0] = (0xffffffffUL << iUsedBits);
		}

		// get free alloc slot and use it
		assert(pPage);
		assert(pPage->m_iSizeBits == iSizeBits);

		for (int i = 0; i < PAGE_BITMAP; i++) // FIXME! optimize, can scan less
		{
			if (pPage->m_uBitmap[i] == 0xffffffffUL)
				continue;

			int iFree = FindBit(pPage->m_uBitmap[i]);
			pPage->m_uBitmap[i] |= (1 << iFree);

			pPage->m_iUsed++;
			if (pPage->m_iUsed == (PAGE_SIZE >> pPage->m_iSizeBits))
			{
				// this page is full now, unchain from the free-list
				assert(m_pFreelistHeads[iSizeSlot] == pPage - m_pPages);
				m_pFreelistHeads[iSizeSlot] = pPage->m_iNext;
				if (pPage->m_iNext >= 0)
				{
					assert(m_pPages[pPage->m_iNext].m_iPrev == pPage - m_pPages);
					m_pPages[pPage->m_iNext].m_iPrev = -1;
				}
				pPage->m_iNext = -1;
			}

#if ARENADEBUG
			(*m_pTotalAllocs)++;
			(*m_pTotalBytes) += (1 << iSizeBits);
#endif

			CheckFreelists();

			int iOffset = (pPage - m_pPages) * PAGE_SIZE + (i * 32 + iFree) * (1 << iSizeBits); // raw internal byte offset (FIXME! optimize with shifts?)
			int iIndex = 2 + (iOffset / sizeof(DWORD)); // dword index with tag and backtrack fixup

			m_pBasePtr[iIndex - 1] = DWORD(-1); // untagged by default
			m_pBasePtr[iIndex - 2] = DWORD(-1); // backtrack nothere
			return iIndex;
		}

		assert(0 && "internal error, no free slots in free page");
		return -1;
	}


	void CSphArena::RawFree(int iIndex)
	{
		CheckFreelists();

		int iOffset = (iIndex - 2) * sizeof(DWORD); // remove tag fixup, and go to raw internal byte offset
		int iPage = iOffset / PAGE_SIZE;

		if (iPage<0 || iPage>m_iPages)
		{
			assert(0 && "internal error, freed index out of arena");
			return;
		}

		PageDesc_t* pPage = m_pPages + iPage;
		int iBit = (iOffset % PAGE_SIZE) >> pPage->m_iSizeBits;
		assert((iOffset % PAGE_SIZE) == (iBit << pPage->m_iSizeBits) && "internal error, freed offset is unaligned");

		if (!(pPage->m_uBitmap[iBit >> 5] & (1UL << (iBit & 31))))
		{
			assert(0 && "internal error, freed index already freed");
			return;
		}

		pPage->m_uBitmap[iBit >> 5] &= ~(1UL << (iBit & 31));
		pPage->m_iUsed--;

#if ARENADEBUG
		(*m_pTotalAllocs)--;
		(*m_pTotalBytes) -= (1 << pPage->m_iSizeBits);
#endif

		CheckFreelists();

		int iSizeSlot = pPage->m_iSizeBits - MIN_BITS + 1;

		if (pPage->m_iUsed == (PAGE_SIZE >> pPage->m_iSizeBits) - 1)
		{
			// this page was full, but it's semi-free now
			// chain to free-list
			assert(pPage->m_iPrev == -1); // full pages must not be in any list
			assert(pPage->m_iNext == -1);

			pPage->m_iNext = m_pFreelistHeads[iSizeSlot];
			if (pPage->m_iNext >= 0)
			{
				assert(m_pPages[pPage->m_iNext].m_iPrev == -1);
				assert(m_pPages[pPage->m_iNext].m_iSizeBits == pPage->m_iSizeBits);
				m_pPages[pPage->m_iNext].m_iPrev = iPage;
			}
			m_pFreelistHeads[iSizeSlot] = iPage;
		}

		if (pPage->m_iUsed == 0)
		{
			// this page is empty now
			// unchain from free-list
			if (pPage->m_iPrev >= 0)
			{
				// non-head page
				assert(m_pPages[pPage->m_iPrev].m_iNext == iPage);
				m_pPages[pPage->m_iPrev].m_iNext = pPage->m_iNext;

				if (pPage->m_iNext >= 0)
				{
					assert(m_pPages[pPage->m_iNext].m_iPrev == iPage);
					m_pPages[pPage->m_iNext].m_iPrev = pPage->m_iPrev;
				}

			}
			else
			{
				// head page
				assert(m_pFreelistHeads[iSizeSlot] == iPage);
				assert(pPage->m_iPrev == -1);

				if (pPage->m_iNext >= 0)
				{
					assert(m_pPages[pPage->m_iNext].m_iPrev == iPage);
					m_pPages[pPage->m_iNext].m_iPrev = -1;
				}
				m_pFreelistHeads[iSizeSlot] = pPage->m_iNext;
			}

			pPage->m_iSizeBits = 0;
			pPage->m_iPrev = -1;
			pPage->m_iNext = m_pFreelistHeads[0];
			if (pPage->m_iNext >= 0)
			{
				assert(m_pPages[pPage->m_iNext].m_iPrev == -1);
				assert(m_pPages[pPage->m_iNext].m_iSizeBits == 0);
				m_pPages[pPage->m_iNext].m_iPrev = iPage;
			}
			m_pFreelistHeads[0] = iPage;
		}

		CheckFreelists();
	}


	int CSphArena::TaggedAlloc(int iTag, int iBytes)
	{
		if (!m_iPages)
			return -1; // uninitialized

		assert(iTag >= 0);
		CSphScopedLock<CSphMutex> tThdLock(m_tThdMutex);

		// find that tag first
		TagDesc_t* pTag = sphBinarySearch(m_pTags, m_pTags + (*m_pTagCount) - 1, bind(&TagDesc_t::m_iTag), iTag);
		if (!pTag)
		{
			if (*m_pTagCount == MAX_TAGS)
				return -1; // out of tags

			int iLogHead = RawAlloc(sizeof(AllocsLogEntry_t));
			if (iLogHead < 0)
				return -1; // out of memory

			assert(iLogHead >= 2);
			AllocsLogEntry_t* pLog = (AllocsLogEntry_t*)(m_pBasePtr + iLogHead);
			pLog->m_iUsed = 0;
			pLog->m_iNext = -1;

			// add new tag
			pTag = m_pTags + (*m_pTagCount)++;
			pTag->m_iTag = iTag;
			pTag->m_iAllocs = 0;
			pTag->m_iLogHead = iLogHead;

			// re-sort
			// OPTIMIZE! full-blown sort is overkill here
			sphSort(m_pTags, *m_pTagCount, sphMemberLess(&TagDesc_t::m_iTag));

			// we must be able to find it now
			pTag = sphBinarySearch(m_pTags, m_pTags + (*m_pTagCount) - 1, bind(&TagDesc_t::m_iTag), iTag);
			assert(pTag && "internal error, fresh tag not found in TaggedAlloc()");

			if (!pTag)
				return -1; // internal error
		}

		// grow the log if needed
		int iLogEntry = pTag->m_iLogHead;
		AllocsLogEntry_t* pLog = (AllocsLogEntry_t*)(m_pBasePtr + pTag->m_iLogHead);
		if (pLog->m_iUsed == MAX_LOGENTRIES)
		{
			int iNewEntry = RawAlloc(sizeof(AllocsLogEntry_t));
			if (iNewEntry < 0)
				return -1; // out of memory

			assert(iNewEntry >= 2);
			iLogEntry = iNewEntry;
			AllocsLogEntry_t* pNew = (AllocsLogEntry_t*)(m_pBasePtr + iNewEntry);
			pNew->m_iUsed = 0;
			pNew->m_iNext = pTag->m_iLogHead;
			pTag->m_iLogHead = iNewEntry;
			pLog = pNew;
		}

		// do the alloc itself
		int iIndex = RawAlloc(iBytes);
		if (iIndex < 0)
			return -1; // out of memory

		assert(iIndex >= 2);
		// tag it
		m_pBasePtr[iIndex - 1] = iTag;
		// set data->AllocsLogEntry_t backtrack
		m_pBasePtr[iIndex - 2] = iLogEntry;

		// log it
		assert(pLog->m_iUsed < MAX_LOGENTRIES);
		pLog->m_dEntries[pLog->m_iUsed++] = iIndex;
		pTag->m_iAllocs++;

		// and we're done
		return iIndex;
	}


	void CSphArena::TaggedFreeIndex(int iTag, int iIndex)
	{
		if (!m_iPages)
			return; // uninitialized

		assert(iTag >= 0);
		CSphScopedLock<CSphMutex> tThdLock(m_tThdMutex);

		// find that tag
		TagDesc_t* pTag = sphBinarySearch(m_pTags, m_pTags + (*m_pTagCount) - 1, bind(&TagDesc_t::m_iTag), iTag);
		assert(pTag && "internal error, unknown tag in TaggedFreeIndex()");
		assert(m_pBasePtr[iIndex - 1] == DWORD(iTag) && "internal error, tag mismatch in TaggedFreeIndex()");

		// defence against internal errors
		if (!pTag)
			return;

		// untag it
		m_pBasePtr[iIndex - 1] = DWORD(-1);

		// free it
		RawFree(iIndex);

		// update AllocsLogEntry_t
		int iLogEntry = m_pBasePtr[iIndex - 2];
		assert(iLogEntry >= 2);
		m_pBasePtr[iIndex - 2] = DWORD(-1);
		AllocsLogEntry_t* pLogEntry = (AllocsLogEntry_t*)(m_pBasePtr + iLogEntry);
		for (int i = 0; i < MAX_LOGENTRIES; i++)
		{
			if (pLogEntry->m_dEntries[i] != iIndex)
				continue;

			pLogEntry->m_dEntries[i] = pLogEntry->m_dEntries[pLogEntry->m_iUsed - 1]; // RemoveFast
			pLogEntry->m_iUsed--;
			break;
		}
		assert(pLogEntry->m_iUsed >= 0);

		// remove from tag entries list
		if (pLogEntry->m_iUsed == 0)
		{
			if (pTag->m_iLogHead == iLogEntry)
			{
				pTag->m_iLogHead = pLogEntry->m_iNext;
			}
			else
			{
				int iLog = pTag->m_iLogHead;
				while (iLog >= 0)
				{
					AllocsLogEntry_t* pLog = (AllocsLogEntry_t*)(m_pBasePtr + iLog);
					if (iLogEntry != pLog->m_iNext)
					{
						iLog = pLog->m_iNext;
						continue;
					}
					else
					{
						pLog->m_iNext = pLogEntry->m_iNext;
						break;
					}
				}
			}
			RawFree(iLogEntry);
		}

		// update the tag descriptor
		pTag->m_iAllocs--;
		assert(pTag->m_iAllocs >= 0);

		// remove the descriptor if its empty now
		if (pTag->m_iAllocs == 0)
			RemoveTag(pTag);
	}


	void CSphArena::TaggedFreeTag(int iTag)
	{
		if (!m_iPages)
			return; // uninitialized

		assert(iTag >= 0);
		CSphScopedLock<CSphMutex> tThdLock(m_tThdMutex);

		// find that tag
		TagDesc_t* pTag = sphBinarySearch(m_pTags, m_pTags + (*m_pTagCount) - 1, bind(&TagDesc_t::m_iTag), iTag);
		if (!pTag)
			return;

		// walk the log and free it
		int iLog = pTag->m_iLogHead;
		while (iLog >= 0)
		{
			AllocsLogEntry_t* pLog = (AllocsLogEntry_t*)(m_pBasePtr + iLog);
			iLog = pLog->m_iNext;

			// free each alloc if tag still matches
			for (int i = 0; i < pLog->m_iUsed; i++)
			{
				int iIndex = pLog->m_dEntries[i];
				if (m_pBasePtr[iIndex - 1] == DWORD(iTag))
				{
					m_pBasePtr[iIndex - 1] = DWORD(-1); // avoid double free
					RawFree(iIndex);
					pTag->m_iAllocs--;
				}
			}
		}

		// check for mismatches
		assert(pTag->m_iAllocs == 0);

		// remove the descriptor
		RemoveTag(pTag);
	}

	void CSphArena::ExamineTag(tTester* pTest, int iTag)
	{
		if (!pTest)
			return;

		pTest->Reset();

		if (!m_iPages)
			return; // uninitialized

		assert(iTag >= 0);
		CSphScopedLock<CSphMutex> tThdLock(m_tThdMutex);

		// find that tag
		TagDesc_t* pTag = sphBinarySearch(m_pTags, m_pTags + (*m_pTagCount) - 1, bind(&TagDesc_t::m_iTag), iTag);
		if (!pTag)
			return;

		// walk the log and tick it's chunks
		int iLog = pTag->m_iLogHead;
		while (iLog >= 0)
		{
			AllocsLogEntry_t* pLog = (AllocsLogEntry_t*)(m_pBasePtr + iLog);
			iLog = pLog->m_iNext;

			// tick each alloc
			for (int i = 0; i < pLog->m_iUsed; i++)
				pTest->TestData(pLog->m_dEntries[i]);
		}
	}

	void CSphArena::RemoveTag(TagDesc_t* pTag)
	{
		assert(pTag);
		assert(pTag->m_iAllocs == 0);

		// dealloc log chain
		int iLog = pTag->m_iLogHead;
		while (iLog >= 0)
		{
			AllocsLogEntry_t* pLog = (AllocsLogEntry_t*)(m_pBasePtr + iLog);
			int iNext = pLog->m_iNext;

			RawFree(iLog);
			iLog = iNext;
		}

		// remove tag from the list
		int iTail = m_pTags + (*m_pTagCount) - pTag - 1;
		memmove(pTag, pTag + 1, iTail * sizeof(TagDesc_t));
		(*m_pTagCount)--;
	}


#if ARENADEBUG
	void CSphArena::CheckFreelists()
	{
		assert(m_pFreelistHeads[0] == -1 || m_pPages[m_pFreelistHeads[0]].m_iSizeBits == 0);
		for (int iSizeSlot = 1; iSizeSlot < NUM_SIZES; iSizeSlot++)
			assert(m_pFreelistHeads[iSizeSlot] == -1 || m_pPages[m_pFreelistHeads[iSizeSlot]].m_iSizeBits - MIN_BITS + 1 == iSizeSlot);
	}
#endif // ARENADEBUG

}