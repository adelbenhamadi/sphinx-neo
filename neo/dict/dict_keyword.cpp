#include "neo/dict/dict_keyword.h"
#include "neo/io/bin.h"
#include "neo/int/queue.h"
#include "neo/core/die.h"
#include "neo/core/keyword_delta_writer.h"
#include "neo/core/infix.h"
#include "neo/tools/docinfo_transformer.h"


namespace NEO {

	CSphDictKeywords::CSphDictKeywords()
		: m_bHitblock(false)
		, m_iMemUse(0)
		, m_iDictLimit(0)
		, m_pEntryChunk(NULL)
		, m_iEntryChunkFree(0)
		, m_pKeywordChunk(NULL)
		, m_iKeywordChunkFree(0)
		, m_pDictChunk(NULL)
		, m_iDictChunkFree(0)
	{
		memset(m_dHash, 0, sizeof(m_dHash));
	}

	CSphDictKeywords::~CSphDictKeywords()
	{
		HitblockReset();
	}

	void CSphDictKeywords::HitblockReset()
	{
		m_dExceptions.Resize(0);

		ARRAY_FOREACH(i, m_dEntryChunks)
			SafeDeleteArray(m_dEntryChunks[i]);
		m_dEntryChunks.Resize(0);
		m_pEntryChunk = NULL;
		m_iEntryChunkFree = 0;

		ARRAY_FOREACH(i, m_dKeywordChunks)
			SafeDeleteArray(m_dKeywordChunks[i]);
		m_dKeywordChunks.Resize(0);
		m_pKeywordChunk = NULL;
		m_iKeywordChunkFree = 0;

		m_iMemUse = 0;

		memset(m_dHash, 0, sizeof(m_dHash));
	}

	CSphDictKeywords::HitblockKeyword_t* CSphDictKeywords::HitblockAddKeyword(DWORD uHash, const char* sWord, int iLen, SphWordID_t uID)
	{
		assert(iLen < MAX_KEYWORD_BYTES);

		// alloc entry
		if (!m_iEntryChunkFree)
		{
			m_pEntryChunk = new HitblockKeyword_t[ENTRY_CHUNK];
			m_iEntryChunkFree = ENTRY_CHUNK;
			m_dEntryChunks.Add(m_pEntryChunk);
			m_iMemUse += sizeof(HitblockKeyword_t) * ENTRY_CHUNK;
		}
		HitblockKeyword_t* pEntry = m_pEntryChunk++;
		m_iEntryChunkFree--;

		// alloc keyword
		iLen++;
		if (m_iKeywordChunkFree < iLen)
		{
			m_pKeywordChunk = new BYTE[KEYWORD_CHUNK];
			m_iKeywordChunkFree = KEYWORD_CHUNK;
			m_dKeywordChunks.Add(m_pKeywordChunk);
			m_iMemUse += KEYWORD_CHUNK;
		}

		// fill it
		memcpy(m_pKeywordChunk, sWord, iLen);
		m_pKeywordChunk[iLen - 1] = '\0';
		pEntry->m_pKeyword = (char*)m_pKeywordChunk;
		pEntry->m_uWordid = uID;
		m_pKeywordChunk += iLen;
		m_iKeywordChunkFree -= iLen;

		// mtf it
		pEntry->m_pNextHash = m_dHash[uHash];
		m_dHash[uHash] = pEntry;

		return pEntry;
	}

	SphWordID_t CSphDictKeywords::HitblockGetID(const char* sWord, int iLen, SphWordID_t uCRC)
	{
		if (iLen >= MAX_KEYWORD_BYTES - 4) // fix of very long word (zones)
		{
			memcpy(m_sClippedWord, sWord, MAX_KEYWORD_BYTES - 4);
			memset(m_sClippedWord + MAX_KEYWORD_BYTES - 4, 0, 4);

			CSphString sOrig;
			sOrig.SetBinary(sWord, iLen);
			sphWarn("word overrun buffer, clipped!!!\n"
				"clipped (len=%d, word='%s')\noriginal (len=%d, word='%s')",
				MAX_KEYWORD_BYTES - 4, m_sClippedWord, iLen, sOrig.cstr());

			sWord = m_sClippedWord;
			iLen = MAX_KEYWORD_BYTES - 4;
			uCRC = sphCRC32(m_sClippedWord, MAX_KEYWORD_BYTES - 4);
		}

		// is this a known one? find it
		// OPTIMIZE? in theory we could use something faster than crc32; but quick lookup3 test did not show any improvements
		const DWORD uHash = (DWORD)(uCRC % SLOTS);

		HitblockKeyword_t* pEntry = m_dHash[uHash];
		HitblockKeyword_t** ppEntry = &m_dHash[uHash];
		while (pEntry)
		{
			// check crc
			if (pEntry->m_uWordid != uCRC)
			{
				// crc mismatch, try next entry
				ppEntry = &pEntry->m_pNextHash;
				pEntry = pEntry->m_pNextHash;
				continue;
			}

			// crc matches, check keyword
			register int iWordLen = iLen;
			register const char* a = pEntry->m_pKeyword;
			register const char* b = sWord;
			while (*a == *b && iWordLen--)
			{
				if (!*a || !iWordLen)
				{
					// known word, mtf it, and return id
					(*ppEntry) = pEntry->m_pNextHash;
					pEntry->m_pNextHash = m_dHash[uHash];
					m_dHash[uHash] = pEntry;
					return pEntry->m_uWordid;
				}
				a++;
				b++;
			}

			// collision detected!
			// our crc is taken as a wordid, but keyword does not match
			// welcome to the land of very tricky magic
			//
			// pEntry might either be a known exception, or a regular keyword
			// sWord might either be a known exception, or a new one
			// if they are not known, they needed to be added as exceptions now
			//
			// in case sWord is new, we need to assign a new unique wordid
			// for that, we keep incrementing the crc until it is unique
			// a starting point for wordid search loop would be handy
			//
			// let's scan the exceptions vector and work on all this
			//
			// NOTE, beware of the order, it is wordid asc, which does NOT guarantee crc asc
			// example, assume crc(w1)==X, crc(w2)==X+1, crc(w3)==X (collides with w1)
			// wordids will be X, X+1, X+2 but crcs will be X, X+1, X
			//
			// OPTIMIZE, might make sense to use binary search
			// OPTIMIZE, add early out somehow
			SphWordID_t uWordid = uCRC + 1;
			const int iExcLen = m_dExceptions.GetLength();
			int iExc = m_dExceptions.GetLength();
			ARRAY_FOREACH(i, m_dExceptions)
			{
				const HitblockKeyword_t* pExcWord = m_dExceptions[i].m_pEntry;

				// incoming word is a known exception? just return the pre-assigned wordid
				if (m_dExceptions[i].m_uCRC == uCRC && strncmp(pExcWord->m_pKeyword, sWord, iLen) == 0)
					return pExcWord->m_uWordid;

				// incoming word collided into a known exception? clear the matched entry; no need to re-add it (see below)
				if (pExcWord == pEntry)
					pEntry = NULL;

				// find first exception with wordid greater or equal to our candidate
				if (pExcWord->m_uWordid >= uWordid && iExc == iExcLen)
					iExc = i;
			}

			// okay, this is a new collision
			// if entry was a regular word, we have to add it
			if (pEntry)
			{
				m_dExceptions.Add();
				m_dExceptions.Last().m_pEntry = pEntry;
				m_dExceptions.Last().m_uCRC = uCRC;
			}

			// need to assign a new unique wordid now
			// keep scanning both exceptions and keywords for collisions
			for (;; )
			{
				// iExc must be either the first exception greater or equal to current candidate, or out of bounds
				assert(iExc == iExcLen || m_dExceptions[iExc].m_pEntry->m_uWordid >= uWordid);
				assert(iExc == 0 || m_dExceptions[iExc - 1].m_pEntry->m_uWordid < uWordid);

				// candidate collides with a known exception? increment it, and keep looking
				if (iExc < iExcLen && m_dExceptions[iExc].m_pEntry->m_uWordid == uWordid)
				{
					uWordid++;
					while (iExc < iExcLen && m_dExceptions[iExc].m_pEntry->m_uWordid < uWordid)
						iExc++;
					continue;
				}

				// candidate collides with a keyword? must be a regular one; add it as an exception, and keep looking
				HitblockKeyword_t* pCheck = m_dHash[(DWORD)(uWordid % SLOTS)];
				while (pCheck)
				{
					if (pCheck->m_uWordid == uWordid)
						break;
					pCheck = pCheck->m_pNextHash;
				}

				// no collisions; we've found our unique wordid!
				if (!pCheck)
					break;

				// got a collision; add it
				HitblockException_t& tColl = m_dExceptions.Add();
				tColl.m_pEntry = pCheck;
				tColl.m_uCRC = pCheck->m_uWordid; // not a known exception; hence, wordid must equal crc

				// and keep looking
				uWordid++;
				continue;
			}

			// and finally, we have that precious new wordid
			// so hash our new unique under its new unique adjusted wordid
			pEntry = HitblockAddKeyword((DWORD)(uWordid % SLOTS), sWord, iLen, uWordid);

			// add it as a collision too
			m_dExceptions.Add();
			m_dExceptions.Last().m_pEntry = pEntry;
			m_dExceptions.Last().m_uCRC = uCRC;

			// keep exceptions list sorted by wordid
			m_dExceptions.Sort();

			return pEntry->m_uWordid;
		}

		// new keyword with unique crc
		pEntry = HitblockAddKeyword(uHash, sWord, iLen, uCRC);
		return pEntry->m_uWordid;
	}

	struct DictKeywordTagged_t : public CSphDictKeywords::DictKeyword_t
	{
		int m_iBlock;
	};

	struct DictKeywordTaggedCmp_fn
	{
		static inline bool IsLess(const DictKeywordTagged_t& a, const DictKeywordTagged_t& b)
		{
			return strcmp(a.m_sKeyword, b.m_sKeyword) < 0;
		}
	};

	static void DictReadEntry(CSphBin* pBin, DictKeywordTagged_t& tEntry, BYTE* pKeyword)
	{
		int iKeywordLen = pBin->ReadByte();
		if (iKeywordLen < 0)
		{
			// early eof or read error; flag must be raised
			assert(pBin->IsError());
			return;
		}

		assert(iKeywordLen > 0 && iKeywordLen < MAX_KEYWORD_BYTES - 1);
		if (pBin->ReadBytes(pKeyword, iKeywordLen) < 0)
		{
			assert(pBin->IsError());
			return;
		}
		pKeyword[iKeywordLen] = '\0';

		tEntry.m_sKeyword = (char*)pKeyword;
		tEntry.m_uOff = pBin->UnzipOffset();
		tEntry.m_iDocs = pBin->UnzipInt();
		tEntry.m_iHits = pBin->UnzipInt();
		tEntry.m_uHint = (BYTE)pBin->ReadByte();
		if (tEntry.m_iDocs > SPH_SKIPLIST_BLOCK)
			tEntry.m_iSkiplistPos = pBin->UnzipInt();
		else
			tEntry.m_iSkiplistPos = 0;
	}

	void CSphDictKeywords::DictBegin(CSphAutofile& tTempDict, CSphAutofile& tDict, int iDictLimit, ThrottleState_t* pThrottle)
	{
		m_iTmpFD = tTempDict.GetFD();
		m_wrTmpDict.CloseFile();
		m_wrTmpDict.SetFile(tTempDict, NULL, m_sWriterError);
		m_wrTmpDict.SetThrottle(pThrottle);

		m_wrDict.CloseFile();
		m_wrDict.SetFile(tDict, NULL, m_sWriterError);
		m_wrDict.SetThrottle(pThrottle);
		m_wrDict.PutByte(1);

		m_iDictLimit = Max(iDictLimit, KEYWORD_CHUNK + DICT_CHUNK * (int)sizeof(DictKeyword_t)); // can't use less than 1 chunk
	}


	bool CSphDictKeywords::DictEnd(DictHeader_t* pHeader, int iMemLimit, CSphString& sError, ThrottleState_t* pThrottle)
	{
		DictFlush();
		m_wrTmpDict.CloseFile(); // tricky: file is not owned, so it won't get closed, and iTmpFD won't get invalidated

		if (!m_dDictBlocks.GetLength())
			m_wrDict.CloseFile();

		if (m_wrTmpDict.IsError() || m_wrDict.IsError())
		{
			sError.SetSprintf("dictionary write error (out of space?)");
			return false;
		}

		if (!m_dDictBlocks.GetLength())
		{
			pHeader->m_iDictCheckpointsOffset = m_wrDict.GetPos();
			pHeader->m_iDictCheckpoints = 0;
			return true;
		}

		// infix builder, if needed
		ISphInfixBuilder* pInfixer = sphCreateInfixBuilder(pHeader->m_iInfixCodepointBytes, &sError);
		if (!sError.IsEmpty())
		{
			SafeDelete(pInfixer);
			return false;
		}

		// initialize readers
		CSphVector<CSphBin*> dBins(m_dDictBlocks.GetLength());

		int iMaxBlock = 0;
		ARRAY_FOREACH(i, m_dDictBlocks)
			iMaxBlock = Max(iMaxBlock, m_dDictBlocks[i].m_iLen);

		iMemLimit = Max(iMemLimit, iMaxBlock * (int)m_dDictBlocks.GetLength());
		int iBinSize = CSphBin::CalcBinSize(iMemLimit,(int) m_dDictBlocks.GetLength(), "sort_dict");

		SphOffset_t iSharedOffset = -1;
		ARRAY_FOREACH(i, m_dDictBlocks)
		{
			dBins[i] = new CSphBin();
			dBins[i]->m_iFileLeft = m_dDictBlocks[i].m_iLen;
			dBins[i]->m_iFilePos = m_dDictBlocks[i].m_iPos;
			dBins[i]->Init(m_iTmpFD, &iSharedOffset, iBinSize);
			dBins[i]->SetThrottle(pThrottle);
		}

		// keywords storage
		BYTE* pKeywords = new BYTE[MAX_KEYWORD_BYTES * dBins.GetLength()];

#define LOC_CLEANUP() \
		{ \
			ARRAY_FOREACH ( iIdx, dBins ) \
				SafeDelete ( dBins[iIdx] ); \
			SafeDeleteArray ( pKeywords ); \
			SafeDelete ( pInfixer ); \
		}

		// do the sort
		CSphQueue < DictKeywordTagged_t, DictKeywordTaggedCmp_fn > qWords(dBins.GetLength());
		DictKeywordTagged_t tEntry;

		ARRAY_FOREACH(i, dBins)
		{
			DictReadEntry(dBins[i], tEntry, pKeywords + i * MAX_KEYWORD_BYTES);
			if (dBins[i]->IsError())
			{
				sError.SetSprintf("entry read error in dictionary sort (bin %d of %d)", i, dBins.GetLength());
				LOC_CLEANUP();
				return false;
			}

			tEntry.m_iBlock = i;
			qWords.Push(tEntry);
		}

		bool bHasMorphology = HasMorphology();
		CSphKeywordDeltaWriter tLastKeyword;
		int iWords = 0;
		while (qWords.GetLength())
		{
			const DictKeywordTagged_t& tWord = qWords.Root();
			const int iLen = strlen(tWord.m_sKeyword); // OPTIMIZE?

			// store checkpoints as needed
			if ((iWords % SPH_WORDLIST_CHECKPOINT) == 0)
			{
				// emit a checkpoint, unless we're at the very dict beginning
				if (iWords)
				{
					m_wrDict.ZipInt(0);
					m_wrDict.ZipInt(0);
				}

				BYTE* sClone = new BYTE[iLen + 1]; // OPTIMIZE? pool these?
				memcpy(sClone, tWord.m_sKeyword, iLen + 1);
				sClone[iLen] = '\0';

				CSphWordlistCheckpoint& tCheckpoint = m_dCheckpoints.Add();
				tCheckpoint.m_sWord = (char*)sClone;
				tCheckpoint.m_iWordlistOffset = m_wrDict.GetPos();

				tLastKeyword.Reset();
			}
			iWords++;

			// write final dict entry
			assert(iLen);
			assert(tWord.m_uOff);
			assert(tWord.m_iDocs);
			assert(tWord.m_iHits);

			tLastKeyword.PutDelta(m_wrDict, (const BYTE*)tWord.m_sKeyword, iLen);
			m_wrDict.ZipOffset(tWord.m_uOff);
			m_wrDict.ZipInt(tWord.m_iDocs);
			m_wrDict.ZipInt(tWord.m_iHits);
			if (tWord.m_uHint)
				m_wrDict.PutByte(tWord.m_uHint);
			if (tWord.m_iDocs > SPH_SKIPLIST_BLOCK)
				m_wrDict.ZipInt(tWord.m_iSkiplistPos);

			// build infixes
			if (pInfixer)
				pInfixer->AddWord((const BYTE*)tWord.m_sKeyword, iLen, m_dCheckpoints.GetLength(), bHasMorphology);

			// next
			int iBin = tWord.m_iBlock;
			qWords.Pop();

			if (!dBins[iBin]->IsDone())
			{
				DictReadEntry(dBins[iBin], tEntry, pKeywords + iBin * MAX_KEYWORD_BYTES);
				if (dBins[iBin]->IsError())
				{
					sError.SetSprintf("entry read error in dictionary sort (bin %d of %d)", iBin, dBins.GetLength());
					LOC_CLEANUP();
					return false;
				}

				tEntry.m_iBlock = iBin;
				qWords.Push(tEntry);
			}
		}

		// end of dictionary block
		m_wrDict.ZipInt(0);
		m_wrDict.ZipInt(0);

		// flush infix hash entries, if any
		if (pInfixer)
			pInfixer->SaveEntries(m_wrDict);

		// flush wordlist checkpoints (blocks)
		pHeader->m_iDictCheckpointsOffset = m_wrDict.GetPos();
		pHeader->m_iDictCheckpoints = m_dCheckpoints.GetLength();

		ARRAY_FOREACH(i, m_dCheckpoints)
		{
			const int iLen = strlen(m_dCheckpoints[i].m_sWord);

			assert(m_dCheckpoints[i].m_iWordlistOffset > 0);
			assert(iLen > 0 && iLen < MAX_KEYWORD_BYTES);

			m_wrDict.PutDword(iLen);
			m_wrDict.PutBytes(m_dCheckpoints[i].m_sWord, iLen);
			m_wrDict.PutOffset(m_dCheckpoints[i].m_iWordlistOffset);

			SafeDeleteArray(m_dCheckpoints[i].m_sWord);
		}

		// flush infix hash blocks
		if (pInfixer)
		{
			pHeader->m_iInfixBlocksOffset = pInfixer->SaveEntryBlocks(m_wrDict);
			pHeader->m_iInfixBlocksWordsSize = pInfixer->GetBlocksWordsSize();
			if (pHeader->m_iInfixBlocksOffset > UINT_MAX) // FIXME!!! change to int64
				sphDie("INTERNAL ERROR: dictionary size " INT64_FMT " overflow at dictend save", pHeader->m_iInfixBlocksOffset);
		}

		// flush header
		// mostly for debugging convenience
		// primary storage is in the index wide header
		m_wrDict.PutBytes("dict-header", 11);
		m_wrDict.ZipInt(pHeader->m_iDictCheckpoints);
		m_wrDict.ZipOffset(pHeader->m_iDictCheckpointsOffset);
		m_wrDict.ZipInt(pHeader->m_iInfixCodepointBytes);
		m_wrDict.ZipInt((DWORD)pHeader->m_iInfixBlocksOffset);

		// about it
		LOC_CLEANUP();
#undef LOC_CLEANUP

		m_wrDict.CloseFile();
		if (m_wrDict.IsError())
			sError.SetSprintf("dictionary write error (out of space?)");
		return !m_wrDict.IsError();
	}

	struct DictKeywordCmp_fn
	{
		inline bool IsLess(CSphDictKeywords::DictKeyword_t* a, CSphDictKeywords::DictKeyword_t* b) const
		{
			return strcmp(a->m_sKeyword, b->m_sKeyword) < 0;
		}
	};

	void CSphDictKeywords::DictFlush()
	{
		if (!m_dDictChunks.GetLength())
			return;
		assert(m_dDictChunks.GetLength() && m_dKeywordChunks.GetLength());

		// sort em
		int iTotalWords = m_dDictChunks.GetLength() * DICT_CHUNK - m_iDictChunkFree;
		CSphVector<DictKeyword_t*> dWords(iTotalWords);

		int iIdx = 0;
		ARRAY_FOREACH(i, m_dDictChunks)
		{
			int iWords = DICT_CHUNK;
			if (i == m_dDictChunks.GetLength() - 1)
				iWords -= m_iDictChunkFree;

			DictKeyword_t* pWord = m_dDictChunks[i];
			for (int j = 0; j < iWords; j++)
				dWords[iIdx++] = pWord++;
		}

		dWords.Sort(DictKeywordCmp_fn());

		// write em
		DictBlock_t& tBlock = m_dDictBlocks.Add();
		tBlock.m_iPos = m_wrTmpDict.GetPos();

		ARRAY_FOREACH(i, dWords)
		{
			const DictKeyword_t* pWord = dWords[i];
			int iLen = strlen(pWord->m_sKeyword);
			m_wrTmpDict.PutByte(iLen);
			m_wrTmpDict.PutBytes(pWord->m_sKeyword, iLen);
			m_wrTmpDict.ZipOffset(pWord->m_uOff);
			m_wrTmpDict.ZipInt(pWord->m_iDocs);
			m_wrTmpDict.ZipInt(pWord->m_iHits);
			m_wrTmpDict.PutByte(pWord->m_uHint);
			assert((pWord->m_iDocs > SPH_SKIPLIST_BLOCK) == (pWord->m_iSkiplistPos != 0));
			if (pWord->m_iDocs > SPH_SKIPLIST_BLOCK)
				m_wrTmpDict.ZipInt(pWord->m_iSkiplistPos);
		}

		tBlock.m_iLen = (int)(m_wrTmpDict.GetPos() - tBlock.m_iPos);

		// clean up buffers
		ARRAY_FOREACH(i, m_dDictChunks)
			SafeDeleteArray(m_dDictChunks[i]);
		m_dDictChunks.Resize(0);
		m_pDictChunk = NULL;
		m_iDictChunkFree = 0;

		ARRAY_FOREACH(i, m_dKeywordChunks)
			SafeDeleteArray(m_dKeywordChunks[i]);
		m_dKeywordChunks.Resize(0);
		m_pKeywordChunk = NULL;
		m_iKeywordChunkFree = 0;

		m_iMemUse = 0;
	}

	void CSphDictKeywords::DictEntry(const CSphDictEntry& tEntry)
	{
		// they say, this might just happen during merge
		// FIXME! can we make merge avoid sending such keywords to dict and assert here?
		if (!tEntry.m_iDocs)
			return;

		assert(tEntry.m_iHits);
		assert(tEntry.m_iDoclistLength > 0);

		DictKeyword_t* pWord = NULL;
		int iLen = strlen((char*)tEntry.m_sKeyword) + 1;

		for (;; )
		{
			// alloc dict entry
			if (!m_iDictChunkFree)
			{
				if (m_iDictLimit && (m_iMemUse + (int)sizeof(DictKeyword_t) * DICT_CHUNK) > m_iDictLimit)
					DictFlush();

				m_pDictChunk = new DictKeyword_t[DICT_CHUNK];
				m_iDictChunkFree = DICT_CHUNK;
				m_dDictChunks.Add(m_pDictChunk);
				m_iMemUse += sizeof(DictKeyword_t) * DICT_CHUNK;
			}

			// alloc keyword
			if (m_iKeywordChunkFree < iLen)
			{
				if (m_iDictLimit && (m_iMemUse + KEYWORD_CHUNK) > m_iDictLimit)
				{
					DictFlush();
					continue; // because we just flushed pWord
				}

				m_pKeywordChunk = new BYTE[KEYWORD_CHUNK];
				m_iKeywordChunkFree = KEYWORD_CHUNK;
				m_dKeywordChunks.Add(m_pKeywordChunk);
				m_iMemUse += KEYWORD_CHUNK;
			}
			// aw kay
			break;
		}

		pWord = m_pDictChunk++;
		m_iDictChunkFree--;
		pWord->m_sKeyword = (char*)m_pKeywordChunk;
		memcpy(m_pKeywordChunk, tEntry.m_sKeyword, iLen);
		m_pKeywordChunk[iLen - 1] = '\0';
		m_pKeywordChunk += iLen;
		m_iKeywordChunkFree -= iLen;

		pWord->m_uOff = tEntry.m_iDoclistOffset;
		pWord->m_iDocs = tEntry.m_iDocs;
		pWord->m_iHits = tEntry.m_iHits;
		pWord->m_uHint = sphDoclistHintPack(tEntry.m_iDocs, tEntry.m_iDoclistLength);
		pWord->m_iSkiplistPos = 0;
		if (tEntry.m_iDocs > SPH_SKIPLIST_BLOCK)
			pWord->m_iSkiplistPos = (int)(tEntry.m_iSkiplistOffset);
	}

	SphWordID_t CSphDictKeywords::GetWordID(BYTE* pWord)
	{
		SphWordID_t uCRC = CSphDictCRC<true>::GetWordID(pWord);
		if (!uCRC || !m_bHitblock)
			return uCRC;

		int iLen = strlen((const char*)pWord);
		return HitblockGetID((const char*)pWord, iLen, uCRC);
	}

	SphWordID_t CSphDictKeywords::GetWordIDWithMarkers(BYTE* pWord)
	{
		SphWordID_t uCRC = CSphDictCRC<true>::GetWordIDWithMarkers(pWord);
		if (!uCRC || !m_bHitblock)
			return uCRC;

		int iLen = strlen((const char*)pWord);
		return HitblockGetID((const char*)pWord, iLen, uCRC);
	}

	SphWordID_t CSphDictKeywords::GetWordIDNonStemmed(BYTE* pWord)
	{
		SphWordID_t uCRC = CSphDictCRC<true>::GetWordIDNonStemmed(pWord);
		if (!uCRC || !m_bHitblock)
			return uCRC;

		int iLen = strlen((const char*)pWord);
		return HitblockGetID((const char*)pWord, iLen, uCRC);
	}

	SphWordID_t CSphDictKeywords::GetWordID(const BYTE* pWord, int iLen, bool bFilterStops)
	{
		SphWordID_t uCRC = CSphDictCRC<true>::GetWordID(pWord, iLen, bFilterStops);
		if (!uCRC || !m_bHitblock)
			return uCRC;

		return HitblockGetID((const char*)pWord, iLen, uCRC); // !COMMIT would break, we kind of strcmp inside; but must never get called?
	}


///////////////////////////////////////////


	KeywordsBlockReader_c::KeywordsBlockReader_c(const BYTE* pBuf, bool bSkips)
	{
		m_bHaveSkips = bSkips;
		Reset(pBuf);
	}


	void KeywordsBlockReader_c::Reset(const BYTE* pBuf)
	{
		m_pBuf = pBuf;
		m_sWord[0] = '\0';
		m_iLen = 0;
		m_sKeyword = m_sWord;
	}


	bool KeywordsBlockReader_c::UnpackWord()
	{
		if (!m_pBuf)
			return false;

		// unpack next word
		// must be in sync with DictEnd()!
		BYTE uPack = *m_pBuf++;
		if (!uPack)
		{
			// ok, this block is over
			m_pBuf = NULL;
			m_iLen = 0;
			return false;
		}

		int iMatch, iDelta;
		if (uPack & 0x80)
		{
			iDelta = ((uPack >> 4) & 7) + 1;
			iMatch = uPack & 15;
		}
		else
		{
			iDelta = uPack & 127;
			iMatch = *m_pBuf++;
		}

		assert(iMatch + iDelta < (int)sizeof(m_sWord) - 1);
		assert(iMatch <= (int)strlen((char*)m_sWord));

		memcpy(m_sWord + iMatch, m_pBuf, iDelta);
		m_pBuf += iDelta;

		m_iLen = iMatch + iDelta;
		m_sWord[m_iLen] = '\0';

		m_iDoclistOffset = sphUnzipOffset(m_pBuf);
		m_iDocs = sphUnzipInt(m_pBuf);
		m_iHits = sphUnzipInt(m_pBuf);
		m_uHint = (m_iDocs >= DOCLIST_HINT_THRESH) ? *m_pBuf++ : 0;
		m_iDoclistHint = DoclistHintUnpack(m_iDocs, m_uHint);
		if (m_bHaveSkips && (m_iDocs > SPH_SKIPLIST_BLOCK))
			m_iSkiplistOffset = sphUnzipInt(m_pBuf);
		else
			m_iSkiplistOffset = 0;

		assert(m_iLen > 0);
		return true;
	}


///////////////////////////////////////////

	/// binary search for the first hit with wordid greater than or equal to reference
	static CSphWordHit* FindFirstGte(CSphWordHit* pHits, int iHits, SphWordID_t uID)
	{
		if (pHits->m_uWordID == uID)
			return pHits;

		CSphWordHit* pL = pHits;
		CSphWordHit* pR = pHits + iHits - 1;
		if (pL->m_uWordID > uID || pR->m_uWordID < uID)
			return NULL;

		while (pR - pL != 1)
		{
			CSphWordHit* pM = pL + (pR - pL) / 2;
			if (pM->m_uWordID < uID)
				pL = pM;
			else
				pR = pM;
		}

		assert(pR - pL == 1);
		assert(pL->m_uWordID < uID);
		assert(pR->m_uWordID >= uID);
		return pR;
	}

	/// full crc and keyword check
	static inline bool FullIsLess(const CSphDictKeywords::HitblockException_t& a, const CSphDictKeywords::HitblockException_t& b)
	{
		if (a.m_uCRC != b.m_uCRC)
			return a.m_uCRC < b.m_uCRC;
		return strcmp(a.m_pEntry->m_pKeyword, b.m_pEntry->m_pKeyword) < 0;
	}

	/// sort functor to compute collided hits reordering
	struct HitblockPatchSort_fn
	{
		const CSphDictKeywords::HitblockException_t* m_pExc;

		explicit HitblockPatchSort_fn(const CSphDictKeywords::HitblockException_t* pExc)
			: m_pExc(pExc)
		{}

		bool IsLess(int a, int b) const
		{
			return FullIsLess(m_pExc[a], m_pExc[b]);
		}
	};

	/// do hit block patching magic
	void CSphDictKeywords::HitblockPatch(CSphWordHit* pHits, int iHits) const
	{
		if (!pHits || iHits <= 0)
			return;

		const CSphVector<HitblockException_t>& dExc = m_dExceptions; // shortcut
		CSphVector<CSphWordHit*> dChunk;

		// reorder hit chunks for exceptions (aka crc collisions)
		for (int iFirst = 0; iFirst < dExc.GetLength() - 1; )
		{
			// find next span of collisions, iFirst inclusive, iMax exclusive ie. [iFirst,iMax)
			// (note that exceptions array is always sorted)
			SphWordID_t uFirstWordid = dExc[iFirst].m_pEntry->m_uWordid;
			assert(dExc[iFirst].m_uCRC == uFirstWordid);

			int iMax = iFirst + 1;
			SphWordID_t uSpan = uFirstWordid + 1;
			while (iMax < dExc.GetLength() && dExc[iMax].m_pEntry->m_uWordid == uSpan)
			{
				iMax++;
				uSpan++;
			}

			// check whether they are in proper order already
			bool bSorted = true;
			for (int i = iFirst; i < iMax - 1 && bSorted; i++)
				if (FullIsLess(dExc[i + 1], dExc[i]))
					bSorted = false;

			// order is ok; skip this span
			if (bSorted)
			{
				iFirst = iMax;
				continue;
			}

			// we need to fix up these collision hits
			// convert them from arbitrary "wordid asc" to strict "crc asc, keyword asc" order
			// lets begin with looking up hit chunks for every wordid
			dChunk.Resize(iMax - iFirst + 1);

			// find the end
			dChunk.Last() = FindFirstGte(pHits, iHits, uFirstWordid + iMax - iFirst);
			if (!dChunk.Last())
			{
				assert(iMax == dExc.GetLength() && pHits[iHits - 1].m_uWordID == uFirstWordid + iMax - 1 - iFirst);
				dChunk.Last() = pHits + iHits;
			}

			// find the start
			dChunk[0] = FindFirstGte(pHits, dChunk.Last() - pHits, uFirstWordid);
			assert(dChunk[0] && dChunk[0]->m_uWordID == uFirstWordid);

			// find the chunk starts
			for (int i = 1; i < dChunk.GetLength() - 1; i++)
			{
				dChunk[i] = FindFirstGte(dChunk[i - 1], dChunk.Last() - dChunk[i - 1], uFirstWordid + i);
				assert(dChunk[i] && dChunk[i]->m_uWordID == uFirstWordid + i);
			}

			CSphWordHit* pTemp;
			if (iMax - iFirst == 2)
			{
				// most frequent case, just two collisions
				// OPTIMIZE? allocate buffer for the smaller chunk, not just first chunk
				pTemp = new CSphWordHit[dChunk[1] - dChunk[0]];
				memcpy(pTemp, dChunk[0], (dChunk[1] - dChunk[0]) * sizeof(CSphWordHit));
				memmove(dChunk[0], dChunk[1], (dChunk[2] - dChunk[1]) * sizeof(CSphWordHit));
				memcpy(dChunk[0] + (dChunk[2] - dChunk[1]), pTemp, (dChunk[1] - dChunk[0]) * sizeof(CSphWordHit));
			}
			else
			{
				// generic case, more than two
				CSphVector<int> dReorder(iMax - iFirst);
				ARRAY_FOREACH(i, dReorder)
					dReorder[i] = i;

				HitblockPatchSort_fn fnSort(&dExc[iFirst]);
				dReorder.Sort(fnSort);

				// OPTIMIZE? could skip heading and trailing blocks that are already in position
				pTemp = new CSphWordHit[dChunk.Last() - dChunk[0]];
				CSphWordHit* pOut = pTemp;

				ARRAY_FOREACH(i, dReorder)
				{
					int iChunk = dReorder[i];
					int iChunkHits = dChunk[iChunk + 1] - dChunk[iChunk];
					memcpy(pOut, dChunk[iChunk], iChunkHits * sizeof(CSphWordHit));
					pOut += iChunkHits;
				}

				assert((pOut - pTemp) == (dChunk.Last() - dChunk[0]));
				memcpy(dChunk[0], pTemp, (dChunk.Last() - dChunk[0]) * sizeof(CSphWordHit));
			}

			// patching done
			SafeDeleteArray(pTemp);
			iFirst = iMax;
		}
	}

	const char* CSphDictKeywords::HitblockGetKeyword(SphWordID_t uWordID)
	{
		const DWORD uHash = (DWORD)(uWordID % SLOTS);

		HitblockKeyword_t* pEntry = m_dHash[uHash];
		while (pEntry)
		{
			// check crc
			if (pEntry->m_uWordid != uWordID)
			{
				// crc mismatch, try next entry
				pEntry = pEntry->m_pNextHash;
				continue;
			}

			return pEntry->m_pKeyword;
		}

		assert(m_dExceptions.GetLength());
		ARRAY_FOREACH(i, m_dExceptions)
			if (m_dExceptions[i].m_pEntry->m_uWordid == uWordID)
				return m_dExceptions[i].m_pEntry->m_pKeyword;

		sphWarning("hash missing value in operator [] (wordid=" INT64_FMT ", hash=%d)", (int64_t)uWordID, uHash);
		assert(0 && "hash missing value in operator []");
		return "\31oops";
	}

	//////////////////

	CSphDict* sphCreateDictionaryKeywords(const CSphDictSettings& tSettings,
		const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex,
		CSphString& sError)
	{
		CSphDict* pDict = new CSphDictKeywords();
		return SetupDictionary(pDict, tSettings, pFiles, pTokenizer, sIndex, sError);
	}


}