#include "neo/core/word_list.h"
#include "neo/core/infix.h"
#include "neo/io/unzip.h"
#include "neo/index/enums.h"
#include "neo/dict/dict_entry.h"
#include "neo/dict/dict_keyword.h"
#include "neo/tools/utf8_tools.h"
#include "neo/core/mapped_checkpoint.h"
#include "neo/io/reader.h"
#include "neo/query/disk_misc.h"
#include "neo/query/node_cache.h"

namespace NEO {

	static const char* g_sTagInfixBlocks = "infix-blocks";
	static const char* g_sTagInfixEntries = "infix-entries";

	CWordlist::CWordlist()
		: m_dCheckpoints(0)
		, m_dInfixBlocks(0)
		, m_pWords(0)
		, m_tMapedCpReader(NULL)
	{
		m_iDictCheckpointsOffset = 0;
		m_bWordDict = false;
		m_pInfixBlocksWords = NULL;
	}

	CWordlist::~CWordlist()
	{
		Reset();
	}

	void CWordlist::Reset()
	{
		m_tBuf.Reset();
		m_dCheckpoints.Reset(0);
		m_pWords.Reset(0);
		SafeDeleteArray(m_pInfixBlocksWords);
		m_tMapedCpReader.Reset();
	}


	bool CWordlist::Preread(const char* sName, DWORD uVersion, bool bWordDict, CSphString& sError)
	{
		assert((uVersion >= 21 && bWordDict) || !bWordDict);
		assert(m_iDictCheckpointsOffset > 0);

		m_bHaveSkips = (uVersion >= 31);
		m_bWordDict = bWordDict;
		m_iWordsEnd = m_iDictCheckpointsOffset; // set wordlist end

		////////////////////////////
		// preload word checkpoints
		////////////////////////////

		////////////////////////////
		// fast path for CRC checkpoints - just maps data and use inplace CP reader
		if (!bWordDict)
		{
			if (!m_tBuf.Setup(sName, sError, false))
				return false;

			// read v.14 checkpoints
			// or convert v.10 checkpoints
			DWORD uFlags = 0x3; // 0x1 - WORD_WIDE, 0x2 - OFFSET_WIDE
			if_const(!USE_64BIT && uVersion < 11)
				uFlags &= 0x2;
			if (uVersion < 11)
				uFlags &= 0x1;
			switch (uFlags)
			{
			case 0x3:	m_tMapedCpReader = new CheckpointReader_T<true, true>(); break;
			case 0x2:	m_tMapedCpReader = new CheckpointReader_T<false, true>(); break;
			case 0x1:	m_tMapedCpReader = new CheckpointReader_T<true, false>(); break;
			case 0x0:
			default:	m_tMapedCpReader = new CheckpointReader_T<false, false>(); break;
			}

			return true;
		}

		////////////////////////////
		// regular path that loads checkpoints data

		CSphAutoreader tReader;
		if (!tReader.Open(sName, sError))
			return false;

		int64_t iFileSize = tReader.GetFilesize();

		int iCheckpointOnlySize = (int)(iFileSize - m_iDictCheckpointsOffset);
		if (m_iInfixCodepointBytes && m_iInfixBlocksOffset)
			iCheckpointOnlySize = (int)(m_iInfixBlocksOffset - strlen(g_sTagInfixBlocks) - m_iDictCheckpointsOffset);

		if (iFileSize - m_iDictCheckpointsOffset >= UINT_MAX)
		{
			sError.SetSprintf("dictionary meta overflow: meta size=" INT64_FMT ", total size=" INT64_FMT ", meta offset=" INT64_FMT,
				iFileSize - m_iDictCheckpointsOffset, iFileSize, (int64_t)m_iDictCheckpointsOffset);
			return false;
		}

		tReader.SeekTo(m_iDictCheckpointsOffset, iCheckpointOnlySize);

		assert(m_bWordDict);
		int iArenaSize = iCheckpointOnlySize
			- (sizeof(DWORD) + sizeof(SphOffset_t)) * m_dCheckpoints.GetLength()
			+ sizeof(BYTE) * m_dCheckpoints.GetLength();
		assert(iArenaSize >= 0);
		m_pWords.Reset(iArenaSize);

		BYTE* pWord = m_pWords.Begin();
		ARRAY_FOREACH(i, m_dCheckpoints)
		{
			m_dCheckpoints[i].m_sWord = (char*)pWord;

			const int iLen = tReader.GetDword();
			assert(iLen > 0);
			assert(iLen + 1 + (pWord - m_pWords.Begin()) <= iArenaSize);
			tReader.GetBytes(pWord, iLen);
			pWord[iLen] = '\0';
			pWord += iLen + 1;

			m_dCheckpoints[i].m_iWordlistOffset = tReader.GetOffset();
		}

		////////////////////////
		// preload infix blocks
		////////////////////////

		if (m_iInfixCodepointBytes && m_iInfixBlocksOffset)
		{
			// reading to vector as old version doesn't store total infix words length
			CSphTightVector<BYTE> dInfixWords;
			dInfixWords.Reserve((int)m_iInfixBlocksWordsSize);

			tReader.SeekTo(m_iInfixBlocksOffset, (int)(iFileSize - m_iInfixBlocksOffset));
			m_dInfixBlocks.Resize(tReader.UnzipInt());
			ARRAY_FOREACH(i, m_dInfixBlocks)
			{
				int iBytes = tReader.UnzipInt();

				int iOff = dInfixWords.GetLength();
				m_dInfixBlocks[i].m_iInfixOffset = iOff;
				dInfixWords.Resize(iOff + iBytes + 1);

				tReader.GetBytes(dInfixWords.Begin() + iOff, iBytes);
				dInfixWords[iOff + iBytes] = '\0';

				m_dInfixBlocks[i].m_iOffset = tReader.UnzipInt();
			}

			// fix-up offset to pointer
			m_pInfixBlocksWords = dInfixWords.LeakData();
			ARRAY_FOREACH(i, m_dInfixBlocks)
				m_dInfixBlocks[i].m_sInfix = (const char*)m_pInfixBlocksWords + m_dInfixBlocks[i].m_iInfixOffset;

			// FIXME!!! store and load that explicitly
			if (m_dInfixBlocks.GetLength())
				m_iWordsEnd = m_dInfixBlocks.Begin()->m_iOffset - strlen(g_sTagInfixEntries);
			else
				m_iWordsEnd -= strlen(g_sTagInfixEntries);
		}

		if (tReader.GetErrorFlag())
		{
			sError = tReader.GetErrorMessage();
			return false;
		}

		tReader.Close();

		// mapping up only wordlist without meta (checkpoints, infixes, etc)
		if (!m_tBuf.Setup(sName, sError, false))
			return false;

		return true;
	}


	void CWordlist::DebugPopulateCheckpoints()
	{
		if (!m_tMapedCpReader.Ptr())
			return;

		const BYTE* pCur = m_tBuf.GetWritePtr() + m_iDictCheckpointsOffset;
		ARRAY_FOREACH(i, m_dCheckpoints)
		{
			pCur = m_tMapedCpReader->ReadEntry(pCur, m_dCheckpoints[i]);
		}

		m_tMapedCpReader.Reset();
	}


	const CSphWordlistCheckpoint* CWordlist::FindCheckpoint(const char* sWord, int iWordLen, SphWordID_t iWordID, bool bStarMode) const
	{
		if (m_tMapedCpReader.Ptr()) // FIXME!!! fall to regular checkpoints after data got read
		{
			MappedCheckpoint_fn tPred(m_dCheckpoints.Begin(), m_tBuf.GetWritePtr() + m_iDictCheckpointsOffset, m_tMapedCpReader.Ptr());
			return sphSearchCheckpoint(sWord, iWordLen, iWordID, bStarMode, m_bWordDict, m_dCheckpoints.Begin(), &m_dCheckpoints.Last(), tPred);
		}
		else
		{
			return sphSearchCheckpoint(sWord, iWordLen, iWordID, bStarMode, m_bWordDict, m_dCheckpoints.Begin(), &m_dCheckpoints.Last());
		}
	}



	bool CWordlist::GetWord(const BYTE* pBuf, SphWordID_t iWordID, CSphDictEntry& tWord) const
	{
		SphWordID_t iLastID = 0;
		SphOffset_t uLastOff = 0;

		for (;; )
		{
			// unpack next word ID
			const SphWordID_t iDeltaWord = sphUnzipWordid(pBuf); // FIXME! slow with 32bit wordids

			if (iDeltaWord == 0) // wordlist chunk is over
				return false;

			iLastID += iDeltaWord;

			// list is sorted, so if there was no match, there's no such word
			if (iLastID > iWordID)
				return false;

			// unpack next offset
			const SphOffset_t iDeltaOffset = sphUnzipOffset(pBuf);
			uLastOff += iDeltaOffset;

			// unpack doc/hit count
			const int iDocs = sphUnzipInt(pBuf);
			const int iHits = sphUnzipInt(pBuf);
			SphOffset_t iSkiplistPos = 0;
			if (m_bHaveSkips && (iDocs > SPH_SKIPLIST_BLOCK))
				iSkiplistPos = sphUnzipOffset(pBuf);

			assert(iDeltaOffset);
			assert(iDocs);
			assert(iHits);

			// it matches?!
			if (iLastID == iWordID)
			{
				sphUnzipWordid(pBuf); // might be 0 at checkpoint
				const SphOffset_t iDoclistLen = sphUnzipOffset(pBuf);

				tWord.m_iDoclistOffset = uLastOff;
				tWord.m_iDocs = iDocs;
				tWord.m_iHits = iHits;
				tWord.m_iDoclistHint = (int)iDoclistLen;
				tWord.m_iSkiplistOffset = iSkiplistPos;
				return true;
			}
		}
	}

	const BYTE* CWordlist::AcquireDict(const CSphWordlistCheckpoint* pCheckpoint) const
	{
		assert(pCheckpoint);
		assert(m_dCheckpoints.GetLength());
		assert(pCheckpoint >= m_dCheckpoints.Begin() && pCheckpoint <= &m_dCheckpoints.Last());

		SphOffset_t iOff = pCheckpoint->m_iWordlistOffset;
		if (m_tMapedCpReader.Ptr())
		{
			MappedCheckpoint_fn tPred(m_dCheckpoints.Begin(), m_tBuf.GetWritePtr() + m_iDictCheckpointsOffset, m_tMapedCpReader.Ptr());
			iOff = tPred(pCheckpoint).m_iWordlistOffset;
		}

		assert(!m_tBuf.IsEmpty());
		assert(iOff > 0 && iOff <= (int64_t)m_tBuf.GetLengthBytes() && iOff < (int64_t)m_tBuf.GetLengthBytes());

		return m_tBuf.GetWritePtr() + iOff;
	}


	void CWordlist::GetPrefixedWords(const char* sSubstring, int iSubLen, const char* sWildcard, Args_t& tArgs) const
	{
		assert(sSubstring && *sSubstring && iSubLen > 0);

		// empty index?
		if (!m_dCheckpoints.GetLength())
			return;

		DictEntryDiskPayload_t tDict2Payload(tArgs.m_bPayload, tArgs.m_eHitless);

		int dWildcard[SPH_MAX_WORD_LEN + 1];
		int* pWildcard = (sphIsUTF8(sWildcard) && sphUTF8ToWideChar(sWildcard, dWildcard, SPH_MAX_WORD_LEN)) ? dWildcard : NULL;

		const CSphWordlistCheckpoint* pCheckpoint = FindCheckpoint(sSubstring, iSubLen, 0, true);
		const int iSkipMagic = (BYTE(*sSubstring) < 0x20); // whether to skip heading magic chars in the prefix, like NONSTEMMED maker
		while (pCheckpoint)
		{
			// decode wordlist chunk
			KeywordsBlockReader_c tDictReader(AcquireDict(pCheckpoint), m_bHaveSkips);
			while (tDictReader.UnpackWord())
			{
				// block is sorted
				// so once keywords are greater than the prefix, no more matches
				int iCmp = sphDictCmp(sSubstring, iSubLen, (const char*)tDictReader.m_sKeyword, tDictReader.GetWordLen());
				if (iCmp < 0)
					break;

				if (sphInterrupted())
					break;

				// does it match the prefix *and* the entire wildcard?
				if (iCmp == 0 && sphWildcardMatch((const char*)tDictReader.m_sKeyword + iSkipMagic, sWildcard, pWildcard))
					tDict2Payload.Add(tDictReader, tDictReader.GetWordLen());
			}

			if (sphInterrupted())
				break;

			pCheckpoint++;
			if (pCheckpoint > &m_dCheckpoints.Last())
				break;

			if (sphDictCmp(sSubstring, iSubLen, pCheckpoint->m_sWord, strlen(pCheckpoint->m_sWord)) < 0)
				break;
		}

		tDict2Payload.Convert(tArgs);
	}


	void CWordlist::GetInfixedWords(const char* sSubstring, int iSubLen, const char* sWildcard, Args_t& tArgs) const
	{
		// dict must be of keywords type, and fully cached
		// mmap()ed in the worst case, should we ever banish it to disk again
		if (m_tBuf.IsEmpty() || !m_dCheckpoints.GetLength())
			return;

		assert(!m_tMapedCpReader.Ptr());

		// extract key1, upto 6 chars from infix start
		int iBytes1 = sphGetInfixLength(sSubstring, iSubLen, m_iInfixCodepointBytes);

		// lookup key1
		// OPTIMIZE? maybe lookup key2 and reduce checkpoint set size, if possible?
		CSphVector<DWORD> dPoints;
		if (!sphLookupInfixCheckpoints(sSubstring, iBytes1, m_tBuf.GetWritePtr(), m_dInfixBlocks, m_iInfixCodepointBytes, dPoints))
			return;

		DictEntryDiskPayload_t tDict2Payload(tArgs.m_bPayload, tArgs.m_eHitless);
		const int iSkipMagic = (tArgs.m_bHasMorphology ? 1 : 0); // whether to skip heading magic chars in the prefix, like NONSTEMMED maker

		int dWildcard[SPH_MAX_WORD_LEN + 1];
		int* pWildcard = (sphIsUTF8(sWildcard) && sphUTF8ToWideChar(sWildcard, dWildcard, SPH_MAX_WORD_LEN)) ? dWildcard : NULL;

		// walk those checkpoints, check all their words
		ARRAY_FOREACH(i, dPoints)
		{
			// OPTIMIZE? add a quicker path than a generic wildcard for "*infix*" case?
			KeywordsBlockReader_c tDictReader(m_tBuf.GetWritePtr() + m_dCheckpoints[dPoints[i] - 1].m_iWordlistOffset, m_bHaveSkips);
			while (tDictReader.UnpackWord())
			{
				if (sphInterrupted())
					break;

				// stemmed terms should not match suffixes
				if (tArgs.m_bHasMorphology && *tDictReader.m_sKeyword != MAGIC_WORD_HEAD_NONSTEMMED)
					continue;

				if (sphWildcardMatch((const char*)tDictReader.m_sKeyword + iSkipMagic, sWildcard, pWildcard))
					tDict2Payload.Add(tDictReader, tDictReader.GetWordLen());
			}

			if (sphInterrupted())
				break;
		}

		tDict2Payload.Convert(tArgs);
	}


	void CWordlist::SuffixGetChekpoints(const SuggestResult_t&, const char* sSuffix, int iLen, CSphVector<DWORD>& dCheckpoints) const
	{
		sphLookupInfixCheckpoints(sSuffix, iLen, m_tBuf.GetWritePtr(), m_dInfixBlocks, m_iInfixCodepointBytes, dCheckpoints);
	}

	void CWordlist::SetCheckpoint(SuggestResult_t& tRes, DWORD iCP) const
	{
		assert(tRes.m_pWordReader);
		KeywordsBlockReader_c* pReader = (KeywordsBlockReader_c*)tRes.m_pWordReader;
		pReader->Reset(m_tBuf.GetWritePtr() + m_dCheckpoints[iCP - 1].m_iWordlistOffset);
	}

	bool CWordlist::ReadNextWord(SuggestResult_t& tRes, DictWord_t& tWord) const
	{
		KeywordsBlockReader_c* pReader = (KeywordsBlockReader_c*)tRes.m_pWordReader;
		if (!pReader->UnpackWord())
			return false;

		tWord.m_sWord = pReader->GetWord();
		tWord.m_iLen = pReader->GetWordLen();
		tWord.m_iDocs = pReader->m_iDocs;
		return true;
	}

}
