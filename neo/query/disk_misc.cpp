#include "neo/query/disk_misc.h"
#include "neo/index/index_VLN.h"
#include "neo/core/die.h"
#include "neo/io/unzip.h"




namespace NEO {

	ISphQword* DiskIndexQwordSetup_c::QwordSpawn(const XQKeyword_t& tWord) const
	{
		if (!tWord.m_pPayload)
		{
			WITH_QWORD(m_pIndex, false, Qword, return new Qword(tWord.m_bExpanded, tWord.m_bExcluded));
		}
		else
		{
			if (m_pIndex->GetSettings().m_eHitFormat == SPH_HIT_FORMAT_INLINE)
			{
				return new DiskPayloadQword_c<true>((const DiskSubstringPayload_t*)tWord.m_pPayload, tWord.m_bExcluded, m_tDoclist, m_tHitlist, m_pProfile);
			}
			else
			{
				return new DiskPayloadQword_c<false>((const DiskSubstringPayload_t*)tWord.m_pPayload, tWord.m_bExcluded, m_tDoclist, m_tHitlist, m_pProfile);
			}
		}
		return NULL;
	}


	bool DiskIndexQwordSetup_c::QwordSetup(ISphQword* pWord) const
	{
		DiskIndexQwordTraits_c* pMyWord = (DiskIndexQwordTraits_c*)pWord;

		// setup attrs
		pMyWord->m_tDoc.Reset(m_iDynamicRowitems);
		pMyWord->m_iMinID = m_uMinDocid;
		pMyWord->m_tDoc.m_uDocID = m_uMinDocid;

		return pMyWord->Setup(this);
	}


	bool DiskIndexQwordSetup_c::Setup(ISphQword* pWord) const
	{
		// there was a dynamic_cast here once but it's not necessary
		// maybe it worth to rewrite class hierarchy to avoid c-cast here?
		DiskIndexQwordTraits_c& tWord = *(DiskIndexQwordTraits_c*)pWord;

		if (m_eDocinfo == SPH_DOCINFO_INLINE)
		{
			tWord.m_iInlineAttrs = m_iInlineRowitems;
			tWord.m_pInlineFixup = m_pMinRow;
		}
		else
		{
			tWord.m_iInlineAttrs = 0;
			tWord.m_pInlineFixup = NULL;
		}

		// setup stats
		tWord.m_iDocs = 0;
		tWord.m_iHits = 0;

		CSphIndex_VLN* pIndex = (CSphIndex_VLN*)m_pIndex;

		// !COMMIT FIXME!
		// the below stuff really belongs in wordlist
		// which in turn really belongs in dictreader
		// which in turn might or might not be a part of dict

		// binary search through checkpoints for a one whose range matches word ID
		assert(pIndex->m_bPassedAlloc);
		assert(!pIndex->m_tWordlist.m_tBuf.IsEmpty());

		// empty index?
		if (!pIndex->m_tWordlist.m_dCheckpoints.GetLength())
			return false;

		const char* sWord = tWord.m_sDictWord.cstr();
		const bool bWordDict = pIndex->m_pDict->GetSettings().m_bWordDict;
		int iWordLen = sWord ? strlen(sWord) : 0;
		if (bWordDict && tWord.m_sWord.Ends("*"))
		{
			iWordLen = Max(iWordLen - 1, 0);

			// might match either infix or prefix
			int iMinLen = Max(pIndex->m_tSettings.m_iMinPrefixLen, pIndex->m_tSettings.m_iMinInfixLen);
			if (pIndex->m_tSettings.m_iMinPrefixLen)
				iMinLen = Min(iMinLen, pIndex->m_tSettings.m_iMinPrefixLen);
			if (pIndex->m_tSettings.m_iMinInfixLen)
				iMinLen = Min(iMinLen, pIndex->m_tSettings.m_iMinInfixLen);

			// bail out term shorter than prefix or infix allowed
			if (iWordLen < iMinLen)
				return false;
		}

		// leading special symbols trimming
		if (bWordDict && tWord.m_sDictWord.Begins("*"))
		{
			sWord++;
			iWordLen = Max(iWordLen - 1, 0);
			// bail out term shorter than infix allowed
			if (iWordLen < pIndex->m_tSettings.m_iMinInfixLen)
				return false;
		}

		const CSphWordlistCheckpoint* pCheckpoint = pIndex->m_tWordlist.FindCheckpoint(sWord, iWordLen, tWord.m_uWordID, false);
		if (!pCheckpoint)
			return false;

		// decode wordlist chunk
		const BYTE* pBuf = pIndex->m_tWordlist.AcquireDict(pCheckpoint);
		assert(pBuf);

		CSphDictEntry tRes;
		if (bWordDict)
		{
			KeywordsBlockReader_c tCtx(pBuf, m_pSkips != NULL);
			while (tCtx.UnpackWord())
			{
				// block is sorted
				// so once keywords are greater than the reference word, no more matches
				assert(tCtx.GetWordLen() > 0);
				int iCmp = sphDictCmpStrictly(sWord, iWordLen, tCtx.GetWord(), tCtx.GetWordLen());
				if (iCmp < 0)
					return false;
				if (iCmp == 0)
					break;
			}
			if (tCtx.GetWordLen() <= 0)
				return false;
			tRes = tCtx;

		}
		else
		{
			if (!pIndex->m_tWordlist.GetWord(pBuf, tWord.m_uWordID, tRes))
				return false;
		}

		const ESphHitless eMode = pIndex->m_tSettings.m_eHitless;
		tWord.m_iDocs = eMode == SPH_HITLESS_SOME ? (tRes.m_iDocs & HITLESS_DOC_MASK) : tRes.m_iDocs;
		tWord.m_iHits = tRes.m_iHits;
		tWord.m_bHasHitlist =
			(eMode == SPH_HITLESS_NONE) ||
			(eMode == SPH_HITLESS_SOME && !(tRes.m_iDocs & HITLESS_DOC_FLAG));

		if (m_bSetupReaders)
		{
			tWord.m_rdDoclist.SetBuffers(g_iReadBuffer, g_iReadUnhinted);
			tWord.m_rdDoclist.SetFile(m_tDoclist);
			tWord.m_rdDoclist.m_pProfile = m_pProfile;
			tWord.m_rdDoclist.m_eProfileState = SPH_QSTATE_READ_DOCS;

			// read in skiplist
			// OPTIMIZE? maybe cache hot decompressed lists?
			// OPTIMIZE? maybe add an option to decompress on preload instead?
			if (m_pSkips && tRes.m_iDocs > SPH_SKIPLIST_BLOCK)
			{
				const BYTE* pSkip = m_pSkips + tRes.m_iSkiplistOffset;

				tWord.m_dSkiplist.Add();
				tWord.m_dSkiplist.Last().m_iBaseDocid = 0;
				tWord.m_dSkiplist.Last().m_iOffset = tRes.m_iDoclistOffset;
				tWord.m_dSkiplist.Last().m_iBaseHitlistPos = 0;

				for (int i = 1; i < (tWord.m_iDocs / SPH_SKIPLIST_BLOCK); i++)
				{
					SkiplistEntry_t& t = tWord.m_dSkiplist.Add();
					SkiplistEntry_t& p = tWord.m_dSkiplist[tWord.m_dSkiplist.GetLength() - 2];
					t.m_iBaseDocid = p.m_iBaseDocid + SPH_SKIPLIST_BLOCK + (SphDocID_t)sphUnzipOffset(pSkip);
					t.m_iOffset = p.m_iOffset + 4 * SPH_SKIPLIST_BLOCK + sphUnzipOffset(pSkip);
					t.m_iBaseHitlistPos = p.m_iBaseHitlistPos + sphUnzipOffset(pSkip);
				}
			}

			tWord.m_rdDoclist.SeekTo(tRes.m_iDoclistOffset, tRes.m_iDoclistHint);

			tWord.m_rdHitlist.SetBuffers(g_iReadBuffer, g_iReadUnhinted);
			tWord.m_rdHitlist.SetFile(m_tHitlist);
			tWord.m_rdHitlist.m_pProfile = m_pProfile;
			tWord.m_rdHitlist.m_eProfileState = SPH_QSTATE_READ_HITS;
		}

		return true;
	}

}