#include "neo/core/hit_builder.h"
#include "neo/tools/docinfo_transformer.h"
#include "neo/query/filter_settings.h"
#include "neo/index/enums.h"
#include "neo/index/index_settings.h"
#include "neo/core/skip_list.h"
#include "neo/core/die.h"
#include "neo/utility/encode.h"
#include "neo/int/throttle_state.h"
#include "neo/io/io.h"

namespace NEO {

	CSphHitBuilder::CSphHitBuilder(const CSphIndexSettings& tSettings,
		const CSphVector<SphWordID_t>& dHitless, bool bMerging, int iBufSize,
		CSphDict* pDict, CSphString* sError)
		: m_dWriteBuffer(iBufSize)
		, m_dMinRow(0)
		, m_iPrevHitPos(0)
		, m_bGotFieldEnd(false)
		, m_dHitlessWords(dHitless)
		, m_pDict(pDict)
		, m_pLastError(sError)
		, m_eHitFormat(tSettings.m_eHitFormat)
		, m_eHitless(tSettings.m_eHitless)
		, m_bMerging(bMerging)
	{
		m_sLastKeyword[0] = '\0';
		HitReset();

		m_iLastHitlistPos = 0;
		m_iLastHitlistDelta = 0;
		m_dLastDocFields.UnsetAll();
		m_uLastDocHits = 0;

		m_tWord.m_iDoclistOffset = 0;
		m_tWord.m_iDocs = 0;
		m_tWord.m_iHits = 0;

		assert(m_pDict);
		assert(m_pLastError);

		m_pThrottle = &g_tThrottle;
	}


	void CSphHitBuilder::SetMin(const CSphRowitem* pDynamic, int iDynamic)
	{
		assert(!iDynamic || pDynamic);

		m_dMinRow.Reset(iDynamic);
		ARRAY_FOREACH(i, m_dMinRow)
		{
			m_dMinRow[i] = pDynamic[i];
		}
	}


	bool CSphHitBuilder::CreateIndexFiles(const char* sDocName, const char* sHitName, const char* sSkipName,
		bool bInplace, int iWriteBuffer, CSphAutofile& tHit, SphOffset_t* pSharedOffset)
	{
		// doclist and hitlist files
		m_wrDoclist.CloseFile();
		m_wrHitlist.CloseFile();
		m_wrSkiplist.CloseFile();

		m_wrDoclist.SetBufferSize(m_dWriteBuffer.GetLength());
		m_wrHitlist.SetBufferSize(bInplace ? iWriteBuffer : m_dWriteBuffer.GetLength());
		m_wrDoclist.SetThrottle(m_pThrottle);
		m_wrHitlist.SetThrottle(m_pThrottle);

		if (!m_wrDoclist.OpenFile(sDocName, *m_pLastError))
			return false;

		if (bInplace)
		{
			sphSeek(tHit.GetFD(), 0, SEEK_SET);
			m_wrHitlist.SetFile(tHit, pSharedOffset, *m_pLastError);
		}
		else
		{
			if (!m_wrHitlist.OpenFile(sHitName, *m_pLastError))
				return false;
		}

		if (!m_wrSkiplist.OpenFile(sSkipName, *m_pLastError))
			return false;

		// put dummy byte (otherwise offset would start from 0, first delta would be 0
		// and VLB encoding of offsets would fuckup)
		BYTE bDummy = 1;
		m_wrDoclist.PutBytes(&bDummy, 1);
		m_wrHitlist.PutBytes(&bDummy, 1);
		m_wrSkiplist.PutBytes(&bDummy, 1);
		return true;
	}


	void CSphHitBuilder::HitReset()
	{
		m_tLastHit.m_uDocID = 0;
		m_tLastHit.m_uWordID = 0;
		m_tLastHit.m_iWordPos = EMPTY_HIT;
		m_tLastHit.m_sKeyword = m_sLastKeyword;
		m_iPrevHitPos = 0;
		m_bGotFieldEnd = false;
	}


	// doclist entry format
	// (with the new and shiny "inline hit" format, that is)
	//
	// zint docid_delta
	// zint[] inline_attrs
	// zint doc_hits
	// if doc_hits==1:
	// 		zint field_pos
	// 		zint field_no
	// else:
	// 		zint field_mask
	// 		zint hlist_offset_delta
	//
	// so 4 bytes/doc minimum
	// avg 4-6 bytes/doc according to our tests


	void CSphHitBuilder::DoclistBeginEntry(SphDocID_t uDocid, const DWORD* pAttrs)
	{
		// build skiplist
		// that is, save decoder state and doclist position per every 128 documents
		if ((m_tWord.m_iDocs & (SPH_SKIPLIST_BLOCK - 1)) == 0)
		{
			SkiplistEntry_t& tBlock = m_dSkiplist.Add();
			tBlock.m_iBaseDocid = m_tLastHit.m_uDocID;
			tBlock.m_iOffset = m_wrDoclist.GetPos();
			tBlock.m_iBaseHitlistPos = m_iLastHitlistPos;
		}

		// begin doclist entry
		m_wrDoclist.ZipOffset(uDocid - m_tLastHit.m_uDocID);
		assert(!pAttrs || m_dMinRow.GetLength());
		if (pAttrs)
		{
			ARRAY_FOREACH(i, m_dMinRow)
				m_wrDoclist.ZipInt(pAttrs[i] - m_dMinRow[i]);
		}
	}


	void CSphHitBuilder::DoclistEndEntry(Hitpos_t uLastPos)
	{
		// end doclist entry
		if (m_eHitFormat == SPH_HIT_FORMAT_INLINE)
		{
			bool bIgnoreHits =
				(m_eHitless == SPH_HITLESS_ALL) ||
				(m_eHitless == SPH_HITLESS_SOME && (m_tWord.m_iDocs & HITLESS_DOC_FLAG));

			// inline the only hit into doclist (unless it is completely discarded)
			// and finish doclist entry
			m_wrDoclist.ZipInt(m_uLastDocHits);
			if (m_uLastDocHits == 1 && !bIgnoreHits)
			{
				m_wrHitlist.SeekTo(m_iLastHitlistPos);
				m_wrDoclist.ZipInt(uLastPos & 0x7FFFFF);
				m_wrDoclist.ZipInt(uLastPos >> 23);
				m_iLastHitlistPos -= m_iLastHitlistDelta;
				assert(m_iLastHitlistPos >= 0);

			}
			else
			{
				m_wrDoclist.ZipInt(m_dLastDocFields.GetMask32());
				m_wrDoclist.ZipOffset(m_iLastHitlistDelta);
			}
		}
		else // plain format - finish doclist entry
		{
			assert(m_eHitFormat == SPH_HIT_FORMAT_PLAIN);
			m_wrDoclist.ZipOffset(m_iLastHitlistDelta);
			m_wrDoclist.ZipInt(m_dLastDocFields.GetMask32());
			m_wrDoclist.ZipInt(m_uLastDocHits);
		}
		m_dLastDocFields.UnsetAll();
		m_uLastDocHits = 0;

		// update keyword stats
		m_tWord.m_iDocs++;
	}


	void CSphHitBuilder::DoclistEndList()
	{
		// emit eof marker
		m_wrDoclist.ZipInt(0);

		// emit skiplist
		// OPTIMIZE? placing it after doclist means an extra seek on searching
		// however placing it before means some (longer) doclist data moves while indexing
		if (m_tWord.m_iDocs > SPH_SKIPLIST_BLOCK)
		{
			assert(m_dSkiplist.GetLength());
			assert(m_dSkiplist[0].m_iOffset == m_tWord.m_iDoclistOffset);
			assert(m_dSkiplist[0].m_iBaseDocid == 0);
			assert(m_dSkiplist[0].m_iBaseHitlistPos == 0);

			m_tWord.m_iSkiplistOffset = m_wrSkiplist.GetPos();

			// delta coding, but with a couple of skiplist specific tricks
			// 1) first entry is omitted, it gets reconstructed from dict itself
			// both base values are zero, and offset equals doclist offset
			// 2) docids are at least SKIPLIST_BLOCK apart
			// doclist entries are at least 4*SKIPLIST_BLOCK bytes apart
			// so we additionally subtract that to improve delta coding
			// 3) zero deltas are allowed and *not* used as any markers,
			// as we know the exact skiplist entry count anyway
			SkiplistEntry_t tLast = m_dSkiplist[0];
			for (int i = 1; i < m_dSkiplist.GetLength(); i++)
			{
				const SkiplistEntry_t& t = m_dSkiplist[i];
				assert(t.m_iBaseDocid - tLast.m_iBaseDocid >= SPH_SKIPLIST_BLOCK);
				assert(t.m_iOffset - tLast.m_iOffset >= 4 * SPH_SKIPLIST_BLOCK);
				m_wrSkiplist.ZipOffset(t.m_iBaseDocid - tLast.m_iBaseDocid - SPH_SKIPLIST_BLOCK);
				m_wrSkiplist.ZipOffset(t.m_iOffset - tLast.m_iOffset - 4 * SPH_SKIPLIST_BLOCK);
				m_wrSkiplist.ZipOffset(t.m_iBaseHitlistPos - tLast.m_iBaseHitlistPos);
				tLast = t;
			}
		}

		// in any event, reset skiplist
		m_dSkiplist.Resize(0);
	}


	void CSphHitBuilder::cidxHit(CSphAggregateHit* pHit, const CSphRowitem* pAttrs)
	{
		assert(
			(pHit->m_uWordID != 0 && pHit->m_iWordPos != EMPTY_HIT && pHit->m_uDocID != 0) || // it's either ok hit
			(pHit->m_uWordID == 0 && pHit->m_iWordPos == EMPTY_HIT)); // or "flush-hit"

		/////////////
		// next word
		/////////////

		bool bNextWord = (m_tLastHit.m_uWordID != pHit->m_uWordID ||
			(m_pDict->GetSettings().m_bWordDict && strcmp((char*)m_tLastHit.m_sKeyword, (char*)pHit->m_sKeyword))); // OPTIMIZE?
		bool bNextDoc = bNextWord || (m_tLastHit.m_uDocID != pHit->m_uDocID);

		if (m_bGotFieldEnd && (bNextWord || bNextDoc))
		{
			// writing hits only without duplicates
			assert(HITMAN::GetPosWithField(m_iPrevHitPos) != HITMAN::GetPosWithField(m_tLastHit.m_iWordPos));
			HITMAN::SetEndMarker(&m_tLastHit.m_iWordPos);
			m_wrHitlist.ZipInt(m_tLastHit.m_iWordPos - m_iPrevHitPos);
			m_bGotFieldEnd = false;
		}


		if (bNextDoc)
		{
			// finish hitlist, if any
			Hitpos_t uLastPos = m_tLastHit.m_iWordPos;
			if (m_tLastHit.m_iWordPos != EMPTY_HIT)
			{
				m_wrHitlist.ZipInt(0);
				m_tLastHit.m_iWordPos = EMPTY_HIT;
				m_iPrevHitPos = EMPTY_HIT;
			}

			// finish doclist entry, if any
			if (m_tLastHit.m_uDocID)
				DoclistEndEntry(uLastPos);
		}

		if (bNextWord)
		{
			// finish doclist, if any
			if (m_tLastHit.m_uDocID)
			{
				// emit end-of-doclist marker
				DoclistEndList();

				// emit dict entry
				m_tWord.m_uWordID = m_tLastHit.m_uWordID;
				m_tWord.m_sKeyword = m_tLastHit.m_sKeyword;
				m_tWord.m_iDoclistLength = m_wrDoclist.GetPos() - m_tWord.m_iDoclistOffset;
				m_pDict->DictEntry(m_tWord);

				// reset trackers
				m_tWord.m_iDocs = 0;
				m_tWord.m_iHits = 0;

				m_tLastHit.m_uDocID = 0;
				m_iLastHitlistPos = 0;
			}

			// flush wordlist, if this is the end
			if (pHit->m_iWordPos == EMPTY_HIT)
			{
				m_pDict->DictEndEntries(m_wrDoclist.GetPos());
				return;
			}

			assert(pHit->m_uWordID > m_tLastHit.m_uWordID
				|| (m_pDict->GetSettings().m_bWordDict &&
					pHit->m_uWordID == m_tLastHit.m_uWordID && strcmp((char*)pHit->m_sKeyword, (char*)m_tLastHit.m_sKeyword) > 0)
				|| m_bMerging);
			m_tWord.m_iDoclistOffset = m_wrDoclist.GetPos();
			m_tLastHit.m_uWordID = pHit->m_uWordID;
			if (m_pDict->GetSettings().m_bWordDict)
			{
				assert(strlen((char*)pHit->m_sKeyword) < sizeof(m_sLastKeyword) - 1);
				strncpy((char*)m_tLastHit.m_sKeyword, (char*)pHit->m_sKeyword, sizeof(m_sLastKeyword)); // OPTIMIZE?
			}
		}

		if (bNextDoc)
		{
			// begin new doclist entry for new doc id
			assert(pHit->m_uDocID > m_tLastHit.m_uDocID);
			assert(m_wrHitlist.GetPos() >= m_iLastHitlistPos);

			DoclistBeginEntry(pHit->m_uDocID, pAttrs);
			m_iLastHitlistDelta = m_wrHitlist.GetPos() - m_iLastHitlistPos;

			m_tLastHit.m_uDocID = pHit->m_uDocID;
			m_iLastHitlistPos = m_wrHitlist.GetPos();
		}

		///////////
		// the hit
		///////////

		if (!pHit->m_dFieldMask.TestAll(false)) // merge aggregate hits into the current hit
		{
			int iHitCount = pHit->GetAggrCount();
			assert(m_eHitless);
			assert(iHitCount);
			assert(!pHit->m_dFieldMask.TestAll(false));

			m_uLastDocHits += iHitCount;
			for (int i = 0; i < FieldMask_t::SIZE; i++)
				m_dLastDocFields[i] |= pHit->m_dFieldMask[i];
			m_tWord.m_iHits += iHitCount;

			if (m_eHitless == SPH_HITLESS_SOME)
				m_tWord.m_iDocs |= HITLESS_DOC_FLAG;

		}
		else // handle normal hits
		{
			Hitpos_t iHitPosPure = HITMAN::GetPosWithField(pHit->m_iWordPos);

			// skip any duplicates and keep only 1st position in place
			// duplicates are hit with same position: [N, N] [N, N | FIELDEND_MASK] [N | FIELDEND_MASK, N] [N | FIELDEND_MASK, N | FIELDEND_MASK]
			if (iHitPosPure == m_tLastHit.m_iWordPos)
				return;

			// storing previous hit that might have a field end flag
			if (m_bGotFieldEnd)
			{
				if (HITMAN::GetField(pHit->m_iWordPos) != HITMAN::GetField(m_tLastHit.m_iWordPos)) // is field end flag real?
					HITMAN::SetEndMarker(&m_tLastHit.m_iWordPos);

				m_wrHitlist.ZipInt(m_tLastHit.m_iWordPos - m_iPrevHitPos);
				m_bGotFieldEnd = false;
			}

			/* duplicate hits from duplicated documents
			... 0x03, 0x03 ...
			... 0x8003, 0x8003 ...
			... 1, 0x8003, 0x03 ...
			... 1, 0x03, 0x8003 ...
			... 1, 0x8003, 0x04 ...
			... 1, 0x03, 0x8003, 0x8003 ...
			... 1, 0x03, 0x8003, 0x03 ...
			*/

			assert(m_tLastHit.m_iWordPos < pHit->m_iWordPos);

			// add hit delta without field end marker
			// or postpone adding to hitlist till got another uniq hit
			if (iHitPosPure == pHit->m_iWordPos)
			{
				m_wrHitlist.ZipInt(pHit->m_iWordPos - m_tLastHit.m_iWordPos);
				m_tLastHit.m_iWordPos = pHit->m_iWordPos;
			}
			else
			{
				assert(HITMAN::IsEnd(pHit->m_iWordPos));
				m_bGotFieldEnd = true;
				m_iPrevHitPos = m_tLastHit.m_iWordPos;
				m_tLastHit.m_iWordPos = HITMAN::GetPosWithField(pHit->m_iWordPos);
			}

			// update matched fields mask
			m_dLastDocFields.Set(HITMAN::GetField(pHit->m_iWordPos));

			m_uLastDocHits++;
			m_tWord.m_iHits++;
		}
	}


	bool CSphHitBuilder::cidxDone(int iMemLimit, int iMinInfixLen, int iMaxCodepointLen, DictHeader_t* pDictHeader)
	{
		assert(pDictHeader);

		if (m_bGotFieldEnd)
		{
			HITMAN::SetEndMarker(&m_tLastHit.m_iWordPos);
			m_wrHitlist.ZipInt(m_tLastHit.m_iWordPos - m_iPrevHitPos);
			m_bGotFieldEnd = false;
		}

		// finalize dictionary
		// in dict=crc mode, just flushes wordlist checkpoints
		// in dict=keyword mode, also creates infix index, if needed

		if (iMinInfixLen > 0 && m_pDict->GetSettings().m_bWordDict)
			pDictHeader->m_iInfixCodepointBytes = iMaxCodepointLen;

		if (!m_pDict->DictEnd(pDictHeader, iMemLimit, *m_pLastError, m_pThrottle))
			return false;

		// close all data files
		m_wrDoclist.CloseFile();
		m_wrHitlist.CloseFile(true);
		return !IsError();
	}



	int CSphHitBuilder::cidxWriteRawVLB(int fd, CSphWordHit* pHit, int iHits, DWORD* pDocinfo, int iDocinfos, int iStride)
	{
		assert(pHit);
		assert(iHits > 0);

		/////////////////////////////
		// do simple bitwise hashing
		/////////////////////////////

		static const int HBITS = 11;
		static const int HSIZE = (1 << HBITS);

		SphDocID_t uStartID = 0;
		int dHash[HSIZE + 1];
		int iShift = 0;

		if (pDocinfo)
		{
			uStartID = DOCINFO2ID(pDocinfo);
			int iBits = sphLog2(DOCINFO2ID(pDocinfo + (iDocinfos - 1) * iStride) - uStartID);
			iShift = (iBits < HBITS) ? 0 : (iBits - HBITS);

#ifndef NDEBUG
			for (int i = 0; i <= HSIZE; i++)
				dHash[i] = -1;
#endif

			dHash[0] = 0;
			int iHashed = 0;
			for (int i = 0; i < iDocinfos; i++)
			{
				int iHash = (int)((DOCINFO2ID(pDocinfo + i * iStride) - uStartID) >> iShift);
				assert(iHash >= 0 && iHash < HSIZE);

				if (iHash > iHashed)
				{
					dHash[iHashed + 1] = i - 1; // right boundary for prev hash value
					dHash[iHash] = i; // left boundary for next hash value
					iHashed = iHash;
				}
			}
			dHash[iHashed + 1] = iDocinfos - 1; // right boundary for last hash value
		}

		///////////////////////////////////////
		// encode through a small write buffer
		///////////////////////////////////////

		BYTE* pBuf, * maxP;
		int n = 0, w;
		SphWordID_t d1, l1 = 0;
		SphDocID_t d2, l2 = 0;
		DWORD d3, l3 = 0; // !COMMIT must be wide enough

		int iGap = Max(16 * sizeof(DWORD) + iStride * sizeof(DWORD) + (m_pDict->GetSettings().m_bWordDict ? MAX_KEYWORD_BYTES : 0), 128u);
		pBuf = m_dWriteBuffer.Begin();
		maxP = m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength() - iGap;

		SphDocID_t uAttrID = 0; // current doc id
		DWORD* pAttrs = NULL; // current doc attrs

		// hit aggregation state
		DWORD uHitCount = 0;
		DWORD uHitFieldMask = 0;

		const int iPositionShift = m_eHitless == SPH_HITLESS_SOME ? 1 : 0;

		while (iHits--)
		{
			// find attributes by id
			if (pDocinfo && uAttrID != pHit->m_uDocID)
			{
				int iHash = (int)((pHit->m_uDocID - uStartID) >> iShift);
				assert(iHash >= 0 && iHash < HSIZE);

				int iStart = dHash[iHash];
				int iEnd = dHash[iHash + 1];

				if (pHit->m_uDocID == DOCINFO2ID(pDocinfo + iStart * iStride))
				{
					pAttrs = DOCINFO2ATTRS(pDocinfo + iStart * iStride);

				}
				else if (pHit->m_uDocID == DOCINFO2ID(pDocinfo + iEnd * iStride))
				{
					pAttrs = DOCINFO2ATTRS(pDocinfo + iEnd * iStride);

				}
				else
				{
					pAttrs = NULL;
					while (iEnd - iStart > 1)
					{
						// check if nothing found
						if (
							pHit->m_uDocID < DOCINFO2ID(pDocinfo + iStart * iStride) ||
							pHit->m_uDocID > DOCINFO2ID(pDocinfo + iEnd * iStride))
							break;
						assert(pHit->m_uDocID > DOCINFO2ID(pDocinfo + iStart * iStride));
						assert(pHit->m_uDocID < DOCINFO2ID(pDocinfo + iEnd * iStride));

						int iMid = iStart + (iEnd - iStart) / 2;
						if (pHit->m_uDocID == DOCINFO2ID(pDocinfo + iMid * iStride))
						{
							pAttrs = DOCINFO2ATTRS(pDocinfo + iMid * iStride);
							break;
						}
						if (pHit->m_uDocID < DOCINFO2ID(pDocinfo + iMid * iStride))
							iEnd = iMid;
						else
							iStart = iMid;
					}
				}

				if (!pAttrs)
					sphDie("INTERNAL ERROR: failed to lookup attributes while saving collected hits");
				assert(DOCINFO2ID(pAttrs - DOCINFO_IDSIZE) == pHit->m_uDocID);
				uAttrID = pHit->m_uDocID;
			}

			// calc deltas
			d1 = pHit->m_uWordID - l1;
			d2 = pHit->m_uDocID - l2;
			d3 = pHit->m_uWordPos - l3;

			// ignore duplicate hits
			if (d1 == 0 && d2 == 0 && d3 == 0) // OPTIMIZE? check if ( 0==(d1|d2|d3) ) is faster
			{
				pHit++;
				continue;
			}

			// checks below are intended handle several "fun" cases
			//
			// case 1, duplicate documents (same docid), different field contents, but ending with
			// the same keyword, ending up in multiple field end markers within the same keyword
			// eg. [foo] in positions {1, 0x800005} in 1st dupe, {3, 0x800007} in 2nd dupe
			//
			// case 2, blended token in the field end, duplicate parts, different positions (as expected)
			// for those parts but still multiple field end markers, eg. [U.S.S.R.] in the end of field

			// replacement of hit itself by field-end form
			if (d1 == 0 && d2 == 0 && HITMAN::GetPosWithField(pHit->m_uWordPos) == HITMAN::GetPosWithField(l3))
			{
				l3 = pHit->m_uWordPos;
				pHit++;
				continue;
			}

			// reset field-end inside token stream due of document duplicates
			if (d1 == 0 && d2 == 0 && HITMAN::IsEnd(l3) && HITMAN::GetField(pHit->m_uWordPos) == HITMAN::GetField(l3))
			{
				l3 = HITMAN::GetPosWithField(l3);
				d3 = HITMAN::GetPosWithField(pHit->m_uWordPos) - l3;

				if (d3 == 0)
				{
					pHit++;
					continue;
				}
			}

			// non-zero delta restarts all the fields after it
			// because their deltas might now be negative
			if (d1) d2 = pHit->m_uDocID;
			if (d2) d3 = pHit->m_uWordPos;

			// when we moved to the next word or document
			bool bFlushed = false;
			if (d1 || d2)
			{
				// flush previous aggregate hit
				if (uHitCount)
				{
					// we either skip all hits or the high bit must be available for marking
					// failing that, we can't produce a consistent index
					assert(m_eHitless != SPH_HITLESS_NONE);
					assert(m_eHitless == SPH_HITLESS_ALL || !(uHitCount & 0x80000000UL));

					if (m_eHitless != SPH_HITLESS_ALL)
						uHitCount = (uHitCount << 1) | 1;
					pBuf += encodeVLB(pBuf, uHitCount);
					pBuf += encodeVLB(pBuf, uHitFieldMask);
					assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());

					uHitCount = 0;
					uHitFieldMask = 0;

					bFlushed = true;
				}

				// start aggregating if we're skipping all hits or this word is in a list of ignored words
				if ((m_eHitless == SPH_HITLESS_ALL) ||
					(m_eHitless == SPH_HITLESS_SOME && m_dHitlessWords.BinarySearch(pHit->m_uWordID)))
				{
					uHitCount = 1;
					uHitFieldMask |= 1 << HITMAN::GetField(pHit->m_uWordPos);
				}

			}
			else if (uHitCount) // next hit for the same word/doc pair, update state if we need it
			{
				uHitCount++;
				uHitFieldMask |= 1 << HITMAN::GetField(pHit->m_uWordPos);
			}

			// encode enough restart markers
			if (d1) pBuf += encodeVLB(pBuf, 0);
			if (d2 && !bFlushed) pBuf += encodeVLB(pBuf, 0);

			assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());

			// encode deltas
#if USE_64BIT
#define LOC_ENCODE sphEncodeVLB8
#else
#define LOC_ENCODE encodeVLB
#endif

		// encode keyword
			if (d1)
			{
				if (m_pDict->GetSettings().m_bWordDict)
					pBuf += encodeKeyword(pBuf, m_pDict->HitblockGetKeyword(pHit->m_uWordID)); // keyword itself in case of keywords dict
				else
					pBuf += LOC_ENCODE(pBuf, d1); // delta in case of CRC dict

				assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());
			}

			// encode docid delta
			if (d2)
			{
				pBuf += LOC_ENCODE(pBuf, d2);
				assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());
			}

#undef LOC_ENCODE

			// encode attrs
			if (d2 && pAttrs)
			{
				for (int i = 0; i < iStride - DOCINFO_IDSIZE; i++)
				{
					pBuf += encodeVLB(pBuf, pAttrs[i]);
					assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());
				}
			}

			assert(d3);
			if (!uHitCount) // encode position delta, unless accumulating hits
			{
				pBuf += encodeVLB(pBuf, d3 << iPositionShift);
				assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());
			}

			// update current state
			l1 = pHit->m_uWordID;
			l2 = pHit->m_uDocID;
			l3 = pHit->m_uWordPos;

			pHit++;

			if (pBuf > maxP)
			{
				w = (int)(pBuf - m_dWriteBuffer.Begin());
				assert(w < m_dWriteBuffer.GetLength());
				if (!sphWriteThrottled(fd, m_dWriteBuffer.Begin(), w, "raw_hits", *m_pLastError, m_pThrottle))
					return -1;
				n += w;
				pBuf = m_dWriteBuffer.Begin();
			}
		}

		// flush last aggregate
		if (uHitCount)
		{
			assert(m_eHitless != SPH_HITLESS_NONE);
			assert(m_eHitless == SPH_HITLESS_ALL || !(uHitCount & 0x80000000UL));

			if (m_eHitless != SPH_HITLESS_ALL)
				uHitCount = (uHitCount << 1) | 1;
			pBuf += encodeVLB(pBuf, uHitCount);
			pBuf += encodeVLB(pBuf, uHitFieldMask);

			assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());
		}

		pBuf += encodeVLB(pBuf, 0);
		pBuf += encodeVLB(pBuf, 0);
		pBuf += encodeVLB(pBuf, 0);
		assert(pBuf < m_dWriteBuffer.Begin() + m_dWriteBuffer.GetLength());
		w = (int)(pBuf - m_dWriteBuffer.Begin());
		assert(w < m_dWriteBuffer.GetLength());
		if (!sphWriteThrottled(fd, m_dWriteBuffer.Begin(), w, "raw_hits", *m_pLastError, m_pThrottle))
			return -1;
		n += w;

		return n;
	}
}
