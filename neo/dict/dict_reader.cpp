#include "neo/dict/dict_reader.h"
#include "neo/tools/docinfo_transformer.h"
#include "neo/io/crc32.h"


namespace NEO {

	bool CSphDictReader::Setup(const CSphString& sFilename, SphOffset_t iMaxPos, ESphHitless eHitless,
		CSphString& sError, bool bWordDict, ThrottleState_t* pThrottle, bool bHasSkips)
	{
		if (!m_tMyReader.Open(sFilename, sError))
			return false;
		Setup(&m_tMyReader, iMaxPos, eHitless, bWordDict, pThrottle, bHasSkips);
		return true;
	}

	void CSphDictReader::Setup(CSphReader* pReader, SphOffset_t iMaxPos, ESphHitless eHitless, bool bWordDict, ThrottleState_t* pThrottle, bool bHasSkips)
	{
		m_pReader = pReader;
		m_pReader->SetThrottle(pThrottle);
		m_pReader->SeekTo(1, READ_NO_SIZE_HINT);

		m_iMaxPos = iMaxPos;
		m_eHitless = eHitless;
		m_bWordDict = bWordDict;
		m_sWord[0] = '\0';
		m_iCheckpoint = 1;
		m_bHasSkips = bHasSkips;
	}

	bool CSphDictReader::Read()
	{
		if (m_pReader->GetPos() >= m_iMaxPos)
			return false;

		// get leading value
		SphWordID_t iWord0 = m_bWordDict ? m_pReader->GetByte() : m_pReader->UnzipWordid();
		if (!iWord0)
		{
			// handle checkpoint
			m_iCheckpoint++;
			m_pReader->UnzipOffset();

			m_uWordID = 0;
			m_iDoclistOffset = 0;
			m_sWord[0] = '\0';

			if (m_pReader->GetPos() >= m_iMaxPos)
				return false;

			iWord0 = m_bWordDict ? m_pReader->GetByte() : m_pReader->UnzipWordid(); // get next word
		}
		if (!iWord0)
			return false; // some failure

		// get word entry
		if (m_bWordDict)
		{
			// unpack next word
			// must be in sync with DictEnd()!
			assert(iWord0 <= 255);
			BYTE uPack = (BYTE)iWord0;

			int iMatch, iDelta;
			if (uPack & 0x80)
			{
				iDelta = ((uPack >> 4) & 7) + 1;
				iMatch = uPack & 15;
			}
			else
			{
				iDelta = uPack & 127;
				iMatch = m_pReader->GetByte();
			}
			assert(iMatch + iDelta < (int)sizeof(m_sWord) - 1);
			assert(iMatch <= (int)strlen(m_sWord));

			m_pReader->GetBytes(m_sWord + iMatch, iDelta);
			m_sWord[iMatch + iDelta] = '\0';

			m_iDoclistOffset = m_pReader->UnzipOffset();
			m_iDocs = m_pReader->UnzipInt();
			m_iHits = m_pReader->UnzipInt();
			m_iHint = 0;
			if (m_iDocs >= DOCLIST_HINT_THRESH)
				m_iHint = m_pReader->GetByte();
			if (m_bHasSkips && (m_iDocs > SPH_SKIPLIST_BLOCK))
				m_pReader->UnzipInt();

			m_uWordID = (SphWordID_t)sphCRC32(GetWord()); // set wordID for indexing

		}
		else
		{
			m_uWordID += iWord0;
			m_iDoclistOffset += m_pReader->UnzipOffset();
			m_iDocs = m_pReader->UnzipInt();
			m_iHits = m_pReader->UnzipInt();
			if (m_bHasSkips && (m_iDocs > SPH_SKIPLIST_BLOCK))
				m_pReader->UnzipOffset();
		}

		m_bHasHitlist =
			(m_eHitless == SPH_HITLESS_NONE) ||
			(m_eHitless == SPH_HITLESS_SOME && !(m_iDocs & HITLESS_DOC_FLAG));
		m_iDocs = m_eHitless == SPH_HITLESS_SOME ? (m_iDocs & HITLESS_DOC_MASK) : m_iDocs;

		return true; // FIXME? errorflag?
	}

	int CSphDictReader::CmpWord(const CSphDictReader& tOther) const
	{
		if (m_bWordDict)
			return strcmp(m_sWord, tOther.m_sWord);

		int iRes = 0;
		iRes = m_uWordID < tOther.m_uWordID ? -1 : iRes;
		iRes = m_uWordID > tOther.m_uWordID ? 1 : iRes;
		return iRes;
	}

}