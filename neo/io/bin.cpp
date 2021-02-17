#include "neo/int/types.h"
#include "neo/io/io.h"
#include "neo/io/bin.h"
#include "neo/core/die.h"

namespace NEO {

	CSphBin::CSphBin(ESphHitless eMode, bool bWordDict)
		: m_eMode(eMode)
		, m_dBuffer(NULL)
		, m_pCurrent(NULL)
		, m_iLeft(0)
		, m_iDone(0)
		, m_eState(BIN_POS)
		, m_bWordDict(bWordDict)
		, m_bError(false)
		, m_iFile(-1)
		, m_pFilePos(NULL)
		, m_iFilePos(0)
		, m_iFileLeft(0)
	{
		m_tHit.m_sKeyword = bWordDict ? m_sKeyword : NULL;
		m_sKeyword[0] = '\0';
		m_pThrottle = &g_tThrottle;

#ifndef NDEBUG
		m_iLastWordID = 0;
		m_sLastKeyword[0] = '\0';
#endif
	}


	int CSphBin::CalcBinSize(int iMemoryLimit, int iBlocks, const char* sPhase, bool bWarn)
	{
		if (iBlocks <= 0)
			return CSphBin::MIN_SIZE;

		int iBinSize = ((iMemoryLimit / iBlocks + 2048) >> 12) << 12; // round to 4k

		if (iBinSize < CSphBin::MIN_SIZE)
		{
			iBinSize = CSphBin::MIN_SIZE;
			sphWarn("%s: mem_limit=%d kb extremely low, increasing to %d kb",
				sPhase, iMemoryLimit / 1024, iBinSize * iBlocks / 1024);
		}

		if (iBinSize < CSphBin::WARN_SIZE && bWarn)
		{
			sphWarn("%s: merge_block_size=%d kb too low, increasing mem_limit may improve performance",
				sPhase, iBinSize / 1024);
		}

		return iBinSize;
	}


	void CSphBin::Init(int iFD, SphOffset_t* pSharedOffset, const int iBinSize)
	{
		assert(!m_dBuffer);
		assert(iBinSize >= MIN_SIZE);
		assert(pSharedOffset);

		m_iFile = iFD;
		m_pFilePos = pSharedOffset;

		m_iSize = iBinSize;
		m_dBuffer = new BYTE[iBinSize];
		m_pCurrent = m_dBuffer;

		m_tHit.m_uDocID = 0;
		m_tHit.m_uWordID = 0;
		m_tHit.m_iWordPos = EMPTY_HIT;
		m_tHit.m_dFieldMask.UnsetAll();

		m_bError = false;
	}


	CSphBin::~CSphBin()
	{
		SafeDeleteArray(m_dBuffer);
	}


	int CSphBin::ReadByte()
	{
		BYTE r;

		if (!m_iLeft)
		{
			if (*m_pFilePos != m_iFilePos)
			{
				sphSeek(m_iFile, m_iFilePos, SEEK_SET);
				*m_pFilePos = m_iFilePos;
			}

			int n = m_iFileLeft > m_iSize
				? m_iSize
				: (int)m_iFileLeft;
			if (n == 0)
			{
				m_iDone = 1;
				m_iLeft = 1;
			}
			else
			{
				assert(m_dBuffer);

				if (sphReadThrottled(m_iFile, m_dBuffer, n, m_pThrottle) != (size_t)n)
				{
					m_bError = true;
					return -2;
				}
				m_iLeft = n;

				m_iFilePos += n;
				m_iFileLeft -= n;
				m_pCurrent = m_dBuffer;
				*m_pFilePos += n;
			}
		}
		if (m_iDone)
		{
			m_bError = true; // unexpected (!) eof
			return -1;
		}

		m_iLeft--;
		r = *(m_pCurrent);
		m_pCurrent++;
		return r;
	}


	ESphBinRead CSphBin::ReadBytes(void* pDest, int iBytes)
	{
		assert(iBytes > 0);
		assert(iBytes <= m_iSize);

		if (m_iDone)
			return BIN_READ_EOF;

		if (m_iLeft < iBytes)
		{
			if (*m_pFilePos != m_iFilePos)
			{
				sphSeek(m_iFile, m_iFilePos, SEEK_SET);
				*m_pFilePos = m_iFilePos;
			}

			int n = Min(m_iFileLeft, m_iSize - m_iLeft);
			if (n == 0)
			{
				m_iDone = 1;
				m_bError = true; // unexpected (!) eof
				return BIN_READ_EOF;
			}

			assert(m_dBuffer);
			memmove(m_dBuffer, m_pCurrent, m_iLeft);

			if (sphReadThrottled(m_iFile, m_dBuffer + m_iLeft, n, m_pThrottle) != (size_t)n)
			{
				m_bError = true;
				return BIN_READ_ERROR;
			}

			m_iLeft += n;
			m_iFilePos += n;
			m_iFileLeft -= n;
			m_pCurrent = m_dBuffer;
			*m_pFilePos += n;
		}

		assert(m_iLeft >= iBytes);
		m_iLeft -= iBytes;

		memcpy(pDest, m_pCurrent, iBytes);
		m_pCurrent += iBytes;

		return BIN_READ_OK;
	}


	SphWordID_t CSphBin::ReadVLB()
	{
		SphWordID_t uValue = 0;
		int iByte, iOffset = 0;
		do
		{
			if ((iByte = ReadByte()) < 0)
				return 0;
			uValue += ((SphWordID_t(iByte & 0x7f)) << iOffset);
			iOffset += 7;
		} while (iByte & 0x80);
		return uValue;
	}

	DWORD CSphBin::UnzipInt()
	{
		register int b = 0;
		register DWORD v = 0;
		do
		{
			b = ReadByte();
			if (b < 0)
				b = 0;
			v = (v << 7) + (b & 0x7f);
		} while (b & 0x80);
		return v;
	}

	SphOffset_t CSphBin::UnzipOffset()
	{
		register int b = 0;
		register SphOffset_t v = 0;
		do
		{
			b = ReadByte();
			if (b < 0)
				b = 0;
			v = (v << 7) + (b & 0x7f);
		} while (b & 0x80);
		return v;
	}

	int CSphBin::ReadHit(CSphAggregateHit* pOut, int iRowitems, CSphRowitem* pRowitems)
	{
		// expected EOB
		if (m_iDone)
		{
			pOut->m_uWordID = 0;
			return 1;
		}

		CSphAggregateHit& tHit = m_tHit; // shortcut
		for (;; )
		{
			// SPH_MAX_WORD_LEN is now 42 only to keep ReadVLB() below
			// technically, we can just use different functions on different paths, if ever needed
			STATIC_ASSERT(SPH_MAX_WORD_LEN * 3 <= 127, KEYWORD_TOO_LONG);
			SphWordID_t uDelta = ReadVLB();

			if (uDelta)
			{
				switch (m_eState)
				{
				case BIN_WORD:
					if (m_bWordDict)
					{
#ifdef NDEBUG
						// FIXME?! move this under PARANOID or something?
						// or just introduce an assert() checked release build?
						if (uDelta >= sizeof(m_sKeyword))
							sphDie("INTERNAL ERROR: corrupted keyword length (len=" UINT64_FMT ", deltapos=" UINT64_FMT ")",
								(uint64_t)uDelta, (uint64_t)(m_iFilePos - m_iLeft));
#else
						assert(uDelta > 0 && uDelta < sizeof(m_sKeyword) - 1);
#endif

						ReadBytes(m_sKeyword, (int)uDelta);
						m_sKeyword[uDelta] = '\0';
						tHit.m_uWordID = sphCRC32(m_sKeyword); // must be in sync with dict!

#ifndef NDEBUG
						assert((m_iLastWordID < tHit.m_uWordID)
							|| (m_iLastWordID == tHit.m_uWordID && strcmp((char*)m_sLastKeyword, (char*)m_sKeyword) < 0));
						strncpy((char*)m_sLastKeyword, (char*)m_sKeyword, sizeof(m_sLastKeyword));
#endif

					}
					else
					{
						tHit.m_uWordID += uDelta;
					}
					tHit.m_uDocID = 0;
					tHit.m_iWordPos = EMPTY_HIT;
					tHit.m_dFieldMask.UnsetAll();
					m_eState = BIN_DOC;
					break;

				case BIN_DOC:
					// doc id
					m_eState = BIN_POS;
					tHit.m_uDocID += uDelta;
					tHit.m_iWordPos = EMPTY_HIT;
					for (int i = 0; i < iRowitems; i++, pRowitems++)
						*pRowitems = (DWORD)ReadVLB(); // FIXME? check range?
					break;

				case BIN_POS:
					if (m_eMode == SPH_HITLESS_ALL)
					{
						tHit.m_dFieldMask.Assign32((DWORD)ReadVLB());
						m_eState = BIN_DOC;

					}
					else if (m_eMode == SPH_HITLESS_SOME)
					{
						if (uDelta & 1)
						{
							tHit.m_dFieldMask.Assign32((DWORD)ReadVLB());
							m_eState = BIN_DOC;
						}
						uDelta >>= 1;
					}
					tHit.m_iWordPos += (DWORD)uDelta;
					*pOut = tHit;
					return 1;

				default:
					sphDie("INTERNAL ERROR: unknown bin state (state=%d)", m_eState);
				}
			}
			else
			{
				switch (m_eState)
				{
				case BIN_POS:	m_eState = BIN_DOC; break;
				case BIN_DOC:	m_eState = BIN_WORD; break;
				case BIN_WORD:	m_iDone = 1; pOut->m_uWordID = 0; return 1;
				default:		sphDie("INTERNAL ERROR: unknown bin state (state=%d)", m_eState);
				}
			}
		}
	}


	bool CSphBin::IsEOF() const
	{
		return m_iDone != 0 || m_iFileLeft <= 0;
	}


	bool CSphBin::IsDone() const
	{
		return m_iDone != 0 || (m_iFileLeft <= 0 && m_iLeft <= 0);
	}


	ESphBinRead CSphBin::Precache()
	{
		if (m_iFileLeft > m_iSize - m_iLeft)
		{
			m_bError = true;
			return BIN_PRECACHE_ERROR;
		}

		if (!m_iFileLeft)
			return BIN_PRECACHE_OK;

		if (*m_pFilePos != m_iFilePos)
		{
			sphSeek(m_iFile, m_iFilePos, SEEK_SET);
			*m_pFilePos = m_iFilePos;
		}

		assert(m_dBuffer);
		memmove(m_dBuffer, m_pCurrent, m_iLeft);

		if (sphReadThrottled(m_iFile, m_dBuffer + m_iLeft, m_iFileLeft, m_pThrottle) != (size_t)m_iFileLeft)
		{
			m_bError = true;
			return BIN_READ_ERROR;
		}

		m_iLeft += m_iFileLeft;
		m_iFilePos += m_iFileLeft;
		m_iFileLeft -= m_iFileLeft;
		m_pCurrent = m_dBuffer;
		*m_pFilePos += m_iFileLeft;

		return BIN_PRECACHE_OK;
	}
}

