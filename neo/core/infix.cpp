#include "neo/core/infix.h"
#include "neo/int/types.h"
#include "neo/core/globals.h"
#include  "neo/io/crc32.h"
#include "neo/utility/inline_misc.h"
#include "neo/core/die.h"
#include "neo/core/generic.h"

namespace NEO {

	template<>
	bool Infix_t<2>::operator == (const Infix_t<2>& rhs) const
	{
		return m_Data[0] == rhs.m_Data[0] && m_Data[1] == rhs.m_Data[1];
	}


	template<>
	bool Infix_t<3>::operator == (const Infix_t<3>& rhs) const
	{
		return m_Data[0] == rhs.m_Data[0] && m_Data[1] == rhs.m_Data[1] && m_Data[2] == rhs.m_Data[2];
	}


	template<>
	bool Infix_t<5>::operator == (const Infix_t<5>& rhs) const
	{
		return m_Data[0] == rhs.m_Data[0] && m_Data[1] == rhs.m_Data[1] && m_Data[2] == rhs.m_Data[2]
			&& m_Data[3] == rhs.m_Data[3] && m_Data[4] == rhs.m_Data[4];
	}

	////////////////

	void Swap(InfixIntvec_t& a, InfixIntvec_t& b)
	{
		Swap(a.m_dData[0], b.m_dData[0]);
		Swap(a.m_dData[1], b.m_dData[1]);
		Swap(a.m_dData[2], b.m_dData[2]);
		Swap(a.m_dData[3], b.m_dData[3]);
	}



	/////////////////

	template < int SIZE >
	InfixBuilder_c<SIZE>::InfixBuilder_c()
	{
		// init the hash
		for (int i = 0; i < LENGTH; i++)
			m_dHash[i] = 0;
		m_dArena.Reserve(1048576);
		m_dArena.Resize(1); // 0 is a reserved index
	}


	/// single-byte case, 2-dword infixes
	template<>
	void InfixBuilder_c<2>::AddWord(const BYTE* pWord, int iWordLength, int iCheckpoint, bool bHasMorphology)
	{
		if (bHasMorphology && *pWord != MAGIC_WORD_HEAD_NONSTEMMED)
			return;

		if (*pWord < 0x20) // skip heading magic chars, like NONSTEMMED maker
		{
			pWord++;
			iWordLength--;
		}

		Infix_t<2> sKey;
		for (int p = 0; p <= iWordLength - 2; p++)
		{
			sKey.Reset();

			BYTE* pKey = (BYTE*)sKey.m_Data;
			const BYTE* s = pWord + p;
			const BYTE* sMax = s + Min(6, iWordLength - p);

			DWORD uHash = 0xffffffUL ^ g_dSphinxCRC32[0xff ^ *s];
			*pKey++ = *s++; // copy first infix byte

			while (s < sMax)
			{
				uHash = (uHash >> 8) ^ g_dSphinxCRC32[(uHash ^ *s) & 0xff];
				*pKey++ = *s++; // copy another infix byte

				InfixIntvec_t* pVal = LookupEntry(sKey, uHash);
				if (pVal)
					pVal->Add(iCheckpoint);
				else
					AddEntry(sKey, uHash, iCheckpoint);
			}
		}
	}


	/// UTF-8 case, 3/5-dword infixes
	template < int SIZE >
	void InfixBuilder_c<SIZE>::AddWord(const BYTE* pWord, int iWordLength, int iCheckpoint, bool bHasMorphology)
	{
		if (bHasMorphology && *pWord != MAGIC_WORD_HEAD_NONSTEMMED)
			return;

		if (*pWord < 0x20) // skip heading magic chars, like NONSTEMMED maker
		{
			pWord++;
			iWordLength--;
		}

		int iCodes = 0; // codepoints in current word
		BYTE dBytes[SPH_MAX_WORD_LEN + 1]; // byte offset for each codepoints

		// build an offsets table into the bytestring
		dBytes[0] = 0;
		for (const BYTE* p = (const BYTE*)pWord; p < pWord + iWordLength && iCodes < SPH_MAX_WORD_LEN; )
		{
			int iLen = 0;
			BYTE uVal = *p;
			while (uVal & 0x80)
			{
				uVal <<= 1;
				iLen++;
			}
			if (!iLen)
				iLen = 1;

			// skip word with large codepoints
			if (iLen > SIZE)
				return;

			assert(iLen >= 1 && iLen <= 4);
			p += iLen;

			dBytes[iCodes + 1] = dBytes[iCodes] + (BYTE)iLen;
			iCodes++;
		}
		assert(pWord[dBytes[iCodes]] == 0 || iCodes == SPH_MAX_WORD_LEN);

		// generate infixes
		Infix_t<SIZE> sKey;
		for (int p = 0; p <= iCodes - 2; p++)
		{
			sKey.Reset();
			BYTE* pKey = (BYTE*)sKey.m_Data;

			const BYTE* s = pWord + dBytes[p];
			const BYTE* sMax = pWord + dBytes[p + Min(6, iCodes - p)];

			// copy first infix codepoint
			DWORD uHash = 0xffffffffUL;
			do
			{
				uHash = (uHash >> 8) ^ g_dSphinxCRC32[(uHash ^ *s) & 0xff];
				*pKey++ = *s++;
			} while ((*s & 0xC0) == 0x80);

			while (s < sMax)
			{
				// copy next infix codepoint
				do
				{
					uHash = (uHash >> 8) ^ g_dSphinxCRC32[(uHash ^ *s) & 0xff];
					*pKey++ = *s++;
				} while ((*s & 0xC0) == 0x80);

				InfixIntvec_t* pVal = LookupEntry(sKey, uHash);
				if (pVal)
					pVal->Add(iCheckpoint);
				else
					AddEntry(sKey, uHash, iCheckpoint);
			}
		}
	}


	template < int SIZE >
	void InfixBuilder_c<SIZE>::SaveEntries(CSphWriter& wrDict)
	{
		// intentionally local to this function
		// we mark the block end with an editcode of 0
		const int INFIX_BLOCK_SIZE = 64;

		wrDict.PutBytes(g_sTagInfixEntries, strlen(g_sTagInfixEntries));

		CSphVector<int> dIndex;
		dIndex.Resize(m_dArena.GetLength() - 1);
		for (int i = 0; i < m_dArena.GetLength() - 1; i++)
			dIndex[i] = i + 1;

		InfixHashCmp_fn<SIZE> fnCmp(m_dArena.Begin());
		dIndex.Sort(fnCmp);

		m_dBlocksWords.Reserve(m_dArena.GetLength() / INFIX_BLOCK_SIZE * sizeof(DWORD) * SIZE);
		int iBlock = 0;
		int iPrevKey = -1;
		ARRAY_FOREACH(iIndex, dIndex)
		{
			InfixIntvec_t& dData = m_dArena[dIndex[iIndex]].m_tValue;
			const BYTE* sKey = (const BYTE*)m_dArena[dIndex[iIndex]].m_tKey.m_Data;
			int iChars = (SIZE == 2)
				? strnlen((const char*)sKey, sizeof(DWORD) * SIZE)
				: sphUTF8Len((const char*)sKey, sizeof(DWORD) * SIZE);
			assert(iChars >= 2 && iChars<int(1 + sizeof(Infix_t<SIZE>)));

			// keep track of N-infix blocks
			int iAppendBytes = strnlen((const char*)sKey, sizeof(DWORD) * SIZE);
			if (!iBlock)
			{
				int iOff = m_dBlocksWords.GetLength();
				m_dBlocksWords.Resize(iOff + iAppendBytes + 1);

				InfixBlock_t& tBlock = m_dBlocks.Add();
				tBlock.m_iInfixOffset = iOff;
				tBlock.m_iOffset = (DWORD)wrDict.GetPos();

				memcpy(m_dBlocksWords.Begin() + iOff, sKey, iAppendBytes);
				m_dBlocksWords[iOff + iAppendBytes] = '\0';
			}

			// compute max common prefix
			// edit_code = ( num_keep_chars<<4 ) + num_append_chars
			int iEditCode = iChars;
			if (iPrevKey >= 0)
			{
				const BYTE* sPrev = (const BYTE*)m_dArena[dIndex[iPrevKey]].m_tKey.m_Data;
				const BYTE* sCur = (const BYTE*)sKey;
				const BYTE* sMax = sCur + iAppendBytes;

				int iKeepChars = 0;
				if_const(SIZE == 2)
				{
					// SBCS path
					while (sCur < sMax && *sCur && *sCur == *sPrev)
					{
						sCur++;
						sPrev++;
					}
					iKeepChars = (int)(sCur - (const BYTE*)sKey);

					assert(iKeepChars >= 0 && iKeepChars < 16);
					assert(iChars - iKeepChars >= 0);
					assert(iChars - iKeepChars < 16);

					iEditCode = (iKeepChars << 4) + (iChars - iKeepChars);
					iAppendBytes = (iChars - iKeepChars);
					sKey = sCur;

				}
			else
			{
				// UTF-8 path
				const BYTE* sKeyMax = sCur; // track max matching sPrev prefix in [sKey,sKeyMax)
				while (sCur < sMax && *sCur && *sCur == *sPrev)
				{
					// current byte matches, move the pointer
					sCur++;
					sPrev++;

					// tricky bit
					// if the next (!) byte is a valid UTF-8 char start (or eof!)
					// then we just matched not just a byte, but a full char
					// so bump the matching prefix boundary and length
					if (sCur >= sMax || (*sCur & 0xC0) != 0x80)
					{
						sKeyMax = sCur;
						iKeepChars++;
					}
				}

				assert(iKeepChars >= 0 && iKeepChars < 16);
				assert(iChars - iKeepChars >= 0);
				assert(iChars - iKeepChars < 16);

				iEditCode = (iKeepChars << 4) + (iChars - iKeepChars);
				iAppendBytes -= (int)(sKeyMax - sKey);
				sKey = sKeyMax;
			}
			}

			// write edit code, postfix
			wrDict.PutByte(iEditCode);
			wrDict.PutBytes(sKey, iAppendBytes);

			// compute data length
			int iDataLen = ZippedIntSize(dData[0]);
			for (int j = 1; j < dData.GetLength(); j++)
				iDataLen += ZippedIntSize(dData[j] - dData[j - 1]);

			// write data length, data
			wrDict.ZipInt(iDataLen);
			wrDict.ZipInt(dData[0]);
			for (int j = 1; j < dData.GetLength(); j++)
				wrDict.ZipInt(dData[j] - dData[j - 1]);

			// mark block end, restart deltas
			iPrevKey = iIndex;
			if (++iBlock == INFIX_BLOCK_SIZE)
			{
				iBlock = 0;
				iPrevKey = -1;
				wrDict.PutByte(0);
			}
		}

		// put end marker
		if (iBlock)
			wrDict.PutByte(0);

		const char* pBlockWords = (const char*)m_dBlocksWords.Begin();
		ARRAY_FOREACH(i, m_dBlocks)
			m_dBlocks[i].m_sInfix = pBlockWords + m_dBlocks[i].m_iInfixOffset;

		if (wrDict.GetPos() > UINT_MAX) // FIXME!!! change to int64
			sphDie("INTERNAL ERROR: dictionary size " INT64_FMT " overflow at infix save", wrDict.GetPos());
	}


	template < int SIZE >
	int64_t InfixBuilder_c<SIZE>::SaveEntryBlocks(CSphWriter& wrDict)
	{
		// save the blocks
		wrDict.PutBytes(g_sTagInfixBlocks, strlen(g_sTagInfixBlocks));

		SphOffset_t iInfixBlocksOffset = wrDict.GetPos();
		assert(iInfixBlocksOffset <= INT_MAX);

		wrDict.ZipInt(m_dBlocks.GetLength());
		ARRAY_FOREACH(i, m_dBlocks)
		{
			int iBytes = strlen(m_dBlocks[i].m_sInfix);
			wrDict.PutByte(iBytes);
			wrDict.PutBytes(m_dBlocks[i].m_sInfix, iBytes);
			wrDict.ZipInt(m_dBlocks[i].m_iOffset); // maybe delta these on top?
		}

		return iInfixBlocksOffset;
	}

	bool operator < (const InfixBlock_t& a, const char* b)
	{
		return strcmp(a.m_sInfix, b) < 0;
	}

	bool operator == (const InfixBlock_t& a, const char* b)
	{
		return strcmp(a.m_sInfix, b) == 0;
	}

	bool operator < (const char* a, const InfixBlock_t& b)
	{
		return strcmp(a, b.m_sInfix) < 0;
	}


	ISphInfixBuilder* sphCreateInfixBuilder(int iCodepointBytes, CSphString* pError)
	{
		assert(pError);
		*pError = CSphString();
		switch (iCodepointBytes)
		{
		case 0:		return NULL;
		case 1:		return new InfixBuilder_c<2>(); // upto 6x1 bytes, 2 dwords, sbcs
		case 2:		return new InfixBuilder_c<3>(); // upto 6x2 bytes, 3 dwords, utf-8
		case 3:		return new InfixBuilder_c<5>(); // upto 6x3 bytes, 5 dwords, utf-8
		default:	pError->SetSprintf("unhandled max infix codepoint size %d", iCodepointBytes); return NULL;
		}
	}

	bool sphLookupInfixCheckpoints(const char* sInfix, int iBytes, const BYTE* pInfixes, const CSphVector<InfixBlock_t>& dInfixBlocks, int iInfixCodepointBytes, CSphVector<DWORD>& dCheckpoints)
	{
		assert(pInfixes);

		char dInfixBuf[3 * SPH_MAX_WORD_LEN + 4];
		memcpy(dInfixBuf, sInfix, iBytes);
		dInfixBuf[iBytes] = '\0';

		// lookup block
		int iBlock = FindSpan(dInfixBlocks, dInfixBuf);
		if (iBlock < 0)
			return false;
		const BYTE* pBlock = pInfixes + dInfixBlocks[iBlock].m_iOffset;

		// decode block and check for exact infix match
		// block entry is { byte edit_code, byte[] key_append, zint data_len, zint data_deltas[] }
		// zero edit_code marks block end
		BYTE sKey[32];
		for (;; )
		{
			// unpack next key
			int iCode = *pBlock++;
			if (!iCode)
				break;

			BYTE* pOut = sKey;
			if (iInfixCodepointBytes == 1)
			{
				pOut = sKey + (iCode >> 4);
				iCode &= 15;
				while (iCode--)
					*pOut++ = *pBlock++;
			}
			else
			{
				int iKeep = (iCode >> 4);
				while (iKeep--)
					pOut += sphUtf8CharBytes(*pOut); //wtf? *pOut (=sKey) is NOT initialized?
				assert(pOut - sKey <= (int)sizeof(sKey));
				iCode &= 15;
				while (iCode--)
				{
					int i = sphUtf8CharBytes(*pBlock);
					while (i--)
						*pOut++ = *pBlock++;
				}
				assert(pOut - sKey <= (int)sizeof(sKey));
			}
			assert(pOut - sKey < (int)sizeof(sKey));
#ifndef NDEBUG
			* pOut = '\0'; // handy for debugging, but not used for real matching
#endif

			if (pOut == sKey + iBytes && memcmp(sKey, dInfixBuf, iBytes) == 0)
			{
				// found you! decompress the data
				int iLast = 0;
				int iPackedLen = sphUnzipInt(pBlock);
				const BYTE* pMax = pBlock + iPackedLen;
				while (pBlock < pMax)
				{
					iLast += sphUnzipInt(pBlock);
					dCheckpoints.Add((DWORD)iLast);
				}
				return true;
			}

			int iSkip = sphUnzipInt(pBlock);
			pBlock += iSkip;
		}
		return false;
	}


	// calculate length, upto iInfixCodepointBytes chars from infix start
	int sphGetInfixLength(const char* sInfix, int iBytes, int iInfixCodepointBytes)
	{
		int iBytes1 = Min(6, iBytes);
		if (iInfixCodepointBytes != 1)
		{
			int iCharsLeft = 6;
			const char* s = sInfix;
			const char* sMax = sInfix + iBytes;
			while (iCharsLeft-- && s < sMax)
				s += sphUtf8CharBytes(*s);
			iBytes1 = (int)(s - sInfix);
		}

		return iBytes1;
	}


}