#include "neo/dict/dict_star.h"


namespace NEO {

	SphWordID_t CSphDictStar::GetWordID(BYTE* pWord)
	{
		char sBuf[16 + 3 * SPH_MAX_WORD_LEN];
		assert(strlen((const char*)pWord) < 16 + 3 * SPH_MAX_WORD_LEN);

		if (m_pDict->GetSettings().m_bStopwordsUnstemmed && m_pDict->IsStopWord(pWord))
			return 0;

		m_pDict->ApplyStemmers(pWord);

		int iLen = strlen((const char*)pWord);
		assert(iLen < 16 + 3 * SPH_MAX_WORD_LEN - 1);
		// stemmer might squeeze out the word
		if (iLen && !pWord[0])
			return 0;

		memcpy(sBuf, pWord, iLen + 1);

		if (iLen)
		{
			if (sBuf[iLen - 1] == '*')
			{
				iLen--;
				sBuf[iLen] = '\0';
			}
			else
			{
				sBuf[iLen] = MAGIC_WORD_TAIL;
				iLen++;
				sBuf[iLen] = '\0';
			}
		}

		return m_pDict->GetWordID((BYTE*)sBuf, iLen, !m_pDict->GetSettings().m_bStopwordsUnstemmed);
	}


	SphWordID_t	CSphDictStar::GetWordIDNonStemmed(BYTE* pWord)
	{
		return m_pDict->GetWordIDNonStemmed(pWord);
	}


	//////////////////////////////////////////////////////////////////////////

	CSphDictStarV8::CSphDictStarV8(CSphDict* pDict, bool bPrefixes, bool bInfixes)
		: CSphDictStar(pDict)
		, m_bPrefixes(bPrefixes)
		, m_bInfixes(bInfixes)
	{
	}


	SphWordID_t	CSphDictStarV8::GetWordID(BYTE* pWord)
	{
		char sBuf[16 + 3 * SPH_MAX_WORD_LEN];

		int iLen = strlen((const char*)pWord);
		iLen = Min(iLen, 16 + 3 * SPH_MAX_WORD_LEN - 1);

		if (!iLen)
			return 0;

		bool bHeadStar = (pWord[0] == '*');
		bool bTailStar = (pWord[iLen - 1] == '*') && (iLen > 1);
		bool bMagic = (pWord[0] < ' ');

		if (!bHeadStar && !bTailStar && !bMagic)
		{
			if (m_pDict->GetSettings().m_bStopwordsUnstemmed && IsStopWord(pWord))
				return 0;

			m_pDict->ApplyStemmers(pWord);

			// stemmer might squeeze out the word
			if (!pWord[0])
				return 0;

			if (!m_pDict->GetSettings().m_bStopwordsUnstemmed && IsStopWord(pWord))
				return 0;
		}

		iLen = strlen((const char*)pWord);
		assert(iLen < 16 + 3 * SPH_MAX_WORD_LEN - 2);

		if (!iLen || (bHeadStar && iLen == 1))
			return 0;

		if (bMagic) // pass throu MAGIC_* words
		{
			memcpy(sBuf, pWord, iLen);
			sBuf[iLen] = '\0';

		}
		else if (m_bInfixes)
		{
			////////////////////////////////////
			// infix or mixed infix+prefix mode
			////////////////////////////////////

			// handle head star
			if (bHeadStar)
			{
				memcpy(sBuf, pWord + 1, iLen--); // chops star, copies trailing zero, updates iLen
			}
			else
			{
				sBuf[0] = MAGIC_WORD_HEAD;
				memcpy(sBuf + 1, pWord, ++iLen); // copies everything incl trailing zero, updates iLen
			}

			// handle tail star
			if (bTailStar)
			{
				sBuf[--iLen] = '\0'; // got star, just chop it away
			}
			else
			{
				sBuf[iLen] = MAGIC_WORD_TAIL; // no star, add tail marker
				sBuf[++iLen] = '\0';
			}

		}
		else
		{
			////////////////////
			// prefix-only mode
			////////////////////

			assert(m_bPrefixes);

			// always ignore head star in prefix mode
			if (bHeadStar)
			{
				pWord++;
				iLen--;
			}

			// handle tail star
			if (!bTailStar)
			{
				// exact word search request, always (ie. both in infix/prefix mode) mangles to "\1word\1" in v.8+
				sBuf[0] = MAGIC_WORD_HEAD;
				memcpy(sBuf + 1, pWord, iLen);
				sBuf[iLen + 1] = MAGIC_WORD_TAIL;
				sBuf[iLen + 2] = '\0';
				iLen += 2;

			}
			else
			{
				// prefix search request, mangles to word itself (just chop away the star)
				memcpy(sBuf, pWord, iLen);
				sBuf[--iLen] = '\0';
			}
		}

		// calc id for mangled word
		return m_pDict->GetWordID((BYTE*)sBuf, iLen, !bHeadStar && !bTailStar);
	}


}