#include "neo/dict/dict_exact.h"

namespace NEO {

	SphWordID_t CSphDictExact::GetWordID(BYTE* pWord)
	{
		int iLen = strlen((const char*)pWord);
		iLen = Min(iLen, 16 + 3 * SPH_MAX_WORD_LEN - 1);

		if (!iLen)
			return 0;

		if (pWord[0] == '=')
			pWord[0] = MAGIC_WORD_HEAD_NONSTEMMED;

		if (pWord[0] < ' ')
			return m_pDict->GetWordIDNonStemmed(pWord);

		return m_pDict->GetWordID(pWord);
	}

}