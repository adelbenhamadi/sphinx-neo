#pragma once
#include "neo/dict/dict_traits.h"

namespace NEO {

	/// dict wrapper for exact-word syntax
	class CSphDictExact : public CSphDictTraits
	{
	public:
		explicit CSphDictExact(CSphDict* pDict) : CSphDictTraits(pDict) {}
		virtual SphWordID_t	GetWordID(BYTE* pWord);

		virtual SphWordID_t GetWordIDNonStemmed(BYTE* pWord) { return m_pDict->GetWordIDNonStemmed(pWord); }
	};


}