#pragma once
#include "neo/dict/dict_traits.h"

namespace NEO {


	/// dict wrapper for star-syntax support in prefix-indexes
	class CSphDictStar : public CSphDictTraits
	{
	public:
		explicit			CSphDictStar(CSphDict* pDict) : CSphDictTraits(pDict) {}

		virtual SphWordID_t	GetWordID(BYTE* pWord);
		virtual SphWordID_t	GetWordIDNonStemmed(BYTE* pWord);
	};


	/// star dict for index v.8+
	class CSphDictStarV8 : public CSphDictStar
	{
	public:
		CSphDictStarV8(CSphDict* pDict, bool bPrefixes, bool bInfixes);

		virtual SphWordID_t	GetWordID(BYTE* pWord);

	private:
		bool				m_bPrefixes;
		bool				m_bInfixes;
	};


}