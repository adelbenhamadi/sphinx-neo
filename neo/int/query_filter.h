#pragma once
#include "neo/int/types.h"
#include "neo/int/keyword_info.h"
#include "neo/query/iqword.h"
#include "neo/query/get_keyword_settings.h"
#include "neo/dict/dict.h"

namespace NEO {

	//fwd dec
	class ISphTokenizer;
	class CSphIndexSettings;

	struct ExpansionContext_t;
	//class ISphQwordSetup;
	//class ISphQword;



	struct ISphQueryFilter
	{
		ISphTokenizer* m_pTokenizer;
		CSphDict* m_pDict;
		const CSphIndexSettings* m_pSettings;
		GetKeywordsSettings_t		m_tFoldSettings;

		ISphQueryFilter();
		virtual ~ISphQueryFilter();

		void GetKeywords(CSphVector <CSphKeywordInfo>& dKeywords, const ExpansionContext_t& tCtx);
		virtual void AddKeywordStats(BYTE* sWord, const BYTE* sTokenized, int iQpos, CSphVector <CSphKeywordInfo>& dKeywords) = 0;
	};


	struct CSphPlainQueryFilter : public ISphQueryFilter
	{
		const ISphQwordSetup* m_pTermSetup;
		ISphQword* m_pQueryWord;

		virtual void AddKeywordStats(BYTE* sWord, const BYTE* sTokenized, int iQpos, CSphVector <CSphKeywordInfo>& dKeywords)
		{
			assert(!m_tFoldSettings.m_bStats || (m_pTermSetup && m_pQueryWord));

			SphWordID_t iWord = m_pDict->GetWordID(sWord);
			if (!iWord)
				return;

			if (m_tFoldSettings.m_bStats)
			{
				m_pQueryWord->Reset();
				m_pQueryWord->m_sWord = (const char*)sWord;
				m_pQueryWord->m_sDictWord = (const char*)sWord;
				m_pQueryWord->m_uWordID = iWord;
				m_pTermSetup->QwordSetup(m_pQueryWord);
			}

			CSphKeywordInfo& tInfo = dKeywords.Add();
			tInfo.m_sTokenized = (const char*)sTokenized;
			tInfo.m_sNormalized = (const char*)sWord;
			tInfo.m_iDocs = m_tFoldSettings.m_bStats ? m_pQueryWord->m_iDocs : 0;
			tInfo.m_iHits = m_tFoldSettings.m_bStats ? m_pQueryWord->m_iHits : 0;
			tInfo.m_iQpos = iQpos;

			if (tInfo.m_sNormalized.cstr()[0] == MAGIC_WORD_HEAD_NONSTEMMED)
				*(char*)tInfo.m_sNormalized.cstr() = '=';
		}
	};



	struct CSphTemplateQueryFilter : public ISphQueryFilter
	{
		virtual void AddKeywordStats(BYTE* sWord, const BYTE* sTokenized, int iQpos, CSphVector <CSphKeywordInfo>& dKeywords)
		{
			SphWordID_t iWord = m_pDict->GetWordID(sWord);
			if (!iWord)
				return;

			CSphKeywordInfo& tInfo = dKeywords.Add();
			tInfo.m_sTokenized = (const char*)sTokenized;
			tInfo.m_sNormalized = (const char*)sWord;
			tInfo.m_iDocs = 0;
			tInfo.m_iHits = 0;
			tInfo.m_iQpos = iQpos;

			if (tInfo.m_sNormalized.cstr()[0] == MAGIC_WORD_HEAD_NONSTEMMED)
				*(char*)tInfo.m_sNormalized.cstr() = '=';
		}
	};

}