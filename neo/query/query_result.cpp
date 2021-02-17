#include "neo/query/query_result.h"
#include "neo/core/globals.h"

namespace NEO {

	CSphQueryResult::CSphQueryResult()
	{
		m_iQueryTime = 0;
		m_iRealQueryTime = 0;
		m_iCpuTime = 0;
		m_iMultiplier = 1;
		m_iTotalMatches = 0;
		m_pMva = NULL;
		m_pStrings = NULL;
		m_iOffset = 0;
		m_iCount = 0;
		m_iSuccesses = 0;
		m_pProfile = NULL;
		m_bArenaProhibit = false;
	}


	CSphQueryResult::~CSphQueryResult()
	{
		ARRAY_FOREACH(i, m_dStorage2Free)
		{
			SafeDeleteArray(m_dStorage2Free[i]);
		}
		ARRAY_FOREACH(i, m_dMatches)
			m_tSchema.FreeStringPtrs(&m_dMatches[i]);
	}

	void CSphQueryResult::LeakStorages(CSphQueryResult& tDst)
	{
		ARRAY_FOREACH(i, m_dStorage2Free)
			tDst.m_dStorage2Free.Add(m_dStorage2Free[i]);

		m_dStorage2Free.Reset();
	}


	CSphQueryResultMeta::CSphQueryResultMeta()
		: m_iQueryTime(0)
		, m_iRealQueryTime(0)
		, m_iCpuTime(0)
		, m_iMultiplier(1)
		, m_iMatches(0)
		, m_iTotalMatches(0)
		, m_iAgentCpuTime(0)
		, m_iPredictedTime(0)
		, m_iAgentPredictedTime(0)
		, m_iAgentFetchedDocs(0)
		, m_iAgentFetchedHits(0)
		, m_iAgentFetchedSkips(0)
		, m_bHasPrediction(false)
		, m_iBadRows(0)
	{
	}


	void CSphQueryResultMeta::AddStat(const CSphString& sWord, int64_t iDocs, int64_t iHits)
	{
		CSphString sFixed;
		const CSphString* pFixed = &sWord;
		if (sWord.cstr()[0] == MAGIC_WORD_HEAD)
		{
			sFixed = sWord;
			*(char*)(sFixed.cstr()) = '*';
			pFixed = &sFixed;
		}
		else if (sWord.cstr()[0] == MAGIC_WORD_HEAD_NONSTEMMED)
		{
			sFixed = sWord;
			*(char*)(sFixed.cstr()) = '=';
			pFixed = &sFixed;
		}
		else
		{
			const char* p = strchr(sWord.cstr(), MAGIC_WORD_BIGRAM);
			if (p)
			{
				sFixed.SetSprintf("\"%s\"", sWord.cstr());
				*((char*)sFixed.cstr() + (p - sWord.cstr()) + 1) = ' ';
				pFixed = &sFixed;
			}
		}

		WordStat_t& tStats = m_hWordStats.AddUnique(*pFixed);
		tStats.m_iDocs += iDocs;
		tStats.m_iHits += iHits;
	}


}