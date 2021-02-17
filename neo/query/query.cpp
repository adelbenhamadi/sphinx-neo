#include "neo/core/globals.h"
#include "neo/query/query.h"
#include "neo/query/query_stats.h"
#include "neo/query/select_parser.h"

namespace NEO {

	CSphQuery::CSphQuery()
		: m_sIndexes("*")
		, m_sQuery("")
		, m_sRawQuery("")
		, m_iOffset(0)
		, m_iLimit(20)
		, m_eMode(SPH_MATCH_EXTENDED)
		, m_eRanker(SPH_RANK_DEFAULT)
		, m_eSort(SPH_SORT_RELEVANCE)
		, m_iRandSeed(-1)
		, m_iMaxMatches(DEFAULT_MAX_MATCHES)
		, m_bSortKbuffer(false)
		, m_bZSlist(false)
		, m_bSimplify(false)
		, m_bPlainIDF(false)
		, m_bGlobalIDF(false)
		, m_bNormalizedTFIDF(true)
		, m_bLocalDF(false)
		, m_bLowPriority(false)
		, m_uDebugFlags(0)
		, m_eGroupFunc(SPH_GROUPBY_ATTR)
		, m_sGroupSortBy("@groupby desc")
		, m_sGroupDistinct("")
		, m_iCutoff(0)
		, m_iRetryCount(0)
		, m_iRetryDelay(0)
		, m_iAgentQueryTimeout(0)
		, m_bGeoAnchor(false)
		, m_fGeoLatitude(0.0f)
		, m_fGeoLongitude(0.0f)
		, m_uMaxQueryMsec(0)
		, m_iMaxPredictedMsec(0)
		, m_sComment("")
		, m_sSelect("")
		, m_iOuterOffset(0)
		, m_iOuterLimit(0)
		, m_bHasOuter(false)
		, m_bReverseScan(false)
		, m_bIgnoreNonexistent(false)
		, m_bIgnoreNonexistentIndexes(false)
		, m_bStrict(false)
		, m_bSync(false)
		, m_pTableFunc(NULL)

		, m_iSQLSelectStart(-1)
		, m_iSQLSelectEnd(-1)
		, m_iGroupbyLimit(1)

		, m_eCollation(SPH_COLLATION_DEFAULT)
		, m_bAgent(false)
		, m_bFacet(false)
	{}


	CSphQuery::~CSphQuery()
	{
	}


	bool CSphQuery::ParseSelectList(CSphString& sError)
	{
		m_dItems.Reset();
		if (m_sSelect.IsEmpty())
			return true; // empty is ok; will just return everything

		SelectParser_t tParser;
		tParser.m_pStart = m_sSelect.cstr();
		tParser.m_pCur = tParser.m_pStart;
		tParser.m_pQuery = this;

		yyparse(&tParser);

		sError = tParser.m_sParserError;
		return sError.IsEmpty();
	}

}