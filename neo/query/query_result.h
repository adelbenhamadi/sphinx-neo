#pragma once
#include "neo/query/query_stats.h"
#include "neo/io/io.h"
#include "neo/int/types.h"
#include "neo/utility/hash.h"
#include "neo/core/match.h"
#include "neo/source/schema.h"

namespace NEO {

	/// search query meta-info
	class CSphQueryResultMeta
	{
	public:
		int						m_iQueryTime;		///< query time, milliseconds
		int						m_iRealQueryTime;	///< query time, measured just from start to finish of the query. In milliseconds
		int64_t					m_iCpuTime;			///< user time, microseconds
		int						m_iMultiplier;		///< multi-query multiplier, -1 to indicate error

		struct WordStat_t
		{
			int64_t					m_iDocs;			///< document count for this term
			int64_t					m_iHits;			///< hit count for this term

			WordStat_t()
				: m_iDocs(0)
				, m_iHits(0)
			{}
		};
		SmallStringHash_T<WordStat_t>	m_hWordStats; ///< hash of i-th search term (normalized word form)

		int						m_iMatches;			///< total matches returned (upto MAX_MATCHES)
		int64_t					m_iTotalMatches;	///< total matches found (unlimited)

		STATS::CSphIOStats				m_tIOStats;			///< i/o stats for the query
		int64_t					m_iAgentCpuTime;	///< agent cpu time (for distributed searches)
		STATS::CSphIOStats				m_tAgentIOStats;	///< agent IO stats (for distributed searches)

		int64_t					m_iPredictedTime;		///< local predicted time
		int64_t					m_iAgentPredictedTime;	///< distributed predicted time
		DWORD					m_iAgentFetchedDocs;	///< distributed fetched docs
		DWORD					m_iAgentFetchedHits;	///< distributed fetched hits
		DWORD					m_iAgentFetchedSkips;	///< distributed fetched skips

		CSphQueryStats 			m_tStats;			///< query prediction counters
		bool					m_bHasPrediction;	///< is prediction counters set?

		CSphString				m_sError;			///< error message
		CSphString				m_sWarning;			///< warning message
		int64_t					m_iBadRows;

		CSphQueryResultMeta();													///< ctor
		virtual					~CSphQueryResultMeta() {}						///< dtor
		void					AddStat(const CSphString& sWord, int64_t iDocs, int64_t iHits);
	};



	/// search query result (meta-info plus actual matches)
	class CSphQueryProfile;
	class CSphQueryResult : public CSphQueryResultMeta
	{
	public:
		CSphSwapVector<CSphMatch>	m_dMatches;			///< top matching documents, no more than MAX_MATCHES

		CSphRsetSchema			m_tSchema;			///< result schema
		const DWORD* m_pMva;				///< pointer to MVA storage
		const BYTE* m_pStrings;			///< pointer to strings storage
		bool					m_bArenaProhibit;

		CSphVector<BYTE*>		m_dStorage2Free;	/// < aggregated external storage from rt indexes

		int						m_iOffset;			///< requested offset into matches array
		int						m_iCount;			///< count which will be actually served (computed from total, offset and limit)

		int						m_iSuccesses;

		CSphQueryProfile* m_pProfile;			///< filled when query profiling is enabled; NULL otherwise

	public:
		CSphQueryResult();		///< ctor
		virtual					~CSphQueryResult();	///< dtor, which releases all owned stuff

		void					LeakStorages(CSphQueryResult& tDst);
	};



}