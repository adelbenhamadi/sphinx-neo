#pragma once
#include "neo/int/types.h"
#include "neo/index/enums.h"
#include "neo/query/enums.h"
#include "neo/source/enums.h"
#include "neo/query/filter_settings.h"
#include "neo/query/query_item.h"

namespace NEO {

	/// per-attribute value overrides
	class CSphAttrOverride
	{
	public:
		/// docid+attrvalue pair
		struct IdValuePair_t
		{
			SphDocID_t				m_uDocID;		///< document ID
			union
			{
				SphAttr_t			m_uValue;		///< attribute value
				float				m_fValue;		///< attribute value
			};

			inline bool operator < (const IdValuePair_t& rhs) const
			{
				return m_uDocID < rhs.m_uDocID;
			}
		};

	public:
		CSphString					m_sAttr;		///< attribute name
		ESphAttr					m_eAttrType;	///< attribute type
		CSphVector<IdValuePair_t>	m_dValues;		///< id-value overrides
	};




	/// table function interface
	class CSphQuery;
	struct AggrResult_t;

	class ISphTableFunc
	{
	public:
		virtual			~ISphTableFunc() {}
		virtual bool	ValidateArgs(const CSphVector<CSphString>& dArgs, const CSphQuery& tQuery, CSphString& sError) = 0;
		virtual bool	Process(AggrResult_t* pResult, CSphString& sError) = 0;
		virtual bool	LimitPushdown(int, int) { return false; } // FIXME! implement this
	};


	/// search query
	class CSphQuery
	{
	public:
		CSphString		m_sIndexes;		///< indexes to search
		CSphString		m_sQuery;		///< cooked query string for the engine (possibly transformed during legacy matching modes fixup)
		CSphString		m_sRawQuery;	///< raw query string from the client for searchd log, agents, etc

		int				m_iOffset;		///< offset into result set (as X in MySQL LIMIT X,Y clause)
		int				m_iLimit;		///< limit into result set (as Y in MySQL LIMIT X,Y clause)
		CSphVector<DWORD>	m_dWeights;		///< user-supplied per-field weights. may be NULL. default is NULL
		ESphMatchMode	m_eMode;		///< match mode. default is "match all"
		ESphRankMode	m_eRanker;		///< ranking mode, default is proximity+BM25
		CSphString		m_sRankerExpr;	///< ranking expression for SPH_RANK_EXPR
		CSphString		m_sUDRanker;	///< user-defined ranker name
		CSphString		m_sUDRankerOpts;	///< user-defined ranker options
		ESphSortOrder	m_eSort;		///< sort mode
		CSphString		m_sSortBy;		///< attribute to sort by
		int64_t			m_iRandSeed;	///< random seed for ORDER BY RAND(), -1 means do not set
		int				m_iMaxMatches;	///< max matches to retrieve, default is 1000. more matches use more memory and CPU time to hold and sort them

		bool			m_bSortKbuffer;	///< whether to use PQ or K-buffer sorting algorithm
		bool			m_bZSlist;		///< whether the ranker has to fetch the zonespanlist with this query
		bool			m_bSimplify;	///< whether to apply boolean simplification
		bool			m_bPlainIDF;		///< whether to use PlainIDF=log(N/n) or NormalizedIDF=log((N-n+1)/n)
		bool			m_bGlobalIDF;		///< whether to use local indexes or a global idf file
		bool			m_bNormalizedTFIDF;	///< whether to scale IDFs by query word count, so that TF*IDF is normalized
		bool			m_bLocalDF;			///< whether to use calculate DF among local indexes
		bool			m_bLowPriority;		///< set low thread priority for this query
		DWORD			m_uDebugFlags;

		CSphVector<CSphFilterSettings>	m_dFilters;	///< filters

		CSphString		m_sGroupBy;			///< group-by attribute name(s)
		CSphString		m_sFacetBy;			///< facet-by attribute name(s)
		ESphGroupBy		m_eGroupFunc;		///< function to pre-process group-by attribute value with
		CSphString		m_sGroupSortBy;		///< sorting clause for groups in group-by mode
		CSphString		m_sGroupDistinct;	///< count distinct values for this attribute

		int				m_iCutoff;			///< matches count threshold to stop searching at (default is 0; means to search until all matches are found)

		int				m_iRetryCount;		///< retry count, for distributed queries
		int				m_iRetryDelay;		///< retry delay, for distributed queries
		int				m_iAgentQueryTimeout;	///< agent query timeout override, for distributed queries

		bool			m_bGeoAnchor;		///< do we have an anchor
		CSphString		m_sGeoLatAttr;		///< latitude attr name
		CSphString		m_sGeoLongAttr;		///< longitude attr name
		float			m_fGeoLatitude;		///< anchor latitude
		float			m_fGeoLongitude;	///< anchor longitude

		CSphVector<CSphNamedInt>	m_dIndexWeights;	///< per-index weights
		CSphVector<CSphNamedInt>	m_dFieldWeights;	///< per-field weights

		DWORD			m_uMaxQueryMsec;	///< max local index search time, in milliseconds (default is 0; means no limit)
		int				m_iMaxPredictedMsec; ///< max predicted (!) search time limit, in milliseconds (0 means no limit)
		CSphString		m_sComment;			///< comment to pass verbatim in the log file

		CSphVector<CSphAttrOverride>	m_dOverrides;	///< per-query attribute value overrides

		CSphString		m_sSelect;			///< select-list (attributes and/or expressions)
		CSphString		m_sOrderBy;			///< order-by clause

		CSphString		m_sOuterOrderBy;	///< temporary (?) subselect hack
		int				m_iOuterOffset;		///< keep and apply outer offset at master
		int				m_iOuterLimit;
		bool			m_bHasOuter;

		bool			m_bReverseScan;		///< perform scan in reverse order
		bool			m_bIgnoreNonexistent; ///< whether to warning or not about non-existent columns in select list
		bool			m_bIgnoreNonexistentIndexes; ///< whether to error or not about non-existent indexes in index list
		bool			m_bStrict;			///< whether to warning or not about incompatible types
		bool			m_bSync;			///< whether or not use synchronous operations (optimize, etc.)

		ISphTableFunc* m_pTableFunc;		///< post-query NOT OWNED, WILL NOT BE FREED in dtor.
		CSphFilterSettings	m_tHaving;		///< post aggregate filtering (got applied only on master)

	public:
		int				m_iSQLSelectStart;	///< SQL parser helper
		int				m_iSQLSelectEnd;	///< SQL parser helper

		int				m_iGroupbyLimit;	///< number of elems within group

	public:
		CSphVector<CSphQueryItem>	m_dItems;		///< parsed select-list
		ESphCollation				m_eCollation;	///< ORDER BY collation
		bool						m_bAgent;		///< agent mode (may need extra cols on output)

		CSphString		m_sQueryTokenFilterLib;		///< token filter library name
		CSphString		m_sQueryTokenFilterName;	///< token filter name
		CSphString		m_sQueryTokenFilterOpts;	///< token filter options

	public:
		CSphQuery();		///< ctor, fills defaults
		~CSphQuery();		///< dtor, frees owned stuff

/// parse select list string into items
		bool			ParseSelectList(CSphString& sError);
		bool			m_bFacet;			///< whether this a facet query
	};


}