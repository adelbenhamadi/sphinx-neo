#pragma once

namespace NEO {

	/// match sorting functions
	enum ESphSortFunc
	{
		FUNC_REL_DESC,
		FUNC_ATTR_DESC,
		FUNC_ATTR_ASC,
		FUNC_TIMESEGS,
		FUNC_GENERIC2,
		FUNC_GENERIC3,
		FUNC_GENERIC4,
		FUNC_GENERIC5,
		FUNC_EXPR
	};


	/// match sorting clause parsing outcomes
	enum ESortClauseParseResult
	{
		SORT_CLAUSE_OK,
		SORT_CLAUSE_ERROR,
		SORT_CLAUSE_RANDOM
	};


	/// sorting key part types
	enum ESphSortKeyPart
	{
		SPH_KEYPART_ID,
		SPH_KEYPART_WEIGHT,
		SPH_KEYPART_INT,
		SPH_KEYPART_FLOAT,
		SPH_KEYPART_STRING,
		SPH_KEYPART_STRINGPTR
	};



	/// search query sorting orders
	enum ESphSortOrder
	{
		SPH_SORT_RELEVANCE = 0,	///< sort by document relevance desc, then by date
		SPH_SORT_ATTR_DESC = 1,	///< sort by document date desc, then by relevance desc
		SPH_SORT_ATTR_ASC = 2,	///< sort by document date asc, then by relevance desc
		SPH_SORT_TIME_SEGMENTS = 3,	///< sort by time segments (hour/day/week/etc) desc, then by relevance desc
		SPH_SORT_EXTENDED = 4,	///< sort by SQL-like expression (eg. "@relevance DESC, price ASC, @id DESC")
		SPH_SORT_EXPR = 5,	///< sort by arithmetic expression in descending order (eg. "@id + max(@weight,1000)*boost + log(price)")

		SPH_SORT_TOTAL
	};


	/// search query matching mode
	enum ESphMatchMode
	{
		SPH_MATCH_ALL = 0,			///< match all query words
		SPH_MATCH_ANY,				///< match any query word
		SPH_MATCH_PHRASE,			///< match this exact phrase
		SPH_MATCH_BOOLEAN,			///< match this boolean query
		SPH_MATCH_EXTENDED,			///< match this extended query
		SPH_MATCH_FULLSCAN,			///< match all document IDs w/o fulltext query, apply filters
		SPH_MATCH_EXTENDED2,		///< extended engine V2 (TEMPORARY, WILL BE REMOVED IN 0.9.8-RELEASE)

		SPH_MATCH_TOTAL
	};


	/// search query relevance ranking mode
	enum ESphRankMode
	{
		SPH_RANK_PROXIMITY_BM25 = 0,	///< default mode, phrase proximity major factor and BM25 minor one (aka SPH03)
		SPH_RANK_BM25 = 1,	///< statistical mode, BM25 ranking only (faster but worse quality)
		SPH_RANK_NONE = 2,	///< no ranking, all matches get a weight of 1
		SPH_RANK_WORDCOUNT = 3,	///< simple word-count weighting, rank is a weighted sum of per-field keyword occurence counts
		SPH_RANK_PROXIMITY = 4,	///< phrase proximity (aka SPH01)
		SPH_RANK_MATCHANY = 5,	///< emulate old match-any weighting (aka SPH02)
		SPH_RANK_FIELDMASK = 6,	///< sets bits where there were matches
		SPH_RANK_SPH04 = 7,	///< codename SPH04, phrase proximity + bm25 + head/exact boost
		SPH_RANK_EXPR = 8,	///< rank by user expression (eg. "sum(lcs*user_weight)*1000+bm25")
		SPH_RANK_EXPORT = 9,	///< rank by BM25, but compute and export all user expression factors
		SPH_RANK_PLUGIN = 10,	///< user-defined ranker

		SPH_RANK_TOTAL,
		SPH_RANK_DEFAULT = SPH_RANK_PROXIMITY_BM25
	};


	/// search query grouping mode
	enum ESphGroupBy
	{
		SPH_GROUPBY_DAY = 0,	///< group by day
		SPH_GROUPBY_WEEK = 1,	///< group by week
		SPH_GROUPBY_MONTH = 2,	///< group by month
		SPH_GROUPBY_YEAR = 3,	///< group by year
		SPH_GROUPBY_ATTR = 4,	///< group by attribute value
		SPH_GROUPBY_ATTRPAIR = 5,	///< group by sequential attrs pair (rendered redundant by 64bit attrs support; removed)
		SPH_GROUPBY_MULTIPLE = 6		///< group by on multiple attribute values
	};


	/// search query filter types
	enum ESphFilter
	{
		SPH_FILTER_VALUES = 0,	///< filter by integer values set
		SPH_FILTER_RANGE = 1,	///< filter by integer range
		SPH_FILTER_FLOATRANGE = 2,	///< filter by float range
		SPH_FILTER_STRING = 3,	///< filter by string value
		SPH_FILTER_NULL = 4,	///< filter by NULL
		SPH_FILTER_USERVAR = 5,	///< filter by @uservar
		SPH_FILTER_STRING_LIST = 6		///< filter by string list
	};


	/// MVA folding function
	/// (currently used in filters, eg WHERE ALL(mymva) BETWEEN 1 AND 3)
	enum  ESphMvaFunc
	{
		SPH_MVAFUNC_NONE = 0,
		SPH_MVAFUNC_ANY,
		SPH_MVAFUNC_ALL
	};


	/// generic COM-like uids
	enum ExtraData_e
	{
		EXTRA_GET_DATA_ZONESPANS,
		EXTRA_GET_DATA_ZONESPANLIST,
		EXTRA_GET_DATA_RANKFACTORS,
		EXTRA_GET_DATA_PACKEDFACTORS,
		EXTRA_GET_DATA_RANKER_STATE,

		EXTRA_GET_QUEUE_WORST,
		EXTRA_GET_QUEUE_SORTVAL,

		EXTRA_SET_MVAPOOL,
		EXTRA_SET_STRINGPOOL,
		EXTRA_SET_POOL_CAPACITY,
		EXTRA_SET_MATCHPUSHED,
		EXTRA_SET_MATCHPOPPED,

		EXTRA_SET_RANKER_PLUGIN,
		EXTRA_SET_RANKER_PLUGIN_OPTS,

		EXTRA_GET_POOL_SIZE
	};

	/// extended query operator
	enum XQOperator_e
	{
		SPH_QUERY_AND,
		SPH_QUERY_OR,
		SPH_QUERY_MAYBE,
		SPH_QUERY_NOT,
		SPH_QUERY_ANDNOT,
		SPH_QUERY_BEFORE,
		SPH_QUERY_PHRASE,
		SPH_QUERY_PROXIMITY,
		SPH_QUERY_QUORUM,
		SPH_QUERY_NEAR,
		SPH_QUERY_SENTENCE,
		SPH_QUERY_PARAGRAPH,
		SPH_QUERY_NULL
	};

	/// term modifiers
	enum TermPosFilter_e
	{
		TERM_POS_NONE = 0,
		TERM_POS_FIELD_LIMIT = 1,
		TERM_POS_FIELD_START = 2,
		TERM_POS_FIELD_END = 3,
		TERM_POS_FIELD_STARTEND = 4,
		TERM_POS_ZONES = 5,
	};

}