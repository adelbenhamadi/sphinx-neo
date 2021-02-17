#pragma once

namespace NEO {

	/// available docinfo storage strategies
	enum ESphDocinfo
	{
		SPH_DOCINFO_NONE = 0,	///< no docinfo available
		SPH_DOCINFO_INLINE = 1,	///< inline docinfo into index (specifically, into doclists)
		SPH_DOCINFO_EXTERN = 2		///< store docinfo separately
	};


	enum ESphHitless
	{
		SPH_HITLESS_NONE = 0,	///< all hits are present
		SPH_HITLESS_SOME = 1,	///< some of the hits might be omitted (check the flag bit)
		SPH_HITLESS_ALL = 2	///< no hits in this index
	};


	enum ESphHitFormat
	{
		SPH_HIT_FORMAT_PLAIN = 0,	///< all hits are stored in hitlist
		SPH_HIT_FORMAT_INLINE = 1	///< hits can be split and inlined into doclist (aka 9-23)
	};


	enum ESphRLPFilter
	{
		SPH_RLP_NONE = 0,	///< rlp not used
		SPH_RLP_PLAIN = 1,	///< rlp used to tokenize every document
		SPH_RLP_BATCHED = 2		///< rlp used to batch documents and tokenize several documents at once
	};

	/////////////////////////////////////////////////
	//moved from sphinxexpr.h
	/////////////////////////////////////////////////
	/// expression tree wide commands
	/// FIXME? maybe merge with ExtraData_e?
	enum ESphExprCommand
	{
		SPH_EXPR_SET_MVA_POOL,
		SPH_EXPR_SET_STRING_POOL,
		SPH_EXPR_SET_EXTRA_DATA,
		SPH_EXPR_GET_DEPENDENT_COLS, ///< used to determine proper evaluating stage
		SPH_EXPR_GET_UDF
	};

	enum ESphFactor
	{
		SPH_FACTOR_DISABLE = 0,
		SPH_FACTOR_ENABLE = 1,
		SPH_FACTOR_CALC_ATC = 1 << 1,
		SPH_FACTOR_JSON_OUT = 1 << 2
	};


	/// known collations
	enum ESphCollation
	{
		SPH_COLLATION_LIBC_CI,
		SPH_COLLATION_LIBC_CS,
		SPH_COLLATION_UTF8_GENERAL_CI,
		SPH_COLLATION_BINARY,

		SPH_COLLATION_DEFAULT = SPH_COLLATION_LIBC_CI
	};

	////////////////////////////////////////////////////////

	/// known attribute types
	enum  ESphAttr 
	{
		// these types are full types
		// their typecodes are saved in the index schema, and thus,
		// TYPECODES MUST NOT CHANGE ONCE INTRODUCED
		SPH_ATTR_NONE = 0,			///< not an attribute at all
		SPH_ATTR_INTEGER = 1,			///< unsigned 32-bit integer
		SPH_ATTR_TIMESTAMP = 2,			///< this attr is a timestamp
		// there was SPH_ATTR_ORDINAL=3 once
		SPH_ATTR_BOOL = 4,			///< this attr is a boolean bit field
		SPH_ATTR_FLOAT = 5,			///< floating point number (IEEE 32-bit)
		SPH_ATTR_BIGINT = 6,			///< signed 64-bit integer
		SPH_ATTR_STRING = 7,			///< string (binary; in-memory)
		// there was SPH_ATTR_WORDCOUNT=8 once
		SPH_ATTR_POLY2D = 9,			///< vector of floats, 2D polygon (see POLY2D)
		SPH_ATTR_STRINGPTR = 10,			///< string (binary, in-memory, stored as pointer to the zero-terminated string)
		SPH_ATTR_TOKENCOUNT = 11,			///< field token count, 32-bit integer
		SPH_ATTR_JSON = 12,			///< JSON subset; converted, packed, and stored as string

		SPH_ATTR_UINT32SET = 0x40000001UL,	///< MVA, set of unsigned 32-bit integers
		SPH_ATTR_INT64SET = 0x40000002UL,	///< MVA, set of signed 64-bit integers

		// these types are runtime only
		// used as intermediate types in the expression engine
		SPH_ATTR_MAPARG = 1000,
		SPH_ATTR_FACTORS = 1001,			///< packed search factors (binary, in-memory, pooled)
		SPH_ATTR_JSON_FIELD = 1002,			///< points to particular field in JSON column subset
		SPH_ATTR_FACTORS_JSON = 1003		///< packed search factors (binary, in-memory, pooled, provided to client json encoded)
	};




	/// column evaluation stage
	enum ESphEvalStage
	{
		SPH_EVAL_STATIC = 0,		///< static data, no real evaluation needed
		SPH_EVAL_OVERRIDE,			///< static but possibly overridden
		SPH_EVAL_PREFILTER,			///< expression needed for candidate matches filtering
		SPH_EVAL_PRESORT,			///< expression needed for final matches sorting
		SPH_EVAL_SORTER,			///< expression evaluated by sorter object
		SPH_EVAL_FINAL,				///< expression not (!) used in filters/sorting; can be postponed until final result set cooking
		SPH_EVAL_POSTLIMIT			///< expression needs to be postponed until we apply all the LIMIT clauses (say, too expensive)
	};


}