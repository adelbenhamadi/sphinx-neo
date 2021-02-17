#pragma once

namespace NEO {

	/// how to handle IO errors in file fields
	enum ESphOnFileFieldError
	{
		FFE_IGNORE_FIELD,
		FFE_SKIP_DOCUMENT,
		FFE_FAIL_INDEX
	};


	/// known multi-valued attr sources
	enum ESphAttrSrc
	{
		SPH_ATTRSRC_NONE = 0,	///< not multi-valued
		SPH_ATTRSRC_FIELD = 1,	///< get attr values from text field
		SPH_ATTRSRC_QUERY = 2,	///< get attr values from SQL query
		SPH_ATTRSRC_RANGEDQUERY = 3		///< get attr values from ranged SQL query
	};


	/// wordpart processing type
	enum ESphWordpart
	{
		SPH_WORDPART_WHOLE = 0,	///< whole-word
		SPH_WORDPART_PREFIX = 1,	///< prefix
		SPH_WORDPART_INFIX = 2		///< infix
	};


	/// column unpack format
	enum ESphUnpackFormat
	{
		SPH_UNPACK_NONE = 0,
		SPH_UNPACK_ZLIB = 1,
		SPH_UNPACK_MYSQL_COMPRESS = 2
	};


	/// aggregate function to apply
	enum ESphAggrFunc
	{
		SPH_AGGR_NONE,
		SPH_AGGR_AVG,
		SPH_AGGR_MIN,
		SPH_AGGR_MAX,
		SPH_AGGR_SUM,
		SPH_AGGR_CAT
	};

}

