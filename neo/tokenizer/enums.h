#pragma once

namespace NEO {
	enum ESphBigram
	{
		SPH_BIGRAM_NONE = 0,	///< no bigrams
		SPH_BIGRAM_ALL = 1,	///< index all word pairs
		SPH_BIGRAM_FIRSTFREQ = 2,	///< only index pairs where one of the words is in a frequent words list
		SPH_BIGRAM_BOTHFREQ = 3		///< only index pairs where both words are in a frequent words list
	};


	enum ESphTokenizerClone
	{
		SPH_CLONE_INDEX,				///< clone tokenizer and set indexing mode
		SPH_CLONE_QUERY,				///< clone tokenizer and set querying mode
		SPH_CLONE_QUERY_LIGHTWEIGHT		///< lightweight clone for querying (can parse, can NOT modify settings, shares pointers to the original lowercaser table)
	};


	enum ESphTokenMorph
	{
		SPH_TOKEN_MORPH_RAW,			///< no morphology applied, tokenizer does not handle morphology
		SPH_TOKEN_MORPH_ORIGINAL,		///< no morphology applied, but tokenizer handles morphology
		SPH_TOKEN_MORPH_GUESS			///< morphology applied
	};




}