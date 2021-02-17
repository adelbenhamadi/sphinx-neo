#include "neo/core/globals.h"

namespace NEO {


	bool				g_bJsonStrict = false;
	bool				g_bJsonAutoconvNumbers = false;
	bool				g_bJsonKeynamesToLowercase = false;

	/*static*/ const int	DEFAULT_READ_BUFFER = 262144;
	/*static*/ const int	DEFAULT_READ_UNHINTED = 32768;
	/*static*/ const int	MIN_READ_BUFFER = 8192;
	/*static*/ const int	MIN_READ_UNHINTED = 1024;

	//////////////////////////////////////////////////////////////////////////
	// MAGIC CONSTANTS
	//////////////////////////////////////////////////////////////////////////

	const DWORD		INDEX_MAGIC_HEADER = 0x58485053;		///< my magic 'SPHX' header
	const DWORD		INDEX_FORMAT_VERSION = 42;				///< my format version

	const char		MAGIC_SYNONYM_WHITESPACE = 1;				// used internally in tokenizer only
	//const char		MAGIC_CODE_SENTENCE = 2;				// emitted from tokenizer on sentence boundary
	//const char		MAGIC_CODE_PARAGRAPH = 3;				// emitted from stripper (and passed via tokenizer) on paragraph boundary
	//const char		MAGIC_CODE_ZONE = 4;				// emitted from stripper (and passed via tokenizer) on zone boundary; followed by zero-terminated zone name

	const char		MAGIC_WORD_HEAD = 1;				// prepended to keyword by source, stored in (crc) dictionary
	const char		MAGIC_WORD_TAIL = 1;				// appended to keyword by source, stored in (crc) dictionary
	const char		MAGIC_WORD_HEAD_NONSTEMMED = 2;				// prepended to keyword by source, stored in dictionary
	const char		MAGIC_WORD_BIGRAM = 3;				// used as a bigram (keyword pair) separator, stored in dictionary

	const char* MAGIC_WORD_SENTENCE = "\3sentence";		// emitted from source on sentence boundary, stored in dictionary
	const char* MAGIC_WORD_PARAGRAPH = "\3paragraph";	// emitted from source on paragraph boundary, stored in dictionary


	//////////////////////////////////////////////////////////////////////////

	int			g_iReadBuffer = DEFAULT_READ_BUFFER;
	int			g_iReadUnhinted = DEFAULT_READ_UNHINTED;

	CSphString			g_sLemmatizerBase = SHAREDIR;
	bool				g_bProgressiveMerge = false;

	// quick hack for indexer crash reporting
	// one day, these might turn into a callback or something
	int64_t		g_iIndexerCurrentDocID = 0;
	int64_t		g_iIndexerCurrentHits = 0;
	int64_t		g_iIndexerCurrentRangeMin = 0;
	int64_t		g_iIndexerCurrentRangeMax = 0;
	int64_t		g_iIndexerPoolStartDocID = 0;
	int64_t		g_iIndexerPoolStartHit = 0;


	///////////////////////////////

	SmallStringHash_T <CSphGlobalIDF* >	g_hGlobalIDFs;
	CSphMutex							g_tGlobalIDFLock;


	///////////////////////////////


	const int MIN_KEYWORDS_DICT = 4 * 1048576;


	const char* AOT_LANGUAGES[AOT_LENGTH] = { "ru", "en", "de" };



	//////////////////////////


	const char* g_sTagInfixBlocks = "infix-blocks";

	const char* g_sTagInfixEntries = "infix-entries";

	/////////////////////////


	/// publicly exposed binlog interface
	ISphBinlog* g_pBinlog = NULL;

	/// costs for max_predicted_time estimations, in nanoseconds
	/// YMMV, defaults were estimated in a very specific environment, and then rounded off
	int g_iPredictorCostDoc = 64;
	int g_iPredictorCostHit = 48;
	int g_iPredictorCostSkip = 2048;
	int g_iPredictorCostMatch = 64;


	///////////////////////

	

}