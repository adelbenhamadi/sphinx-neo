#pragma once
#include "neo/int/types.h"
#include "neo/io/buffer.h"
#include "neo/platform/mutex.h"
#include "neo/utility/hash.h"


#define READ_NO_SIZE_HINT 0

#ifndef SHAREDIR
#define SHAREDIR "."
#endif

#ifdef O_BINARY
#define SPH_O_BINARY O_BINARY
#else
#define SPH_O_BINARY 0
#endif

#define SPH_O_READ	( O_RDONLY | SPH_O_BINARY )
#define SPH_O_NEW	( O_CREAT | O_RDWR | O_TRUNC | SPH_O_BINARY )

#define SPH_UNPACK_BUFFER_SIZE	4096
#define SPH_READ_PROGRESS_CHUNK (8192*1024)
#define SPH_READ_NOPROGRESS_CHUNK (32768*1024)

// DWORD caster
#define MVA_DOWNSIZE		DWORD			// MVA32 offset type


namespace NEO {

	extern  const int	DEFAULT_READ_BUFFER;///<  = 262144;
	extern  const int	DEFAULT_READ_UNHINTED;///<  = 32768;
	extern  const int	MIN_READ_BUFFER;///<  = 8192;
	extern  const int	MIN_READ_UNHINTED;///<  = 1024;

	extern  int			g_iReadBuffer;
	extern  int			g_iReadUnhinted;

	extern CSphString			g_sLemmatizerBase;
	extern bool				g_bProgressiveMerge;

	
	extern int64_t		g_iIndexerCurrentDocID;
	extern int64_t		g_iIndexerCurrentHits;
	extern int64_t		g_iIndexerCurrentRangeMin;
	extern int64_t		g_iIndexerCurrentRangeMax;
	extern int64_t		g_iIndexerPoolStartDocID;
	extern int64_t		g_iIndexerPoolStartHit;


	/// global idf definitions hash
	class CSphGlobalIDF;
	extern  SmallStringHash_T <CSphGlobalIDF* >	g_hGlobalIDFs;
	extern  CSphMutex							g_tGlobalIDFLock;


	//////////////////////////////////////////////////////////////////////////

	extern const DWORD		INDEX_MAGIC_HEADER ;		///< my magic 'SPHX' header
	extern const DWORD		INDEX_FORMAT_VERSION ;				///< my format version

	extern const char		MAGIC_SYNONYM_WHITESPACE ;				// used internally in tokenizer only
	constexpr char		MAGIC_CODE_SENTENCE = 2;				// emitted from tokenizer on sentence boundary
	constexpr char		MAGIC_CODE_PARAGRAPH =3;				// emitted from stripper (and passed via tokenizer) on paragraph boundary
	constexpr char		MAGIC_CODE_ZONE = 4;				// emitted from stripper (and passed via tokenizer) on zone boundary; followed by zero-terminated zone name

	extern const char		MAGIC_WORD_HEAD ;				// prepended to keyword by source, stored in (crc) dictionary
	extern const char		MAGIC_WORD_TAIL ;				// appended to keyword by source, stored in (crc) dictionary
	extern const char		MAGIC_WORD_HEAD_NONSTEMMED ;				// prepended to keyword by source, stored in dictionary
	extern const char		MAGIC_WORD_BIGRAM ;				// used as a bigram (keyword pair) separator, stored in dictionary

	extern const char* MAGIC_WORD_SENTENCE;	///< value is "\3sentence"
	extern const char* MAGIC_WORD_PARAGRAPH;	///< value is "\3paragraph"


	//////////////////////////////////////////////////////////////////////////
	class ISphBinlog;
	extern ISphBinlog* g_pBinlog;
	
	extern int g_iPredictorCostSkip;
	extern int g_iPredictorCostDoc;
	extern int g_iPredictorCostHit;
	extern int g_iPredictorCostMatch;

	extern bool g_bJsonStrict;
	extern bool g_bJsonAutoconvNumbers;
	extern bool g_bJsonKeynamesToLowercase;

	
//////////////////////////////////////
// mva
/////////////////////////////////////


	constexpr auto MVA_OFFSET_MASK  = 0x7fffffffUL;	// MVA offset mask
	constexpr auto MVA_ARENA_FLAG = 0x80000000UL;	// MVA global-arena flag

	constexpr auto  DEFAULT_MAX_MATCHES = 1000;

	constexpr auto  MAX_SOURCE_HITS = 32768;

	extern const  int MIN_KEYWORDS_DICT;

	constexpr auto  DOCINFO_INDEX_FREQ = 128;// FIXME? make this configurable
	constexpr auto  SPH_SKIPLIST_BLOCK = 128; ///< must be a power of two

	constexpr auto  SPH_MAX_WORD_LEN = 42;		// so that any UTF-8 word fits 127 bytes
	constexpr auto  SPH_MAX_FILENAME_LEN = 512;
	constexpr auto  SPH_MAX_FIELDS = 256;

	const  size_t MAX_KEYWORD_BYTES = SPH_MAX_WORD_LEN * 3 + 4;

	/////////////////////////////////////////////////////////////////////////////

	// wordlist checkpoints frequency
	constexpr auto  SPH_WORDLIST_CHECKPOINT = 64;


/////////////////////////////////////////////////////////////////////////////
// aot 

// simple order aot languages
	enum AOT_LANGS { AOT_BEGIN = 0, AOT_RU = AOT_BEGIN, AOT_EN, AOT_DE, AOT_LENGTH };

	// aot lemmatize names
	extern const char* AOT_LANGUAGES[AOT_LENGTH];

	/////////////////////////////////////////////////////////////////////////////

	constexpr auto  HITLESS_DOC_MASK = 0x7FFFFFFF;
	constexpr auto  HITLESS_DOC_FLAG = 0x80000000;

/////////////////////////////////

	extern const char* g_sTagInfixBlocks;
	extern const char* g_sTagInfixEntries;

	/////////////////////////////////

}