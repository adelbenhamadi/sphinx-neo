#pragma once

#include "neo/int/types.h"
#include "neo/tokenizer/enums.h"
#include "neo/query/enums.h"
#include "neo/tools/config_parser.h"



#include <ctype.h>
#include <cstdarg>

namespace NEO {

	/// string splitter, extracts sequences of alphas (as in sphIsAlpha)
	void sphSplit(CSphVector<CSphString>& dOut, const char* sIn);

	/// string splitter, splits by the given boundaries
	void sphSplit(CSphVector<CSphString>& dOut, const char* sIn, const char* sBounds);

	/// string wildcard matching (case-sensitive, supports * and ? patterns)
	bool sphWildcardMatch(const char* sSstring, const char* sPattern, const int* pPattern = NULL);


	//////////////////////////////////////////////////////////////////////////

	enum
	{
		// where was TOKENIZER_SBCS=1 once
		TOKENIZER_UTF8 = 2,
		TOKENIZER_NGRAM = 3
	};

	//fwd dec
	struct CSphTokenizerSettings;
	struct CSphFieldFilterSettings;
	struct CSphIndexSettings;
	struct CSphDictSettings;
	class CSphIndex;

	/// load config file
	const char* sphLoadConfig(const char* sOptConfig, bool bQuiet, CSphConfigParser& cp);

	/// configure tokenizer from index definition section
	void			sphConfTokenizer(const CSphConfigSection& hIndex, CSphTokenizerSettings& tSettings);

	/// configure dictionary from index definition section
	void			sphConfDictionary(const CSphConfigSection& hIndex, CSphDictSettings& tSettings);

	/// configure field filter from index definition section
	bool			sphConfFieldFilter(const CSphConfigSection& hIndex, CSphFieldFilterSettings& tSettings, CSphString& sError);

	/// configure index from index definition section
	bool			sphConfIndex(const CSphConfigSection& hIndex, CSphIndexSettings& tSettings, CSphString& sError);

	/// try to set dictionary, tokenizer and misc settings for an index (if not already set)
	bool			sphFixupIndexSettings(CSphIndex* pIndex, const CSphConfigSection& hIndex, CSphString& sError, bool bTemplateDict = false);



	const char* sphBigramName(ESphBigram eType);

	void			RebalanceWeights(const CSphFixedVector<int64_t>& dTimers, WORD* pWeights);

	/////////////////////

	/// how do we properly exit from the crash handler?
#if !USE_WINDOWS
#define CRASH_EXIT_CORE { signal ( sig, SIG_DFL ); kill ( getpid(), sig ); }
#ifndef NDEBUG
	// UNIX debug build, die and dump core
#define CRASH_EXIT CRASH_EXIT_CORE
#else
	// UNIX release build, just die
#define CRASH_EXIT { exit ( 2 ); }
#endif
#else
#define CRASH_EXIT_CORE return EXCEPTION_CONTINUE_SEARCH
#ifndef NDEBUG
	// Windows debug build, show prompt to attach debugger
#define CRASH_EXIT CRASH_EXIT_CORE
#else
	// Windows release build, just die
#define CRASH_EXIT return EXCEPTION_EXECUTE_HANDLER
#endif
#endif

	/////////////////////////


/// simple write wrapper
/// simplifies partial write checks, and also supresses "fortified" glibc warnings
	bool sphWrite(int iFD, const void* pBuf, size_t iSize);

	/// async safe, BUT NOT THREAD SAFE, fprintf
	void sphSafeInfo(int iFD, const char* sFmt, ...);

#if !USE_WINDOWS
	/// UNIX backtrace gets printed out to a stream
	void sphBacktrace(int iFD, bool bSafe = false);
#else
	/// Windows minidump gets saved to a file
	void sphBacktrace(EXCEPTION_POINTERS* pExc, const char* sFile);
#endif

	void sphBacktraceSetBinaryName(const char* sName);

	/// plain backtrace - returns static buffer with the text of the call stack
	const char* DoBacktrace(int iDepth = 0, int iSkip = 0);

	void sphCheckDuplicatePaths(const CSphConfig& hConf);

	/// set globals from the common config section
	void sphConfigureCommon(const CSphConfig& hConf);

	/// my own is chinese
	bool sphIsChineseCode(int iCode);

	/// detect chinese chars in a buffer
	bool sphDetectChinese(const BYTE* szBuffer, int iLength);

	/// returns ranker name as string
	const char* sphGetRankerName(ESphRankMode eRanker);

	/////////////////////////////


	/// set JSON attribute indexing options
	/// bStrict is whether to stop indexing on error, or just ignore the attribute value
	/// bAutoconvNumbers is whether to auto-convert eligible (!) strings to integers and floats, or keep them as strings
	/// bKeynamesToLowercase is whether to convert all key names to lowercase
	void				sphSetJsonOptions(bool bStrict, bool bAutoconvNumbers, bool bKeynamesToLowercase);

	void			sphSetUnlinkOld(bool bUnlink);
	void			sphUnlinkIndex(const char* sName, bool bForce);

	////////////////////////////


	class CSphDynamicLibrary : public ISphNoncopyable
	{
		bool		m_bReady; // whether the lib is valid or not
		void* m_pLibrary; // internal handle

	public:
		CSphDynamicLibrary(const char* sPath);

		// We are suppose, that library is loaded once when necessary, and will alive whole lifetime of utility.
		// So, no need to explicitly desctruct it, this is intended leak.
		~CSphDynamicLibrary() = default;

		bool		LoadSymbols(const char** sNames, void*** pppFuncs, int iNum);
	};

}