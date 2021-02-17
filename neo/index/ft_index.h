#pragma once
#include "neo/platform/atomic.h"
#include "neo/int/types.h"
#include "neo/int/keyword_info.h"
#include "neo/query/enums.h"
#include "neo/index/enums.h"
#include "neo/index/progress.h"
#include "neo/index/keyword_stat.h"
#include "neo/index/index_settings.h"
#include "neo/source/base.h"
#include "neo/source/attrib_locator.h"
#include "neo/source/source_stats.h"
#include "neo/query/query_result.h"
#include "neo/query/query.h"
#include "neo/query/filter_settings.h"
#include "neo/query/match_processor.h"
#include "neo/query/match_sorter.h"
#include "neo/query/get_keyword_settings.h"
#include "neo/dict/dict.h"
#include "neo/core/match.h"
#include "neo/core/kill_list_trait.h"
#include "neo/utility/hash.h"

namespace NEO {

	//fwd dec
	class CSphSchema;
	class CSphRsetSchema;
	struct CSphAttrUpdate;
	struct ISphExpr;
	struct ISphExprHook;
	class ISphTokenizer;
	class CSphDict;



	/// forward refs to internal searcher classes
	class ISphQword;
	class ISphQwordSetup;
	class CSphQueryContext;
	struct ISphFilter;

	struct SuggestArgs_t;
	struct SuggestResult_t;


	struct CSphIndexStatus
	{
		int64_t			m_iRamUse;
		int64_t			m_iDiskUse;
		int64_t			m_iRamChunkSize; // not used for plain
		int				m_iNumChunks; // not used for plain
		int64_t			m_iMemLimit; // not used for plain

		CSphIndexStatus()
			: m_iRamUse(0)
			, m_iDiskUse(0)
			, m_iRamChunkSize(0)
			, m_iNumChunks(0)
			, m_iMemLimit(0)
		{}
	};


	struct CSphMultiQueryArgs : public ISphNoncopyable
	{
		const KillListVector& m_dKillList;
		const int								m_iIndexWeight;
		int										m_iTag;
		DWORD									m_uPackedFactorFlags;
		bool									m_bLocalDF;
		const SmallStringHash_T<int64_t>* m_pLocalDocs;
		int64_t									m_iTotalDocs;

		CSphMultiQueryArgs(const KillListVector& dKillList, int iIndexWeight);
	};



	/// generic fulltext index interface
	class CSphIndex : public ISphKeywordsStat
	{
	public:

		enum
		{
			ATTRS_UPDATED = (1UL << 0),
			ATTRS_MVA_UPDATED = (1UL << 1),
			ATTRS_STRINGS_UPDATED = (1UL << 2)
		};

	public:
		explicit					CSphIndex(const char* sIndexName, const char* sFilename);
		virtual						~CSphIndex();

		virtual const CSphString& GetLastError() const { return m_sLastError; }
		virtual const CSphString& GetLastWarning() const { return m_sLastWarning; }
		virtual const CSphSchema& GetMatchSchema() const { return m_tSchema; }			///< match schema as returned in result set (possibly different from internal storage schema!)

		virtual	void				SetProgressCallback(CSphIndexProgress::IndexingProgress_fn pfnProgress) = 0;
		virtual void				SetInplaceSettings(int iHitGap, int iDocinfoGap, float fRelocFactor, float fWriteFactor);
		virtual void				SetPreopen(bool bValue) { m_bKeepFilesOpen = bValue; }
		void						SetFieldFilter(ISphFieldFilter* pFilter);
		const ISphFieldFilter* GetFieldFilter() const { return m_pFieldFilter; }
		void						SetTokenizer(ISphTokenizer* pTokenizer);
		void						SetupQueryTokenizer();
		const ISphTokenizer* GetTokenizer() const { return m_pTokenizer; }
		const ISphTokenizer* GetQueryTokenizer() const { return m_pQueryTokenizer; }
		ISphTokenizer* LeakTokenizer();
		void						SetDictionary(CSphDict* pDict);
		CSphDict* GetDictionary() const { return m_pDict; }
		CSphDict* LeakDictionary();
		virtual void				SetKeepAttrs(const CSphString&, const CSphVector<CSphString>&) {}
		virtual void				Setup(const CSphIndexSettings& tSettings);
		const CSphIndexSettings& GetSettings() const { return m_tSettings; }
		bool						IsStripperInited() const { return m_bStripperInited; }
		virtual SphDocID_t* GetKillList() const = 0;
		virtual int					GetKillListSize() const = 0;
		virtual bool				HasDocid(SphDocID_t uDocid) const = 0;
		virtual bool				IsRT() const { return false; }
		void						SetBinlog(bool bBinlog) { m_bBinlog = bBinlog; }
		virtual int64_t* GetFieldLens() const { return NULL; }
		virtual bool				IsStarDict() const { return true; }
		int64_t						GetIndexId() const { return m_iIndexId; }

	public:
		/// build index by indexing given sources
		virtual int					Build(const CSphVector<CSphSource*>& dSources, int iMemoryLimit, int iWriteBuffer) = 0;

		/// build index by mering current index with given index
		virtual bool				Merge(CSphIndex* pSource, const CSphVector<CSphFilterSettings>& dFilters, bool bMergeKillLists) = 0;

	public:
		/// check all data files, preload schema, and preallocate enough RAM to load memory-cached data
		virtual bool				Prealloc(bool bStripPath) = 0;

		/// deallocate all previously preallocated shared data
		virtual void				Dealloc() = 0;

		/// precache everything which needs to be precached
		virtual void				Preread() = 0;

		/// set new index base path
		virtual void				SetBase(const char* sNewBase) = 0;

		/// set new index base path, and physically rename index files too
		virtual bool				Rename(const char* sNewBase) = 0;

		/// obtain exclusive lock on this index
		virtual bool				Lock() = 0;

		/// dismiss exclusive lock and unlink lock file
		virtual void				Unlock() = 0;

		/// called when index is loaded and prepared to work
		virtual void				PostSetup() = 0;

	public:
		/// return index document, bytes totals (FIXME? remove this in favor of GetStatus() maybe?)
		virtual const CSphSourceStats& GetStats() const = 0;

		/// return additional index info
		virtual void				GetStatus(CSphIndexStatus*) const = 0;

	public:
		virtual bool				EarlyReject(CSphQueryContext* pCtx, CSphMatch& tMatch) const = 0;
		void						SetCacheSize(int iMaxCachedDocs, int iMaxCachedHits);
		virtual bool				MultiQuery(const CSphQuery* pQuery, CSphQueryResult* pResult, int iSorters, ISphMatchSorter** ppSorters, const CSphMultiQueryArgs& tArgs) const = 0;
		virtual bool				MultiQueryEx(int iQueries, const CSphQuery* ppQueries, CSphQueryResult** ppResults, ISphMatchSorter** ppSorters, const CSphMultiQueryArgs& tArgs) const = 0;
		virtual bool				GetKeywords(CSphVector <CSphKeywordInfo>& dKeywords, const char* szQuery, const GetKeywordsSettings_t& tSettings, CSphString* pError) const = 0;
		virtual bool				FillKeywords(CSphVector <CSphKeywordInfo>& dKeywords) const = 0;
		virtual void				GetSuggest(const SuggestArgs_t&, SuggestResult_t&) const {}

	public:
		/// updates memory-cached attributes in real time
		/// returns non-negative amount of actually found and updated records on success
		/// on failure, -1 is returned and GetLastError() contains error message
		virtual int					UpdateAttributes(const CSphAttrUpdate& tUpd, int iIndex, CSphString& sError, CSphString& sWarning) = 0;

		/// saves memory-cached attributes, if there were any updates to them
		/// on failure, false is returned and GetLastError() contains error message
		virtual bool				SaveAttributes(CSphString& sError) const = 0;

		virtual DWORD				GetAttributeStatus() const = 0;

		virtual bool				AddRemoveAttribute(bool bAddAttr, const CSphString& sAttrName, ESphAttr eAttrType, CSphString& sError) = 0;

	public:
		/// internal debugging hook, DO NOT USE
		virtual void				DebugDumpHeader(FILE* fp, const char* sHeaderName, bool bConfig) = 0;

		/// internal debugging hook, DO NOT USE
		virtual void				DebugDumpDocids(FILE* fp) = 0;

		/// internal debugging hook, DO NOT USE
		virtual void				DebugDumpHitlist(FILE* fp, const char* sKeyword, bool bID) = 0;

		/// internal debugging hook, DO NOT USE
		virtual void				DebugDumpDict(FILE* fp) = 0;

		/// internal debugging hook, DO NOT USE
		virtual int					DebugCheck(FILE* fp) = 0;
		virtual void				SetDebugCheck() {}

		/// getter for name
		const char* GetName() { return m_sIndexName.cstr(); }

		void						SetName(const char* sName) { m_sIndexName = sName; }

		/// get for the base file name
		const char* GetFilename() const { return m_sFilename.cstr(); }

		/// internal make document id list from external docinfo, DO NOT USE
		virtual bool BuildDocList(SphAttr_t** ppDocList, int64_t* pCount, CSphString* pError) const;

		/// internal replace kill-list and rewrite spk file, DO NOT USE
		virtual bool				ReplaceKillList(const SphDocID_t*, int) { return true; }

		virtual void				SetMemorySettings(bool bMlock, bool bOndiskAttrs, bool bOndiskPool) = 0;

		virtual void				GetFieldFilterSettings(CSphFieldFilterSettings& tSettings);

	public:
		int64_t						m_iTID;					///< last committed transaction id

		bool						m_bExpandKeywords;		///< enable automatic query-time keyword expansion (to "( word | =word | *word* )")
		int							m_iExpansionLimit;

	protected:
		static CSphAtomic			m_tIdGenerator;

		int64_t						m_iIndexId;				///< internal (per daemon) unique index id, introduced for caching

		CSphSchema					m_tSchema;
		CSphString					m_sLastError;
		CSphString					m_sLastWarning;

		bool						m_bInplaceSettings;
		int							m_iHitGap;
		int							m_iDocinfoGap;
		float						m_fRelocFactor;
		float						m_fWriteFactor;

		bool						m_bKeepFilesOpen;		///< keep files open to avoid race on seamless rotation
		bool						m_bBinlog;

		bool						m_bStripperInited;		///< was stripper initialized (old index version (<9) handling)

	protected:
		CSphIndexSettings			m_tSettings;

		ISphFieldFilter* m_pFieldFilter;
		ISphTokenizer* m_pTokenizer;
		ISphTokenizer* m_pQueryTokenizer;
		CSphDict* m_pDict;

		int							m_iMaxCachedDocs;
		int							m_iMaxCachedHits;
		CSphString					m_sIndexName;
		CSphString					m_sFilename;

	public:
		void						SetGlobalIDFPath(const CSphString& sPath) { m_sGlobalIDFPath = sPath; }
		float						GetGlobalIDF(const CSphString& sWord, int64_t iDocsLocal, bool bPlainIDF) const;

	protected:
		CSphString					m_sGlobalIDFPath;
	};


	/////////////////////////////////////////////////////////////////////////////



	/// create template (tokenizer) index implementation
	CSphIndex* sphCreateIndexTemplate();



	/// parses sort clause, using a given schema
	/// fills eFunc and tState and optionally sError, returns result code
	ESortClauseParseResult	sphParseSortClause(const CSphQuery* pQuery, const char* sClause, const ISphSchema& tSchema,
		ESphSortFunc& eFunc, CSphMatchComparatorState& tState, CSphString& sError);



	/// convert queue to sorted array, and add its entries to result's matches array
	int					sphFlattenQueue(ISphMatchSorter* pQueue, CSphQueryResult* pResult, int iTag);

	/// setup per-keyword read buffer sizes
	void				sphSetReadBuffers(int iReadBuffer, int iReadUnhinted);

	/// check query for expressions
	bool				sphHasExpressions(const CSphQuery& tQuery, const CSphSchema& tSchema);


	//////////////////////////////////////////////////////////////////////////

	/// this pseudo-index used to store and manage the tokenizer
	/// without any footprint in real files
	//////////////////////////////////////////////////////////////////////////
	static CSphSourceStats g_tTmpDummyStat;
	class CSphTokenizerIndex : public CSphIndex
	{
	public:
		CSphTokenizerIndex() : CSphIndex(NULL, NULL) {}
		virtual SphDocID_t* GetKillList() const { return NULL; }
		virtual int					GetKillListSize() const { return 0; }
		virtual bool				HasDocid(SphDocID_t) const { return false; }
		virtual int					Build(const CSphVector<CSphSource*>&, int, int) { return 0; }
		virtual bool				Merge(CSphIndex*, const CSphVector<CSphFilterSettings>&, bool) { return false; }
		virtual bool				Prealloc(bool) { return false; }
		virtual void				Dealloc() {}
		virtual void				Preread() {}
		virtual void				SetMemorySettings(bool, bool, bool) {}
		virtual void				SetBase(const char*) {}
		virtual bool				Rename(const char*) { return false; }
		virtual bool				Lock() { return false; }
		virtual void				Unlock() {}
		virtual void				PostSetup() {}
		virtual bool				EarlyReject(CSphQueryContext*, CSphMatch&) const { return false; }
		virtual const CSphSourceStats& GetStats() const { return g_tTmpDummyStat; }
		virtual void			GetStatus(CSphIndexStatus* pRes) const { assert(pRes); if (pRes) { pRes->m_iDiskUse = 0; pRes->m_iRamUse = 0; } }
		virtual bool				MultiQuery(const CSphQuery*, CSphQueryResult*, int, ISphMatchSorter**, const CSphMultiQueryArgs&) const { return false; }
		virtual bool				MultiQueryEx(int, const CSphQuery*, CSphQueryResult**, ISphMatchSorter**, const CSphMultiQueryArgs&) const { return false; }
		virtual bool				GetKeywords(CSphVector <CSphKeywordInfo>&, const char*, const GetKeywordsSettings_t& tSettings, CSphString*) const;
		virtual bool				FillKeywords(CSphVector <CSphKeywordInfo>&) const { return true; }
		virtual int					UpdateAttributes(const CSphAttrUpdate&, int, CSphString&, CSphString&) { return -1; }
		virtual bool				SaveAttributes(CSphString&) const { return false; }
		virtual DWORD				GetAttributeStatus() const { return 0; }
		virtual bool				CreateModifiedFiles(bool, const CSphString&, ESphAttr, int, CSphString&) { return true; }
		virtual bool				AddRemoveAttribute(bool, const CSphString&, ESphAttr, CSphString&) { return true; }
		virtual void				DebugDumpHeader(FILE*, const char*, bool) {}
		virtual void				DebugDumpDocids(FILE*) {}
		virtual void				DebugDumpHitlist(FILE*, const char*, bool) {}
		virtual int					DebugCheck(FILE*) { return 0; } // NOLINT
		virtual void				DebugDumpDict(FILE*) {}
		virtual	void				SetProgressCallback(CSphIndexProgress::IndexingProgress_fn) {}
	};


}