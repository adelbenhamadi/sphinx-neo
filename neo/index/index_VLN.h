#pragma once


#include "neo/core/generic.h"
#include "neo/int/types.h"
#include "neo/int/scoped_pointer.h"
#include "neo/int/keyword_info.h"
#include "neo/index/enums.h"
#include "neo/index/index_settings.h"
#include "neo/index/ft_index.h"
#include "neo/core/attrib_index_builder.h"
#include "neo/core/ranker.h"
#include "neo/io/autofile.h"
#include "neo/io/buffer.h"
#include "neo/io//bin.h"
#include "neo/source/base.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/query/query.h"
#include "neo/query/filter_settings.h"
#include "neo/query/match_sorter.h"
#include "neo/core/hit_builder.h"
#include "neo/core/match_engine.h"
#include "neo/core/match.h"
#include "neo/core/build_header.h"
#include "neo/core/word_list.h"
#include "neo/query/get_keyword_settings.h"



#include <cstdio>

namespace NEO {

	//fwd class
	struct XQNode_t;
	struct XQQuery_t;
	class CSphQueryNodeCache;
	struct SphWordStatChecker_t;
	class CSphScopedPayload;


	/// this is my actual VLN-compressed phrase index implementation
	class CSphIndex_VLN : public CSphIndex
	{
		friend class DiskIndexQwordSetup_c;
		friend class CSphMerger;
		friend class AttrIndexBuilder_t<SphDocID_t>;
		friend struct SphFinalMatchCalc_t;

	public:
		explicit					CSphIndex_VLN(const char* sIndexName, const char* sFilename);
		~CSphIndex_VLN();

		virtual int					Build(const CSphVector<CSphSource*>& dSources, int iMemoryLimit, int iWriteBuffer);
		virtual	void				SetProgressCallback(CSphIndexProgress::IndexingProgress_fn pfnProgress) { m_tProgress.m_fnProgress = pfnProgress; }

		virtual bool				LoadHeader(const char* sHeaderName, bool bStripPath, CSphEmbeddedFiles& tEmbeddedFiles, CSphString& sWarning);
		virtual bool				WriteHeader(const BuildHeader_t& tBuildHeader, CSphWriter& fdInfo) const;

		virtual void				DebugDumpHeader(FILE* fp, const char* sHeaderName, bool bConfig);
		virtual void				DebugDumpDocids(FILE* fp);
		virtual void				DebugDumpHitlist(FILE* fp, const char* sKeyword, bool bID);
		virtual void				DebugDumpDict(FILE* fp);
		virtual void				SetDebugCheck();
		virtual int					DebugCheck(FILE* fp);
		template <class Qword> void	DumpHitlist(FILE* fp, const char* sKeyword, bool bID);

		virtual bool				Prealloc(bool bStripPath);
		virtual void				Dealloc();
		virtual void				Preread();
		virtual void				SetMemorySettings(bool bMlock, bool bOndiskAttrs, bool bOndiskPool);

		virtual void				SetBase(const char* sNewBase);
		virtual bool				Rename(const char* sNewBase);

		virtual bool				Lock();
		virtual void				Unlock();
		virtual void				PostSetup() {}

		virtual bool				MultiQuery(const CSphQuery* pQuery, CSphQueryResult* pResult, int iSorters, ISphMatchSorter** ppSorters, const CSphMultiQueryArgs& tArgs) const;
		virtual bool				MultiQueryEx(int iQueries, const CSphQuery* pQueries, CSphQueryResult** ppResults, ISphMatchSorter** ppSorters, const CSphMultiQueryArgs& tArgs) const;
		virtual bool				GetKeywords(CSphVector <CSphKeywordInfo>& dKeywords, const char* szQuery, const GetKeywordsSettings_t& tSettings, CSphString* pError) const;
		template <class Qword> bool	DoGetKeywords(CSphVector <CSphKeywordInfo>& dKeywords, const char* szQuery, const GetKeywordsSettings_t& tSettings, bool bFillOnly, CSphString* pError) const;
		virtual bool 				FillKeywords(CSphVector <CSphKeywordInfo>& dKeywords) const;
		virtual void				GetSuggest(const SuggestArgs_t& tArgs, SuggestResult_t& tRes) const;

		virtual bool				Merge(CSphIndex* pSource, const CSphVector<CSphFilterSettings>& dFilters, bool bMergeKillLists);

		template <class QWORDDST, class QWORDSRC>
		static bool					MergeWords(const CSphIndex_VLN* pDstIndex, const CSphIndex_VLN* pSrcIndex, const ISphFilter* pFilter, const CSphVector<SphDocID_t>& dKillList, SphDocID_t uMinID, CSphHitBuilder* pHitBuilder, CSphString& sError, CSphSourceStats& tStat, CSphIndexProgress& tProgress, ThrottleState_t* pThrottle, volatile bool* pGlobalStop, volatile bool* pLocalStop);
		static bool					DoMerge(const CSphIndex_VLN* pDstIndex, const CSphIndex_VLN* pSrcIndex, bool bMergeKillLists, ISphFilter* pFilter, const CSphVector<SphDocID_t>& dKillList, CSphString& sError, CSphIndexProgress& tProgress, ThrottleState_t* pThrottle, volatile bool* pGlobalStop, volatile bool* pLocalStop);

		virtual int					UpdateAttributes(const CSphAttrUpdate& tUpd, int iIndex, CSphString& sError, CSphString& sWarning);
		virtual bool				SaveAttributes(CSphString& sError) const;
		virtual DWORD				GetAttributeStatus() const;

		virtual bool				AddRemoveAttribute(bool bAddAttr, const CSphString& sAttrName, ESphAttr eAttrType, CSphString& sError);

		bool						EarlyReject(CSphQueryContext* pCtx, CSphMatch& tMatch) const;

		virtual void				SetKeepAttrs(const CSphString& sKeepAttrs, const CSphVector<CSphString>& dAttrs) { m_sKeepAttrs = sKeepAttrs; m_dKeepAttrs = dAttrs; }

		virtual SphDocID_t* GetKillList() const;
		virtual int					GetKillListSize() const;
		virtual bool				HasDocid(SphDocID_t uDocid) const;

		virtual const CSphSourceStats& GetStats() const { return m_tStats; }
		virtual int64_t* GetFieldLens() const { return m_tSettings.m_bIndexFieldLens ? m_dFieldLens.Begin() : NULL; }
		virtual void				GetStatus(CSphIndexStatus*) const;
		virtual bool 				BuildDocList(SphAttr_t** ppDocList, int64_t* pCount, CSphString* pError) const;
		virtual bool				ReplaceKillList(const SphDocID_t* pKillist, int iCount);

	private:

		static const int			MIN_WRITE_BUFFER = 262144;	//min write buffer size
		static const int			DEFAULT_WRITE_BUFFER = 1048576;	//default write buffer size

	private:
		// common stuff
		int								m_iLockFD;
		CSphSourceStats					m_tStats;			//my stats
		int								m_iTotalDups;
		CSphFixedVector<CSphRowitem>	m_dMinRow;
		SphDocID_t						m_uMinDocid;
		CSphFixedVector<int64_t>		m_dFieldLens;	//total per-field lengths summed over entire indexed data, in tokens
		CSphString						m_sKeepAttrs;			//retain attributes of that index reindexing
		CSphVector<CSphString>			m_dKeepAttrs;

	private:

		CSphIndexProgress			m_tProgress;

		bool						LoadHitlessWords(CSphVector<SphWordID_t>& dHitlessWords);

	private:
		// searching-only, per-index
		static const int			DOCINFO_HASH_BITS = 18;	// FIXME! make this configurable

		int64_t						m_iDocinfo;				//my docinfo cache size
		int64_t						m_iDocinfoIndex;		//docinfo "index" entries count (each entry is 2x docinfo rows, for min/max)
		DWORD* m_pDocinfoIndex;		//docinfo "index", to accelerate filtering during full-scan (2x rows for each block, and 2x rows for the whole index, 1+m_uDocinfoIndex entries)
		int64_t						m_iMinMaxIndex;			//stored min/max cache offset (counted in DWORDs)

		// !COMMIT slow setup data
		CSphMappedBuffer<DWORD>			m_tAttr;
		CSphMappedBuffer<DWORD>			m_tMva;
		CSphMappedBuffer<BYTE>			m_tString;
		CSphMappedBuffer<SphDocID_t>	m_tKillList;		//killlist
		CSphMappedBuffer<BYTE>			m_tSkiplists;		//(compressed) skiplists data
		CWordlist										m_tWordlist;		//my wordlist
		// recalculate on attr load complete
		CSphLargeBuffer<DWORD>							m_tDocinfoHash;		//hashed ids, to accelerate lookups
		CSphLargeBuffer<DWORD>							m_tMinMaxLegacy;

		bool						m_bMlock;
		bool						m_bOndiskAllAttr;
		bool						m_bOndiskPoolAttr;
		bool						m_bArenaProhibit;

		DWORD						m_uVersion;				//data files version
		volatile bool				m_bPassedRead;
		volatile bool				m_bPassedAlloc;
		bool						m_bIsEmpty;				//do we have actually indexed documents (m_iTotalDocuments is just fetched documents, not indexed!)
		bool						m_bHaveSkips;			//whether we have skiplists
		bool						m_bDebugCheck;

		DWORD						m_uAttrsStatus;
		int							m_iIndexTag;			//my ids for MVA updates pool
		static volatile int			m_iIndexTagSeq;			//static ids sequence

		CSphAutofile				m_tDoclistFile;			//doclist file
		CSphAutofile				m_tHitlistFile;			//hitlist file

	private:
		CSphString					GetIndexFileName(const char* sExt) const;

		bool						ParsedMultiQuery(const CSphQuery* pQuery, CSphQueryResult* pResult, int iSorters, ISphMatchSorter** ppSorters, const XQQuery_t& tXQ, CSphDict* pDict, const CSphMultiQueryArgs& tArgs, CSphQueryNodeCache* pNodeCache, const SphWordStatChecker_t& tStatDiff) const;
		bool						MultiScan(const CSphQuery* pQuery, CSphQueryResult* pResult, int iSorters, ISphMatchSorter** ppSorters, const CSphMultiQueryArgs& tArgs) const;
		void						MatchExtended(CSphQueryContext* pCtx, const CSphQuery* pQuery, int iSorters, ISphMatchSorter** ppSorters, ISphRanker* pRanker, int iTag, int iIndexWeight) const;

		const DWORD* FindDocinfo(SphDocID_t uDocID) const;
		void						CopyDocinfo(const CSphQueryContext* pCtx, CSphMatch& tMatch, const DWORD* pFound) const;

		bool						BuildMVA(const CSphVector<CSphSource*>& dSources, CSphFixedVector<CSphWordHit>& dHits, int iArenaSize, int iFieldFD, int nFieldMVAs, int iFieldMVAInPool, CSphIndex_VLN* pPrevIndex, const CSphBitvec* pPrevMva);

		bool						IsStarDict() const;
		CSphDict* SetupStarDict(CSphScopedPtr<CSphDict>& tContainer, CSphDict* pPrevDict) const;
		CSphDict* SetupExactDict(CSphScopedPtr<CSphDict>& tContainer, CSphDict* pPrevDict) const;

		bool						RelocateBlock(int iFile, BYTE* pBuffer, int iRelocationSize, SphOffset_t* pFileSize, CSphBin* pMinBin, SphOffset_t* pSharedOffset);
		bool						PrecomputeMinMax();

	private:
		bool						LoadPersistentMVA(CSphString& sError);

		bool						JuggleFile(const char* szExt, CSphString& sError, bool bNeedOrigin = true) const;
		XQNode_t* ExpandPrefix(XQNode_t* pNode, CSphQueryResultMeta* pResult, CSphScopedPayload* pPayloads, DWORD uQueryDebugFlags) const;

		bool						BuildDone(const BuildHeader_t& tBuildHeader, CSphString& sError) const;
	};



	bool			sphMerge(const CSphIndex* pDst, const CSphIndex* pSrc, const CSphVector<SphDocID_t>& dKillList, CSphString& sError, CSphIndexProgress& tProgress, ThrottleState_t* pThrottle, volatile bool* pGlobalStop, volatile bool* pLocalStop);

	/////////////////


	struct SphFinalMatchCalc_t : ISphMatchProcessor, ISphNoncopyable
	{
		const CSphIndex_VLN* m_pDocinfoSrc;
		const CSphQueryContext& m_tCtx;
		int64_t						m_iBadRows;
		int							m_iTag;

		SphFinalMatchCalc_t(int iTag, const CSphIndex_VLN* pIndex, const CSphQueryContext& tCtx)
			: m_pDocinfoSrc(pIndex)
			, m_tCtx(tCtx)
			, m_iBadRows(0)
			, m_iTag(iTag)
		{ }

		virtual void Process(CSphMatch* pMatch)
		{
			if (pMatch->m_iTag >= 0)
				return;

			if (m_pDocinfoSrc)
			{
				const CSphRowitem* pRow = m_pDocinfoSrc->FindDocinfo(pMatch->m_uDocID);
				if (!pRow && m_pDocinfoSrc->m_tSettings.m_eDocinfo == SPH_DOCINFO_EXTERN)
				{
					m_iBadRows++;
					pMatch->m_iTag = m_iTag;
					return;
				}
				m_pDocinfoSrc->CopyDocinfo(&m_tCtx, *pMatch, pRow);
			}

			m_tCtx.CalcFinal(*pMatch);
			pMatch->m_iTag = m_iTag;
		}
	};



	/// create phrase fulltext index implementation
	CSphIndex* sphCreateIndexPhrase(const char* szIndexName, const char* sFilename);

}