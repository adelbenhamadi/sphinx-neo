#include "neo/index/ft_index.h"
#include "neo/platform/atomic.h"
#include "neo/tokenizer/tokenizer.h"
#include "neo/int/scoped_pointer.h"
#include "neo/dict/dict_star.h"
#include "neo/dict/dict_exact.h"
#include "neo/index/enums.h"
#include "neo/source/schema.h"
#include "neo/query/attr_update.h"
#include "neo/int/query_filter.h"
#include "neo/query/get_keyword_settings.h"
#include "neo/core/global_idf.h"


#include "neo/sphinxint.h"
#include "neo/sphinxexpr.h"
#include "neo/sphinx/xqcache.h"
#include "neo/sphinx/xquery.h"



namespace NEO {

	CSphMultiQueryArgs::CSphMultiQueryArgs(const KillListVector& dKillList, int iIndexWeight)
		: m_dKillList(dKillList)
		, m_iIndexWeight(iIndexWeight)
		, m_iTag(0)
		, m_uPackedFactorFlags(SPH_FACTOR_DISABLE)
		, m_bLocalDF(false)
		, m_pLocalDocs(NULL)
		, m_iTotalDocs(0)
	{
		assert(iIndexWeight > 0);
	}



	CSphAtomic CSphIndex::m_tIdGenerator;

	CSphIndex::CSphIndex(const char* sIndexName, const char* sFilename)
		: m_iTID(0)
		, m_bExpandKeywords(false)
		, m_iExpansionLimit(0)
		, m_tSchema(sFilename)
		, m_bInplaceSettings(false)
		, m_iHitGap(0)
		, m_iDocinfoGap(0)
		, m_fRelocFactor(0.0f)
		, m_fWriteFactor(0.0f)
		, m_bKeepFilesOpen(false)
		, m_bBinlog(true)
		, m_bStripperInited(true)
		, m_pFieldFilter(NULL)
		, m_pTokenizer(NULL)
		, m_pQueryTokenizer(NULL)
		, m_pDict(NULL)
		, m_iMaxCachedDocs(0)
		, m_iMaxCachedHits(0)
		, m_sIndexName(sIndexName)
		, m_sFilename(sFilename)
	{
		m_iIndexId = m_tIdGenerator.Inc();
	}


	CSphIndex::~CSphIndex()
	{
		QcacheDeleteIndex(m_iIndexId);
		SafeDelete(m_pFieldFilter);
		SafeDelete(m_pQueryTokenizer);
		SafeDelete(m_pTokenizer);
		SafeDelete(m_pDict);
	}


	void CSphIndex::SetInplaceSettings(int iHitGap, int iDocinfoGap, float fRelocFactor, float fWriteFactor)
	{
		m_iHitGap = iHitGap;
		m_iDocinfoGap = iDocinfoGap;
		m_fRelocFactor = fRelocFactor;
		m_fWriteFactor = fWriteFactor;
		m_bInplaceSettings = true;
	}


	void CSphIndex::SetFieldFilter(ISphFieldFilter* pFieldFilter)
	{
		if (m_pFieldFilter != pFieldFilter)
			SafeDelete(m_pFieldFilter);
		m_pFieldFilter = pFieldFilter;
	}


	void CSphIndex::SetTokenizer(ISphTokenizer* pTokenizer)
	{
		if (m_pTokenizer != pTokenizer)
			SafeDelete(m_pTokenizer);
		m_pTokenizer = pTokenizer;
	}


	void CSphIndex::SetupQueryTokenizer()
	{
		// create and setup a master copy of query time tokenizer
		// that we can then use to create lightweight clones
		SafeDelete(m_pQueryTokenizer);
		m_pQueryTokenizer = m_pTokenizer->Clone(SPH_CLONE_QUERY);
		sphSetupQueryTokenizer(m_pQueryTokenizer, IsStarDict(), m_tSettings.m_bIndexExactWords);
	}


	ISphTokenizer* CSphIndex::LeakTokenizer()
	{
		ISphTokenizer* pTokenizer = m_pTokenizer;
		m_pTokenizer = NULL;
		return pTokenizer;
	}


	void CSphIndex::SetDictionary(CSphDict* pDict)
	{
		if (m_pDict != pDict)
			SafeDelete(m_pDict);

		m_pDict = pDict;
	}


	CSphDict* CSphIndex::LeakDictionary()
	{
		CSphDict* pDict = m_pDict;
		m_pDict = NULL;
		return pDict;
	}


	void CSphIndex::Setup(const CSphIndexSettings& tSettings)
	{
		m_bStripperInited = true;
		m_tSettings = tSettings;
	}


	void CSphIndex::SetCacheSize(int iMaxCachedDocs, int iMaxCachedHits)
	{
		m_iMaxCachedDocs = iMaxCachedDocs;
		m_iMaxCachedHits = iMaxCachedHits;
	}


	float CSphIndex::GetGlobalIDF(const CSphString& sWord, int64_t iDocsLocal, bool bPlainIDF) const
	{
		g_tGlobalIDFLock.Lock();
		CSphGlobalIDF** ppGlobalIDF = g_hGlobalIDFs(m_sGlobalIDFPath);
		float fIDF = ppGlobalIDF && *ppGlobalIDF ? (*ppGlobalIDF)->GetIDF(sWord, iDocsLocal, bPlainIDF) : 0.0f;
		g_tGlobalIDFLock.Unlock();
		return fIDF;
	}


	bool CSphIndex::BuildDocList(SphAttr_t** ppDocList, int64_t* pCount, CSphString*) const
	{
		assert(*ppDocList && pCount);
		*ppDocList = NULL;
		*pCount = 0;
		return true;
	}

	void CSphIndex::GetFieldFilterSettings(CSphFieldFilterSettings& tSettings)
	{
		if (m_pFieldFilter)
			m_pFieldFilter->GetSettings(tSettings);
	}

	/////////////////////////////////////////////////////////////////////////////


	bool CSphTokenizerIndex::GetKeywords(CSphVector <CSphKeywordInfo>& dKeywords, const char* szQuery, const GetKeywordsSettings_t& tSettings, CSphString*) const
	{
		// short-cut if no query or keywords to fill
		if (!szQuery || !szQuery[0])
			return true;

		CSphScopedPtr<ISphTokenizer> pTokenizer(m_pTokenizer->Clone(SPH_CLONE_INDEX)); // avoid race
		pTokenizer->EnableTokenizedMultiformTracking();

		// need to support '*' and '=' but not the other specials
		// so m_pQueryTokenizer does not work for us, gotta clone and setup one manually
		if (IsStarDict())
			pTokenizer->AddPlainChar('*');
		if (m_tSettings.m_bIndexExactWords)
			pTokenizer->AddPlainChar('=');

		CSphScopedPtr<CSphDict> tDictCloned(NULL);
		CSphDict* pDictBase = m_pDict;
		if (pDictBase->HasState())
			tDictCloned = pDictBase = pDictBase->Clone();

		CSphDict* pDict = pDictBase;
		if (IsStarDict())
			pDict = new CSphDictStar(pDictBase);

		if (m_tSettings.m_bIndexExactWords)
			pDict = new CSphDictExact(pDict);

		dKeywords.Resize(0);

		CSphVector<BYTE> dFiltered;
		CSphScopedPtr<ISphFieldFilter> pFieldFilter(NULL);
		const BYTE* sModifiedQuery = (const BYTE*)szQuery;
		if (m_pFieldFilter && szQuery)
		{
			pFieldFilter = m_pFieldFilter->Clone();
			if (pFieldFilter.Ptr() && pFieldFilter->Apply(sModifiedQuery, strlen((char*)sModifiedQuery), dFiltered, true))
				sModifiedQuery = dFiltered.Begin();
		}

		pTokenizer->SetBuffer(sModifiedQuery, strlen((const char*)sModifiedQuery));

		CSphTemplateQueryFilter tAotFilter;
		tAotFilter.m_pTokenizer = pTokenizer.Ptr();
		tAotFilter.m_pDict = pDict;
		tAotFilter.m_pSettings = &m_tSettings;
		tAotFilter.m_tFoldSettings = tSettings;
		tAotFilter.m_tFoldSettings.m_bStats = false;
		tAotFilter.m_tFoldSettings.m_bFoldWildcards = true;

		ExpansionContext_t tExpCtx;

		tAotFilter.GetKeywords(dKeywords, tExpCtx);

		return true;
	}

	//////////////////////////////////////////////////////////////////////////////


	CSphIndex* sphCreateIndexTemplate()
	{
		return new CSphTokenizerIndex();
	}


}