#pragma once
#include "neo/int/types.h"
#include "neo/source/attrib_locator.h"
#include "neo/query/grouper.h"

namespace NEO {

	//fwd dec
	class ISphSchema;
	class CSphQuery;
	class CSphQueryProfile;
	struct ISphExprHook;
	class CSphFilterSettings;
	class CSphAttrUpdateEx;
	class CSphAttrUpdateEx;
	class ISphMatchSorter;

	struct SphQueueSettings_t : public ISphNoncopyable
	{
		const CSphQuery& m_tQuery;
		const ISphSchema& m_tSchema;
		CSphString& m_sError;
		CSphQueryProfile* m_pProfiler;
		bool						m_bComputeItems;
		CSphSchema* m_pExtra;
		CSphAttrUpdateEx* m_pUpdate;
		CSphVector<SphDocID_t>* m_pDeletes;
		bool						m_bZonespanlist;
		DWORD						m_uPackedFactorFlags;
		ISphExprHook* m_pHook;
		const CSphFilterSettings* m_pAggrFilter;

		SphQueueSettings_t(const CSphQuery& tQuery, const ISphSchema& tSchema, CSphString& sError, CSphQueryProfile* pProfiler)
			: m_tQuery(tQuery)
			, m_tSchema(tSchema)
			, m_sError(sError)
			, m_pProfiler(pProfiler)
			, m_bComputeItems(true)
			, m_pExtra(NULL)
			, m_pUpdate(NULL)
			, m_pDeletes(NULL)
			, m_bZonespanlist(false)
			, m_uPackedFactorFlags(SPH_FACTOR_DISABLE)
			, m_pHook(NULL)
			, m_pAggrFilter(NULL)
		{ }
	};



	/// creates proper queue for given query
	/// may return NULL on error; in this case, error message is placed in sError
	/// if the pUpdate is given, creates the updater's queue and perform the index update
	/// instead of searching
	ISphMatchSorter* sphCreateQueue(SphQueueSettings_t& tQueue);

	static CSphGrouper* sphCreateGrouperString(const CSphAttrLocator& tLoc, ESphCollation eCollation);
	static CSphGrouper* sphCreateGrouperMulti(const CSphVector<CSphAttrLocator>& dLocators, const CSphVector<ESphAttr>& dAttrTypes,
		const CSphVector<ISphExpr*>& dJsonKeys, ESphCollation eCollation);

	/////////////////////////

	struct SphStringSorterRemap_t
	{
		CSphAttrLocator m_tSrc;
		CSphAttrLocator m_tDst;
	};

	bool			sphSortGetStringRemap(const ISphSchema& tSorterSchema, const ISphSchema& tIndexSchema, CSphVector<SphStringSorterRemap_t>& dAttrs);


}