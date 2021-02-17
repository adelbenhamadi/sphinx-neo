#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/index/enums.h"
#include "neo/query/filter_settings.h"
#include "neo/core/doc_collector.h"
#include "neo/source/attrib_locator.h"
#include "neo/query/match_sorter.h"
#include "neo/core/kill_list_trait.h"
#include "neo/core/ranker.h"

namespace NEO {

	//fwd dec
	struct ISphFilter;
	class CSphQueryResultMeta;
	class CSphQueryResult;
	class CSphQuery;
	class CSphAttrOverride;
	class UservarIntSet_c;
	class CSphSchema;
	class CSphMatch;

	/// per-query search context
	/// everything that index needs to compute/create to process the query
	class CSphQueryContext : public ISphNoncopyable
	{
	public:
		// searching-only, per-query
		const CSphQuery& m_tQuery;

		int							m_iWeights;						///< search query field weights count
		int							m_dWeights[SPH_MAX_FIELDS];	///< search query field weights

		bool						m_bLookupFilter;				///< row data lookup required at filtering stage
		bool						m_bLookupSort;					///< row data lookup required at sorting stage

		DWORD						m_uPackedFactorFlags;			///< whether we need to calculate packed factors (and some extra options)

		ISphFilter* m_pFilter;
		ISphFilter* m_pWeightFilter;

		struct CalcItem_t
		{
			CSphAttrLocator			m_tLoc;					///< result locator
			ESphAttr				m_eType;				///< result type
			ISphExpr* m_pExpr;				///< evaluator (non-owned)
		};
		CSphVector<CalcItem_t>		m_dCalcFilter;			///< items to compute for filtering
		CSphVector<CalcItem_t>		m_dCalcSort;			///< items to compute for sorting/grouping
		CSphVector<CalcItem_t>		m_dCalcFinal;			///< items to compute when finalizing result set

		const CSphVector<CSphAttrOverride>* m_pOverrides;		///< overridden attribute values
		CSphVector<CSphAttrLocator>				m_dOverrideIn;
		CSphVector<CSphAttrLocator>				m_dOverrideOut;

		const void* m_pIndexData;			///< backend specific data
		CSphQueryProfile* m_pProfile;
		const SmallStringHash_T<int64_t>* m_pLocalDocs;
		int64_t									m_iTotalDocs;
		int64_t									m_iBadRows;

	public:
		explicit CSphQueryContext(const CSphQuery& q);
		~CSphQueryContext();

		void						BindWeights(const CSphQuery* pQuery, const CSphSchema& tSchema, CSphString& sWarning);
		bool						SetupCalc(CSphQueryResult* pResult, const ISphSchema& tInSchema, const CSphSchema& tSchema, const DWORD* pMvaPool, bool bArenaProhibit);
		bool						CreateFilters(bool bFullscan, const CSphVector<CSphFilterSettings>* pdFilters, const ISphSchema& tSchema, const DWORD* pMvaPool, const BYTE* pStrings, CSphString& sError, CSphString& sWarning, ESphCollation eCollation, bool bArenaProhibit, const KillListVector& dKillList);
		bool						SetupOverrides(const CSphQuery* pQuery, CSphQueryResult* pResult, const CSphSchema& tIndexSchema, const ISphSchema& tOutgoingSchema);

		void						CalcFilter(CSphMatch& tMatch) const;
		void						CalcSort(CSphMatch& tMatch) const;
		void						CalcFinal(CSphMatch& tMatch) const;

		void						FreeStrFilter(CSphMatch& tMatch) const;
		void						FreeStrSort(CSphMatch& tMatch) const;

		// note that RT index bind pools at segment searching, not at time it setups context
		void						ExprCommand(ESphExprCommand eCmd, void* pArg);
		void						SetStringPool(const BYTE* pStrings);
		void						SetMVAPool(const DWORD* pMva, bool bArenaProhibit);
		void						SetupExtraData(ISphRanker* pRanker, ISphMatchSorter* pSorter);

	private:
		CSphVector<const UservarIntSet_c*>		m_dUserVals;
	};



}