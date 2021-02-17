#pragma once
#include "neo/index/enums.h"
#include "neo/query/grouper.h"


namespace NEO {

	//fwd dec
	class CSphAttrLocator;
	struct ISphFilter;


	/// additional group-by sorter settings
	struct CSphGroupSorterSettings
	{
		CSphAttrLocator		m_tLocGroupby;		///< locator for @groupby
		CSphAttrLocator		m_tLocCount;		///< locator for @count
		CSphAttrLocator		m_tLocDistinct;		///< locator for @distinct
		CSphAttrLocator		m_tDistinctLoc;		///< locator for attribute to compute count(distinct) for
		ESphAttr			m_eDistinctAttr;	///< type of attribute to compute count(distinct) for
		bool				m_bDistinct;		///< whether we need distinct
		bool				m_bMVA;				///< whether we're grouping by MVA attribute
		bool				m_bMva64;
		CSphGrouper* m_pGrouper;			///< group key calculator
		bool				m_bImplicit;		///< for queries with aggregate functions but without group by clause
		const ISphFilter* m_pAggrFilterTrait; ///< aggregate filter that got owned by grouper
		bool				m_bJson;			///< whether we're grouping by Json attribute
		CSphAttrLocator		m_tLocGroupbyStr;	///< locator for @groupbystr

		CSphGroupSorterSettings()
			: m_eDistinctAttr(ESphAttr::SPH_ATTR_NONE)
			, m_bDistinct(false)
			, m_bMVA(false)
			, m_bMva64(false)
			, m_pGrouper(NULL)
			, m_bImplicit(false)
			, m_pAggrFilterTrait(NULL)
			, m_bJson(false)
		{}
	};


}