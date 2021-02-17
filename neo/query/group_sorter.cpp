#include "neo/query/group_sorter.h"
#include "neo/int/types.h"
#include "neo/core/match.h"

#include <cassert>


namespace NEO {

	bool sphIsSortStringInternal(const char* sColumnName)
	{
		assert(sColumnName);
		return (strncmp(sColumnName, g_sIntAttrPrefix, sizeof(g_sIntAttrPrefix) - 1) == 0);
	}

	template<>
	DWORD IAggrFuncTraits<DWORD>::GetValue(const CSphMatch* pRow)
	{
		return (DWORD)pRow->GetAttr(m_tLocator);
	}

	template<>
	void IAggrFuncTraits<DWORD>::SetValue(CSphMatch* pRow, DWORD val)
	{
		pRow->SetAttr(m_tLocator, val);
	}

	template<>
	int64_t IAggrFuncTraits<int64_t>::GetValue(const CSphMatch* pRow)
	{
		return pRow->GetAttr(m_tLocator);
	}

	template<>
	void IAggrFuncTraits<int64_t>::SetValue(CSphMatch* pRow, int64_t val)
	{
		pRow->SetAttr(m_tLocator, val);
	}

	template<>
	float IAggrFuncTraits<float>::GetValue(const CSphMatch* pRow)
	{
		return pRow->GetAttrFloat(m_tLocator);
	}

	template<>
	void IAggrFuncTraits<float>::SetValue(CSphMatch* pRow, float val)
	{
		pRow->SetAttrFloat(m_tLocator, val);
	}

}