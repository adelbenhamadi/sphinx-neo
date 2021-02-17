#pragma once
#include "neo/int/types.h"
#include "neo/query/grouper.h"

namespace NEO {

	/// (group,attrvalue) pair
	struct SphGroupedValue_t
	{
	public:
		SphGroupKey_t	m_uGroup;
		SphAttr_t		m_uValue;
		int				m_iCount;

	public:
		SphGroupedValue_t()
		{}

		SphGroupedValue_t(SphGroupKey_t uGroup, SphAttr_t uValue, int iCount)
			: m_uGroup(uGroup)
			, m_uValue(uValue)
			, m_iCount(iCount)
		{}

		inline bool operator < (const SphGroupedValue_t& rhs) const
		{
			if (m_uGroup != rhs.m_uGroup) return m_uGroup < rhs.m_uGroup;
			if (m_uValue != rhs.m_uValue) return m_uValue < rhs.m_uValue;
			return m_iCount > rhs.m_iCount;
		}

		inline bool operator == (const SphGroupedValue_t& rhs) const
		{
			return m_uGroup == rhs.m_uGroup && m_uValue == rhs.m_uValue;
		}
	};

	/// same as SphGroupedValue_t but without group
	struct SphUngroupedValue_t
	{
	public:
		SphAttr_t		m_uValue;
		int				m_iCount;

	public:
		SphUngroupedValue_t()
		{}

		SphUngroupedValue_t(SphAttr_t uValue, int iCount)
			: m_uValue(uValue)
			, m_iCount(iCount)
		{}

		inline bool operator < (const SphUngroupedValue_t& rhs) const
		{
			if (m_uValue != rhs.m_uValue) return m_uValue < rhs.m_uValue;
			return m_iCount > rhs.m_iCount;
		}

		inline bool operator == (const SphUngroupedValue_t& rhs) const
		{
			return m_uValue == rhs.m_uValue;
		}
	};


	/// unique values counter
	/// used for COUNT(DISTINCT xxx) GROUP BY yyy queries
	class CSphUniqounter : public CSphVector<SphGroupedValue_t>
	{
	public:
#ifndef NDEBUG
		CSphUniqounter() : m_iCountPos(0), m_bSorted(true) { Reserve(16384); }
		void			Add(const SphGroupedValue_t& tValue) { CSphVector<SphGroupedValue_t>::Add(tValue); m_bSorted = false; }
		void			Sort() { CSphVector<SphGroupedValue_t>::Sort(); m_bSorted = true; }

#else
		CSphUniqounter() : m_iCountPos(0) {}
#endif

	public:
		int				CountStart(SphGroupKey_t* pOutGroup);	///< starting counting distinct values, returns count and group key (0 if empty)
		int				CountNext(SphGroupKey_t* pOutGroup);	///< continues counting distinct values, returns count and group key (0 if done)
		void			Compact(SphGroupKey_t* pRemoveGroups, int iRemoveGroups);

	protected:
		int				m_iCountPos;

#ifndef NDEBUG
		bool			m_bSorted;
#endif
	};



}