#pragma once
#include "neo/int/types.h"
#include "neo/query/enums.h"

#include <cassert>

namespace NEO {

	/// search query filter
	class CSphFilterSettings
	{
	public:
		CSphString			m_sAttrName;	///< filtered attribute name
		bool				m_bExclude;		///< whether this is "include" or "exclude" filter (default is "include")
		bool				m_bHasEqual;	///< has filter "equal" component (gte\lte) or pure greater\less

		ESphFilter			m_eType;		///< filter type
		ESphMvaFunc			m_eMvaFunc;		///< MVA folding function
		union
		{
			SphAttr_t		m_iMinValue;	///< range min
			float			m_fMinValue;	///< range min
		};
		union
		{
			SphAttr_t		m_iMaxValue;	///< range max
			float			m_fMaxValue;	///< range max
		};
		CSphVector<SphAttr_t>	m_dValues;	///< integer values set
		CSphVector<CSphString>	m_dStrings;	///< string values

	public:
		CSphFilterSettings();

		void				SetExternalValues(const SphAttr_t* pValues, int nValues);

		SphAttr_t			GetValue(int iIdx) const { assert(iIdx < GetNumValues()); return m_pValues ? m_pValues[iIdx] : m_dValues[iIdx]; }
		const SphAttr_t* GetValueArray() const { return m_pValues ? m_pValues : &(m_dValues[0]); }
		int					GetNumValues() const { return m_pValues ? m_nValues :(int) m_dValues.GetLength(); }

		bool				operator == (const CSphFilterSettings& rhs) const;
		bool				operator != (const CSphFilterSettings& rhs) const { return !((*this) == rhs); }

		uint64_t			GetHash() const;

	protected:
		const SphAttr_t* m_pValues;		///< external value array
		int					m_nValues;		///< external array size
	};


}