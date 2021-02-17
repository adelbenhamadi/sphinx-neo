#include "neo/query/filter_settings.h"
#include "neo/io/fnv64.h"
#include "neo/query/enums.h"

namespace NEO {


	CSphFilterSettings::CSphFilterSettings()
		: m_sAttrName("")
		, m_bExclude(false)
		, m_bHasEqual(true)
		, m_eMvaFunc(SPH_MVAFUNC_NONE)
		, m_iMinValue(LLONG_MIN)
		, m_iMaxValue(LLONG_MAX)
		, m_pValues(NULL)
		, m_nValues(0)
	{}


	void CSphFilterSettings::SetExternalValues(const SphAttr_t* pValues, int nValues)
	{
		m_pValues = pValues;
		m_nValues = nValues;
	}


	bool CSphFilterSettings::operator == (const CSphFilterSettings& rhs) const
	{
		// check name, mode, type
		if (m_sAttrName != rhs.m_sAttrName || m_bExclude != rhs.m_bExclude || m_eType != rhs.m_eType)
			return false;

		bool bSameStrings = false;
		switch (m_eType)
		{
		case SPH_FILTER_RANGE:
			return m_iMinValue == rhs.m_iMinValue && m_iMaxValue == rhs.m_iMaxValue;

		case SPH_FILTER_FLOATRANGE:
			return m_fMinValue == rhs.m_fMinValue && m_fMaxValue == rhs.m_fMaxValue;

		case SPH_FILTER_VALUES:
			if (m_dValues.GetLength() != rhs.m_dValues.GetLength())
				return false;

			ARRAY_FOREACH(i, m_dValues)
				if (m_dValues[i] != rhs.m_dValues[i])
					return false;

			return true;

		case SPH_FILTER_STRING:
		case SPH_FILTER_USERVAR:
		case SPH_FILTER_STRING_LIST:
			if (m_dStrings.GetLength() != rhs.m_dStrings.GetLength())
				return false;
			bSameStrings = ARRAY_ALL(bSameStrings, m_dStrings, m_dStrings[_all] == rhs.m_dStrings[_all]);
			return bSameStrings;

		default:
			assert(0 && "internal error: unhandled filter type in comparison");
			return false;
		}
	}


	uint64_t CSphFilterSettings::GetHash() const
	{
		uint64_t h = sphFNV64(&m_eType, sizeof(m_eType));
		h = sphFNV64(&m_bExclude, sizeof(m_bExclude), h);
		switch (m_eType)
		{
		case SPH_FILTER_VALUES:
		{
			int t = m_dValues.GetLength();
			h = sphFNV64(&t, sizeof(t), h);
			h = sphFNV64(m_dValues.Begin(), t * sizeof(SphAttr_t), h);
			break;
		}
		case SPH_FILTER_RANGE:
			h = sphFNV64(&m_iMaxValue, sizeof(m_iMaxValue), sphFNV64(&m_iMinValue, sizeof(m_iMinValue), h));
			break;
		case SPH_FILTER_FLOATRANGE:
			h = sphFNV64(&m_fMaxValue, sizeof(m_fMaxValue), sphFNV64(&m_fMinValue, sizeof(m_fMinValue), h));
			break;
		case SPH_FILTER_STRING:
		case SPH_FILTER_USERVAR:
		case SPH_FILTER_STRING_LIST:
			ARRAY_FOREACH(iString, m_dStrings)
				h = sphFNV64cont(m_dStrings[iString].cstr(), h);
			break;
		case SPH_FILTER_NULL:
			break;
		default:
			assert(0 && "internal error: unhandled filter type in GetHash()");
		}
		return h;
	}


}