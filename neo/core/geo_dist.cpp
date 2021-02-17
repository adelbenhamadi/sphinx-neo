#include "neo/core/geo_dist.h"
#include "neo/int/types.h"
#include "neo/query/query.h"
#include "neo/io/fnv64.h"
#include "neo/source/schema.h"
#include "neo/core/match.h"
#include "neo/source/attrib_locator.h"

#include "neo/sphinxint.h"
#include "neo/sphinxexpr.h"

namespace NEO {

	bool ExprGeodist_t::Setup(const CSphQuery* pQuery, const ISphSchema& tSchema, CSphString& sError)
	{
		if (!pQuery->m_bGeoAnchor)
		{
			sError.SetSprintf("INTERNAL ERROR: no geoanchor, can not create geodist evaluator");
			return false;
		}

		int iLat = tSchema.GetAttrIndex(pQuery->m_sGeoLatAttr.cstr());
		if (iLat < 0)
		{
			sError.SetSprintf("unknown latitude attribute '%s'", pQuery->m_sGeoLatAttr.cstr());
			return false;
		}

		int iLong = tSchema.GetAttrIndex(pQuery->m_sGeoLongAttr.cstr());
		if (iLong < 0)
		{
			sError.SetSprintf("unknown latitude attribute '%s'", pQuery->m_sGeoLongAttr.cstr());
			return false;
		}

		m_tGeoLatLoc = tSchema.GetAttr(iLat).m_tLocator;
		m_tGeoLongLoc = tSchema.GetAttr(iLong).m_tLocator;
		m_fGeoAnchorLat = pQuery->m_fGeoLatitude;
		m_fGeoAnchorLong = pQuery->m_fGeoLongitude;
		m_iLat = iLat;
		m_iLon = iLong;
		return true;
	}


	static inline double sphSqr(double v)
	{
		return v * v;
	}


	float ExprGeodist_t::Eval(const CSphMatch& tMatch) const
	{
		const double R = 6384000;
		float plat = tMatch.GetAttrFloat(m_tGeoLatLoc);
		float plon = tMatch.GetAttrFloat(m_tGeoLongLoc);
		double dlat = plat - m_fGeoAnchorLat;
		double dlon = plon - m_fGeoAnchorLong;
		double a = sphSqr(sin(dlat / 2)) + cos(plat) * cos(m_fGeoAnchorLat) * sphSqr(sin(dlon / 2));
		double c = 2 * asin(Min(1.0, sqrt(a)));
		return (float)(R * c);
	}

	void ExprGeodist_t::Command(ESphExprCommand eCmd, void* pArg)
	{
		if (eCmd == SPH_EXPR_GET_DEPENDENT_COLS)
		{
			static_cast <CSphVector<int>*>(pArg)->Add(m_iLat);
			static_cast <CSphVector<int>*>(pArg)->Add(m_iLon);
		}
	}

	uint64_t ExprGeodist_t::GetHash(const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable)
	{
		uint64_t uHash = sphCalcExprDepHash(this, tSorterSchema, uPrevHash, bDisable);

		static const char* EXPR_TAG = "ExprGeodist_t";
		uHash = sphFNV64(EXPR_TAG, strlen(EXPR_TAG), uHash);
		uHash = sphFNV64(&m_fGeoAnchorLat, sizeof(m_fGeoAnchorLat), uHash);
		uHash = sphFNV64(&m_fGeoAnchorLong, sizeof(m_fGeoAnchorLong), uHash);

		return uHash;
	}

}