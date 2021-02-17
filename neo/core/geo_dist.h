#pragma once
#include "neo/source/attrib_locator.h"
#include "neo/int/types.h"
#include "neo/index/enums.h"

#include "neo/sphinxexpr.h"

namespace NEO {

	//fwd dec
	class CSphQuery;
	class ISphSchema;
	class CSphMatch;

	struct ExprGeodist_t : public ISphExpr
	{
	public:
		ExprGeodist_t() {}
		bool				Setup(const CSphQuery* pQuery, const ISphSchema& tSchema, CSphString& sError);
		virtual float		Eval(const CSphMatch& tMatch) const;
		virtual void		Command(ESphExprCommand eCmd, void* pArg);
		virtual uint64_t	GetHash(const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable);

	protected:
		CSphAttrLocator		m_tGeoLatLoc;
		CSphAttrLocator		m_tGeoLongLoc;
		float				m_fGeoAnchorLat;
		float				m_fGeoAnchorLong;
		int					m_iLat;
		int					m_iLon;
	};

}