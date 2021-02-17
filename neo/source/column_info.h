#pragma once
#include "neo/int/types.h"
#include "neo/int/ref_counted.h"
#include "neo/index/enums.h"
#include "neo/source/enums.h"
#include "neo/utility/inline_misc.h"
#include "neo/source/attrib_locator.h"

#include "neo/sphinxexpr.h"

namespace NEO {

	/// source column info
	struct CSphColumnInfo
	{
		CSphString		m_sName;		///< column name
		ESphAttr		m_eAttrType;	///< attribute type
		ESphWordpart	m_eWordpart;	///< wordpart processing type
		bool			m_bIndexed;		///< whether to index this column as fulltext field too

		int				m_iIndex;		///< index into source result set (-1 for joined fields)
		CSphAttrLocator	m_tLocator;		///< attribute locator in the row

		ESphAttrSrc		m_eSrc;			///< attr source (for multi-valued attrs only)
		CSphString		m_sQuery;		///< query to retrieve values (for multi-valued attrs only)
		CSphString		m_sQueryRange;	///< query to retrieve range (for multi-valued attrs only)

		CSphRefcountedPtr<ISphExpr>		m_pExpr;		///< evaluator for expression items
		ESphAggrFunc					m_eAggrFunc;	///< aggregate function on top of expression (for GROUP BY)
		ESphEvalStage					m_eStage;		///< column evaluation stage (who and how computes this column)
		bool							m_bPayload;
		bool							m_bFilename;	///< column is a file name
		bool							m_bWeight;		///< is a weight column

		WORD							m_uNext;		///< next in linked list for hash in CSphSchema

		/// handy ctor
		CSphColumnInfo(const char* sName = NULL, ESphAttr eType = ESphAttr::SPH_ATTR_NONE);

		/// equality comparison checks name, type, and locator
		bool operator == (const CSphColumnInfo& rhs) const
		{
			return m_sName == rhs.m_sName
				&& m_eAttrType == rhs.m_eAttrType
				&& m_tLocator.m_iBitCount == rhs.m_tLocator.m_iBitCount
				&& m_tLocator.m_iBitOffset == rhs.m_tLocator.m_iBitOffset
				&& m_tLocator.m_bDynamic == rhs.m_tLocator.m_bDynamic;
		}
	};


	static CSphString sphDumpAttr(const CSphColumnInfo& tAttr)
	{
		CSphString sRes;
		sRes.SetSprintf("%s %s:%d@%d", sphTypeName(tAttr.m_eAttrType), tAttr.m_sName.cstr(),
			tAttr.m_tLocator.m_iBitCount, tAttr.m_tLocator.m_iBitOffset);
		return sRes;
	}


	/// make string lowercase but keep case of JSON.field
	void sphColumnToLowercase(char* sVal);


}