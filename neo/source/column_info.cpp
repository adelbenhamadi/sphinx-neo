#include "neo/source/column_info.h"

namespace NEO {

	CSphColumnInfo::CSphColumnInfo(const char* sName, ESphAttr eType)
		: m_sName(sName)
		, m_eAttrType(eType)
		, m_eWordpart(SPH_WORDPART_WHOLE)
		, m_bIndexed(false)
		, m_iIndex(-1)
		, m_eSrc(SPH_ATTRSRC_NONE)
		, m_pExpr(NULL)
		, m_eAggrFunc(SPH_AGGR_NONE)
		, m_eStage(SPH_EVAL_STATIC)
		, m_bPayload(false)
		, m_bFilename(false)
		, m_bWeight(false)
		, m_uNext(0xffff)
	{
		sphColumnToLowercase(const_cast<char*>(m_sName.cstr()));
	}


	void sphColumnToLowercase(char* sVal)
	{
		if (!sVal || !*sVal)
			return;

		// make all chars lowercase but only prior to '.', ',', and '[' delimiters
		// leave quoted values unchanged
		for (bool bQuoted = false; *sVal && *sVal != '.' && *sVal != ',' && *sVal != '['; sVal++)
		{
			if (!bQuoted)
				*sVal = (char)tolower(*sVal);
			if (*sVal == '\'')
				bQuoted = !bQuoted;
		}
	}


}