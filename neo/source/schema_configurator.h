#pragma once
#include "neo/int/types.h"

namespace NEO {

	class CSphSchema;

	template < typename T >
	struct CSphSchemaConfigurator
	{
		bool ConfigureAttrs(const CSphVariant* pHead, ESphAttr eAttrType, CSphSchema& tSchema, CSphString& sError) const
		{
			for (const CSphVariant* pCur = pHead; pCur; pCur = pCur->m_pNext)
			{
				CSphColumnInfo tCol(pCur->strval().cstr(), eAttrType);
				char* pColon = strchr(const_cast<char*> (tCol.m_sName.cstr()), ':');
				if (pColon)
				{
					*pColon = '\0';

					if (eAttrType == ESphAttr::SPH_ATTR_INTEGER)
					{
						int iBits = strtol(pColon + 1, NULL, 10);
						if (iBits <= 0 || iBits > ROWITEM_BITS)
						{
							sphWarn("%s", ((T*)this)->DecorateMessage("attribute '%s': invalid bitcount=%d (bitcount ignored)", tCol.m_sName.cstr(), iBits));
							iBits = -1;
						}

						tCol.m_tLocator.m_iBitCount = iBits;
					}
					else
					{
						sphWarn("%s", ((T*)this)->DecorateMessage("attribute '%s': bitcount is only supported for integer types", tCol.m_sName.cstr()));
					}
				}

				tCol.m_iIndex = tSchema.GetAttrsCount();

				if (eAttrType == ESphAttr::SPH_ATTR_UINT32SET || eAttrType == ESphAttr::SPH_ATTR_INT64SET)
				{
					tCol.m_eAttrType = eAttrType;
					tCol.m_eSrc = SPH_ATTRSRC_FIELD;
				}

				if (CSphSchema::IsReserved(tCol.m_sName.cstr()))
				{
					sError.SetSprintf("%s is not a valid attribute name", tCol.m_sName.cstr());
					return false;
				}

				tSchema.AddAttr(tCol, true); // all attributes are dynamic at indexing time
			}

			return true;
		}

		void ConfigureFields(const CSphVariant* pHead, bool bWordDict, CSphSchema& tSchema) const
		{
			for (const CSphVariant* pCur = pHead; pCur; pCur = pCur->m_pNext)
			{
				const char* sFieldName = pCur->strval().cstr();

				bool bFound = false;
				for (int i = 0; i < tSchema.m_dFields.GetLength() && !bFound; i++)
					bFound = (tSchema.m_dFields[i].m_sName == sFieldName);

				if (bFound)
					sphWarn("%s", ((T*)this)->DecorateMessage("duplicate field '%s'", sFieldName));
				else
					AddFieldToSchema(sFieldName, bWordDict, tSchema);
			}
		}

		void AddFieldToSchema(const char* sFieldName, bool bWordDict, CSphSchema& tSchema) const
		{
			CSphColumnInfo tCol(sFieldName);
			tCol.m_eWordpart = ((T*)this)->GetWordpart(tCol.m_sName.cstr(), bWordDict);
			tSchema.m_dFields.Add(tCol);
		}
	};


}