#include "neo/int/types.h"
#include "neo/index/enums.h"
#include "neo/source/column_info.h"
#include "neo/utility/hash.h"
#include "neo/io/crc32.h"
#include "neo/utility/inline_misc.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/source/schema.h"

namespace NEO {

	void ISphSchema::Reset()
	{
		m_dPtrAttrs.Reset();
		m_dFactorAttrs.Reset();
	}


	void ISphSchema::InsertAttr(CSphVector<CSphColumnInfo>& dAttrs, CSphVector<int>& dUsed, int iPos, const CSphColumnInfo& tCol, bool bDynamic)
	{
		assert(0 <= iPos && iPos <= dAttrs.GetLength());
		assert(tCol.m_eAttrType != ESphAttr::SPH_ATTR_NONE && !tCol.m_tLocator.IsID()); // not via this orifice bro
		if (tCol.m_eAttrType == ESphAttr::SPH_ATTR_NONE || tCol.m_tLocator.IsID())
			return;

		dAttrs.Insert(iPos, tCol);
		CSphAttrLocator& tLoc = dAttrs[iPos].m_tLocator;

		int iBits = ROWITEM_BITS;
		if (tLoc.m_iBitCount > 0)
			iBits = tLoc.m_iBitCount;
		if (tCol.m_eAttrType == ESphAttr::SPH_ATTR_BOOL)
			iBits = 1;
		if (tCol.m_eAttrType == ESphAttr::SPH_ATTR_BIGINT || tCol.m_eAttrType == ESphAttr::SPH_ATTR_JSON_FIELD)
			iBits = 64;

		if (tCol.m_eAttrType == ESphAttr::SPH_ATTR_STRINGPTR || tCol.m_eAttrType == ESphAttr::SPH_ATTR_FACTORS || tCol.m_eAttrType == ESphAttr::SPH_ATTR_FACTORS_JSON)
		{
			assert(bDynamic);
			iBits = ROWITEMPTR_BITS;
			CSphNamedInt& t = (tCol.m_eAttrType == ESphAttr::SPH_ATTR_STRINGPTR)
				? m_dPtrAttrs.Add()
				: m_dFactorAttrs.Add();
			t.m_iValue = dUsed.GetLength();
			t.m_sName = tCol.m_sName;
		}

		tLoc.m_iBitCount = iBits;
		tLoc.m_bDynamic = bDynamic;

		if (iBits >= ROWITEM_BITS)
		{
			tLoc.m_iBitOffset = dUsed.GetLength() * ROWITEM_BITS;
			int iItems = (iBits + ROWITEM_BITS - 1) / ROWITEM_BITS;
			for (int i = 0; i < iItems; i++)
				dUsed.Add(ROWITEM_BITS);
		}
		else
		{
			int iItem;
			for (iItem = 0; iItem < dUsed.GetLength(); iItem++)
				if (dUsed[iItem] + iBits <= ROWITEM_BITS)
					break;
			if (iItem == dUsed.GetLength())
				dUsed.Add(0);
			tLoc.m_iBitOffset = iItem * ROWITEM_BITS + dUsed[iItem];
			dUsed[iItem] += iBits;
		}
	}


	void ISphSchema::CloneWholeMatch(CSphMatch* pDst, const CSphMatch& rhs) const
	{
		assert(pDst);
		FreeStringPtrs(pDst);
		pDst->Combine(rhs, GetRowSize());
		CopyPtrs(pDst, rhs);
	}


	void ISphSchema::CopyPtrs(CSphMatch* pDst, const CSphMatch& rhs) const
	{
		ARRAY_FOREACH(i, m_dPtrAttrs)
			* (const char**)(pDst->m_pDynamic + m_dPtrAttrs[i].m_iValue) = CSphString(*(const char**)(rhs.m_pDynamic + m_dPtrAttrs[i].m_iValue)).Leak();

		// not immediately obvious: this is not needed while pushing matches to sorters; factors are held in an outer hash table
		// but it is necessary to copy factors when combining results from several indexes via a sorter because at this moment matches are the owners of factor data
		ARRAY_FOREACH(i, m_dFactorAttrs)
		{
			int iOffset = m_dFactorAttrs[i].m_iValue;
			BYTE* pData = *(BYTE**)(rhs.m_pDynamic + iOffset);
			if (pData)
			{
				DWORD uDataSize = *(DWORD*)pData;
				assert(uDataSize);

				BYTE* pCopy = new BYTE[uDataSize];
				memcpy(pCopy, pData, uDataSize);
				*(BYTE**)(pDst->m_pDynamic + iOffset) = pCopy;
			}
		}
	}


	void ISphSchema::FreeStringPtrs(CSphMatch* pMatch) const
	{
		assert(pMatch);
		if (!pMatch->m_pDynamic)
			return;

		if (m_dPtrAttrs.GetLength())
		{
			CSphString sStr;
			ARRAY_FOREACH(i, m_dPtrAttrs)
			{
				sStr.Adopt((char**)(pMatch->m_pDynamic + m_dPtrAttrs[i].m_iValue));
			}
		}

		ARRAY_FOREACH(i, m_dFactorAttrs)
		{
			int iOffset = m_dFactorAttrs[i].m_iValue;
			BYTE* pData = *(BYTE**)(pMatch->m_pDynamic + iOffset);
			if (pData)
			{
				delete[] pData;
				*(BYTE**)(pMatch->m_pDynamic + iOffset) = NULL;
			}
		}
	}

	//////////////////////////////////////////////////////////////////////////

	CSphSchema::CSphSchema(const char* sName)
		: m_sName(sName)
		, m_iStaticSize(0)
		, m_iFirstFieldLenAttr(-1)
		, m_iLastFieldLenAttr(-1)
	{
		for (int i = 0; i < BUCKET_COUNT; i++)
			m_dBuckets[i] = 0xffff;
	}


	bool CSphSchema::CompareTo(const CSphSchema& rhs, CSphString& sError, bool bFullComparison) const
	{
		// check attr count
		if (GetAttrsCount() != rhs.GetAttrsCount())
		{
			sError.SetSprintf("attribute count mismatch (me=%s, in=%s, myattrs=%d, inattrs=%d)",
				m_sName.cstr(), rhs.m_sName.cstr(),
				GetAttrsCount(), rhs.GetAttrsCount());
			return false;
		}

		// check attrs
		ARRAY_FOREACH(i, m_dAttrs)
		{
			const CSphColumnInfo& tAttr1 = rhs.m_dAttrs[i];
			const CSphColumnInfo& tAttr2 = m_dAttrs[i];

			bool bMismatch;
			if (bFullComparison)
				bMismatch = !(tAttr1 == tAttr2);
			else
			{
				ESphAttr eAttr1 = tAttr1.m_eAttrType;
				ESphAttr eAttr2 = tAttr2.m_eAttrType;

				bMismatch = tAttr1.m_sName != tAttr2.m_sName || eAttr1 != eAttr2 || tAttr1.m_eWordpart != tAttr2.m_eWordpart ||
					tAttr1.m_tLocator.m_iBitCount != tAttr2.m_tLocator.m_iBitCount ||
					tAttr1.m_tLocator.m_iBitOffset != tAttr2.m_tLocator.m_iBitOffset;
			}

			if (bMismatch)
			{
				sError.SetSprintf("attribute mismatch (me=%s, in=%s, idx=%d, myattr=%s, inattr=%s)",
					m_sName.cstr(), rhs.m_sName.cstr(), i, sphDumpAttr(m_dAttrs[i]).cstr(), sphDumpAttr(rhs.m_dAttrs[i]).cstr());
				return false;
			}
		}

		// check field count
		if (rhs.m_dFields.GetLength() != m_dFields.GetLength())
		{
			sError.SetSprintf("fulltext fields count mismatch (me=%s, in=%s, myfields=%d, infields=%d)",
				m_sName.cstr(), rhs.m_sName.cstr(),
				m_dFields.GetLength(), rhs.m_dFields.GetLength());
			return false;
		}

		// check fulltext field names
		ARRAY_FOREACH(i, rhs.m_dFields)
			if (rhs.m_dFields[i].m_sName != m_dFields[i].m_sName)
			{
				sError.SetSprintf("fulltext field mismatch (me=%s, myfield=%s, idx=%d, in=%s, infield=%s)",
					m_sName.cstr(), rhs.m_sName.cstr(),
					i, m_dFields[i].m_sName.cstr(), rhs.m_dFields[i].m_sName.cstr());
				return false;
			}

		return true;
	}


	int CSphSchema::GetFieldIndex(const char* sName) const
	{
		if (!sName)
			return -1;
		ARRAY_FOREACH(i, m_dFields)
			if (strcasecmp(m_dFields[i].m_sName.cstr(), sName) == 0)
				return i;
		return -1;
	}


	int CSphSchema::GetAttrIndex(const char* sName) const
	{
		if (!sName)
			return -1;

		if (m_dAttrs.GetLength() >= HASH_THRESH)
		{
			DWORD uCrc = sphCRC32(sName);
			DWORD uPos = m_dBuckets[uCrc % BUCKET_COUNT];
			while (uPos != 0xffff && m_dAttrs[uPos].m_sName != sName)
				uPos = m_dAttrs[uPos].m_uNext;

			return (short)uPos; // 0xffff == -1 is our "end of list" marker
		}

		ARRAY_FOREACH(i, m_dAttrs)
			if (m_dAttrs[i].m_sName == sName)
				return i;

		return -1;
	}


	const CSphColumnInfo* CSphSchema::GetAttr(const char* sName) const
	{
		int iIndex = GetAttrIndex(sName);
		if (iIndex >= 0)
			return &m_dAttrs[iIndex];
		return NULL;
	}


	void CSphSchema::Reset()
	{
		ISphSchema::Reset();
		m_dFields.Reset();
		m_dAttrs.Reset();
		for (int i = 0; i < BUCKET_COUNT; i++)
			m_dBuckets[i] = 0xffff;
		m_dStaticUsed.Reset();
		m_dDynamicUsed.Reset();
		m_iStaticSize = 0;
	}


	void CSphSchema::InsertAttr(int iPos, const CSphColumnInfo& tCol, bool bDynamic)
	{
		// it's redundant in case of AddAttr
		if (iPos != m_dAttrs.GetLength())
			UpdateHash(iPos - 1, 1);

		ISphSchema::InsertAttr(m_dAttrs, bDynamic ? m_dDynamicUsed : m_dStaticUsed, iPos, tCol, bDynamic);

		// update static size
		m_iStaticSize = m_dStaticUsed.GetLength();

		// update field length locators
		if (tCol.m_eAttrType == ESphAttr::SPH_ATTR_TOKENCOUNT)
		{
			m_iFirstFieldLenAttr = m_iFirstFieldLenAttr == -1 ? iPos : Min(m_iFirstFieldLenAttr, iPos);
			m_iLastFieldLenAttr = Max(m_iLastFieldLenAttr, iPos);
		}

		// do hash add
		if (m_dAttrs.GetLength() == HASH_THRESH)
			RebuildHash();
		else if (m_dAttrs.GetLength() > HASH_THRESH)
		{
			WORD& uPos = GetBucketPos(m_dAttrs[iPos].m_sName.cstr());
			m_dAttrs[iPos].m_uNext = uPos;
			uPos = (WORD)iPos;
		}
	}


	void CSphSchema::RemoveAttr(const char* szAttr, bool bDynamic)
	{
		int iIndex = GetAttrIndex(szAttr);
		if (iIndex < 0)
			return;

		CSphVector<CSphColumnInfo> dBackup = m_dAttrs;

		if (bDynamic)
			m_dDynamicUsed.Reset();
		else
		{
			m_dStaticUsed.Reset();
			m_iStaticSize = 0;
		}

		ISphSchema::Reset();
		m_dAttrs.Reset();
		m_iFirstFieldLenAttr = -1;
		m_iLastFieldLenAttr = -1;

		ARRAY_FOREACH(i, dBackup)
			if (i != iIndex)
				AddAttr(dBackup[i], bDynamic);
	}


	void CSphSchema::AddAttr(const CSphColumnInfo& tCol, bool bDynamic)
	{
		InsertAttr(m_dAttrs.GetLength(), tCol, bDynamic);
	}


	int CSphSchema::GetAttrId_FirstFieldLen() const
	{
		return m_iFirstFieldLenAttr;
	}


	int CSphSchema::GetAttrId_LastFieldLen() const
	{
		return m_iLastFieldLenAttr;
	}


	bool CSphSchema::IsReserved(const char* szToken)
	{
		static const char* dReserved[] =
		{
			"AND", "AS", "BY", "DIV", "FACET", "FALSE", "FROM", "ID", "IN", "IS", "LIMIT",
			"MOD", "NOT", "NULL", "OR", "ORDER", "SELECT", "TRUE", NULL
		};

		const char** p = dReserved;
		while (*p)
			if (strcasecmp(szToken, *p++) == 0)
				return true;
		return false;
	}


	WORD& CSphSchema::GetBucketPos(const char* sName)
	{
		DWORD uCrc = sphCRC32(sName);
		return m_dBuckets[uCrc % BUCKET_COUNT];
	}


	void CSphSchema::RebuildHash()
	{
		if (m_dAttrs.GetLength() < HASH_THRESH)
			return;

		for (int i = 0; i < BUCKET_COUNT; i++)
			m_dBuckets[i] = 0xffff;

		ARRAY_FOREACH(i, m_dAttrs)
		{
			WORD& uPos = GetBucketPos(m_dAttrs[i].m_sName.cstr());
			m_dAttrs[i].m_uNext = uPos;
			uPos = WORD(i);
		}
	}


	void CSphSchema::UpdateHash(int iStartIndex, int iAddVal)
	{
		if (m_dAttrs.GetLength() < HASH_THRESH)
			return;

		ARRAY_FOREACH(i, m_dAttrs)
		{
			WORD& uPos = m_dAttrs[i].m_uNext;
			if (uPos != 0xffff && uPos > iStartIndex)
				uPos = (WORD)(uPos + iAddVal);
		}
		for (int i = 0; i < BUCKET_COUNT; i++)
		{
			WORD& uPos = m_dBuckets[i];
			if (uPos != 0xffff && uPos > iStartIndex)
				uPos = (WORD)(uPos + iAddVal);
		}
	}


	void CSphSchema::AssignTo(CSphRsetSchema& lhs) const
	{
		lhs = *this;
	}

	//////////////////////////////////////////////////////////////////////////

	CSphRsetSchema::CSphRsetSchema()
		: m_pIndexSchema(NULL)
	{}


	void CSphRsetSchema::Reset()
	{
		ISphSchema::Reset();
		m_pIndexSchema = NULL;
		m_dExtraAttrs.Reset();
		m_dDynamicUsed.Reset();
		m_dFields.Reset();
	}


	void CSphRsetSchema::AddDynamicAttr(const CSphColumnInfo& tCol)
	{
		ISphSchema::InsertAttr(m_dExtraAttrs, m_dDynamicUsed, m_dExtraAttrs.GetLength(), tCol, true);
	}


	int CSphRsetSchema::GetRowSize() const
	{
		// we copy over dynamic map in case index schema has dynamic attributes
		// (that happens in case of inline attributes, or RAM segments in RT indexes)
		// so there is no need to add GetDynamicSize() here
		return m_pIndexSchema
			? m_dDynamicUsed.GetLength() + m_pIndexSchema->GetStaticSize()
			: m_dDynamicUsed.GetLength();
	}


	int CSphRsetSchema::GetStaticSize() const
	{
		// result set schemas additions are always dynamic
		return m_pIndexSchema ? m_pIndexSchema->GetStaticSize() : 0;
	}


	int CSphRsetSchema::GetDynamicSize() const
	{
		// we copy over dynamic map in case index schema has dynamic attributes
		return m_dDynamicUsed.GetLength();
	}


	int CSphRsetSchema::GetAttrsCount() const
	{
		return m_pIndexSchema
			? m_dExtraAttrs.GetLength() + m_pIndexSchema->GetAttrsCount() - m_dRemoved.GetLength()
			: m_dExtraAttrs.GetLength();
	}


	int CSphRsetSchema::GetAttrIndex(const char* sName) const
	{
		ARRAY_FOREACH(i, m_dExtraAttrs)
			if (m_dExtraAttrs[i].m_sName == sName)
				return i + (m_pIndexSchema ? m_pIndexSchema->GetAttrsCount() - m_dRemoved.GetLength() : 0);

		if (!m_pIndexSchema)
			return -1;

		int iRes = m_pIndexSchema->GetAttrIndex(sName);
		if (iRes >= 0)
		{
			if (m_dRemoved.Contains(iRes))
				return -1;
			int iSub = 0;
			ARRAY_FOREACH_COND(i, m_dRemoved, iRes >= m_dRemoved[i])
				iSub++;
			return iRes - iSub;
		}
		return -1;
	}


	const CSphColumnInfo& CSphRsetSchema::GetAttr(int iIndex) const
	{
		if (!m_pIndexSchema)
			return m_dExtraAttrs[iIndex];

		if (iIndex < m_pIndexSchema->GetAttrsCount() - m_dRemoved.GetLength())
		{
			ARRAY_FOREACH_COND(i, m_dRemoved, iIndex >= m_dRemoved[i])
				iIndex++;
			return m_pIndexSchema->GetAttr(iIndex);
		}

		return m_dExtraAttrs[iIndex - m_pIndexSchema->GetAttrsCount() + m_dRemoved.GetLength()];
	}


	const CSphColumnInfo* CSphRsetSchema::GetAttr(const char* sName) const
	{
		ARRAY_FOREACH(i, m_dExtraAttrs)
			if (m_dExtraAttrs[i].m_sName == sName)
				return &m_dExtraAttrs[i];
		if (m_pIndexSchema)
			return m_pIndexSchema->GetAttr(sName);
		return NULL;
	}


	int CSphRsetSchema::GetAttrId_FirstFieldLen() const
	{
		// we assume that field_lens are in the base schema
		return m_pIndexSchema->GetAttrId_FirstFieldLen();
	}


	int CSphRsetSchema::GetAttrId_LastFieldLen() const
	{
		// we assume that field_lens are in the base schema
		return m_pIndexSchema->GetAttrId_LastFieldLen();
	}


	CSphRsetSchema& CSphRsetSchema::operator = (const ISphSchema& rhs)
	{
		rhs.AssignTo(*this);
		return *this;
	}


	CSphRsetSchema& CSphRsetSchema::operator = (const CSphSchema& rhs)
	{
		Reset();
		m_dFields = rhs.m_dFields; // OPTIMIZE? sad but copied
		m_pIndexSchema = &rhs;

		// copy over dynamic rowitems map
		// so that the new attributes we might add would not overlap
		m_dDynamicUsed = rhs.m_dDynamicUsed;
		return *this;
	}


	void CSphRsetSchema::RemoveStaticAttr(int iAttr)
	{
		assert(m_pIndexSchema);
		assert(iAttr >= 0);
		assert(iAttr < (m_pIndexSchema->GetAttrsCount() - m_dRemoved.GetLength()));

		// map from rset indexes (adjusted for removal) to index schema indexes (the original ones)
		ARRAY_FOREACH_COND(i, m_dRemoved, iAttr >= m_dRemoved[i])
			iAttr++;
		m_dRemoved.Add(iAttr);
		m_dRemoved.Uniq();
	}


	void CSphRsetSchema::SwapAttrs(CSphVector<CSphColumnInfo>& dAttrs)
	{
#ifndef NDEBUG
		// ensure that every incoming column has a matching original column
		// only check locators and attribute types, because at this stage,
		// names that are used in dAttrs are already overwritten by the aliases
		// (example: SELECT col1 a, col2 b, count(*) c FROM test)
		//
		// FIXME? maybe also lockdown the schema from further swaps, adds etc from here?
		ARRAY_FOREACH(i, dAttrs)
		{
			if (dAttrs[i].m_tLocator.IsID())
				continue;
			bool bFound1 = false;
			if (m_pIndexSchema)
			{
				const CSphVector<CSphColumnInfo>& dSrc = m_pIndexSchema->m_dAttrs;
				bFound1 = ARRAY_ANY(bFound1, dSrc, dSrc[_any].m_tLocator == dAttrs[i].m_tLocator && dSrc[_any].m_eAttrType == dAttrs[i].m_eAttrType)
			}
			bool bFound2 = ARRAY_ANY(bFound2, m_dExtraAttrs,
				m_dExtraAttrs[_any].m_tLocator == dAttrs[i].m_tLocator && m_dExtraAttrs[_any].m_eAttrType == dAttrs[i].m_eAttrType)
				assert(bFound1 || bFound2);
		}
#endif
		m_dExtraAttrs.SwapData(dAttrs);
		m_pIndexSchema = NULL;
	}


	void CSphRsetSchema::CloneMatch(CSphMatch* pDst, const CSphMatch& rhs) const
	{
		assert(pDst);
		FreeStringPtrs(pDst);
		pDst->Combine(rhs, GetDynamicSize());
		CopyPtrs(pDst, rhs);
	}


	static void ReadSchemaColumn(CSphReader& rdInfo, CSphColumnInfo& tCol, DWORD uVersion)
	{
		tCol.m_sName = rdInfo.GetString();
		if (tCol.m_sName.IsEmpty())
			tCol.m_sName = "@emptyname";

		tCol.m_sName.ToLower();
		tCol.m_eAttrType = (ESphAttr)rdInfo.GetDword(); // FIXME? check/fixup?

		if (uVersion >= 5) // m_uVersion for searching
		{
			rdInfo.GetDword(); // ignore rowitem
			tCol.m_tLocator.m_iBitOffset = rdInfo.GetDword();
			tCol.m_tLocator.m_iBitCount = rdInfo.GetDword();
		}
		else
		{
			tCol.m_tLocator.m_iBitOffset = -1;
			tCol.m_tLocator.m_iBitCount = -1;
		}

		if (uVersion >= 16) // m_uVersion for searching
			tCol.m_bPayload = (rdInfo.GetByte() != 0);

		// WARNING! max version used here must be in sync with RtIndex_t::Prealloc
	}



	void ReadSchema(CSphReader& rdInfo, CSphSchema& m_tSchema, DWORD uVersion, bool bDynamic)
	{
		m_tSchema.Reset();

		m_tSchema.m_dFields.Resize(rdInfo.GetDword());
		ARRAY_FOREACH(i, m_tSchema.m_dFields)
			ReadSchemaColumn(rdInfo, m_tSchema.m_dFields[i], uVersion);

		int iNumAttrs = rdInfo.GetDword();

		for (int i = 0; i < iNumAttrs; i++)
		{
			CSphColumnInfo tCol;
			ReadSchemaColumn(rdInfo, tCol, uVersion);
			m_tSchema.AddAttr(tCol, bDynamic);
		}
	}


	static void WriteSchemaColumn(CSphWriter& fdInfo, const CSphColumnInfo& tCol)
	{
		int iLen = strlen(tCol.m_sName.cstr());
		fdInfo.PutDword(iLen);
		fdInfo.PutBytes(tCol.m_sName.cstr(), iLen);

		ESphAttr eAttrType = tCol.m_eAttrType;
		fdInfo.PutDword(eAttrType);

		fdInfo.PutDword(tCol.m_tLocator.CalcRowitem()); // for backwards compatibility
		fdInfo.PutDword(tCol.m_tLocator.m_iBitOffset);
		fdInfo.PutDword(tCol.m_tLocator.m_iBitCount);

		fdInfo.PutByte(tCol.m_bPayload);
	}

	void WriteSchema(CSphWriter& fdInfo, const CSphSchema& tSchema)
	{
		// schema
		fdInfo.PutDword(tSchema.m_dFields.GetLength());
		ARRAY_FOREACH(i, tSchema.m_dFields)
			WriteSchemaColumn(fdInfo, tSchema.m_dFields[i]);

		fdInfo.PutDword(tSchema.GetAttrsCount());
		for (int i = 0; i < tSchema.GetAttrsCount(); i++)
			WriteSchemaColumn(fdInfo, tSchema.GetAttr(i));
	}

}