#pragma once
#include "neo/int/types.h"
#include "neo/source/schema.h"


namespace NEO {

	inline int64_t MVA_UPSIZE(const DWORD* pMva)
	{
		int64_t iMva = (int64_t)((uint64_t)pMva[0] | (((uint64_t)pMva[1]) << 32));
		return iMva;
	}


	// FIXME!!! for over INT_MAX attributes
	/// attr min-max builder
	template < typename DOCID = SphDocID_t >
	class AttrIndexBuilder_t : ISphNoncopyable
	{
	private:
		CSphVector<CSphAttrLocator>	m_dIntAttrs;
		CSphVector<CSphAttrLocator>	m_dFloatAttrs;
		CSphVector<CSphAttrLocator>	m_dMvaAttrs;
		CSphVector<SphAttr_t>		m_dIntMin;
		CSphVector<SphAttr_t>		m_dIntMax;
		CSphVector<SphAttr_t>		m_dIntIndexMin;
		CSphVector<SphAttr_t>		m_dIntIndexMax;
		CSphVector<float>			m_dFloatMin;
		CSphVector<float>			m_dFloatMax;
		CSphVector<float>			m_dFloatIndexMin;
		CSphVector<float>			m_dFloatIndexMax;
		CSphVector<int64_t>			m_dMvaMin;
		CSphVector<int64_t>			m_dMvaMax;
		CSphVector<int64_t>			m_dMvaIndexMin;
		CSphVector<int64_t>			m_dMvaIndexMax;
		DWORD						m_uStride;		// size of attribute's chunk (in DWORDs)
		DWORD						m_uElements;	// counts total number of collected min/max pairs
		int							m_iLoop;		// loop inside one set
		DWORD* m_pOutBuffer;	// storage for collected min/max
		DWORD const* m_pOutMax;		// storage max for bound checking
		DOCID						m_uStart;		// first and last docids of current chunk
		DOCID						m_uLast;
		DOCID						m_uIndexStart;	// first and last docids of whole index
		DOCID						m_uIndexLast;
		int							m_iMva64;

	private:
		void ResetLocal();
		void FlushComputed();
		void UpdateMinMaxDocids(DOCID uDocID);
		void CollectRowMVA(int iAttr, DWORD uCount, const DWORD* pMva);
		void CollectWithoutMvas(const DWORD* pCur);

	public:
		explicit AttrIndexBuilder_t(const CSphSchema& tSchema);

		void Prepare(DWORD* pOutBuffer, const DWORD* pOutMax);

		bool Collect(const DWORD* pCur, const DWORD* pMvas, int64_t iMvasCount, CSphString& sError, bool bHasMvaID);

		void FinishCollect();

		/// actually used part of output buffer, only used with index merge
		/// (we reserve space for rows from both indexes, but might kill some rows)
		inline int64_t GetActualSize() const
		{
			return int64_t(m_uElements) * m_uStride * 2;
		}

		/// how many DWORDs will we need for block index
		inline int64_t GetExpectedSize(int64_t iMaxDocs) const
		{
			assert(iMaxDocs >= 0);
			int64_t iDocinfoIndex = (iMaxDocs + DOCINFO_INDEX_FREQ - 1) / DOCINFO_INDEX_FREQ;
			return (iDocinfoIndex + 1) * m_uStride * 2;
		}
	};

	typedef AttrIndexBuilder_t<> AttrIndexBuilder_c;

	// dirty hack for some build systems which not has LLONG_MAX
#ifndef LLONG_MAX
#define LLONG_MAX (((unsigned long long)(-1))>>1)
#endif

#ifndef LLONG_MIN
#define LLONG_MIN (-LLONG_MAX-1)
#endif

#ifndef ULLONG_MAX
#define ULLONG_MAX	(LLONG_MAX * 2ULL + 1)
#endif


	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::ResetLocal()
	{
		ARRAY_FOREACH(i, m_dIntMin)
		{
			m_dIntMin[i] = LLONG_MAX;
			m_dIntMax[i] = 0;
		}
		ARRAY_FOREACH(i, m_dFloatMin)
		{
			m_dFloatMin[i] = FLT_MAX;
			m_dFloatMax[i] = -FLT_MAX;
		}
		ARRAY_FOREACH(i, m_dMvaMin)
		{
			m_dMvaMin[i] = LLONG_MAX;
			m_dMvaMax[i] = (i >= m_iMva64 ? LLONG_MIN : 0);
		}
		m_uStart = m_uLast = 0;
		m_iLoop = 0;
	}

	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::FlushComputed()
	{
		assert(m_pOutBuffer);
		DWORD* pMinEntry = m_pOutBuffer + 2 * m_uElements * m_uStride;
		DWORD* pMaxEntry = pMinEntry + m_uStride;
		CSphRowitem* pMinAttrs = DOCINFO2ATTRS_T<DOCID>(pMinEntry);
		CSphRowitem* pMaxAttrs = pMinAttrs + m_uStride;

		assert(pMaxEntry + m_uStride <= m_pOutMax);
		assert(pMaxAttrs + m_uStride - DOCINFO_IDSIZE <= m_pOutMax);

		m_uIndexLast = m_uLast;

		DOCINFOSETID(pMinEntry, m_uStart);
		DOCINFOSETID(pMaxEntry, m_uLast);

		ARRAY_FOREACH(i, m_dIntAttrs)
		{
			m_dIntIndexMin[i] = Min(m_dIntIndexMin[i], m_dIntMin[i]);
			m_dIntIndexMax[i] = Max(m_dIntIndexMax[i], m_dIntMax[i]);
			sphSetRowAttr(pMinAttrs, m_dIntAttrs[i], m_dIntMin[i]);
			sphSetRowAttr(pMaxAttrs, m_dIntAttrs[i], m_dIntMax[i]);
		}
		ARRAY_FOREACH(i, m_dFloatAttrs)
		{
			m_dFloatIndexMin[i] = Min(m_dFloatIndexMin[i], m_dFloatMin[i]);
			m_dFloatIndexMax[i] = Max(m_dFloatIndexMax[i], m_dFloatMax[i]);
			sphSetRowAttr(pMinAttrs, m_dFloatAttrs[i], sphF2DW(m_dFloatMin[i]));
			sphSetRowAttr(pMaxAttrs, m_dFloatAttrs[i], sphF2DW(m_dFloatMax[i]));
		}

		ARRAY_FOREACH(i, m_dMvaAttrs)
		{
			m_dMvaIndexMin[i] = Min(m_dMvaIndexMin[i], m_dMvaMin[i]);
			m_dMvaIndexMax[i] = Max(m_dMvaIndexMax[i], m_dMvaMax[i]);
			sphSetRowAttr(pMinAttrs, m_dMvaAttrs[i], m_dMvaMin[i]);
			sphSetRowAttr(pMaxAttrs, m_dMvaAttrs[i], m_dMvaMax[i]);
		}

		m_uElements++;
		ResetLocal();
	}

	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::UpdateMinMaxDocids(DOCID uDocID)
	{
		if (!m_uStart)
			m_uStart = uDocID;
		if (!m_uIndexStart)
			m_uIndexStart = uDocID;
		m_uLast = uDocID;
	}

	template < typename DOCID >
	AttrIndexBuilder_t<DOCID>::AttrIndexBuilder_t(const CSphSchema& tSchema)
		: m_uStride(DWSIZEOF(DOCID) + tSchema.GetRowSize())
		, m_uElements(0)
		, m_iLoop(0)
		, m_pOutBuffer(NULL)
		, m_pOutMax(NULL)
		, m_uStart(0)
		, m_uLast(0)
		, m_uIndexStart(0)
		, m_uIndexLast(0)
	{
		for (int i = 0; i < tSchema.GetAttrsCount(); i++)
		{
			const CSphColumnInfo& tCol = tSchema.GetAttr(i);
			switch (tCol.m_eAttrType)
			{
			case ESphAttr::SPH_ATTR_INTEGER:
			case ESphAttr::SPH_ATTR_TIMESTAMP:
			case ESphAttr::SPH_ATTR_BOOL:
			case ESphAttr::SPH_ATTR_BIGINT:
			case ESphAttr::SPH_ATTR_TOKENCOUNT:
				m_dIntAttrs.Add(tCol.m_tLocator);
				break;

			case ESphAttr::SPH_ATTR_FLOAT:
				m_dFloatAttrs.Add(tCol.m_tLocator);
				break;

			case ESphAttr::SPH_ATTR_UINT32SET:
				m_dMvaAttrs.Add(tCol.m_tLocator);
				break;

			default:
				break;
			}
		}

		m_iMva64 = m_dMvaAttrs.GetLength();
		for (int i = 0; i < tSchema.GetAttrsCount(); i++)
		{
			const CSphColumnInfo& tCol = tSchema.GetAttr(i);
			if (tCol.m_eAttrType == ESphAttr::SPH_ATTR_INT64SET)
				m_dMvaAttrs.Add(tCol.m_tLocator);
		}


		m_dIntMin.Resize(m_dIntAttrs.GetLength());
		m_dIntMax.Resize(m_dIntAttrs.GetLength());
		m_dIntIndexMin.Resize(m_dIntAttrs.GetLength());
		m_dIntIndexMax.Resize(m_dIntAttrs.GetLength());
		m_dFloatMin.Resize(m_dFloatAttrs.GetLength());
		m_dFloatMax.Resize(m_dFloatAttrs.GetLength());
		m_dFloatIndexMin.Resize(m_dFloatAttrs.GetLength());
		m_dFloatIndexMax.Resize(m_dFloatAttrs.GetLength());
		m_dMvaMin.Resize(m_dMvaAttrs.GetLength());
		m_dMvaMax.Resize(m_dMvaAttrs.GetLength());
		m_dMvaIndexMin.Resize(m_dMvaAttrs.GetLength());
		m_dMvaIndexMax.Resize(m_dMvaAttrs.GetLength());
	}

	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::Prepare(DWORD* pOutBuffer, const DWORD* pOutMax)
	{
		m_pOutBuffer = pOutBuffer;
		m_pOutMax = pOutMax;
		memset(pOutBuffer, 0, (pOutMax - pOutBuffer) * sizeof(DWORD));

		m_uElements = 0;
		m_uIndexStart = m_uIndexLast = 0;
		ARRAY_FOREACH(i, m_dIntIndexMin)
		{
			m_dIntIndexMin[i] = LLONG_MAX;
			m_dIntIndexMax[i] = 0;
		}
		ARRAY_FOREACH(i, m_dFloatIndexMin)
		{
			m_dFloatIndexMin[i] = FLT_MAX;
			m_dFloatIndexMax[i] = -FLT_MAX;
		}
		ARRAY_FOREACH(i, m_dMvaIndexMin)
		{
			m_dMvaIndexMin[i] = LLONG_MAX;
			m_dMvaIndexMax[i] = (i >= m_iMva64 ? LLONG_MIN : 0);
		}
		ResetLocal();
	}

	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::CollectWithoutMvas(const DWORD* pCur)
	{
		// check if it is time to flush already collected values
		if (m_iLoop >= DOCINFO_INDEX_FREQ)
			FlushComputed();

		const DWORD* pRow = DOCINFO2ATTRS_T<DOCID>(pCur);
		UpdateMinMaxDocids(DOCINFO2ID_T<DOCID>(pCur));
		m_iLoop++;

		// ints
		ARRAY_FOREACH(i, m_dIntAttrs)
		{
			SphAttr_t uVal = sphGetRowAttr(pRow, m_dIntAttrs[i]);
			m_dIntMin[i] = Min(m_dIntMin[i], uVal);
			m_dIntMax[i] = Max(m_dIntMax[i], uVal);
		}

		// floats
		ARRAY_FOREACH(i, m_dFloatAttrs)
		{
			float fVal = sphDW2F((DWORD)sphGetRowAttr(pRow, m_dFloatAttrs[i]));
			m_dFloatMin[i] = Min(m_dFloatMin[i], fVal);
			m_dFloatMax[i] = Max(m_dFloatMax[i], fVal);
		}
	}

	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::CollectRowMVA(int iAttr, DWORD uCount, const DWORD* pMva)
	{
		if (iAttr >= m_iMva64)
		{
			assert((uCount % 2) == 0);
			for (; uCount > 0; uCount -= 2, pMva += 2)
			{
				int64_t iVal = MVA_UPSIZE(pMva);
				m_dMvaMin[iAttr] = Min(m_dMvaMin[iAttr], iVal);
				m_dMvaMax[iAttr] = Max(m_dMvaMax[iAttr], iVal);
			}
		}
		else
		{
			for (; uCount > 0; uCount--, pMva++)
			{
				DWORD uVal = *pMva;
				m_dMvaMin[iAttr] = Min(m_dMvaMin[iAttr], uVal);
				m_dMvaMax[iAttr] = Max(m_dMvaMax[iAttr], uVal);
			}
		}
	}

	template < typename DOCID >
	bool AttrIndexBuilder_t<DOCID>::Collect(const DWORD* pCur, const DWORD* pMvas, int64_t iMvasCount, CSphString& sError, bool bHasMvaID)
	{
		CollectWithoutMvas(pCur);

		const DWORD* pRow = DOCINFO2ATTRS_T<DOCID>(pCur);
		SphDocID_t uDocID = DOCINFO2ID_T<DOCID>(pCur);

		// MVAs
		ARRAY_FOREACH(i, m_dMvaAttrs)
		{
			SphAttr_t uOff = sphGetRowAttr(pRow, m_dMvaAttrs[i]);
			if (!uOff)
				continue;

			// sanity checks
			if (uOff >= iMvasCount)
			{
				sError.SetSprintf("broken index: mva offset out of bounds, id=" DOCID_FMT, (SphDocID_t)uDocID);
				return false;
			}

			const DWORD* pMva = pMvas + uOff; // don't care about updates at this point

			if (bHasMvaID && i == 0 && DOCINFO2ID_T<DOCID>(pMva - DWSIZEOF(DOCID)) != uDocID)
			{
				sError.SetSprintf("broken index: mva docid verification failed, id=" DOCID_FMT, (SphDocID_t)uDocID);
				return false;
			}

			DWORD uCount = *pMva++;
			if ((uOff + uCount >= iMvasCount) || (i >= m_iMva64 && (uCount % 2) != 0))
			{
				sError.SetSprintf("broken index: mva list out of bounds, id=" DOCID_FMT, (SphDocID_t)uDocID);
				return false;
			}

			// walk and calc
			CollectRowMVA(i, uCount, pMva);
		}
		return true;
	}


	template < typename DOCID >
	void AttrIndexBuilder_t<DOCID>::FinishCollect()
	{
		assert(m_pOutBuffer);
		if (m_iLoop)
			FlushComputed();

		DWORD* pMinEntry = m_pOutBuffer + 2 * m_uElements * m_uStride;
		DWORD* pMaxEntry = pMinEntry + m_uStride;
		CSphRowitem* pMinAttrs = DOCINFO2ATTRS_T<DOCID>(pMinEntry);
		CSphRowitem* pMaxAttrs = pMinAttrs + m_uStride;

		assert(pMaxEntry + m_uStride <= m_pOutMax);
		assert(pMaxAttrs + m_uStride - DWSIZEOF(DOCID) <= m_pOutMax);

		DOCINFOSETID(pMinEntry, m_uIndexStart);
		DOCINFOSETID(pMaxEntry, m_uIndexLast);

		ARRAY_FOREACH(i, m_dMvaAttrs)
		{
			sphSetRowAttr(pMinAttrs, m_dMvaAttrs[i], m_dMvaIndexMin[i]);
			sphSetRowAttr(pMaxAttrs, m_dMvaAttrs[i], m_dMvaIndexMax[i]);
		}

		ARRAY_FOREACH(i, m_dIntAttrs)
		{
			sphSetRowAttr(pMinAttrs, m_dIntAttrs[i], m_dIntIndexMin[i]);
			sphSetRowAttr(pMaxAttrs, m_dIntAttrs[i], m_dIntIndexMax[i]);
		}
		ARRAY_FOREACH(i, m_dFloatAttrs)
		{
			sphSetRowAttr(pMinAttrs, m_dFloatAttrs[i], sphF2DW(m_dFloatIndexMin[i]));
			sphSetRowAttr(pMaxAttrs, m_dFloatAttrs[i], sphF2DW(m_dFloatIndexMax[i]));
		}
		m_uElements++;
	}


}