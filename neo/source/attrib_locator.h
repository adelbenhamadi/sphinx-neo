#pragma once
#include "neo/int/types.h"

#include <cassert>

namespace NEO {
	/// attribute locator within the row
	struct CSphAttrLocator
	{
		// OPTIMIZE? try packing these
		int				m_iBitOffset;
		int				m_iBitCount;
		bool			m_bDynamic;

		CSphAttrLocator()
			: m_iBitOffset(-1)
			, m_iBitCount(-1)
			, m_bDynamic(false)
		{}

		explicit CSphAttrLocator(int iBitOffset, int iBitCount = ROWITEM_BITS)
			: m_iBitOffset(iBitOffset)
			, m_iBitCount(iBitCount)
			, m_bDynamic(true)
		{}

		inline bool IsBitfield() const
		{
			return (m_iBitCount < ROWITEM_BITS || (m_iBitOffset % ROWITEM_BITS) != 0);
		}

		int CalcRowitem() const
		{
			return IsBitfield() ? -1 : (m_iBitOffset / ROWITEM_BITS);
		}

		bool IsID() const
		{
			return m_iBitOffset == -8 * (int)sizeof(SphDocID_t) && m_iBitCount == 8 * sizeof(SphDocID_t);
		}

#ifndef NDEBUG
		/// get last item touched by this attr (for debugging checks only)
		int GetMaxRowitem() const
		{
			return (m_iBitOffset + m_iBitCount - 1) / ROWITEM_BITS;
		}
#endif

		bool operator == (const CSphAttrLocator& rhs) const
		{
			return m_iBitOffset == rhs.m_iBitOffset && m_iBitCount == rhs.m_iBitCount && m_bDynamic == rhs.m_bDynamic;
		}
	};


	/// getter
	inline SphAttr_t sphGetRowAttr(const CSphRowitem* pRow, const CSphAttrLocator& tLoc)
	{
		assert(pRow);
		int iItem = tLoc.m_iBitOffset >> ROWITEM_SHIFT;

		if (tLoc.m_iBitCount == ROWITEM_BITS)
			return pRow[iItem];

		if (tLoc.m_iBitCount == 2 * ROWITEM_BITS) // FIXME? write a generalized version, perhaps
			return SphAttr_t(pRow[iItem]) + (SphAttr_t(pRow[iItem + 1]) << ROWITEM_BITS);

		int iShift = tLoc.m_iBitOffset & ((1 << ROWITEM_SHIFT) - 1);
		return (pRow[iItem] >> iShift) & ((1UL << tLoc.m_iBitCount) - 1);
	}


	/// setter
	inline void sphSetRowAttr(CSphRowitem* pRow, const CSphAttrLocator& tLoc, SphAttr_t uValue)
	{
		assert(pRow);
		int iItem = tLoc.m_iBitOffset >> ROWITEM_SHIFT;
		if (tLoc.m_iBitCount == 2 * ROWITEM_BITS)
		{
			// FIXME? write a generalized version, perhaps
			pRow[iItem] = CSphRowitem(uValue & ((SphAttr_t(1) << ROWITEM_BITS) - 1));
			pRow[iItem + 1] = CSphRowitem(uValue >> ROWITEM_BITS);

		}
		else if (tLoc.m_iBitCount == ROWITEM_BITS)
		{
			pRow[iItem] = CSphRowitem(uValue);

		}
		else
		{
			int iShift = tLoc.m_iBitOffset & ((1 << ROWITEM_SHIFT) - 1);
			CSphRowitem uMask = ((1UL << tLoc.m_iBitCount) - 1) << iShift;
			pRow[iItem] &= ~uMask;
			pRow[iItem] |= (uMask & (uValue << iShift));
		}
	}


}