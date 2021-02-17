#pragma once
#include "neo/int/types.h"
#include "neo/source/attrib_locator.h"
#include "neo/core/macros.h"
#include "neo/tools/convert.h"
#include "neo/core/generic.h"



namespace NEO {

	/// search query match (document info plus weight/tag)
	class CSphMatch
	{
		friend class ISphSchema;
		friend class CSphRsetSchema;

	public:
		SphDocID_t				m_uDocID;		///< document ID
		const CSphRowitem* m_pStatic;		///< static part (stored in and owned by the index)
		CSphRowitem* m_pDynamic;		///< dynamic part (computed per query; owned by the match)
		int						m_iWeight;		///< my computed weight
		int						m_iTag;			///< my index tag

	public:
		/// ctor. clears everything
		CSphMatch()
			: m_uDocID(0)
			, m_pStatic(NULL)
			, m_pDynamic(NULL)
			, m_iWeight(0)
			, m_iTag(0)
		{
		}

	private:
		/// copy ctor. just in case
		CSphMatch(const CSphMatch& rhs)
			: m_pStatic(0)
			, m_pDynamic(NULL)
		{
			*this = rhs;
		}

	public:
		/// dtor. frees everything
		~CSphMatch()
		{
#ifndef NDEBUG
			if (m_pDynamic)
				m_pDynamic--;
#endif
			SafeDeleteArray(m_pDynamic);
		}

		/// reset
		void Reset(int iDynamic)
		{
			// check that we're either initializing a new one, or NOT changing the current size
			assert(iDynamic >= 0);
			assert(!m_pDynamic || iDynamic == (int)m_pDynamic[-1]);

			m_uDocID = 0;
			if (!m_pDynamic && iDynamic)
			{
#ifndef NDEBUG
				m_pDynamic = new CSphRowitem[iDynamic + 1];
				*m_pDynamic++ = iDynamic;
#else
				m_pDynamic = new CSphRowitem[iDynamic];
#endif
				// dynamic stuff might contain pointers now (STRINGPTR type)
				// so we gotta cleanup
				memset(m_pDynamic, 0, iDynamic * sizeof(CSphRowitem));
			}
		}

	private:
		/// assignment
		void Combine(const CSphMatch& rhs, int iDynamic)
		{
			// check that we're either initializing a new one, or NOT changing the current size
			assert(iDynamic >= 0);
			assert(!m_pDynamic || iDynamic == (int)m_pDynamic[-1]);

			if (this != &rhs)
			{
				m_uDocID = rhs.m_uDocID;
				m_iWeight = rhs.m_iWeight;
				m_pStatic = rhs.m_pStatic;
				m_iTag = rhs.m_iTag;
			}

			if (iDynamic)
			{
				if (!m_pDynamic)
				{
#ifndef NDEBUG
					m_pDynamic = new CSphRowitem[iDynamic + 1];
					*m_pDynamic++ = iDynamic;
#else
					m_pDynamic = new CSphRowitem[iDynamic];
#endif
				}

				if (this != &rhs)
				{
					assert(rhs.m_pDynamic);
					assert(m_pDynamic[-1] == rhs.m_pDynamic[-1]); // ensure we're not changing X to Y
					memcpy(m_pDynamic, rhs.m_pDynamic, iDynamic * sizeof(CSphRowitem));
				}
			}
		}

	public:
		/// integer getter
		SphAttr_t GetAttr(const CSphAttrLocator& tLoc) const
		{
			// m_pRowpart[tLoc.m_bDynamic] is 30% faster on MSVC 2005
			// same time on gcc 4.x though, ~1 msec per 1M calls, so lets avoid the hassle for now
			if (tLoc.m_iBitOffset >= 0)
				return sphGetRowAttr(tLoc.m_bDynamic ? m_pDynamic : m_pStatic, tLoc);
			if (tLoc.IsID())
				return m_uDocID;
			assert(false && "Unknown negative-bitoffset locator");
			return 0;
		}

		/// float getter
		float GetAttrFloat(const CSphAttrLocator& tLoc) const
		{
			return sphDW2F((DWORD)sphGetRowAttr(tLoc.m_bDynamic ? m_pDynamic : m_pStatic, tLoc));
		}

		/// integer setter
		void SetAttr(const CSphAttrLocator& tLoc, SphAttr_t uValue)
		{
			if (tLoc.IsID())
				return;
			assert(tLoc.m_bDynamic);
			assert(tLoc.GetMaxRowitem() < (int)m_pDynamic[-1]);
			sphSetRowAttr(m_pDynamic, tLoc, uValue);
		}

		/// float setter
		void SetAttrFloat(const CSphAttrLocator& tLoc, float fValue)
		{
			assert(tLoc.m_bDynamic);
			assert(tLoc.GetMaxRowitem() < (int)m_pDynamic[-1]);
			sphSetRowAttr(m_pDynamic, tLoc, sphF2DW(fValue));
		}

		/// MVA getter
		const DWORD* GetAttrMVA(const CSphAttrLocator& tLoc, const DWORD* pPool, bool bArenaProhibit) const;

	private:
		/// "manually" prevent copying
		const CSphMatch& operator = (const CSphMatch&)
		{
			assert(0 && "internal error (CSphMatch::operator= called)");
			return *this;
		}
	};

	/// specialized swapper
	inline void Swap(CSphMatch& a, CSphMatch& b);

}