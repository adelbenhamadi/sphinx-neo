#pragma once
#include "neo/query/match_sorter.h"
#include "neo/core/match_engine.h"

namespace NEO {

	//////////////////////////////////////////////////////////////////////////
	// TRAITS
	//////////////////////////////////////////////////////////////////////////


	static bool HasString(const CSphMatchComparatorState* pState)
	{
		assert(pState);

		for (int i = 0; i < CSphMatchComparatorState::MAX_ATTRS; i++)
		{
			if (pState->m_eKeypart[i] == SPH_KEYPART_STRING || pState->m_eKeypart[i] == SPH_KEYPART_STRINGPTR || (pState->m_tSubKeys[i].m_sKey.cstr()))
				return true;
		}

		return false;
	}


	/// match-sorting priority queue traits
	class CSphMatchQueueTraits : public ISphMatchSorter, ISphNoncopyable
	{
	protected:
		CSphMatch* m_pData;
		int							m_iUsed;
		int							m_iSize;
		const bool					m_bUsesAttrs;

	private:
		const int					m_iDataLength;

	public:
		/// ctor
		CSphMatchQueueTraits(int iSize, bool bUsesAttrs)
			: m_iUsed(0)
			, m_iSize(iSize)
			, m_bUsesAttrs(bUsesAttrs)
			, m_iDataLength(iSize)
		{
			assert(iSize > 0);
			m_pData = new CSphMatch[m_iDataLength];
			assert(m_pData);

			m_tState.m_iNow = (DWORD)time(NULL);
			m_iMatchCapacity = m_iDataLength;
		}

		/// dtor
		virtual ~CSphMatchQueueTraits()
		{
			for (int i = 0; i < m_iDataLength; ++i)
				m_tSchema.FreeStringPtrs(m_pData + i);
			SafeDeleteArray(m_pData);
		}

	public:
		bool				UsesAttrs() const { return m_bUsesAttrs; }
		virtual int			GetLength() const { return m_iUsed; }
		virtual int			GetDataLength() const { return m_iDataLength; }

		virtual bool CanMulti() const
		{
			return !HasString(&m_tState);
		}
	};

	//////////////////////////////////////////////////////////////////////////
	// SORTING QUEUES
	//////////////////////////////////////////////////////////////////////////

	template < typename COMP >
	struct CompareIndex_fn
	{
		const CSphMatch* m_pBase;
		const CSphMatchComparatorState* m_pState;

		CompareIndex_fn(const CSphMatch* pBase, const CSphMatchComparatorState* pState)
			: m_pBase(pBase)
			, m_pState(pState)
		{}

		bool IsLess(int a, int b) const
		{
			return COMP::IsLess(m_pBase[b], m_pBase[a], *m_pState);
		}
	};

	/// heap sorter
	/// plain binary heap based PQ
	template < typename COMP, bool NOTIFICATIONS >
	class CSphMatchQueue : public CSphMatchQueueTraits
	{
	public:
		/// ctor
		CSphMatchQueue(int iSize, bool bUsesAttrs)
			: CSphMatchQueueTraits(iSize, bUsesAttrs)
		{
			if_const(NOTIFICATIONS)
				m_dJustPopped.Reserve(1);
		}

		/// check if this sorter does groupby
		virtual bool IsGroupby() const
		{
			return false;
		}

		virtual const CSphMatch* GetWorst() const
		{
			return m_pData;
		}

		/// add entry to the queue
		virtual bool Push(const CSphMatch& tEntry)
		{
			m_iTotal++;

			if_const(NOTIFICATIONS)
			{
				m_iJustPushed = 0;
				m_dJustPopped.Resize(0);
			}

			if (m_iUsed == m_iSize)
			{
				// if it's worse that current min, reject it, else pop off current min
				if (COMP::IsLess(tEntry, m_pData[0], m_tState))
					return true;
				else
					Pop();
			}

			// do add
			m_tSchema.CloneMatch(m_pData + m_iUsed, tEntry);

			if_const(NOTIFICATIONS)
				m_iJustPushed = tEntry.m_uDocID;

			int iEntry = m_iUsed++;

			// sift up if needed, so that worst (lesser) ones float to the top
			while (iEntry)
			{
				int iParent = (iEntry - 1) >> 1;
				if (!COMP::IsLess(m_pData[iEntry], m_pData[iParent], m_tState))
					break;

				// entry is less than parent, should float to the top
				Swap(m_pData[iEntry], m_pData[iParent]);
				iEntry = iParent;
			}

			return true;
		}

		/// add grouped entry (must not happen)
		virtual bool PushGrouped(const CSphMatch&, bool)
		{
			assert(0);
			return false;
		}

		/// remove root (ie. top priority) entry
		virtual void Pop()
		{
			assert(m_iUsed);
			if (!(--m_iUsed)) // empty queue? just return
				return;

			// make the last entry my new root
			Swap(m_pData[0], m_pData[m_iUsed]);
			m_tSchema.FreeStringPtrs(&m_pData[m_iUsed]);

			if_const(NOTIFICATIONS)
			{
				if (m_dJustPopped.GetLength())
					m_dJustPopped[0] = m_pData[m_iUsed].m_uDocID;
				else
					m_dJustPopped.Add(m_pData[m_iUsed].m_uDocID);
			}

			// sift down if needed
			int iEntry = 0;
			for (;; )
			{
				// select child
				int iChild = (iEntry << 1) + 1;
				if (iChild >= m_iUsed)
					break;

				// select smallest child
				if (iChild + 1 < m_iUsed)
					if (COMP::IsLess(m_pData[iChild + 1], m_pData[iChild], m_tState))
						iChild++;

				// if smallest child is less than entry, do float it to the top
				if (COMP::IsLess(m_pData[iChild], m_pData[iEntry], m_tState))
				{
					Swap(m_pData[iChild], m_pData[iEntry]);
					iEntry = iChild;
					continue;
				}

				break;
			}
		}

		/// store all entries into specified location in sorted order, and remove them from queue
		int Flatten(CSphMatch* pTo, int iTag)
		{
			assert(m_iUsed >= 0);
			pTo += m_iUsed;
			int iCopied = m_iUsed;
			while (m_iUsed > 0)
			{
				--pTo;
				m_tSchema.FreeStringPtrs(pTo);
				Swap(*pTo, *m_pData);
				if (iTag >= 0)
					pTo->m_iTag = iTag;
				Pop();
			}
			m_iTotal = 0;
			return iCopied;
		}

		void Finalize(ISphMatchProcessor& tProcessor, bool bCallProcessInResultSetOrder)
		{
			if (!GetLength())
				return;

			if (!bCallProcessInResultSetOrder)
			{
				// just evaluate in heap order
				CSphMatch* pCur = m_pData;
				const CSphMatch* pEnd = m_pData + m_iUsed;
				while (pCur < pEnd)
				{
					tProcessor.Process(pCur++);
				}
			}
			else
			{
				// means final-stage calls will be evaluated
				// a) over the final, pre-limit result set
				// b) in the final result set order
				CSphFixedVector<int> dIndexes(GetLength());
				ARRAY_FOREACH(i, dIndexes)
					dIndexes[i] = i;
				sphSort(dIndexes.Begin(), dIndexes.GetLength(), CompareIndex_fn<COMP>(m_pData, &m_tState));

				ARRAY_FOREACH(i, dIndexes)
				{
					tProcessor.Process(m_pData + dIndexes[i]);
				}
			}
		}
	};

	//////////////////////////////////////////////////////////////////////////

	/// match sorting functor
	template < typename COMP >
	struct MatchSort_fn : public MatchSortAccessor_t
	{
		CSphMatchComparatorState	m_tState;

		explicit MatchSort_fn(const CSphMatchComparatorState& tState)
			: m_tState(tState)
		{}

		bool IsLess(const MEDIAN_TYPE a, const MEDIAN_TYPE b)
		{
			return COMP::IsLess(*a, *b, m_tState);
		}
	};


	/// K-buffer (generalized double buffer) sorter
	/// faster worst-case but slower average-case than the heap sorter
	template < typename COMP, bool NOTIFICATIONS >
	class CSphKbufferMatchQueue : public CSphMatchQueueTraits
	{
	protected:
		static const int	COEFF = 4;

		CSphMatch* m_pEnd;
		CSphMatch* m_pWorst;
		bool				m_bFinalized;

	public:
		/// ctor
		CSphKbufferMatchQueue(int iSize, bool bUsesAttrs)
			: CSphMatchQueueTraits(iSize* COEFF, bUsesAttrs)
			, m_pEnd(m_pData + iSize * COEFF)
			, m_pWorst(NULL)
			, m_bFinalized(false)
		{
			if_const(NOTIFICATIONS)
				m_dJustPopped.Reserve(m_iSize);

			m_iSize /= COEFF;
		}

		/// check if this sorter does groupby
		virtual bool IsGroupby() const
		{
			return false;
		}

		/// add entry to the queue
		virtual bool Push(const CSphMatch& tEntry)
		{
			if_const(NOTIFICATIONS)
			{
				m_iJustPushed = 0;
				m_dJustPopped.Resize(0);
			}

			// quick early rejection checks
			m_iTotal++;
			if (m_pWorst && COMP::IsLess(tEntry, *m_pWorst, m_tState))
				return true;

			// quick check passed
			// fill the data, back to front
			m_bFinalized = false;
			m_iUsed++;
			m_tSchema.CloneMatch(m_pEnd - m_iUsed, tEntry);

			if_const(NOTIFICATIONS)
				m_iJustPushed = tEntry.m_uDocID;

			// do the initial sort once
			if (m_iTotal == m_iSize)
			{
				assert(m_iUsed == m_iSize && !m_pWorst);
				MatchSort_fn<COMP> tComp(m_tState);
				sphSort(m_pEnd - m_iSize, m_iSize, tComp, tComp);
				m_pWorst = m_pEnd - m_iSize;
				m_bFinalized = true;
				return true;
			}

			// do the sort/cut when the K-buffer is full
			if (m_iUsed == m_iSize * COEFF)
			{
				MatchSort_fn<COMP> tComp(m_tState);
				sphSort(m_pData, m_iUsed, tComp, tComp);

				if_const(NOTIFICATIONS)
				{
					for (CSphMatch* pMatch = m_pData; pMatch < m_pEnd - m_iSize; pMatch++)
						m_dJustPopped.Add(pMatch->m_uDocID);
				}

				m_iUsed = m_iSize;
				m_pWorst = m_pEnd - m_iSize;
				m_bFinalized = true;
			}
			return true;
		}

		/// add grouped entry (must not happen)
		virtual bool PushGrouped(const CSphMatch&, bool)
		{
			assert(0);
			return false;
		}

		/// finalize, perform final sort/cut as needed
		virtual void Finalize(ISphMatchProcessor& tProcessor, bool)
		{
			if (!GetLength())
				return;

			if (!m_bFinalized)
			{
				MatchSort_fn<COMP> tComp(m_tState);
				sphSort(m_pEnd - m_iUsed, m_iUsed, tComp, tComp);
				m_iUsed = Min(m_iUsed, m_iSize);
				m_bFinalized = true;
			}

			// reverse order iteration
			CSphMatch* pCur = m_pEnd - 1;
			const CSphMatch* pEnd = m_pEnd - m_iUsed;
			while (pCur >= pEnd)
			{
				tProcessor.Process(pCur--);
			}
		}

		/// current result set length
		virtual int GetLength() const
		{
			return Min(m_iUsed, m_iSize);
		}

		/// store all entries into specified location in sorted order, and remove them from queue
		int Flatten(CSphMatch* pTo, int iTag)
		{
			const CSphMatch* pBegin = pTo;

			// ensure we are sorted
			if (m_iUsed)
			{
				MatchSort_fn<COMP> tComp(m_tState);
				sphSort(m_pEnd - m_iUsed, m_iUsed, tComp, tComp);
			}

			// reverse copy
			for (int i = 1; i <= Min(m_iUsed, m_iSize); i++)
			{
				m_tSchema.FreeStringPtrs(pTo);
				Swap(*pTo, m_pEnd[-i]);
				if (iTag >= 0)
					pTo->m_iTag = iTag;
				pTo++;
			}

			// clean up for the next work session
			m_iTotal = 0;
			m_iUsed = 0;
			m_iSize = 0;
			m_bFinalized = false;

			return (pTo - pBegin);
		}
	};




}