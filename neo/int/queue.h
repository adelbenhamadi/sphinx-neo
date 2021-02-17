#pragma once

namespace NEO {
	/// generic stateless priority queue
	template < typename T, typename COMP > class CSphQueue
	{
	protected:
		T* m_pData;
		int		m_iUsed;
		int		m_iSize;

	public:
		/// ctor
		explicit CSphQueue(int iSize)
			: m_pData(NULL)
			, m_iUsed(0)
			, m_iSize(iSize)
		{
			Reset(iSize);
		}

		/// dtor
		~CSphQueue()
		{
			SafeDeleteArray(m_pData);
		}

		void Reset(int iSize)
		{
			SafeDeleteArray(m_pData);
			assert(iSize >= 0);
			m_iSize = iSize;
			m_pData = new T[iSize];
			assert(!iSize || m_pData);
		}

		/// add entry to the queue
		bool Push(const T& tEntry)
		{
			assert(m_pData);
			if (m_iUsed == m_iSize)
			{
				// if it's worse that current min, reject it, else pop off current min
				if (COMP::IsLess(tEntry, m_pData[0]))
					return false;
				else
					Pop();
			}

			// do add
			m_pData[m_iUsed] = tEntry;
			int iEntry = m_iUsed++;

			// sift up if needed, so that worst (lesser) ones float to the top
			while (iEntry)
			{
				int iParent = (iEntry - 1) >> 1;
				if (!COMP::IsLess(m_pData[iEntry], m_pData[iParent]))
					break;

				// entry is less than parent, should float to the top
				Swap(m_pData[iEntry], m_pData[iParent]);
				iEntry = iParent;
			}

			return true;
		}

		/// remove root (ie. top priority) entry
		void Pop()
		{
			assert(m_iUsed && m_pData);
			if (!(--m_iUsed)) // empty queue? just return
				return;

			// make the last entry my new root
			m_pData[0] = m_pData[m_iUsed];

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
					if (COMP::IsLess(m_pData[iChild + 1], m_pData[iChild]))
						iChild++;

				// if smallest child is less than entry, do float it to the top
				if (COMP::IsLess(m_pData[iChild], m_pData[iEntry]))
				{
					Swap(m_pData[iChild], m_pData[iEntry]);
					iEntry = iChild;
					continue;
				}

				break;
			}
		}

		/// get entries count
		inline int GetLength() const
		{
			return m_iUsed;
		}

		/// get current root
		inline const T& Root() const
		{
			assert(m_iUsed && m_pData);
			return m_pData[0];
		}
	};

}