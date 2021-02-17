#pragma once
#include "neo/int/types.h"
#include "neo/platform/mutex.h"
#include "neo/utility/hash.h"


namespace NEO {

	// More than just sorted vector.
	// OrderedHash is for fast (without sorting potentially big vector) inserts.
	class CSphKilllist : public ISphNoncopyable
	{
	private:
		static const int				MAX_SMALL_SIZE = 512;
		CSphVector<SphDocID_t>			m_dLargeKlist;
		CSphOrderedHash < bool, SphDocID_t, IdentityHash_fn, MAX_SMALL_SIZE >	m_hSmallKlist;
		CSphRwlock						m_tLock;

	public:

		CSphKilllist()
		{
			m_tLock.Init();
		}

		virtual ~CSphKilllist()
		{
			m_tLock.Done();
		}

		void Flush(CSphVector<SphDocID_t>& dKlist)
		{
			{
				CSphScopedRLock tRguard(m_tLock);
				bool bGotHash = (m_hSmallKlist.GetLength() > 0);
				if (!bGotHash)
					NakedCopy(dKlist);

				if (!bGotHash)
					return;
			}

			CSphScopedWLock tWguard(m_tLock);
			NakedFlush(NULL, 0);
			NakedCopy(dKlist);
		}

		inline void Add(SphDocID_t* pDocs, int iCount)
		{
			if (!iCount)
				return;

			CSphScopedWLock tWlock(m_tLock);
			if (m_hSmallKlist.GetLength() + iCount >= MAX_SMALL_SIZE)
			{
				NakedFlush(pDocs, iCount);
			}
			else
			{
				while (iCount--)
					m_hSmallKlist.Add(true, *pDocs++);
			}
		}

		bool Exists(SphDocID_t uDoc)
		{
			CSphScopedRLock tRguard(m_tLock);
			bool bGot = (m_hSmallKlist.Exists(uDoc) || m_dLargeKlist.BinarySearch(uDoc) != NULL);
			return bGot;
		}

		void Reset(SphDocID_t* pDocs, int iCount)
		{
			m_tLock.WriteLock();
			m_dLargeKlist.Reset();
			m_hSmallKlist.Reset();

			NakedFlush(pDocs, iCount);

			m_tLock.Unlock();
		}

		void LoadFromFile(const char* sFilename);
		void SaveToFile(const char* sFilename);

	private:

		void NakedCopy(CSphVector<SphDocID_t>& dKlist)
		{
			assert(m_hSmallKlist.GetLength() == 0);
			if (!m_dLargeKlist.GetLength())
				return;

			int iOff = dKlist.GetLength();
			dKlist.Resize(m_dLargeKlist.GetLength() + iOff);
			memcpy(dKlist.Begin() + iOff, m_dLargeKlist.Begin(), sizeof(m_dLargeKlist[0]) * m_dLargeKlist.GetLength());
		}

		void NakedFlush(SphDocID_t* pDocs, int iCount)
		{
			if (m_hSmallKlist.GetLength() == 0 && iCount == 0)
				return;

			m_dLargeKlist.Reserve(m_dLargeKlist.GetLength() + m_hSmallKlist.GetLength() + iCount);
			m_hSmallKlist.IterateStart();
			while (m_hSmallKlist.IterateNext())
				m_dLargeKlist.Add(m_hSmallKlist.IterateGetKey());
			if (pDocs && iCount)
			{
				int iOff = m_dLargeKlist.GetLength();
				m_dLargeKlist.Resize(iOff + iCount);
				memcpy(m_dLargeKlist.Begin() + iOff, pDocs, sizeof(m_dLargeKlist[0]) * iCount);
			}
			m_dLargeKlist.Uniq();
			m_hSmallKlist.Reset();
		}
	};


}