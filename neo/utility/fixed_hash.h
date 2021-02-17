#pragma once

namespace NEO {

	/// simple fixed-size hash
	/// doesn't keep the order
	template < typename T, typename KEY, typename HASHFUNC >
	class CSphFixedHash : ISphNoncopyable
	{
	protected:
		static const int			HASH_LIST_END = -1;
		static const int			HASH_DELETED = -2;

		struct HashEntry_t
		{
			KEY		m_tKey;
			T		m_tValue;
			int		m_iNext;
		};

	protected:
		CSphVector<HashEntry_t>		m_dEntries;		///< key-value pairs storage pool
		CSphVector<int>				m_dHash;		///< hash into m_dEntries pool

		int							m_iFree;		///< free pairs count
		CSphVector<int>				m_dFree;		///< free pair indexes

	public:
		/// ctor
		explicit CSphFixedHash(int iLength)
		{
			int iBuckets = (2 << sphLog2(iLength - 1)); // less than 50% bucket usage guaranteed
			assert(iLength > 0);
			assert(iLength <= iBuckets);

			m_dEntries.Resize(iLength);
			m_dHash.Resize(iBuckets);
			m_dFree.Resize(iLength);

			Reset();
		}

		/// cleanup
		void Reset()
		{
			ARRAY_FOREACH(i, m_dEntries)
				m_dEntries[i].m_iNext = HASH_DELETED;

			ARRAY_FOREACH(i, m_dHash)
				m_dHash[i] = HASH_LIST_END;

			m_iFree = m_dFree.GetLength();
			ARRAY_FOREACH(i, m_dFree)
				m_dFree[i] = i;
		}

		/// add new entry
		/// returns NULL on success
		/// returns pointer to value if already hashed, or replace it with new one, if insisted.
		T* Add(const T& tValue, const KEY& tKey, bool bReplace = false)
		{
			assert(m_iFree > 0 && "hash overflow");

			// check if it's already hashed
			DWORD uHash = DWORD(HASHFUNC::Hash(tKey)) & (m_dHash.GetLength() - 1);
			int iPrev = -1, iEntry;

			for (iEntry = m_dHash[uHash]; iEntry >= 0; iPrev = iEntry, iEntry = m_dEntries[iEntry].m_iNext)
				if (m_dEntries[iEntry].m_tKey == tKey)
				{
					if (bReplace)
						m_dEntries[iEntry].m_tValue = tValue;
					return &m_dEntries[iEntry].m_tValue;
				}
			assert(iEntry != HASH_DELETED);

			// if it's not, do add
			int iNew = m_dFree[--m_iFree];

			HashEntry_t& tNew = m_dEntries[iNew];
			assert(tNew.m_iNext == HASH_DELETED);

			tNew.m_tKey = tKey;
			tNew.m_tValue = tValue;
			tNew.m_iNext = HASH_LIST_END;

			if (iPrev >= 0)
			{
				assert(m_dEntries[iPrev].m_iNext == HASH_LIST_END);
				m_dEntries[iPrev].m_iNext = iNew;
			}
			else
			{
				assert(m_dHash[uHash] == HASH_LIST_END);
				m_dHash[uHash] = iNew;
			}
			return NULL;
		}

		/// remove entry from hash
		void Remove(const KEY& tKey)
		{
			// check if it's already hashed
			DWORD uHash = DWORD(HASHFUNC::Hash(tKey)) & (m_dHash.GetLength() - 1);
			int iPrev = -1, iEntry;

			for (iEntry = m_dHash[uHash]; iEntry >= 0; iPrev = iEntry, iEntry = m_dEntries[iEntry].m_iNext)
				if (m_dEntries[iEntry].m_tKey == tKey)
				{
					// found, remove it
					assert(m_dEntries[iEntry].m_iNext != HASH_DELETED);
					if (iPrev >= 0)
						m_dEntries[iPrev].m_iNext = m_dEntries[iEntry].m_iNext;
					else
						m_dHash[uHash] = m_dEntries[iEntry].m_iNext;

#ifndef NDEBUG
					m_dEntries[iEntry].m_iNext = HASH_DELETED;
#endif

					m_dFree[m_iFree++] = iEntry;
					return;
				}
			assert(iEntry != HASH_DELETED);
		}

		/// get value pointer by key
		T* operator () (const KEY& tKey) const
		{
			DWORD uHash = DWORD(HASHFUNC::Hash(tKey)) & (m_dHash.GetLength() - 1);
			int iEntry;

			for (iEntry = m_dHash[uHash]; iEntry >= 0; iEntry = m_dEntries[iEntry].m_iNext)
				if (m_dEntries[iEntry].m_tKey == tKey)
					return (T*)&m_dEntries[iEntry].m_tValue;

			assert(iEntry != HASH_DELETED);
			return NULL;
		}
	};


}