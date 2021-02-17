#pragma once
#include "neo/int/types.h"

#include <climits>

namespace NEO {

	/// simple dynamic hash
	/// implementation: fixed-size bucket + chaining
	/// keeps the order, so Iterate() return the entries in the order they was inserted
	/// WARNING: slow copy
	template < typename T, typename KEY, typename HASHFUNC, int LENGTH >
	class CSphOrderedHash
	{
	protected:
		struct HashEntry_t
		{
			KEY				m_tKey;				///< key, owned by the hash
			T 				m_tValue;			///< data, owned by the hash
			HashEntry_t* m_pNextByHash;		///< next entry in hash list
			HashEntry_t* m_pPrevByOrder;		///< prev entry in the insertion order
			HashEntry_t* m_pNextByOrder;		///< next entry in the insertion order
		};


	protected:
		HashEntry_t* m_dHash[LENGTH];		///< all the hash entries
		HashEntry_t* m_pFirstByOrder;		///< first entry in the insertion order
		HashEntry_t* m_pLastByOrder;			///< last entry in the insertion order
		int				m_iLength;				///< entries count

	protected:
		/// find entry by key
		HashEntry_t* FindByKey(const KEY& tKey) const
		{
			unsigned int uHash = ((unsigned int)HASHFUNC::Hash(tKey)) % LENGTH;
			HashEntry_t* pEntry = m_dHash[uHash];

			while (pEntry)
			{
				if (pEntry->m_tKey == tKey)
					return pEntry;
				pEntry = pEntry->m_pNextByHash;
			}
			return NULL;
		}

		HashEntry_t* AddImpl(const KEY& tKey)
		{
			unsigned int uHash = ((unsigned int)HASHFUNC::Hash(tKey)) % LENGTH;

			// check if this key is already hashed
			HashEntry_t* pEntry = m_dHash[uHash];
			HashEntry_t** ppEntry = &m_dHash[uHash];
			while (pEntry)
			{
				if (pEntry->m_tKey == tKey)
					return nullptr;

				ppEntry = &pEntry->m_pNextByHash;
				pEntry = pEntry->m_pNextByHash;
			}

			// it's not; let's add the entry
			assert(!pEntry);
			assert(!*ppEntry);

			pEntry = new HashEntry_t;
			pEntry->m_tKey = tKey;
			pEntry->m_pNextByHash = NULL;
			pEntry->m_pPrevByOrder = NULL;
			pEntry->m_pNextByOrder = NULL;

			*ppEntry = pEntry;

			if (!m_pFirstByOrder)
				m_pFirstByOrder = pEntry;

			if (m_pLastByOrder)
			{
				assert(!m_pLastByOrder->m_pNextByOrder);
				assert(!pEntry->m_pNextByOrder);
				m_pLastByOrder->m_pNextByOrder = pEntry;
				pEntry->m_pPrevByOrder = m_pLastByOrder;
			}
			m_pLastByOrder = pEntry;

			m_iLength++;
			return pEntry;
		}

	public:
		/// ctor
		CSphOrderedHash()
			: m_pFirstByOrder(NULL)
			, m_pLastByOrder(NULL)
			, m_iLength(0)
			, m_pIterator(NULL)
		{
			for (int i = 0; i < LENGTH; i++)
				m_dHash[i] = NULL;
		}

		/// dtor
		~CSphOrderedHash()
		{
			Reset();
		}

		/// reset
		void Reset()
		{
			assert((m_pFirstByOrder && m_iLength) || (!m_pFirstByOrder && !m_iLength));
			HashEntry_t* pKill = m_pFirstByOrder;
			while (pKill)
			{
				HashEntry_t* pNext = pKill->m_pNextByOrder;
				SafeDelete(pKill);
				pKill = pNext;
			}

			for (int i = 0; i < LENGTH; i++)
				m_dHash[i] = 0;

			m_pFirstByOrder = NULL;
			m_pLastByOrder = NULL;
			m_pIterator = NULL;
			m_iLength = 0;
		}

		/// add new entry
		/// returns true on success
		/// returns false if this key is already hashed
		bool Add(T&& tValue, const KEY& tKey)
		{
			// check if this key is already hashed
			HashEntry_t* pEntry = AddImpl(tKey);
			if (!pEntry)
				return false;
			pEntry->m_tValue = std::move(tValue);
			return true;
		}

		bool Add(const T& tValue, const KEY& tKey)
		{
			// check if this key is already hashed
			HashEntry_t* pEntry = AddImpl(tKey);
			if (!pEntry)
				return false;
			pEntry->m_tValue = tValue;
			return true;
		}

		/// add new entry
		/// returns the pointer to just inserted or previously cached (if dupe) value
		T& AddUnique(const KEY& tKey)
		{
			unsigned int uHash = ((unsigned int)HASHFUNC::Hash(tKey)) % LENGTH;

			// check if this key is already hashed
			HashEntry_t* pEntry = m_dHash[uHash];
			HashEntry_t** ppEntry = &m_dHash[uHash];
			while (pEntry)
			{
				if (pEntry->m_tKey == tKey)
					return pEntry->m_tValue;

				ppEntry = &pEntry->m_pNextByHash;
				pEntry = pEntry->m_pNextByHash;
			}

			// it's not; let's add the entry
			assert(!pEntry);
			assert(!*ppEntry);

			pEntry = new HashEntry_t;
			pEntry->m_tKey = tKey;
			pEntry->m_pNextByHash = NULL;
			pEntry->m_pPrevByOrder = NULL;
			pEntry->m_pNextByOrder = NULL;

			*ppEntry = pEntry;

			if (!m_pFirstByOrder)
				m_pFirstByOrder = pEntry;

			if (m_pLastByOrder)
			{
				assert(!m_pLastByOrder->m_pNextByOrder);
				assert(!pEntry->m_pNextByOrder);
				m_pLastByOrder->m_pNextByOrder = pEntry;
				pEntry->m_pPrevByOrder = m_pLastByOrder;
			}
			m_pLastByOrder = pEntry;

			m_iLength++;
			return pEntry->m_tValue;
		}

		/// delete an entry
		bool Delete(const KEY& tKey)
		{
			unsigned int uHash = ((unsigned int)HASHFUNC::Hash(tKey)) % LENGTH;
			HashEntry_t* pEntry = m_dHash[uHash];

			HashEntry_t* pPrevEntry = NULL;
			HashEntry_t* pToDelete = NULL;
			while (pEntry)
			{
				if (pEntry->m_tKey == tKey)
				{
					pToDelete = pEntry;
					if (pPrevEntry)
						pPrevEntry->m_pNextByHash = pEntry->m_pNextByHash;
					else
						m_dHash[uHash] = pEntry->m_pNextByHash;

					break;
				}

				pPrevEntry = pEntry;
				pEntry = pEntry->m_pNextByHash;
			}

			if (!pToDelete)
				return false;

			if (pToDelete->m_pPrevByOrder)
				pToDelete->m_pPrevByOrder->m_pNextByOrder = pToDelete->m_pNextByOrder;
			else
				m_pFirstByOrder = pToDelete->m_pNextByOrder;

			if (pToDelete->m_pNextByOrder)
				pToDelete->m_pNextByOrder->m_pPrevByOrder = pToDelete->m_pPrevByOrder;
			else
				m_pLastByOrder = pToDelete->m_pPrevByOrder;

			// step the iterator one item back - to gracefully hold deletion in iteration cycle
			if (pToDelete == m_pIterator)
				m_pIterator = pToDelete->m_pPrevByOrder;

			SafeDelete(pToDelete);
			--m_iLength;

			return true;
		}

		/// check if key exists
		bool Exists(const KEY& tKey) const
		{
			return FindByKey(tKey) != NULL;
		}

		/// get value pointer by key
		T* operator () (const KEY& tKey) const
		{
			HashEntry_t* pEntry = FindByKey(tKey);
			return pEntry ? &pEntry->m_tValue : NULL;
		}

		/// get value reference by key, asserting that the key exists in hash
		T& operator [] (const KEY& tKey) const
		{
			HashEntry_t* pEntry = FindByKey(tKey);
			assert(pEntry && "hash missing value in operator []");

			return pEntry->m_tValue;
		}

		/// get pointer to key storage
		const KEY* GetKeyPtr(const KEY& tKey) const
		{
			HashEntry_t* pEntry = FindByKey(tKey);
			return pEntry ? &pEntry->m_tKey : NULL;
		}

		/// copying
		const CSphOrderedHash<T, KEY, HASHFUNC, LENGTH>& operator = (const CSphOrderedHash<T, KEY, HASHFUNC, LENGTH>& rhs)
		{
			if (this != &rhs)
			{
				Reset();

				rhs.IterateStart();
				while (rhs.IterateNext())
					Add(rhs.IterateGet(), rhs.IterateGetKey());
			}
			return *this;
		}

		/// copying ctor
		CSphOrderedHash<T, KEY, HASHFUNC, LENGTH>(const CSphOrderedHash<T, KEY, HASHFUNC, LENGTH>& rhs)
			: m_pFirstByOrder(NULL)
			, m_pLastByOrder(NULL)
			, m_iLength(0)
			, m_pIterator(NULL)
		{
			for (int i = 0; i < LENGTH; i++)
				m_dHash[i] = NULL;
			*this = rhs;
		}

		/// length query
		int GetLength() const
		{
			return m_iLength;
		}

	public:
		/// start iterating
		void IterateStart() const
		{
			m_pIterator = NULL;
		}

		/// start iterating from key element
		bool IterateStart(const KEY& tKey) const
		{
			m_pIterator = FindByKey(tKey);
			return m_pIterator != NULL;
		}

		/// go to next existing entry
		bool IterateNext() const
		{
			m_pIterator = m_pIterator ? m_pIterator->m_pNextByOrder : m_pFirstByOrder;
			return m_pIterator != NULL;
		}

		/// get entry value
		T& IterateGet() const
		{
			assert(m_pIterator);
			return m_pIterator->m_tValue;
		}

		/// get entry key
		const KEY& IterateGetKey() const
		{
			assert(m_pIterator);
			return m_pIterator->m_tKey;
		}

		/// go to next existing entry in terms of external independed iterator
		bool IterateNext(void** ppCookie) const
		{
			HashEntry_t** ppIterator = reinterpret_cast <HashEntry_t**> (ppCookie);
			*ppIterator = (*ppIterator) ? (*ppIterator)->m_pNextByOrder : m_pFirstByOrder;
			return (*ppIterator) != NULL;
		}

		/// get entry value in terms of external independed iterator
		static T& IterateGet(void** ppCookie)
		{
			assert(ppCookie);
			HashEntry_t** ppIterator = reinterpret_cast <HashEntry_t**> (ppCookie);
			assert(*ppIterator);
			return (*ppIterator)->m_tValue;
		}

		/// get entry key in terms of external independed iterator
		static const KEY& IterateGetKey(void** ppCookie)
		{
			assert(ppCookie);
			HashEntry_t** ppIterator = reinterpret_cast <HashEntry_t**> (ppCookie);
			assert(*ppIterator);
			return (*ppIterator)->m_tKey;
		}


	private:
		/// current iterator
		mutable HashEntry_t* m_pIterator;
	};

	/// very popular and so, moved here
	/// use integer values as hash values (like document IDs, for example)
	struct IdentityHash_fn
	{
		template <typename INT>
		static inline INT Hash(INT iValue) { return iValue; }
	};


	/// simple open-addressing hash
	/// for now, with int64_t keys (for docids), maybe i will templatize this later
	template < typename VALUE >
	class CSphHash
	{
	protected:
		typedef int64_t		KEY;
		static const KEY	NO_ENTRY = LLONG_MAX;		///< final entry in a chain, we can now safely stop searching
		static const KEY	DEAD_ENTRY = LLONG_MAX - 1;	///< dead entry in a chain, more alive entries may follow

		struct Entry
		{
			KEY		m_Key;
			VALUE	m_Value;

			Entry() : m_Key(NO_ENTRY) {}
		};

		Entry* m_pHash;	///< hash entries
		int			m_iSize;	///< total hash size
		int			m_iUsed;	///< how many entries are actually used
		int			m_iMaxUsed;	///< resize threshold

	public:
		/// initialize hash of a given initial size
		explicit CSphHash(int iSize = 256)
		{
			m_pHash = NULL;
			Reset(iSize);
		}

		/// reset to a given size
		void Reset(int iSize)
		{
			SafeDeleteArray(m_pHash);
			if (iSize <= 0)
			{
				m_iSize = m_iUsed = m_iMaxUsed = 0;
				return;
			}
			iSize = (1 << sphLog2(iSize - 1));
			m_pHash = new Entry[iSize];
			m_iSize = iSize;
			m_iUsed = 0;
			m_iMaxUsed = GetMaxLoad(iSize);
		}

		~CSphHash()
		{
			SafeDeleteArray(m_pHash);
		}

		/// hash me the key, quick!
		static inline DWORD GetHash(KEY k)
		{
			return (DWORD(k) * 0x607cbb77UL) ^ (k >> 32);
		}

		/// acquire value by key (ie. get existing hashed value, or add a new default value)
		VALUE& Acquire(KEY k)
		{
			assert(k != NO_ENTRY && k != DEAD_ENTRY);
			DWORD uHash = GetHash(k);
			int iIndex = uHash & (m_iSize - 1);
			int iDead = -1;
			for (;; )
			{
				// found matching key? great, return the value
				Entry* p = m_pHash + iIndex;
				if (p->m_Key == k)
					return p->m_Value;

				// no matching keys? add it
				if (p->m_Key == NO_ENTRY)
				{
					// not enough space? grow the hash and force rescan
					if (m_iUsed >= m_iMaxUsed)
					{
						Grow();
						iIndex = uHash & (m_iSize - 1);
						iDead = -1;
						continue;
					}

					// did we walk past a dead entry while probing? if so, lets reuse it
					if (iDead >= 0)
						p = m_pHash + iDead;

					// store the newly added key
					p->m_Key = k;
					m_iUsed++;
					return p->m_Value;
				}

				// is this a dead entry? store its index for (possible) reuse
				if (p->m_Key == DEAD_ENTRY)
					iDead = iIndex;

				// no match so far, keep probing
				iIndex = (iIndex + 1) & (m_iSize - 1);
			}
		}

		/// find an existing value by key
		VALUE* Find(KEY k) const
		{
			Entry* e = FindEntry(k);
			return e ? &e->m_Value : NULL;
		}

		/// add or fail (if key already exists)
		bool Add(KEY k, const VALUE& v)
		{
			int u = m_iUsed;
			VALUE& x = Acquire(k);
			if (u == m_iUsed)
				return false; // found an existing value by k, can not add v
			x = v;
			return true;
		}

		/// find existing value, or add a new value
		VALUE& FindOrAdd(KEY k, const VALUE& v)
		{
			int u = m_iUsed;
			VALUE& x = Acquire(k);
			if (u != m_iUsed)
				x = v; // did not find an existing value by k, so add v
			return x;
		}

		/// delete by key
		bool Delete(KEY k)
		{
			Entry* e = FindEntry(k);
			if (e)
				e->m_Key = DEAD_ENTRY;
			return e != NULL;
		}

		/// get number of inserted key-value pairs
		int GetLength() const
		{
			return m_iUsed;
		}

		/// iterate the hash by entry index, starting from 0
		/// finds the next alive key-value pair starting from the given index
		/// returns that pair and updates the index on success
		/// returns NULL when the hash is over
		VALUE* Iterate(int* pIndex, KEY* pKey) const
		{
			if (!pIndex || *pIndex < 0)
				return NULL;
			for (int i = *pIndex; i < m_iSize; i++)
			{
				if (m_pHash[i].m_Key != NO_ENTRY && m_pHash[i].m_Key != DEAD_ENTRY)
				{
					*pIndex = i + 1;
					if (pKey)
						*pKey = m_pHash[i].m_Key;
					return &m_pHash[i].m_Value;
				}
			}
			return NULL;
		}

	protected:
		/// get max load, ie. max number of actually used entries at given size
		int GetMaxLoad(int iSize) const
		{
			return (int)(iSize * 0.95f);
		}

		/// we are overloaded, lets grow 2x and rehash
		void Grow()
		{
			Entry* pNew = new Entry[2 * Max(m_iSize, 8)];

			for (int i = 0; i < m_iSize; i++)
				if (m_pHash[i].m_Key != NO_ENTRY && m_pHash[i].m_Key != DEAD_ENTRY)
				{
					int j = GetHash(m_pHash[i].m_Key) & (2 * m_iSize - 1);
					while (pNew[j].m_Key != NO_ENTRY)
						j = (j + 1) & (2 * m_iSize - 1);
					pNew[j] = m_pHash[i];
				}

			SafeDeleteArray(m_pHash);
			m_pHash = pNew;
			m_iSize *= 2;
			m_iMaxUsed = GetMaxLoad(m_iSize);
		}

		/// find (and do not touch!) entry by key
		inline Entry* FindEntry(KEY k) const
		{
			assert(k != NO_ENTRY && k != DEAD_ENTRY);
			DWORD uHash = GetHash(k);
			int iIndex = uHash & (m_iSize - 1);
			for (;; )
			{
				Entry* p = m_pHash + iIndex;
				if (p->m_Key == k)
					return p;
				if (p->m_Key == NO_ENTRY)
					return NULL;
				iIndex = (iIndex + 1) & (m_iSize - 1);
			}
		}
	};


	template<> inline CSphHash<int>::Entry::Entry() : m_Key(NO_ENTRY), m_Value(0) {}
	template<> inline CSphHash<DWORD>::Entry::Entry() : m_Key(NO_ENTRY), m_Value(0) {}
	template<> inline CSphHash<float>::Entry::Entry() : m_Key(NO_ENTRY), m_Value(0.0f) {}
	template<> inline CSphHash<int64_t>::Entry::Entry() : m_Key(NO_ENTRY), m_Value(0) {}
	template<> inline CSphHash<uint64_t>::Entry::Entry() : m_Key(NO_ENTRY), m_Value(0) {}


	/// string hash function
	struct CSphStrHashFunc
	{
		static int Hash(const CSphString& sKey);
	};

	/// small hash with string keys
	template < typename T >
	class SmallStringHash_T : public CSphOrderedHash < T, CSphString, CSphStrHashFunc, 256 > {};

}