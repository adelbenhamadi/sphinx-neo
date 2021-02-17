#pragma once
#include "neo/platform/compat.h"
#include "neo/core/macros.h"
#include "neo/utility/string_tools.h"
#include "neo/core/generic.h"

#include <utility>
#include <cassert>


namespace NEO {


	/// default vector policy
	/// grow 2x and copy using assignment operator on resize
	template < typename T >
	class CSphVectorPolicy
	{
	protected:
		static const int MAGIC_INITIAL_LIMIT = 8;

	public:
		static inline void Copy(T* pNew, T* pData, int iLength)
		{
			for (int i = 0; i < iLength; i++)
				pNew[i] = std::move(pData[i]);
		}

		static inline int Relimit(int iLimit, int iNewLimit)
		{
			if (!iLimit)
				iLimit = MAGIC_INITIAL_LIMIT;
			while (iLimit < iNewLimit)
			{
				iLimit *= 2;
				assert(iLimit > 0);
			}
			return iLimit;
		}
	};

	/// generic vector
	/// (don't even ask why it's not std::vector)
	template < typename T, typename POLICY = CSphVectorPolicy<T> > class CSphVector
	{
	public:
		/// ctor
		CSphVector()
			: m_iLength(0)
			, m_iLimit(0)
			, m_pData(NULL)
		{
		}

		/// ctor with initial size
		explicit CSphVector(int iCount)
			: m_iLength(0)
			, m_iLimit(0)
			, m_pData(NULL)
		{
			Resize(iCount);
		}

		/// copy ctor
		CSphVector(const CSphVector<T>& rhs)
		{
			m_iLength = 0;
			m_iLimit = 0;
			m_pData = NULL;
			*this = rhs;
		}

		/// dtor
		~CSphVector()
		{
			Reset();
		}

		/// add entry
		T& Add()
		{
			if (m_iLength >= m_iLimit)
				Reserve(1 + m_iLength);
			return m_pData[m_iLength++];
		}

		/// add entry
		void Add(const T& tValue)
		{
			assert((&tValue < m_pData || &tValue >= (m_pData + m_iLength)) && "inserting own value (like last()) by ref!");
			if (m_iLength >= m_iLimit)
				Reserve(1 + m_iLength);
			m_pData[m_iLength++] = tValue;
		}

		void Add(T&& tValue)
		{
			assert((&tValue < m_pData || &tValue >= (m_pData + m_iLength)) && "inserting own value (like last()) by ref!");
			if (m_iLength >= m_iLimit)
				Reserve(1 + m_iLength);
			m_pData[m_iLength++] = std::move(tValue);
		}

		/// add N more entries, and return a pointer to that buffer
		T* AddN(int iCount)
		{
			if (m_iLength + iCount > m_iLimit)
				Reserve(m_iLength + iCount);
			m_iLength += iCount;
			return m_pData + m_iLength - iCount;
		}

		/// add unique entry (ie. do not add if equal to last one)
		void AddUnique(const T& tValue)
		{
			assert((&tValue < m_pData || &tValue >= (m_pData + m_iLength)) && "inserting own value (like last()) by ref!");
			if (m_iLength >= m_iLimit)
				Reserve(1 + m_iLength);

			if (m_iLength == 0 || m_pData[m_iLength - 1] != tValue)
				m_pData[m_iLength++] = tValue;
		}

		/// get first entry ptr
		T* Begin()
		{
			return m_iLength ? m_pData : NULL;
		}

		/// get first entry ptr
		const T* Begin() const
		{
			return m_iLength ? m_pData : NULL;
		}

		/// make happy C++11 ranged for loops
		T* begin()
		{
			return Begin();
		}

		const T* begin() const
		{
			return Begin();
		}

		T* end()
		{
			return m_iLength ? m_pData + m_iLength : NULL;
		}

		const T* end() const
		{
			return m_iLength ? m_pData + m_iLength : NULL;
		}

		/// get last entry
		T& Last()
		{
			return (*this)[m_iLength - 1];
		}

		/// get last entry
		const T& Last() const
		{
			return (*this)[m_iLength - 1];
		}

		/// remove element by index
		void Remove(int iIndex)
		{
			assert(iIndex >= 0 && iIndex < m_iLength);

			m_iLength--;
			for (int i = iIndex; i < m_iLength; i++)
				m_pData[i] = m_pData[i + 1];
		}

		/// remove element by index, swapping it with the tail
		void RemoveFast(int iIndex)
		{
			assert(iIndex >= 0 && iIndex < m_iLength);
			if (iIndex != --m_iLength)
				Swap(m_pData[iIndex], m_pData[m_iLength]);
		}

		/// remove element by value (warning, linear O(n) search)
		bool RemoveValue(T tValue)
		{
			for (int i = 0; i < m_iLength; i++)
				if (m_pData[i] == tValue)
				{
					Remove(i);
					return true;
				}
			return false;
		}

		/// remove element by value (warning, linear O(n) search)
		template < typename FUNCTOR, typename U >
		bool RemoveValue(FUNCTOR COMP, U tValue)
		{
			for (int i = 0; i < m_iLength; i++)
				if (COMP.IsEq(m_pData[i], tValue))
				{
					Remove(i);
					return true;
				}
			return false;
		}

		/// pop last value
		const T& Pop()
		{
			assert(m_iLength > 0);
			return m_pData[--m_iLength];
		}

	public:
		/// grow enough to hold that much entries, if needed, but do *not* change the length
		void Reserve(int iNewLimit)
		{
			// check that we really need to be called
			assert(iNewLimit >= 0);
			if (iNewLimit <= m_iLimit)
				return;

			// calc new limit
			m_iLimit = POLICY::Relimit(m_iLimit, iNewLimit);

			// realloc
			// FIXME! optimize for POD case
			T* pNew = NULL;
			if (m_iLimit)
				pNew = new T[m_iLimit];
			__analysis_assume(m_iLength <= m_iLimit);

			POLICY::Copy(pNew, m_pData, m_iLength);
			delete[] m_pData;

			m_pData = pNew;
		}

		/// resize
		void Resize(int iNewLength)
		{
			assert(iNewLength >= 0);
			if ((unsigned int)iNewLength > (unsigned int)m_iLength)
				Reserve(iNewLength);
			m_iLength = iNewLength;
		}

		/// reset
		void Reset()
		{
			m_iLength = 0;
			m_iLimit = 0;
			SafeDeleteArray(m_pData);
		}

		/// query current length, in elements
		inline int GetLength() const
		{
			return m_iLength;
		}

		/// query current reserved size, in elements
		inline int GetLimit() const
		{
			return m_iLimit;
		}

		/// query currently used RAM, in bytes
		inline int GetSizeBytes() const
		{
			return m_iLimit * sizeof(T);
		}

	public:
		/// filter unique
		void Uniq()
		{
			if (!m_iLength)
				return;

			Sort();
			int iLeft = sphUniq(m_pData, m_iLength);
			Resize(iLeft);
		}

		/// default sort
		void Sort(int iStart = 0, int iEnd = -1)
		{
			Sort(SphLess_T<T>(), iStart, iEnd);
		}

		/// default reverse sort
		void RSort(int iStart = 0, int iEnd = -1);

		
		/// generic sort
		template < typename F > void Sort(F COMP, int iStart = 0, int iEnd = -1);

		/// accessor by forward index
		const T& operator [] (int iIndex) const
		{
			assert(iIndex >= 0 && iIndex < m_iLength);
			return m_pData[iIndex];
		}

		/// accessor by forward index
		T& operator [] (int iIndex)
		{
			assert(iIndex >= 0 && iIndex < m_iLength);
			return m_pData[iIndex];
		}

		/// copy
		const CSphVector<T>& operator = (const CSphVector<T>& rhs)
		{
			Reset();

			m_iLength = rhs.m_iLength;
			m_iLimit = rhs.m_iLimit;
			if (m_iLimit)
				m_pData = new T[m_iLimit];
			__analysis_assume(m_iLength <= m_iLimit);
			for (int i = 0; i < rhs.m_iLength; i++)
				m_pData[i] = rhs.m_pData[i];

			return *this;
		}

		/// move
		const CSphVector<T>& operator= (CSphVector<T>&& rhs)
		{
			Reset();

			m_iLength = rhs.m_iLength;
			m_iLimit = rhs.m_iLimit;
			m_pData = rhs.m_pData;

			rhs.m_pData = nullptr;
			rhs.m_iLength = 0;
			rhs.m_iLimit = 0;

			return *this;
		}

		/// swap
		void SwapData(CSphVector<T, POLICY>& rhs)
		{
			Swap(m_iLength, rhs.m_iLength);
			Swap(m_iLimit, rhs.m_iLimit);
			Swap(m_pData, rhs.m_pData);
		}

		/// leak
		T* LeakData()
		{
			T* pData = m_pData;
			m_pData = NULL;
			Reset();
			return pData;
		}

		/// generic binary search
		/// assumes that the array is sorted in ascending order
		template < typename U, typename PRED >
		const T* BinarySearch(const PRED& tPred, U tRef) const
		{
			return sphBinarySearch(m_pData, m_pData + m_iLength - 1, tPred, tRef);
		}

		/// generic binary search
		/// assumes that the array is sorted in ascending order
		const T* BinarySearch(T tRef) const
		{
			return sphBinarySearch(m_pData, m_pData + m_iLength - 1, tRef);
		}

		/// generic linear search
		bool Contains(T tRef) const
		{
			for (int i = 0; i < m_iLength; i++)
				if (m_pData[i] == tRef)
					return true;
			return false;
		}

		/// generic linear search
		template < typename FUNCTOR, typename U >
		bool Contains(FUNCTOR COMP, U tValue)
		{
			for (int i = 0; i < m_iLength; i++)
				if (COMP.IsEq(m_pData[i], tValue))
					return true;
			return false;
		}

		/// fill with given value
		void Fill(const T& rhs)
		{
			for (int i = 0; i < m_iLength; i++)
				m_pData[i] = rhs;
		}

		/// insert into a middle
		void Insert(int iIndex, const T& tValue)
		{
			assert(iIndex >= 0 && iIndex <= m_iLength);

			if (m_iLength >= m_iLimit)
				Reserve(m_iLength + 1);

			// FIXME! this will not work for SwapVector
			for (int i = m_iLength - 1; i >= iIndex; i--)
				m_pData[i + 1] = m_pData[i];
			m_pData[iIndex] = tValue;
			m_iLength++;
		}

	protected:
		int		m_iLength;		///< entries actually used
		int		m_iLimit;		///< entries allocated
		T* m_pData;		///< entries
	};


#define ARRAY_FOREACH(_index,_array) \
	for ( int _index=0; _index<_array.GetLength(); _index++ )

#define ARRAY_FOREACH_COND(_index,_array,_cond) \
	for ( int _index=0; _index<_array.GetLength() && (_cond); _index++ )

#define ARRAY_ANY(_res,_array,_cond) \
	false; \
	for ( int _any=0; _any<_array.GetLength() && !_res; _any++ ) \
		_res |= ( _cond ); \

#define ARRAY_ALL(_res,_array,_cond) \
	true; \
	for ( int _all=0; _all<_array.GetLength() && _res; _all++ ) \
		_res &= ( _cond ); \




	//////////////////////////////////////////////////////////////////////////



	/// swap-vector policy (for non-copyable classes)
	/// use Swap() instead of assignment on resize
	template < typename T >
	class CSphSwapVectorPolicy : public CSphVectorPolicy<T>
	{
	public:
		static inline void Copy(T* pNew, T* pData, size_t iLength)
		{
			for (size_t i = 0; i < iLength; i++)
				Swap(pNew[i], pData[i]);
		}
	};

	/// tight-vector policy
	/// grow only 1.2x on resize (not 2x) starting from a certain threshold
	template < typename T >
	class CSphTightVectorPolicy : public CSphVectorPolicy<T>
	{
	protected:
		static const size_t SLOW_GROW_TRESHOLD = 1024;

	public:
		static inline size_t Relimit(size_t iLimit, size_t iNewLimit)
		{
			if (!iLimit)
				iLimit = CSphVectorPolicy<T>::MAGIC_INITIAL_LIMIT;
			while (iLimit < iNewLimit && iLimit < SLOW_GROW_TRESHOLD)
			{
				iLimit *= 2;
				assert(iLimit > 0);
			}
			while (iLimit < iNewLimit)
			{
				iLimit = (size_t)(iLimit * 1.2f);
				assert(iLimit > 0);
			}
			return iLimit;
		}
	};

	/// swap-vector
	template < typename T >
	class CSphSwapVector : public CSphVector < T, CSphSwapVectorPolicy<T> >
	{
	};

	/// tight-vector
	template < typename T >
	class CSphTightVector : public CSphVector < T, CSphTightVectorPolicy<T> >
	{
	};

	//////////////////////////////////////////////////////////////////////////

	/// generic dynamic bitvector
	/// with a preallocated part for small-size cases, and a dynamic route for big-size ones
	class CSphBitvec
	{
	protected:
		DWORD* m_pData;
		DWORD		m_uStatic[4];
		size_t			m_iElements;

	public:
		CSphBitvec()
			: m_pData(NULL)
			, m_iElements(0)
		{}

		explicit CSphBitvec(size_t iElements)
		{
			Init(iElements);
		}

		~CSphBitvec()
		{
			if (m_pData != m_uStatic)
				SafeDeleteArray(m_pData);
		}

		/// copy ctor
		CSphBitvec(const CSphBitvec& rhs)
		{
			m_pData = NULL;
			m_iElements = 0;
			*this = rhs;
		}

		/// copy
		const CSphBitvec& operator = (const CSphBitvec& rhs)
		{
			if (m_pData != m_uStatic)
				SafeDeleteArray(m_pData);

			Init(rhs.m_iElements);
			memcpy(m_pData, rhs.m_pData, sizeof(m_uStatic[0]) * GetSize());

			return *this;
		}

		void Init(size_t iElements)
		{
			assert(iElements >= 0);
			m_iElements = iElements;
			if (iElements > sizeof(m_uStatic) * 8)
			{
				auto iSize = GetSize();
				m_pData = new DWORD[iSize];
			}
			else
			{
				m_pData = m_uStatic;
			}
			Clear();
		}

		void Clear()
		{
			auto iSize = GetSize();
			memset(m_pData, 0, sizeof(DWORD) * iSize);
		}

		bool BitGet(size_t iIndex) const
		{
			assert(m_pData);
			assert(iIndex >= 0);
			assert(iIndex < m_iElements);
			return (m_pData[iIndex >> 5] & (1UL << (iIndex & 31))) != 0; // NOLINT
		}

		void BitSet(size_t iIndex)
		{
			assert(iIndex >= 0);
			assert(iIndex < m_iElements);
			m_pData[iIndex >> 5] |= (1UL << (iIndex & 31)); // NOLINT
		}

		void BitClear(size_t iIndex)
		{
			assert(iIndex >= 0);
			assert(iIndex < m_iElements);
			m_pData[iIndex >> 5] &= ~(1UL << (iIndex & 31)); // NOLINT
		}

		const DWORD* Begin() const
		{
			return m_pData;
		}

		DWORD* Begin()
		{
			return m_pData;
		}

		size_t GetSize() const
		{
			return (m_iElements + 31) / 32;
		}

		size_t GetBits() const
		{
			return m_iElements;
		}

		size_t BitCount() const
		{
			auto iBitSet = 0;
			for (size_t i = 0; i < GetSize(); i++)
				iBitSet += sphBitCount(m_pData[i]);

			return iBitSet;
		}
	};

	/////////////////////


	struct PoolPtrs_t
	{
		const DWORD* m_pMva;
		const BYTE* m_pStrings;
		bool			m_bArenaProhibit;

		PoolPtrs_t()
			: m_pMva(NULL)
			, m_pStrings(NULL)
			, m_bArenaProhibit(false)
		{}
	};

	class CSphTaggedVector
	{
	public:
		const PoolPtrs_t& operator [] (int iTag) const
		{
			return m_dPool[iTag & 0x7FFFFFF];
		}
		PoolPtrs_t& operator [] (int iTag)
		{
			return m_dPool[iTag & 0x7FFFFFF];
		}

		void Resize(int iSize)
		{
			m_dPool.Resize(iSize);
		}

	private:
		CSphVector<PoolPtrs_t> m_dPool;
	};

	class CSphFreeList
	{
	private:
		CSphTightVector<int>	m_dFree;
		int						m_iNextFree;
#ifndef NDEBUG
		int						m_iSize;
#endif

	public:
		CSphFreeList()
			: m_iNextFree(0)
#ifndef NDEBUG
			, m_iSize(0)
#endif
		{}

		void Reset(int iSize)
		{
#ifndef NDEBUG
			m_iSize = iSize;
#endif
			m_iNextFree = 0;
			m_dFree.Reserve(iSize);
		}

		int Get()
		{
			int iRes = -1;
			if (m_dFree.GetLength())
				iRes = m_dFree.Pop();
			else
				iRes = m_iNextFree++;
			assert(iRes >= 0 && iRes < m_iSize);
			return iRes;
		}

		void Free(int iIndex)
		{
			assert(iIndex >= 0 && iIndex < m_iSize);
			m_dFree.Add(iIndex);
		}
	};


	///////////////////////

	/// find a value-enclosing span in a sorted vector (aka an index at which vec[i] <= val < vec[i+1])
	template < typename T, typename U >
	/*static*/ int FindSpan(const CSphVector<T>& dVec, U tRef, int iSmallTreshold = 8);
	
	


	

}