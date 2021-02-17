#pragma once
#include "neo/core/config.h"
#include "neo/core/macros.h"
#include "neo/int/non_copyable.h"

#include <cstdint>
#include <cassert>

namespace NEO {
	template <typename T> T Min(T a, T b) { return a < b ? a : b; }
	template <typename T, typename U> T Min(T a, U b)
	{
		STATIC_ASSERT(sizeof(U) <= sizeof(T), WIDEST_ARG_FIRST);
		return a < b ? a : b;
	}
	template <typename T> T Max(T a, T b) { return a < b ? b : a; }
	template <typename T, typename U> T Max(T a, U b)
	{
		STATIC_ASSERT(sizeof(U) <= sizeof(T), WIDEST_ARG_FIRST);
		return a < b ? b : a;
	}
	
	/// swap
	template < typename T > inline void Swap(T& v1, T& v2)
	{
		T temp = v1;
		v1 = v2;
		v2 = temp;
	}


	//////////////////////////////////////////////////////////////////////////////

	/// generic comparator
	template < typename T >
	struct SphLess_T
	{
		inline bool IsLess(const T& a, const T& b) const
		{
			return a < b;
		}
	};


	/// generic comparator
	template < typename T >
	struct SphGreater_T
	{
		inline bool IsLess(const T& a, const T& b) const
		{
			return b < a;
		}
	};


	/// generic comparator
	template < typename T, typename C >
	struct SphMemberLess_T
	{
		const T C::* m_pMember;

		explicit SphMemberLess_T(T C::* pMember)
			: m_pMember(pMember)
		{}

		inline bool IsLess(const C& a, const C& b) const
		{
			return ((&a)->*m_pMember) < ((&b)->*m_pMember);
		}
	};

	template < typename T, typename C >
	inline SphMemberLess_T<T, C>
		sphMemberLess(T C::* pMember);


	/// generic accessor
	template < typename T >
	struct SphAccessor_T
	{
		typedef T MEDIAN_TYPE;

		MEDIAN_TYPE& Key(T* a) const
		{
			return *a;
		}

		void CopyKey(MEDIAN_TYPE* pMed, T* pVal) const
		{
			*pMed = Key(pVal);
		}

		void Swap(T* a, T* b) const;
		

		T* Add(T* p, int i) const
		{
			return p + i;
		}

		int Sub(T* b, T* a) const
		{
			return (int)(b - a);
		}
	};


	/// heap sort helper
	template < typename T, typename U, typename V >
	void sphSiftDown(T* pData, int iStart, int iEnd, U COMP, V ACC);


	/// heap sort
	template < typename T, typename U, typename V >
	void sphHeapSort(T* pData, int iCount, U COMP, V ACC);


	/// generic sort
	template < typename T, typename U, typename V >
	void sphSort(T* pData, int iCount, U COMP, V ACC);


	template < typename T, typename U >
	void sphSort(T* pData, int iCount, U COMP);


	template < typename T >
	void sphSort(T* pData, int iCount);

	//////////////////////////////////////////////////////////////////////////

	/// member functor, wraps object member access
	template < typename T, typename CLASS >
	struct SphMemberFunctor_T
	{
		const T CLASS::* m_pMember;

		explicit			SphMemberFunctor_T(T CLASS::* pMember) : m_pMember(pMember) {}
		const T& operator () (const CLASS& arg) const { return (&arg)->*m_pMember; }

		inline bool IsLess(const CLASS& a, const CLASS& b) const
		{
			return (&a)->*m_pMember < (&b)->*m_pMember;
		}

		inline bool IsEq(const CLASS& a, T b)
		{
			return ((&a)->*m_pMember) == b;
		}
	};


	/// handy member functor generator
	/// this sugar allows you to write like this
	/// dArr.Sort ( bind ( &CSphType::m_iMember ) );
	/// dArr.BinarySearch ( bind ( &CSphType::m_iMember ), iValue );
	template < typename T, typename CLASS >
	inline SphMemberFunctor_T < T, CLASS >
		bind(T CLASS::* ptr);


	/// identity functor
	template < typename T >
	struct SphIdentityFunctor_T
	{
		const T& operator () (const T& arg) const { return arg; }
	};

	//////////////////////////////////////////////////////////////////////////

	/// generic binary search
	template < typename T, typename U, typename PRED >
	T* sphBinarySearch(T* pStart, T* pEnd, const PRED& tPred, U tRef);


	/// generic binary search
	template < typename T >
	T* sphBinarySearch(T* pStart, T* pEnd, T& tRef);

	/// generic uniq
	template < typename T, typename T_COUNTER >
	T_COUNTER sphUniq(T* pData, T_COUNTER iCount);


	/// dynamically allocated fixed-size vector
	template < typename T >
	class CSphFixedVector : public ISphNoncopyable
	{
	protected:
		T* m_pData;
		int			m_iSize;

	public:
		explicit CSphFixedVector(int iSize)
			: m_iSize(iSize)
		{
			assert(iSize >= 0);
			m_pData = (iSize > 0) ? new T[iSize] : NULL;
		}

		~CSphFixedVector()
		{
			SafeDeleteArray(m_pData);
		}

		CSphFixedVector(CSphFixedVector&& rhs)
			: m_pData(std::move(rhs.m_pData))
			, m_iSize(std::move(rhs.m_iSize))
		{
			rhs.m_pData = nullptr;
			rhs.m_iSize = 0;
		}

		CSphFixedVector& operator= (CSphFixedVector&& rhs)
		{
			if (&rhs != this)
			{
				m_pData = std::move(rhs.m_pData);
				m_iSize = std::move(rhs.m_iSize);

				rhs.m_pData = nullptr;
				rhs.m_iSize = 0;
			}
			return *this;
		}

		T& operator [] (int iIndex) const
		{
			assert(iIndex >= 0 && iIndex < m_iSize);
			return m_pData[iIndex];
		}

		T* Begin() const
		{
			return m_pData;
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
			return m_iSize ? m_pData + m_iSize : NULL;
		}

		const T* end() const
		{
			return m_iSize ? m_pData + m_iSize : NULL;
		}

		T& Last() const
		{
			return (*this)[m_iSize - 1];
		}

		void Reset(int iSize)
		{
			SafeDeleteArray(m_pData);
			assert(iSize >= 0);
			m_pData = (iSize > 0) ? new T[iSize] : NULL;
			m_iSize = iSize;
		}

		int GetLength() const
		{
			return m_iSize;
		}

		int GetSizeBytes() const
		{
			return m_iSize * sizeof(T);
		}

		T* LeakData()
		{
			T* pData = m_pData;
			m_pData = NULL;
			Reset(0);
			return pData;
		}

		/// swap
		void SwapData(CSphFixedVector<T>& rhs)
		{
			Swap(m_pData, rhs.m_pData);
			Swap(m_iSize, rhs.m_iSize);
		}

		void Set(T* pData, int iSize)
		{
			SafeDeleteArray(m_pData);
			m_pData = pData;
			m_iSize = iSize;
		}

		const T* BinarySearch(T tRef) const
		{
			return sphBinarySearch(m_pData, m_pData + m_iSize - 1, tRef);
		}
	};

	// simple circular buffer
	template < typename T >
	class CircularBuffer_T
	{
	public:
		explicit CircularBuffer_T(int iInitialSize = 256, float fGrowFactor = 1.5f)
			: m_dValues(iInitialSize)
			, m_fGrowFactor(fGrowFactor)
			, m_iHead(0)
			, m_iTail(0)
			, m_iUsed(0)
		{}

		CircularBuffer_T(CircularBuffer_T&& rhs)
			: m_dValues(std::move(rhs.m_dValues))
			, m_fGrowFactor(rhs.m_fGrowFactor)
			, m_iHead(rhs.m_iHead)
			, m_iTail(rhs.m_iTail)
			, m_iUsed(rhs.m_iUsed)
		{
			rhs.m_iHead = 0;
			rhs.m_iTail = 0;
			rhs.m_iUsed = 0;
		}

		CircularBuffer_T& operator= (CircularBuffer_T&& rhs)
		{
			if (&rhs != this)
			{
				m_dValues = std::move(rhs.m_dValues);
				m_fGrowFactor = rhs.m_fGrowFactor;
				m_iHead = rhs.m_iHead;
				m_iTail = rhs.m_iTail;
				m_iUsed = rhs.m_iUsed;

				rhs.m_iHead = 0;
				rhs.m_iTail = 0;
				rhs.m_iUsed = 0;
			}
			return *this;
		}


		void Push(const T& tValue)
		{
			if (m_iUsed == m_dValues.GetLength())
				Resize(int(m_iUsed * m_fGrowFactor));

			m_dValues[m_iTail] = tValue;
			m_iTail = (m_iTail + 1) % m_dValues.GetLength();
			m_iUsed++;
		}

		T& Push()
		{
			if (m_iUsed == m_dValues.GetLength())
				Resize(int(m_iUsed * m_fGrowFactor));

			int iOldTail = m_iTail;
			m_iTail = (m_iTail + 1) % m_dValues.GetLength();
			m_iUsed++;

			return m_dValues[iOldTail];
		}


		T& Pop()
		{
			assert(!IsEmpty());
			int iOldHead = m_iHead;
			m_iHead = (m_iHead + 1) % m_dValues.GetLength();
			m_iUsed--;

			return m_dValues[iOldHead];
		}

		const T& Last() const
		{
			assert(!IsEmpty());
			return operator[](GetLength() - 1);
		}

		T& Last()
		{
			assert(!IsEmpty());
			int iIndex = GetLength() - 1;
			return m_dValues[(iIndex + m_iHead) % m_dValues.GetLength()];
		}

		const T& operator [] (int iIndex) const
		{
			assert(iIndex < m_iUsed);
			return m_dValues[(iIndex + m_iHead) % m_dValues.GetLength()];
		}

		bool IsEmpty() const
		{
			return m_iUsed == 0;
		}

		int GetLength() const
		{
			return m_iUsed;
		}

	private:
		CSphFixedVector<T>	m_dValues;
		float				m_fGrowFactor;
		int					m_iHead;
		int					m_iTail;
		int					m_iUsed;

		void Resize(int iNewLength)
		{
			CSphFixedVector<T> dNew(iNewLength);
			for (int i = 0; i < GetLength(); i++)
				dNew[i] = m_dValues[(i + m_iHead) % m_dValues.GetLength()];

			m_dValues.SwapData(dNew);

			m_iHead = 0;
			m_iTail = m_iUsed;
		}
	};

	//////////////////////////////////////////////////////////////////////////
	/// simple linked list
	//////////////////////////////////////////////////////////////////////////
	struct ListNode_t
	{
		ListNode_t* m_pPrev;
		ListNode_t* m_pNext;

		ListNode_t() : m_pPrev(NULL), m_pNext(NULL)
		{
		}
	};

	class List_t
	{
	public:
		List_t()
		{
			m_tStub.m_pPrev = &m_tStub;
			m_tStub.m_pNext = &m_tStub;
			m_iCount = 0;
		}

		void Add(ListNode_t* pNode)
		{
			assert(!pNode->m_pNext && !pNode->m_pPrev);
			pNode->m_pNext = m_tStub.m_pNext;
			pNode->m_pPrev = &m_tStub;
			m_tStub.m_pNext->m_pPrev = pNode;
			m_tStub.m_pNext = pNode;

			m_iCount++;
		}

		void Remove(ListNode_t* pNode)
		{
			assert(pNode->m_pNext && pNode->m_pPrev);
			pNode->m_pNext->m_pPrev = pNode->m_pPrev;
			pNode->m_pPrev->m_pNext = pNode->m_pNext;
			pNode->m_pNext = NULL;
			pNode->m_pPrev = NULL;

			m_iCount--;
		}

		int GetLength() const
		{
			return m_iCount;
		}

		const ListNode_t* Begin() const
		{
			return m_tStub.m_pNext;
		}

		const ListNode_t* End() const
		{
			return &m_tStub;
		}

	private:
		ListNode_t m_tStub;    // stub node
		int m_iCount;    // elements counter
	};

	////////////////////////

	


}