#include "neo/core/generic.h"


	
	template < typename T > 
	void NEO::SphAccessor_T<T>::Swap(T* a, T* b) const
	{
		Swap(*a, *b);
	}

	
	template<typename T, typename C>
	NEO::SphMemberLess_T<T, C> sphMemberLess(T C::* pMember)
	{
		return SphMemberLess_T<T, C>(pMember);
	}

	/// heap sort helper
	template<typename T, typename U, typename V>
	void NEO::sphSiftDown(T* pData, int iStart, int iEnd, U COMP, V ACC)
	{
		for (;; )
		{
			int iChild = iStart * 2 + 1;
			if (iChild > iEnd)
				return;

			int iChild1 = iChild + 1;
			if (iChild1 <= iEnd && COMP.IsLess(ACC.Key(ACC.Add(pData, iChild)), ACC.Key(ACC.Add(pData, iChild1))))
				iChild = iChild1;

			if (COMP.IsLess(ACC.Key(ACC.Add(pData, iChild)), ACC.Key(ACC.Add(pData, iStart))))
				return;
			ACC.Swap(ACC.Add(pData, iChild), ACC.Add(pData, iStart));
			iStart = iChild;
		}
	}

	/// heap sort
	template<typename T, typename U, typename V>
	void NEO::sphHeapSort(T* pData, int iCount, U COMP, V ACC)
	{
		if (!pData || iCount <= 1)
			return;

		// build a max-heap, so that the largest element is root
		for (int iStart = (iCount - 2) >> 1; iStart >= 0; iStart--)
			sphSiftDown(pData, iStart, iCount - 1, COMP, ACC);

		// now keep popping root into the end of array
		for (int iEnd = iCount - 1; iEnd > 0; )
		{
			ACC.Swap(pData, ACC.Add(pData, iEnd));
			sphSiftDown(pData, 0, --iEnd, COMP, ACC);
		}
	}

	/// generic sort
	template<typename T, typename U, typename V>
	void NEO::sphSort(T* pData, int iCount, U COMP, V ACC)
	{
		if (iCount < 2)
			return;

		typedef T* P;
		// st0 and st1 are stacks with left and right bounds of array-part.
		// They allow us to avoid recursion in quicksort implementation.
		P st0[32], st1[32], a, b, i, j;
		typename V::MEDIAN_TYPE x;
		int k;

		const int SMALL_THRESH = 32;
		int iDepthLimit = sphLog2(iCount);
		iDepthLimit = ((iDepthLimit << 2) + iDepthLimit) >> 1; // x2.5

		k = 1;
		st0[0] = pData;
		st1[0] = ACC.Add(pData, iCount - 1);
		while (k)
		{
			k--;
			i = a = st0[k];
			j = b = st1[k];

			// if quicksort fails on this data; switch to heapsort
			if (!k)
			{
				if (!--iDepthLimit)
				{
					sphHeapSort(a, ACC.Sub(b, a) + 1, COMP, ACC);
					return;
				}
			}

			// for tiny arrays, switch to insertion sort
			int iLen = ACC.Sub(b, a);
			if (iLen <= SMALL_THRESH)
			{
				for (i = ACC.Add(a, 1); i <= b; i = ACC.Add(i, 1))
				{
					for (j = i; j > a; )
					{
						P j1 = ACC.Add(j, -1);
						if (COMP.IsLess(ACC.Key(j1), ACC.Key(j)))
							break;
						ACC.Swap(j, j1);
						j = j1;
					}
				}
				continue;
			}

			// ATTENTION! This copy can lead to memleaks if your CopyKey
			// copies something which is not freed by objects destructor.
			ACC.CopyKey(&x, ACC.Add(a, iLen / 2));
			while (a < b)
			{
				while (i <= j)
				{
					while (COMP.IsLess(ACC.Key(i), x))
						i = ACC.Add(i, 1);
					while (COMP.IsLess(x, ACC.Key(j)))
						j = ACC.Add(j, -1);
					if (i <= j)
					{
						ACC.Swap(i, j);
						i = ACC.Add(i, 1);
						j = ACC.Add(j, -1);
					}
				}

				// Not so obvious optimization. We put smaller array-parts
				// to the top of stack. That reduces peak stack size.
				if (ACC.Sub(j, a) >= ACC.Sub(b, i))
				{
					if (a < j) { st0[k] = a; st1[k] = j; k++; }
					a = i;
				}
				else
				{
					if (i < b) { st0[k] = i; st1[k] = b; k++; }
					b = j;
				}
			}
		}
	}
	
	template<typename T, typename U>
	void NEO::sphSort(T* pData, int iCount, U COMP)
	{
		sphSort(pData, iCount, COMP, SphAccessor_T<T>());
	}
	
	template<typename T>
	void NEO::sphSort(T* pData, int iCount)
	{
		sphSort(pData, iCount, SphLess_T<T>());
	}

	/// handy member functor generator
	/// this sugar allows you to write like this
	/// dArr.Sort ( bind ( &CSphType::m_iMember ) );
	/// dArr.BinarySearch ( bind ( &CSphType::m_iMember ), iValue );
	template<typename T, typename CLASS>
	NEO::SphMemberFunctor_T<T, CLASS> bind(T CLASS::* ptr)
	{
		return SphMemberFunctor_T < T, CLASS >(ptr);
	}

	/// generic binary search
	template<typename T, typename U, typename PRED>
	T* sphBinarySearch(T* pStart, T* pEnd, const PRED& tPred, U tRef)
	{
		if (!pStart || pEnd < pStart)
			return NULL;

		if (tPred(*pStart) == tRef)
			return pStart;

		if (tPred(*pEnd) == tRef)
			return pEnd;

		while (pEnd - pStart > 1)
		{
			if (tRef < tPred(*pStart) || tPred(*pEnd) < tRef)
				break;
			assert(tPred(*pStart) < tRef);
			assert(tRef < tPred(*pEnd));

			T* pMid = pStart + (pEnd - pStart) / 2;
			if (tRef == tPred(*pMid))
				return pMid;

			if (tRef < tPred(*pMid))
				pEnd = pMid;
			else
				pStart = pMid;
		}
		return NULL;
	}

	/// generic binary search
	template<typename T>
	T* NEO::sphBinarySearch(T* pStart, T* pEnd, T& tRef)
	{
		return sphBinarySearch(pStart, pEnd, SphIdentityFunctor_T<T>(), tRef);
	}

	/// generic uniq
	template<typename T, typename T_COUNTER>
	T_COUNTER NEO::sphUniq(T* pData, T_COUNTER iCount)
	{
		if (!iCount)
			return 0;

		T_COUNTER iSrc = 1, iDst = 1;
		while (iSrc < iCount)
		{
			if (pData[iDst - 1] == pData[iSrc])
				iSrc++;
			else
				pData[iDst++] = pData[iSrc++];
		}
		return iDst;
	}

	
