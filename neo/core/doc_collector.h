#pragma once
#include "neo/int/types.h"
#include "neo/core/arena.h"

namespace NEO {

	class tDocCollector : public tTester
	{
		CSphVector<SphDocID_t>* m_dCollection;
	public:
		explicit tDocCollector(CSphVector<SphDocID_t>& dCollection)
			: m_dCollection(&dCollection)
		{}
		virtual void Reset()
		{
			m_dCollection->Reset();
		}
		virtual void TestData(int iData)
		{
			if (!g_pMvaArena)
				return;

			m_dCollection->Add(*(SphDocID_t*)(g_pMvaArena + iData));
		}
	};

}