#pragma once
#include "neo/query/enums.h"
#include "neo/index/enums.h"
#include "neo/source/attrib_locator.h"
#include "neo/query/match_sorter.h"


namespace NEO {

	//fwd dec
	class CSphMatch;



	/// match comparator interface from group-by sorter point of view
	struct ISphMatchComparator
	{
		virtual ~ISphMatchComparator() {}
		virtual bool VirtualIsLess(const CSphMatch& a, const CSphMatch& b, const CSphMatchComparatorState& tState) const = 0;
	};


}