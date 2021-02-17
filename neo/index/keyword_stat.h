#pragma once
#include "neo/int/vector.h"
#include "neo/int/keyword_info.h"

namespace NEO {

	struct ISphKeywordsStat
	{
		virtual			~ISphKeywordsStat() {}
		virtual bool	FillKeywords(CSphVector <CSphKeywordInfo>& dKeywords) const = 0;
	};


}