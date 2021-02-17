#include "neo/dict/dict_keyword_rt.h"

namespace NEO {

	ISphRtDictWraper* sphCreateRtKeywordsDictionaryWrapper(CSphDict* pBase)
	{
		return new CRtDictKeywords(pBase);
	}

}