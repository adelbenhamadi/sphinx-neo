#pragma once
#include "neo/int/types.h"

namespace NEO {
	/// global binlog interface
	class ISphBinlog : ISphNoncopyable
	{
	public:
		virtual				~ISphBinlog() {}

		virtual void		BinlogUpdateAttributes(int64_t* pTID, const char* sIndexName, const CSphAttrUpdate& tUpd) = 0;
		virtual void		NotifyIndexFlush(const char* sIndexName, int64_t iTID, bool bShutdown) = 0;
	};

}