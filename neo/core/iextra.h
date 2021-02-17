#pragma once
#include "neo/query/enums.h"

namespace NEO {

	/// generic COM-like interface
	class ISphExtra
	{
	public:
		virtual						~ISphExtra() {}
		inline bool					ExtraData(ExtraData_e eType, void** ppData)
		{
			return ExtraDataImpl(eType, ppData);
		}
	private:
		virtual bool ExtraDataImpl(ExtraData_e, void**)
		{
			return false;
		}
	};


}