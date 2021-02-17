#pragma once
#include "neo/core/iextra.h"

namespace NEO {

	class ISphQwordSetup;
	class ISphSchema;
	class CSphMatch;

	/// generic ranker interface
	class ISphRanker : public ISphExtra
	{
	public:
		virtual						~ISphRanker() {}
		virtual CSphMatch* GetMatchesBuffer() = 0;
		virtual int					GetMatches() = 0;
		virtual void				Reset(const ISphQwordSetup& tSetup) = 0;
		virtual bool				IsCache() const { return false; }
		virtual void				FinalizeCache(const ISphSchema&) {}
	};



}