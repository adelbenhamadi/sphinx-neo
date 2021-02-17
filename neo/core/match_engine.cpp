#include "neo/core/match_engine.h"



void NEO::MatchSortAccessor_t::Swap(T* a, T* b) const
{
	NEO::Swap(*a, *b);
}