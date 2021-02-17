#pragma once
#include "neo/int/types.h"

namespace NEO {

	/// seed RNG
	void		sphSrand(DWORD uSeed);

	/// auto-seed RNG based on time and PID
	void		sphAutoSrand();

	/// generate another random
	DWORD		sphRand();

}