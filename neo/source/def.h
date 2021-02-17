#pragma once


	// a little staff for using static/dynamic libraries.
	// for dynamic we declare the type and define the originally nullptr pointer.
	// for static we define const pointer as alias to target function.

#define F_DL(name) static decltype(&name) sph_##name = nullptr
#define F_DR(name) static decltype(&name) sph_##name = &name

