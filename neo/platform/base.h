#pragma once

//compiler check
#if defined(_MSC_VER)
#include "neo/platform/msvc.h"
#elif defined(__GNUC__)
#include "neo/platform/gcc.h"
#else
#error "Unsupported compiler family"
#endif

#ifndef __GNUC__
#define __attribute__(x) /*just ignore this*/
#endif