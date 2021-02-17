#pragma once
#include "neo/core/generic.h"
#include "neo/int/types.h"


namespace NEO {

	enum ESphLogLevel
	{
		SPH_LOG_FATAL = 0,
		SPH_LOG_WARNING = 1,
		SPH_LOG_INFO = 2,
		SPH_LOG_DEBUG = 3,
		SPH_LOG_VERBOSE_DEBUG = 4,
		SPH_LOG_VERY_VERBOSE_DEBUG = 5
	};

	typedef void (*SphLogger_fn)(ESphLogLevel, const char*, va_list);



	void sphWarning(const char* sFmt, ...) __attribute__((format(printf, 1, 2))); //NOLINT
	void sphInfo(const char* sFmt, ...) __attribute__((format(printf, 1, 2))); //NOLINT
	void sphLogFatal(const char* sFmt, ...) __attribute__((format(printf, 1, 2))); //NOLINT
	void sphLogDebug(const char* sFmt, ...) __attribute__((format(printf, 1, 2))); //NOLINT
	void sphLogDebugv(const char* sFmt, ...) __attribute__((format(printf, 1, 2))); //NOLINT
	void sphLogDebugvv(const char* sFmt, ...) __attribute__((format(printf, 1, 2))); //NOLINT
	void sphSetLogger(SphLogger_fn fnLog);

	//indexer warn
	void sphWarn(const char*, ...) __attribute__((format(printf, 1, 2)));//NOLINT


}