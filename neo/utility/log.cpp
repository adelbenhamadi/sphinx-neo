#include "neo/utility/log.h"


namespace NEO {

	static void StdoutLogger(ESphLogLevel eLevel, const char* sFmt, va_list ap)
	{
		if (eLevel >= SPH_LOG_DEBUG)
			return;

		switch (eLevel)
		{
		case SPH_LOG_FATAL: fprintf(stdout, "FATAL: "); break;
		case SPH_LOG_WARNING: fprintf(stdout, "WARNING: "); break;
		case SPH_LOG_INFO: fprintf(stdout, "WARNING: "); break;
		case SPH_LOG_DEBUG: // yes, I know that this branch will never execute because of the condition above.
		case SPH_LOG_VERBOSE_DEBUG:
		case SPH_LOG_VERY_VERBOSE_DEBUG: fprintf(stdout, "DEBUG: "); break;
		}

		vfprintf(stdout, sFmt, ap);
		fprintf(stdout, "\n");
	}

	static SphLogger_fn g_pLogger = &StdoutLogger;

	inline void Log(ESphLogLevel eLevel, const char* sFmt, va_list ap)
	{
		if (!g_pLogger) return;
		(*g_pLogger) (eLevel, sFmt, ap);
	}

	void sphWarning(const char* sFmt, ...)
	{
		va_list ap;
		va_start(ap, sFmt);
		Log(SPH_LOG_WARNING, sFmt, ap);
		va_end(ap);
	}


	void sphInfo(const char* sFmt, ...)
	{
		va_list ap;
		va_start(ap, sFmt);
		Log(SPH_LOG_INFO, sFmt, ap);
		va_end(ap);
	}

	void sphLogFatal(const char* sFmt, ...)
	{
		va_list ap;
		va_start(ap, sFmt);
		Log(SPH_LOG_FATAL, sFmt, ap);
		va_end(ap);
	}

	void sphLogDebug(const char* sFmt, ...)
	{
		va_list ap;
		va_start(ap, sFmt);
		Log(SPH_LOG_DEBUG, sFmt, ap);
		va_end(ap);
	}

	void sphLogDebugv(const char* sFmt, ...)
	{
		va_list ap;
		va_start(ap, sFmt);
		Log(SPH_LOG_VERBOSE_DEBUG, sFmt, ap);
		va_end(ap);
	}

	void sphLogDebugvv(const char* sFmt, ...)
	{
		va_list ap;
		va_start(ap, sFmt);
		Log(SPH_LOG_VERY_VERBOSE_DEBUG, sFmt, ap);
		va_end(ap);
	}

	void sphSetLogger(SphLogger_fn fnLog)
	{
		g_pLogger = fnLog;
	}

	/// indexer warning
	void sphWarn(const char* sTemplate, ...)
	{
		va_list ap;
		va_start(ap, sTemplate);
		fprintf(stdout, "WARNING: ");
		vfprintf(stdout, sTemplate, ap);
		fprintf(stdout, "\n");
		va_end(ap);
	}

}