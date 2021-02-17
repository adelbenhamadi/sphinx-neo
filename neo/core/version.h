#pragma once
#include "neo/int/types.h"
#include "neo/core/globals.h"

namespace NEO {

#ifndef SPHINX_TAG
#define SPHINX_TAG "-dev"
#endif

#ifndef SPH_SVN_TAGREV
#define SPH_SVN_TAGREV "unknown"
#endif


	// below is for easier extraction of the ver. by any external scripts
#define SPHINX_VERSION_NUMBERS    "2.3.3"
#define SPH_GIT_COMMIT_ID		  "gcd?"		
#define SPHINX_VERSION           SPHINX_VERSION_NUMBERS SPHINX_BITS_TAG SPHINX_TAG " (" SPH_GIT_COMMIT_ID ")"
#define SPHINX_BANNER			"Sphinx " SPHINX_VERSION "\nCopyright (c) 2001-2016, Andrew Aksyonoff\nCopyright (c) 2008-2016, Sphinx Technologies Inc (http://sphinxsearch.com)\n\n"
#define SPHINX_SEARCHD_PROTO	1
#define SPHINX_CLIENT_VERSION	1


	enum ESphExtType
	{
		SPH_EXT_TYPE_CUR = 0,
		SPH_EXT_TYPE_NEW,
		SPH_EXT_TYPE_OLD,
		SPH_EXT_TYPE_LOC
	};

	enum ESphExt
	{
		SPH_EXT_SPH = 0,
		SPH_EXT_SPA = 1,
		SPH_EXT_MVP = 9
	};

	const char** sphGetExts(ESphExtType eType, DWORD uVersion = INDEX_FORMAT_VERSION);
	int sphGetExtCount(DWORD uVersion = INDEX_FORMAT_VERSION);
	const char* sphGetExt(ESphExtType eType, ESphExt eExt);

	DWORD			ReadVersion(const char* sPath, CSphString& sError);
}
