// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

#ifndef _sphinx_
#define _sphinx_

/////////////////////////////////////////////////////////////////////////////

#include "neo/core/config.h"
#include "neo/int/types.h"
#include "neo/core/globals.h"
#include "neo/platform/compat.h"
#include "neo/core/version.h"
#include "neo/core/exception.h"

////////////////////////////////////////////////////////////////////////////

/*
#ifdef BUILD_WITH_CMAKE
	#include "neo/gen_sphinxversion.h"
#else
	#include "neo/sphinxversion.h"
#endif
*/

namespace NEO {



/////////// libs

#if USE_LIBSTEMMER
#include <libstemmer.h>
#endif

#if USE_LIBEXPAT
#define XMLIMPORT
#include "expat.h"

// workaround for expat versions prior to 1.95.7
#ifndef XMLCALL
#define XMLCALL
#endif
#endif

#if USE_LIBICONV
#include "iconv.h"
#endif

#if USE_ZLIB
#include <zlib.h>
#endif


#if USE_RE2
#include <string>
#include <re2/re2.h>
#endif

////////// sources

/**/	
#if USE_MYSQL
#include "mysql.h"
#include "neo/source/source_mysql.h"
#endif


#if USE_PGSQL
#include "neo/source/source_pgsql.h
#endif

#if USE_ODBC
#include "neo/source/source_odbc.h"
#include "neo/source/source_mssql.h"
#endif


#if USE_LIBEXPAT
#include "neo/source/source_xml_pipe2.h"
	class CSphConfigSection;
	CSphSource* sphCreateSourceXmlpipe2(const CSphConfigSection* pSource, FILE* pPipe, const char* szSourceName, int iMaxFieldLen, bool bProxy, CSphString& sError);
#endif

#include "neo/source/source_sv.h"



}

#endif // _sphinx_
