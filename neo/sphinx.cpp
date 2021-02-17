//
// $Id$
//

//
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

#include <cerrno>
#include <ctype.h>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <cstdarg>
#include <sys/types.h>
#include <sys/stat.h>
#include <climits>
#include <ctime>
#include <cmath>
#include <cfloat>


#include "neo/sphinx.h"

/*
#include "neo/sphinx/xstemmer.h"
#include "neo/sphinx/xquery.h"
#include "neo/sphinx/xutility.h"
#include "neo/sphinxexpr.h"
#include "neo/sphinx/xfilter.h"
#include "neo/sphinxint.h"
#include "neo/sphinx/xsearch.h"
#include "neo/sphinx/xjson.h"
#include "neo/sphinx/xplugin.h"
#include "neo/sphinx/xqcache.h"
#include "neo/sphinx/xrlp.h"
*/

/////////////////////////////////////////////



#if ( USE_WINDOWS && !BUILD_WITH_CMAKE ) // on windows with cmake manual linkage is not necessary
#if ( USE_MYSQL )
	#pragma comment(linker, "/defaultlib:libmysql.lib")
	#pragma message("Automatically linking with libmysql.lib")
#endif

#if ( USE_PGSQL )
	#pragma comment(linker, "/defaultlib:libpq.lib")
	#pragma message("Automatically linking with libpq.lib")
#endif

#if ( USE_LIBSTEMMER )
	#pragma comment(linker, "/defaultlib:libstemmer_c.lib")
	#pragma message("Automatically linking with libstemmer_c.lib")
#endif

#if ( USE_LIBEXPAT )
	#pragma comment(linker, "/defaultlib:libexpat.lib")
	#pragma message("Automatically linking with libexpat.lib")
#endif

#if ( USE_LIBICONV )
	#pragma comment(linker, "/defaultlib:iconv.lib")
	#pragma message("Automatically linking with iconv.lib")
#endif

#if ( USE_RE2 )
	#pragma comment(linker, "/defaultlib:re2.lib")
	#pragma message("Automatically linking with re2.lib")
#endif
#endif




//
// $Id$
//
