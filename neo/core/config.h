#pragma once

#ifndef HAVE_CONFIG_H
#define HAVE_CONFIG_H 0
#endif // !HAVE_CONFIG_H



#ifdef _WIN32
#define USE_MYSQL		1	/// whether to compile MySQL support
#define USE_PGSQL		0	/// whether to compile PgSQL support
#define USE_ODBC		0	/// whether to compile ODBC support
#define USE_LIBEXPAT	0	/// whether to compile libexpat support
#define USE_LIBICONV	0	/// whether to compile iconv support
#define	USE_LIBSTEMMER	0	/// whether to compile libstemmber support
#define	USE_RE2			0	/// whether to compile RE2 support
#define USE_RLP			0	/// whether to compile RLP support
#define USE_WINDOWS		1	/// whether to compile for Windows
#define USE_SYSLOG		0	/// whether to use syslog for logging
#define HAVE_STRNLEN	1	

#define UNALIGNED_RAM_ACCESS	1
#define USE_LITTLE_ENDIAN		1
#else
#define USE_WINDOWS		0	/// whether to compile for Windows
#endif

#ifndef USE_64BIT
#define USE_64BIT 1
#endif

