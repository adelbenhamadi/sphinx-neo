#pragma once
#include "neo/core/config.h"
#include "neo/platform/base.h"
#include "neo/platform/compiler.h"

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


#if HAVE_CONFIG_H
#include "config.h"
#endif


// for 64-bit types
#if HAVE_STDINT_H
#include <cstdint>
#endif

#if HAVE_INTTYPES_H
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#endif

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifndef USE_WINDOWS
#ifdef _MSC_VER
#define USE_WINDOWS 1
#else
#define USE_WINDOWS 0
#endif // _MSC_VER
#endif

#if !USE_WINDOWS
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#endif


#if USE_WINDOWS
#include <winsock2.h>
#else
#include <sys/types.h>
#include <unistd.h>
#endif



#if USE_ODBC
#include <sqlext.h>
#endif

#if USE_WINDOWS
#include <io.h> // for open()

// workaround Windows quirks
#define popen		_popen
#define pclose		_pclose
#define snprintf	_snprintf
#define sphSeek		_lseeki64

#define stat		_stat64
#define fstat		_fstat64
#if _MSC_VER<1400
#define struct_stat	__stat64
#else
#define struct_stat	struct _stat64
#endif

#define ICONV_INBUF_CONST	1
#else
#include <unistd.h>
#include <sys/time.h>

#define sphSeek		lseek

#define struct_stat		struct stat
#endif

#if _WIN32

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>

#include <intrin.h> // for bsr
#pragma intrinsic(_BitScanReverse)

#define strcasecmp			_strcmpi
#define strncasecmp			_strnicmp
#define strtoll				_strtoi64
#define strtoull			_strtoui64

#else //NOT _WIN32

#if USE_ODBC
// UnixODBC compatible DWORD
#if defined(__alpha) || defined(__sparcv9) || defined(__LP64__) || (defined(__HOS_AIX__) && defined(_LP64)) || defined(__APPLE__)
typedef unsigned int		DWORD;
#else
typedef unsigned long		DWORD;
#endif
#else
// default DWORD
typedef unsigned int		DWORD;
#endif // USE_ODBC

typedef unsigned short		WORD;
typedef unsigned char		BYTE;

#endif // _WIN32