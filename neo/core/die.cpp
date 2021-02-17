#include "neo/core/die.h"
#include "neo/int/types.h"

static SphDieCallback_t g_pfDieCallback = NULL;


void sphSetDieCallback ( SphDieCallback_t pfDieCallback )
{
	g_pfDieCallback = pfDieCallback;
}


void sphDie ( const char * sTemplate, ... )
{
	char sBuf[1024];

	va_list ap;
	va_start ( ap, sTemplate );
	vsnprintf ( sBuf, sizeof(sBuf), sTemplate, ap );
	va_end ( ap );

	// if there's no callback,
	// or if callback returns true,
	// log to stdout
	if ( !g_pfDieCallback || g_pfDieCallback ( sBuf ) )
		fprintf ( stdout, "FATAL: %s\n", sBuf );

	exit ( 1 );
}


void sphDieRestart ( const char * sTemplate, ... )
{
	char sBuf[1024];

	va_list ap;
	va_start ( ap, sTemplate );
	vsnprintf ( sBuf, sizeof(sBuf), sTemplate, ap );
	va_end ( ap );

	// if there's no callback,
	// or if callback returns true,
	// log to stdout
	if ( !g_pfDieCallback || g_pfDieCallback ( sBuf ) )
		fprintf ( stdout, "FATAL: %s\n", sBuf );

	exit ( 2 ); // almost CRASH_EXIT
}