#pragma once
#include "neo/platform/compat.h"


typedef			bool ( *SphDieCallback_t ) ( const char * );

/// crash with an error message, and do not have searchd watchdog attempt to resurrect
void			sphDie ( const char * sMessage, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) );

/// crash with an error message, but have searchd watchdog attempt to resurrect
void			sphDieRestart ( const char * sMessage, ... ) __attribute__ ( ( format ( printf, 1, 2 ) ) );

/// setup a callback function to call from sphDie() before exit
/// if callback returns false, sphDie() will not log to stdout
void			sphSetDieCallback ( SphDieCallback_t pfDieCallback );

