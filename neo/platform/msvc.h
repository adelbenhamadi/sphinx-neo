#pragma once


#if _MSC_VER>=1400
#define _CRT_SECURE_NO_DEPRECATE 1
#define _CRT_NONSTDC_NO_DEPRECATE 1
#endif

#if _MSC_VER>=1600
#define HAVE_STDINT_H 1
#endif

#if (_MSC_VER>=1000) && !defined(__midl) && defined(_PREFAST_)
typedef int __declspec("SAL_nokernel") __declspec("SAL_nodriver") __prefast_flag_kernel_driver_mode;
#endif

#if defined(_MSC_VER) && (_MSC_VER<1400)
#define vsnprintf _vsnprintf
#endif
