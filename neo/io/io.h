#pragma once
#include "neo/io/crc32.h"
#include "neo/io/file.h"
#include "neo/io/io_stats.h"
#include "neo/int/throttle_state.h"

namespace NEO {
	extern ThrottleState_t g_tThrottle;

	/// Fast check if our endianess is correct
	const char* sphCheckEndian();


	void FormatEscaped(FILE* fp, const char* sLine);

	/// try to obtain an exclusive lock on specified file
	/// bWait specifies whether to wait
	bool			sphLockEx(int iFile, bool bWait);

	/// remove existing locks
	void			sphLockUn(int iFile);

	/// millisecond-precision sleep
	void			sphSleepMsec(int iMsec);

	/// check if file exists and is a readable file
	bool			sphIsReadable(const char* sFilename, CSphString* pError = NULL);

	/// set throttling options
	void			sphSetThrottling(int iMaxIOps, int iMaxIOSize);

	/*static*/ bool sphWriteThrottled(int iFD, const void* pBuf, int64_t iCount, const char* sName, CSphString& sError, ThrottleState_t* pThrottle);

	/*static*/ size_t sphReadThrottled(int iFD, void* pBuf, size_t iCount, ThrottleState_t* pThrottle);

	/*static*/ bool sphTruncate(int iFD);


	void SafeClose(int& iFD);

	/// open file for reading
	int				sphOpenFile(const char* sFile, CSphString& sError, bool bWrite);

	/// return size of file descriptor
	int64_t			sphGetFileSize(int iFD, CSphString& sError);


	

	/*static*/ bool CopyFile(const char* sSrc, const char* sDst, CSphString& sErrStr, ThrottleState_t* pThrottle, volatile bool* pGlobalStop, volatile bool* pLocalStop);




}