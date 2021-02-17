#include "neo/platform/compat.h"
#include "neo/core/globals.h"
#include "neo/io/io.h"
#include "neo/int/types.h"
#include "neo/core/generic.h"
#include "neo/platform/thread.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>


namespace NEO {

	void FormatEscaped(FILE* fp, const char* sLine)
	{
		// handle empty lines
		if (!sLine || !*sLine)
		{
			fprintf(fp, "''");
			return;
		}

		// pass one, count the needed buffer size
		int iLen = strlen(sLine);
		int iOut = 0;
		for (int i = 0; i < iLen; i++)
			switch (sLine[i])
			{
			case '\t':
			case '\'':
			case '\\':
				iOut += 2;
				break;

			default:
				iOut++;
				break;
			}
		iOut += 2; // quotes

		// allocate the buffer
		char sMinibuffer[8192];
		char* sMaxibuffer = NULL;
		char* sBuffer = sMinibuffer;

		if (iOut > (int)sizeof(sMinibuffer))
		{
			sMaxibuffer = new char[iOut + 4]; // 4 is just my safety gap
			sBuffer = sMaxibuffer;
		}

		// pass two, escape it
		char* sOut = sBuffer;
		*sOut++ = '\'';

		for (int i = 0; i < iLen; i++)
			switch (sLine[i])
			{
			case '\t':
			case '\'':
			case '\\':	*sOut++ = '\\'; // no break intended
			default:	*sOut++ = sLine[i];
			}
		*sOut++ = '\'';

		// print!
		assert(sOut == sBuffer + iOut);
		fwrite(sBuffer, 1, iOut, fp);

		// cleanup
		SafeDeleteArray(sMaxibuffer);
	}


	void SafeClose(int& iFD)
	{
		if (iFD >= 0)
			::close(iFD);
		iFD = -1;
	}


#if USE_WINDOWS

	bool sphLockEx(int iFile, bool bWait)
	{
		HANDLE hHandle = (HANDLE)_get_osfhandle(iFile);
		if (hHandle != INVALID_HANDLE_VALUE)
		{
			OVERLAPPED tOverlapped;
			memset(&tOverlapped, 0, sizeof(tOverlapped));
			return !!LockFileEx(hHandle, LOCKFILE_EXCLUSIVE_LOCK | (bWait ? 0 : LOCKFILE_FAIL_IMMEDIATELY), 0, 1, 0, &tOverlapped);
		}

		return false;
	}

	void sphLockUn(int iFile)
	{
		HANDLE hHandle = (HANDLE)_get_osfhandle(iFile);
		if (hHandle != INVALID_HANDLE_VALUE)
		{
			OVERLAPPED tOverlapped;
			memset(&tOverlapped, 0, sizeof(tOverlapped));
			UnlockFileEx(hHandle, 0, 1, 0, &tOverlapped);
		}
	}

#else

	bool sphLockEx(int iFile, bool bWait)
	{
		struct flock tLock;
		tLock.l_type = F_WRLCK;
		tLock.l_whence = SEEK_SET;
		tLock.l_start = 0;
		tLock.l_len = 0;

		int iCmd = bWait ? F_SETLKW : F_SETLK; // FIXME! check for HAVE_F_SETLKW?
		return (fcntl(iFile, iCmd, &tLock) != -1);
	}


	void sphLockUn(int iFile)
	{
		struct flock tLock;
		tLock.l_type = F_UNLCK;
		tLock.l_whence = SEEK_SET;
		tLock.l_start = 0;
		tLock.l_len = 0;

		fcntl(iFile, F_SETLK, &tLock);
	}
#endif


	void sphSleepMsec(int iMsec)
	{
		if (iMsec < 0)
			return;

#if USE_WINDOWS
		Sleep(iMsec);

#else
		struct timeval tvTimeout;
		tvTimeout.tv_sec = iMsec / 1000; // full seconds
		tvTimeout.tv_usec = (iMsec % 1000) * 1000; // remainder is msec, so *1000 for usec

		select(0, NULL, NULL, NULL, &tvTimeout); // FIXME? could handle EINTR
#endif
	}


	bool sphIsReadable(const char* sPath, CSphString* pError)
	{
		int iFD = ::open(sPath, O_RDONLY);

		if (iFD < 0)
		{
			if (pError)
				pError->SetSprintf("%s unreadable: %s", sPath, strerror(errno));
			return false;
		}

		close(iFD);
		return true;
	}


	int sphOpenFile(const char* sFile, CSphString& sError, bool bWrite)
	{
		int iFlags = bWrite ? O_RDWR : SPH_O_READ;
		int iFD = ::open(sFile, iFlags, 0644);
		if (iFD < 0)
		{
			sError.SetSprintf("failed to open file '%s': '%s'", sFile, strerror(errno));
			return -1;
		}

		return iFD;
	}


	int64_t sphGetFileSize(int iFD, CSphString& sError)
	{
		if (iFD < 0)
		{
			sError.SetSprintf("invalid descriptor to fstat '%d'", iFD);
			return -1;
		}

		struct_stat st;
		if (fstat(iFD, &st) < 0)
		{
			sError.SetSprintf("failed to fstat file '%d': '%s'", iFD, strerror(errno));
			return -1;
		}

		return st.st_size;
	}


	void sphSetReadBuffers(int iReadBuffer, int iReadUnhinted)
	{
		if (iReadBuffer <= 0)
			iReadBuffer = DEFAULT_READ_BUFFER;
		g_iReadBuffer = Max(iReadBuffer, MIN_READ_BUFFER);

		if (iReadUnhinted <= 0)
			iReadUnhinted = DEFAULT_READ_UNHINTED;
		g_iReadUnhinted = Max(iReadUnhinted, MIN_READ_UNHINTED);
	}


	/*static*/ bool sphTruncate(int iFD)
	{
#if USE_WINDOWS
		return SetEndOfFile((HANDLE)_get_osfhandle(iFD)) != 0;
#else
		return ::ftruncate(iFD, ::lseek(iFD, 0, SEEK_CUR)) == 0;
#endif
	}


	/*static*/ bool CopyFile(const char* sSrc, const char* sDst, CSphString& sErrStr, ThrottleState_t* pThrottle, volatile bool* pGlobalStop, volatile bool* pLocalStop)
	{
		assert(sSrc);
		assert(sDst);

		const DWORD iMaxBufSize = 1024 * 1024;

		CSphAutofile tSrcFile(sSrc, SPH_O_READ, sErrStr);
		CSphAutofile tDstFile(sDst, SPH_O_NEW, sErrStr);

		if (tSrcFile.GetFD() < 0 || tDstFile.GetFD() < 0)
			return false;

		SphOffset_t iFileSize = tSrcFile.GetSize();
		DWORD iBufSize = (DWORD)Min(iFileSize, (SphOffset_t)iMaxBufSize);

		if (iFileSize)
		{
			CSphFixedVector<BYTE> dData(iBufSize);
			bool bError = true;

			while (iFileSize > 0)
			{
				if (*pGlobalStop || *pLocalStop)
					return false;

				DWORD iSize = (DWORD)Min(iFileSize, (SphOffset_t)iBufSize);

				size_t iRead = sphReadThrottled(tSrcFile.GetFD(), dData.Begin(), iSize, pThrottle);
				if (iRead != iSize)
				{
					sErrStr.SetSprintf("read error in %s; " INT64_FMT " of %d bytes read", sSrc, (int64_t)iRead, iSize);
					break;
				}

				if (!sphWriteThrottled(tDstFile.GetFD(), dData.Begin(), iSize, "CopyFile", sErrStr, pThrottle))
					break;

				iFileSize -= iSize;

				if (!iFileSize)
					bError = false;
			}

			return (bError == false);
		}

		return true;
	}


	//throttling


	/*static*/ ThrottleState_t g_tThrottle;

	void sphSetThrottling(int iMaxIOps, int iMaxIOSize)
	{
		g_tThrottle.m_iMaxIOps = iMaxIOps;
		g_tThrottle.m_iMaxIOSize = iMaxIOSize;
	}



	/*static*/ inline void sphThrottleSleep(ThrottleState_t* pState)
	{
		assert(pState);
		if (pState->m_iMaxIOps > 0)
		{
			int64_t tmTimer = sphMicroTimer();
			int64_t tmSleep = Max(pState->m_tmLastIOTime + 1000000 / pState->m_iMaxIOps - tmTimer, 0);
			sphSleepMsec((int)(tmSleep / 1000));
			pState->m_tmLastIOTime = tmTimer + tmSleep;
		}
	}


	/*static*/ bool sphWriteThrottled(int iFD, const void* pBuf, int64_t iCount, const char* sName, CSphString& sError, ThrottleState_t* pThrottle)
	{
		assert(pThrottle);
		if (iCount <= 0)
			return true;

		// by default, slice ios by at most 1 GB
		int iChunkSize = (1UL << 30);

		// when there's a sane max_iosize (4K to 1GB), use it
		if (pThrottle->m_iMaxIOSize >= 4096)
			iChunkSize = Min(iChunkSize, pThrottle->m_iMaxIOSize);

		STATS::CSphIOStats* pIOStats = STATS::GetIOStats();

		// while there's data, write it chunk by chunk
		const BYTE* p = (const BYTE*)pBuf;
		while (iCount > 0)
		{
			// wait for a timely occasion
			sphThrottleSleep(pThrottle);

			// write (and maybe time)
			int64_t tmTimer = 0;
			if (pIOStats)
				tmTimer = sphMicroTimer();

			int iToWrite = iChunkSize;
			if (iCount < iChunkSize)
				iToWrite = (int)iCount;
			int iWritten = ::write(iFD, p, iToWrite);

			if (pIOStats)
			{
				pIOStats->m_iWriteTime += sphMicroTimer() - tmTimer;
				pIOStats->m_iWriteOps++;
				pIOStats->m_iWriteBytes += iToWrite;
			}

			// success? rinse, repeat
			if (iWritten == iToWrite)
			{
				iCount -= iToWrite;
				p += iToWrite;
				continue;
			}

			// failure? report, bailout
			if (iWritten < 0)
				sError.SetSprintf("%s: write error: %s", sName, strerror(errno));
			else
				sError.SetSprintf("%s: write error: %d of %d bytes written", sName, iWritten, iToWrite);
			return false;
		}
		return true;
	}


	/*static*/ size_t sphReadThrottled(int iFD, void* pBuf, size_t iCount, ThrottleState_t* pThrottle)
	{
		assert(pThrottle);
		if (pThrottle->m_iMaxIOSize && int(iCount) > pThrottle->m_iMaxIOSize)
		{
			size_t nChunks = iCount / pThrottle->m_iMaxIOSize;
			size_t nBytesLeft = iCount % pThrottle->m_iMaxIOSize;

			size_t nBytesRead = 0;
			size_t iRead = 0;

			for (size_t i = 0; i < nChunks; i++)
			{
				iRead = sphReadThrottled(iFD, (char*)pBuf + i * pThrottle->m_iMaxIOSize, pThrottle->m_iMaxIOSize, pThrottle);
				nBytesRead += iRead;
				if (iRead != (size_t)pThrottle->m_iMaxIOSize)
					return nBytesRead;
			}

			if (nBytesLeft > 0)
			{
				iRead = sphReadThrottled(iFD, (char*)pBuf + nChunks * pThrottle->m_iMaxIOSize, nBytesLeft, pThrottle);
				nBytesRead += iRead;
				if (iRead != nBytesLeft)
					return nBytesRead;
			}

			return nBytesRead;
		}

		sphThrottleSleep(pThrottle);
		return (size_t) STATS::sphRead(iFD, pBuf, iCount); // FIXME? we sure this is under 2gb?
	}

}