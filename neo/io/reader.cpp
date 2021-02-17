#include "neo/io/io.h"
#include "neo/io/reader.h"
#include "neo/io/unzip.h"
#include "neo/core/globals.h"

namespace NEO {
	
		CSphReader::CSphReader(BYTE* pBuf, int iSize)
			: m_pProfile(NULL)
			, m_eProfileState(SPH_QSTATE_IO)
			, m_iFD(-1)
			, m_iPos(0)
			, m_iBuffPos(0)
			, m_iBuffUsed(0)
			, m_pBuff(pBuf)
			, m_iSizeHint(0)
			, m_iBufSize(iSize)
			, m_bBufOwned(false)
			, m_iReadUnhinted(DEFAULT_READ_UNHINTED)
			, m_bError(false)
		{
			assert(pBuf == NULL || iSize > 0);
			m_pThrottle = &g_tThrottle;
		}


		CSphReader::~CSphReader()
		{
			if (m_bBufOwned)
				SafeDeleteArray(m_pBuff);
		}


		void CSphReader::SetBuffers(int iReadBuffer, int iReadUnhinted)
		{
			if (!m_pBuff)
				m_iBufSize = iReadBuffer;
			m_iReadUnhinted = iReadUnhinted;
		}


		void CSphReader::SetFile(int iFD, const char* sFilename)
		{
			m_iFD = iFD;
			m_iPos = 0;
			m_iBuffPos = 0;
			m_iBuffUsed = 0;
			m_sFilename = sFilename;
		}


		void CSphReader::SetFile(const CSphAutofile& tFile)
		{
			SetFile(tFile.GetFD(), tFile.GetFilename());
		}


		void CSphReader::Reset()
		{
			SetFile(-1, "");
		}


		/// sizehint > 0 means we expect to read approx that much bytes
		/// sizehint == 0 means no hint, use default (happens later in UpdateCache())
		/// sizehint == -1 means reposition and adjust current hint
		void CSphReader::SeekTo(SphOffset_t iPos, int iSizeHint)
		{
			assert(iPos >= 0);
			assert(iSizeHint >= -1);

#ifndef NDEBUG
#if PARANOID
			struct_stat tStat;
			fstat(m_iFD, &tStat);
			if (iPos > tStat.st_size)
				sphDie("INTERNAL ERROR: seeking past the end of file");
#endif
#endif

			if (iPos >= m_iPos && iPos < m_iPos + m_iBuffUsed)
			{
				m_iBuffPos = (int)(iPos - m_iPos); // reposition to proper byte
				m_iSizeHint = iSizeHint - (m_iBuffUsed - m_iBuffPos); // we already have some bytes cached, so let's adjust size hint
				assert(m_iBuffPos < m_iBuffUsed);
			}
			else
			{
				m_iPos = iPos;
				m_iBuffPos = 0; // for GetPos() to work properly, aaaargh
				m_iBuffUsed = 0;

				if (iSizeHint == -1)
				{
					// the adjustment bureau
					// we need to seek but still keep the current hint
					// happens on a skiplist jump, for instance
					int64_t iHintLeft = m_iPos + m_iSizeHint - iPos;
					if (iHintLeft > 0 && iHintLeft < INT_MAX)
						iSizeHint = (int)iHintLeft;
					else
						iSizeHint = 0;
				}

				// get that hint
				assert(iSizeHint >= 0);
				m_iSizeHint = iSizeHint;
			}
		}


		void CSphReader::SkipBytes(int iCount)
		{
			// 0 means "no hint", so this clamp works alright
			SeekTo(m_iPos + m_iBuffPos + iCount, Max(m_iSizeHint - m_iBuffPos - iCount, 0));
		}




		int CSphReader::GetByte()
		{
			if (m_iBuffPos >= m_iBuffUsed)
			{
				UpdateCache();
				if (m_iBuffPos >= m_iBuffUsed)
					return 0; // unexpected io failure
			}

			assert(m_iBuffPos < m_iBuffUsed);
			return m_pBuff[m_iBuffPos++];
		}


		void CSphReader::GetBytes(void* pData, int iSize)
		{
			BYTE* pOut = (BYTE*)pData;

			while (iSize > m_iBufSize)
			{
				int iLen = m_iBuffUsed - m_iBuffPos;
				assert(iLen <= m_iBufSize);

				memcpy(pOut, m_pBuff + m_iBuffPos, iLen);
				m_iBuffPos += iLen;
				pOut += iLen;
				iSize -= iLen;
				m_iSizeHint = Max(m_iReadUnhinted, iSize);

				if (iSize > 0)
				{
					UpdateCache();
					if (!m_iBuffUsed)
					{
						memset(pData, 0, iSize);
						return; // unexpected io failure
					}
				}
			}

			if (m_iBuffPos + iSize > m_iBuffUsed)
			{
				// move old buffer tail to buffer head to avoid losing the data
				const int iLen = m_iBuffUsed - m_iBuffPos;
				if (iLen > 0)
				{
					memcpy(pOut, m_pBuff + m_iBuffPos, iLen);
					m_iBuffPos += iLen;
					pOut += iLen;
					iSize -= iLen;
				}

				m_iSizeHint = Max(m_iReadUnhinted, iSize);
				UpdateCache();
				if (m_iBuffPos + iSize > m_iBuffUsed)
				{
					memset(pData, 0, iSize); // unexpected io failure
					return;
				}
			}

			assert((m_iBuffPos + iSize) <= m_iBuffUsed);
			memcpy(pOut, m_pBuff + m_iBuffPos, iSize);
			m_iBuffPos += iSize;
		}


		int CSphReader::GetBytesZerocopy(const BYTE** ppData, int iMax)
		{
			if (m_iBuffPos >= m_iBuffUsed)
			{
				UpdateCache();
				if (m_iBuffPos >= m_iBuffUsed)
					return 0; // unexpected io failure
			}

			int iChunk = Min(m_iBuffUsed - m_iBuffPos, iMax);
			*ppData = m_pBuff + m_iBuffPos;
			m_iBuffPos += iChunk;
			return iChunk;
		}


		int CSphReader::GetLine(char* sBuffer, int iMaxLen)
		{
			int iOutPos = 0;
			iMaxLen--; // reserve space for trailing '\0'

			// grab as many chars as we can
			while (iOutPos < iMaxLen)
			{
				// read next chunk if necessary
				if (m_iBuffPos >= m_iBuffUsed)
				{
					UpdateCache();
					if (m_iBuffPos >= m_iBuffUsed)
					{
						if (iOutPos == 0) return -1; // current line is empty; indicate eof
						break; // return current line; will return eof next time
					}
				}

				// break on CR or LF
				if (m_pBuff[m_iBuffPos] == '\r' || m_pBuff[m_iBuffPos] == '\n')
					break;

				// one more valid char
				sBuffer[iOutPos++] = m_pBuff[m_iBuffPos++];
			}

			// skip everything until the newline or eof
			for (;; )
			{
				// read next chunk if necessary
				if (m_iBuffPos >= m_iBuffUsed)
					UpdateCache();

				// eof?
				if (m_iBuffPos >= m_iBuffUsed)
					break;

				// newline?
				if (m_pBuff[m_iBuffPos++] == '\n')
					break;
			}

			// finalize
			sBuffer[iOutPos] = '\0';
			return iOutPos;
		}

		void CSphReader::ResetError()
		{
			m_bError = false;
			m_sError = "";
		}


#if USE_WINDOWS

		// atomic seek+read for Windows
		int sphPread(int iFD, void* pBuf, int iBytes, SphOffset_t iOffset)
		{
			if (iBytes == 0)
				return 0;

			STATS::CSphIOStats* pIOStats = STATS::GetIOStats();
			int64_t tmStart = 0;
			if (pIOStats)
				tmStart = sphMicroTimer();

			HANDLE hFile;
			hFile = (HANDLE)_get_osfhandle(iFD);
			if (hFile == INVALID_HANDLE_VALUE)
				return -1;

			STATIC_SIZE_ASSERT(SphOffset_t, 8);
			OVERLAPPED tOverlapped = { 0 };
			tOverlapped.Offset = (DWORD)(iOffset & I64C(0xffffffff));
			tOverlapped.OffsetHigh = (DWORD)(iOffset >> 32);

			DWORD uRes;
			if (!ReadFile(hFile, pBuf, iBytes, &uRes, &tOverlapped))
			{
				DWORD uErr = GetLastError();
				if (uErr == ERROR_HANDLE_EOF)
					return 0;

				errno = uErr; // FIXME! should remap from Win to POSIX
				return -1;
			}

			if (pIOStats)
			{
				pIOStats->m_iReadTime += sphMicroTimer() - tmStart;
				pIOStats->m_iReadOps++;
				pIOStats->m_iReadBytes += iBytes;
			}

			return uRes;
		}

#else
#if HAVE_PREAD

		// atomic seek+read for non-Windows systems with pread() call
		int sphPread(int iFD, void* pBuf, int iBytes, SphOffset_t iOffset)
		{
			CSphIOStats* pIOStats = GetIOStats();
			if (!pIOStats)
				return ::pread(iFD, pBuf, iBytes, iOffset);

			int64_t tmStart = sphMicroTimer();
			int iRes = (int) ::pread(iFD, pBuf, iBytes, iOffset);
			if (pIOStats)
			{
				pIOStats->m_iReadTime += sphMicroTimer() - tmStart;
				pIOStats->m_iReadOps++;
				pIOStats->m_iReadBytes += iBytes;
			}
			return iRes;
		}

#else

		// generic fallback; prone to races between seek and read
		int sphPread(int iFD, void* pBuf, int iBytes, SphOffset_t iOffset)
		{
			if (sphSeek(iFD, iOffset, SEEK_SET) == -1)
				return -1;

			return sphReadThrottled(iFD, pBuf, iBytes, &g_tThrottle);
		}

#endif // HAVE_PREAD
#endif // USE_WINDOWS



		DWORD CSphReader::UnzipInt() { SPH_VARINT_DECODE(DWORD, GetByte()); }
		uint64_t CSphReader::UnzipOffset() { SPH_VARINT_DECODE(uint64_t, GetByte()); }



		/////////////////////////////////////////////////////////////////////////////


		void CSphReader::UpdateCache()
		{
			CSphScopedProfile tProf(m_pProfile, m_eProfileState);

			assert(m_iFD >= 0);

			// alloc buf on first actual read
			if (!m_pBuff)
			{
				if (m_iBufSize <= 0)
					m_iBufSize = DEFAULT_READ_BUFFER;

				m_bBufOwned = true;
				m_pBuff = new BYTE[m_iBufSize];
			}

			// stream position could be changed externally
			// so let's just hope that the OS optimizes redundant seeks
			SphOffset_t iNewPos = m_iPos + Min(m_iBuffPos, m_iBuffUsed);

			if (m_iSizeHint <= 0)
				m_iSizeHint = (m_iReadUnhinted > 0) ? m_iReadUnhinted : DEFAULT_READ_UNHINTED;
			int iReadLen = Min(m_iSizeHint, m_iBufSize);

			m_iBuffPos = 0;
			m_iBuffUsed = sphPread(m_iFD, m_pBuff, iReadLen, iNewPos); // FIXME! what about throttling?

			if (m_iBuffUsed < 0)
			{
				m_iBuffUsed = m_iBuffPos = 0;
				m_bError = true;
				m_sError.SetSprintf("pread error in %s: pos=" INT64_FMT ", len=%d, code=%d, msg=%s",
					m_sFilename.cstr(), (int64_t)iNewPos, iReadLen, errno, strerror(errno));
				return;
			}

			// all fine, adjust offset and hint
			m_iSizeHint -= m_iBuffUsed;
			m_iPos = iNewPos;
		}


		const CSphReader& CSphReader::operator = (const CSphReader& rhs)
		{
			SetFile(rhs.m_iFD, rhs.m_sFilename.cstr());
			SeekTo(rhs.m_iPos + rhs.m_iBuffPos, rhs.m_iSizeHint);
			return *this;
		}


		DWORD CSphReader::GetDword()
		{
			DWORD uRes = 0;
			GetBytes(&uRes, sizeof(DWORD));
			return uRes;
		}


		SphOffset_t CSphReader::GetOffset()
		{
			SphOffset_t uRes = 0;
			GetBytes(&uRes, sizeof(SphOffset_t));
			return uRes;
		}


		CSphString CSphReader::GetString()
		{
			CSphString sRes;

			DWORD iLen = GetDword();
			if (iLen)
			{
				char* sBuf = new char[iLen];
				GetBytes(sBuf, iLen);
				sRes.SetBinary(sBuf, iLen);
				SafeDeleteArray(sBuf);
			}

			return sRes;
		}

		bool CSphReader::Tag(const char* sTag)
		{
			if (m_bError)
				return false;

			assert(sTag && *sTag); // empty tags are nonsense
			assert(strlen(sTag) < 64); // huge tags are nonsense

			int iLen = strlen(sTag);
			char sBuf[64];
			GetBytes(sBuf, iLen);
			if (!memcmp(sBuf, sTag, iLen))
				return true;
			m_bError = true;
			m_sError.SetSprintf("expected tag %s was not found", sTag);
			return false;
		}


		CSphAutoreader::~CSphAutoreader()
		{
			Close();
		}


		bool CSphAutoreader::Open(const CSphString& sFilename, CSphString& sError)
		{
			assert(m_iFD < 0);
			assert(!sFilename.IsEmpty());

			m_iFD = ::open(sFilename.cstr(), SPH_O_READ, 0644);
			m_iPos = 0;
			m_iBuffPos = 0;
			m_iBuffUsed = 0;
			m_sFilename = sFilename;

			if (m_iFD < 0)
				sError.SetSprintf("failed to open %s: %s", sFilename.cstr(), strerror(errno));
			return (m_iFD >= 0);
		}


		void CSphAutoreader::Close()
		{
			if (m_iFD >= 0)
				::close(m_iFD);
			m_iFD = -1;
		}


		SphOffset_t CSphAutoreader::GetFilesize()
		{
			assert(m_iFD >= 0);

			struct_stat st;
			if (m_iFD < 0 || fstat(m_iFD, &st) < 0)
				return -1;

			return st.st_size;
		}

	
}