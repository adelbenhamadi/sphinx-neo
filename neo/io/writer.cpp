#include "neo/core/globals.h"
#include "neo/io/writer.h"
#include "neo/io/io.h"

namespace NEO {
	

		CSphWriter::CSphWriter()
			: m_sName("")
			, m_iPos(-1)
			, m_iWritten(0)

			, m_iFD(-1)
			, m_iPoolUsed(0)
			, m_pBuffer(NULL)
			, m_pPool(NULL)
			, m_bOwnFile(false)
			, m_pSharedOffset(NULL)
			, m_iBufferSize(262144)

			, m_bError(false)
			, m_pError(NULL)
		{
			m_pThrottle = &g_tThrottle;
		}


		void CSphWriter::SetBufferSize(int iBufferSize)
		{
			if (iBufferSize != m_iBufferSize)
			{
				m_iBufferSize = Max(iBufferSize, 262144);
				SafeDeleteArray(m_pBuffer);
			}
		}


		bool CSphWriter::OpenFile(const CSphString& sName, CSphString& sErrorBuffer)
		{
			assert(!sName.IsEmpty());
			assert(m_iFD < 0 && "already open");

			m_bOwnFile = true;
			m_sName = sName;
			m_pError = &sErrorBuffer;

			if (!m_pBuffer)
				m_pBuffer = new BYTE[m_iBufferSize];

			m_iFD = ::open(m_sName.cstr(), SPH_O_NEW, 0644);
			m_pPool = m_pBuffer;
			m_iPoolUsed = 0;
			m_iPos = 0;
			m_iWritten = 0;
			m_bError = (m_iFD < 0);

			if (m_bError)
				m_pError->SetSprintf("failed to create %s: %s", sName.cstr(), strerror(errno));

			return !m_bError;
		}


		void CSphWriter::SetFile(CSphAutofile& tAuto, SphOffset_t* pSharedOffset, CSphString& sError)
		{
			assert(m_iFD < 0 && "already open");
			m_bOwnFile = false;

			if (!m_pBuffer)
				m_pBuffer = new BYTE[m_iBufferSize];

			m_iFD = tAuto.GetFD();
			m_sName = tAuto.GetFilename();
			m_pPool = m_pBuffer;
			m_iPoolUsed = 0;
			m_iPos = 0;
			m_iWritten = 0;
			m_pSharedOffset = pSharedOffset;
			m_pError = &sError;
			assert(m_pError);
		}


		CSphWriter::~CSphWriter()
		{
			CloseFile();
			SafeDeleteArray(m_pBuffer);
		}


		void CSphWriter::CloseFile(bool bTruncate)
		{
			if (m_iFD >= 0)
			{
				Flush();
				if (bTruncate)
					sphTruncate(m_iFD);
				if (m_bOwnFile)
					::close(m_iFD);
				m_iFD = -1;
			}
		}

		void CSphWriter::UnlinkFile()
		{
			if (m_bOwnFile)
			{
				if (m_iFD >= 0)
					::close(m_iFD);

				m_iFD = -1;
				::unlink(m_sName.cstr());
				m_sName = "";
			}
			SafeDeleteArray(m_pBuffer);
		}


		void CSphWriter::PutByte(int data)
		{
			assert(m_pPool);
			if (m_iPoolUsed == m_iBufferSize)
				Flush();
			*m_pPool++ = BYTE(data & 0xff);
			m_iPoolUsed++;
			m_iPos++;
		}


		void CSphWriter::PutBytes(const void* pData, int64_t iSize)
		{
			assert(m_pPool);
			const BYTE* pBuf = (const BYTE*)pData;
			while (iSize > 0)
			{
				int iPut = (iSize < m_iBufferSize ? int(iSize) : m_iBufferSize); // comparison int64 to int32
				if (m_iPoolUsed + iPut > m_iBufferSize)
					Flush();
				assert(m_iPoolUsed + iPut <= m_iBufferSize);

				memcpy(m_pPool, pBuf, iPut);
				m_pPool += iPut;
				m_iPoolUsed += iPut;
				m_iPos += iPut;

				pBuf += iPut;
				iSize -= iPut;
			}
		}


		void CSphWriter::ZipInt(DWORD uValue)
		{
			int iBytes = 1;

			DWORD u = (uValue >> 7);
			while (u)
			{
				u >>= 7;
				iBytes++;
			}

			while (iBytes--)
				PutByte(
					(0x7f & (uValue >> (7 * iBytes)))
					| (iBytes ? 0x80 : 0));
		}


		void CSphWriter::ZipOffset(uint64_t uValue)
		{
			int iBytes = 1;

			uint64_t u = ((uint64_t)uValue) >> 7;
			while (u)
			{
				u >>= 7;
				iBytes++;
			}

			while (iBytes--)
				PutByte(
					(0x7f & (DWORD)(uValue >> (7 * iBytes)))
					| (iBytes ? 0x80 : 0));
		}


		void CSphWriter::ZipOffsets(CSphVector<SphOffset_t>* pData)
		{
			assert(pData);

			SphOffset_t* pValue = &((*pData)[0]);
			int n = pData->GetLength();

			while (n-- > 0)
			{
				SphOffset_t uValue = *pValue++;

				int iBytes = 1;

				uint64_t u = ((uint64_t)uValue) >> 7;
				while (u)
				{
					u >>= 7;
					iBytes++;
				}

				while (iBytes--)
					PutByte(
						(0x7f & (DWORD)(uValue >> (7 * iBytes)))
						| (iBytes ? 0x80 : 0));
			}
		}


		void CSphWriter::Flush()
		{
			if (m_pSharedOffset && *m_pSharedOffset != m_iWritten)
				sphSeek(m_iFD, m_iWritten, SEEK_SET);

			if (!sphWriteThrottled(m_iFD, m_pBuffer, m_iPoolUsed, m_sName.cstr(), *m_pError, m_pThrottle))
				m_bError = true;

			m_iWritten += m_iPoolUsed;
			m_iPoolUsed = 0;
			m_pPool = m_pBuffer;

			if (m_pSharedOffset)
				*m_pSharedOffset = m_iWritten;
		}


		void CSphWriter::PutString(const char* szString)
		{
			int iLen = szString ? strlen(szString) : 0;
			PutDword(iLen);
			if (iLen)
				PutBytes(szString, iLen);
		}


		void CSphWriter::PutString(const CSphString& sString)
		{
			int iLen = sString.Length();
			PutDword(iLen);
			if (iLen)
				PutBytes(sString.cstr(), iLen);
		}


		void CSphWriter::Tag(const char* sTag)
		{
			assert(sTag && *sTag); // empty tags are nonsense
			assert(strlen(sTag) < 64); // huge tags are nonsense
			PutBytes(sTag, strlen(sTag));
		}


		void CSphWriter::SeekTo(SphOffset_t iPos)
		{
			assert(iPos >= 0);

			if (iPos >= m_iWritten && iPos <= (m_iWritten + m_iPoolUsed))
			{
				// seeking inside the buffer
				m_iPoolUsed = (int)(iPos - m_iWritten);
				m_pPool = m_pBuffer + m_iPoolUsed;
			}
			else
			{
				assert(iPos < m_iWritten); // seeking forward in a writer, we don't support it
				sphSeek(m_iFD, iPos, SEEK_SET);

				// seeking outside the buffer; so the buffer must be discarded
				// also, current write position must be adjusted
				m_pPool = m_pBuffer;
				m_iPoolUsed = 0;
				m_iWritten = iPos;
			}
			m_iPos = iPos;
		}
	
}