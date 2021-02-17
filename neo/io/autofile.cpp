#include "neo/io/autofile.h"
#include "neo/core/globals.h"
#include "neo/io/io_stats.h"

namespace NEO {

	CSphAutofile::CSphAutofile()
		: m_iFD(-1)
		, m_bTemporary(false)
		, m_bWouldTemporary(false)
		, m_pStat(NULL)
	{
	}


	CSphAutofile::CSphAutofile(const CSphString& sName, int iMode, CSphString& sError, bool bTemp)
		: m_iFD(-1)
		, m_bTemporary(false)
		, m_bWouldTemporary(false)
		, m_pStat(NULL)
	{
		Open(sName, iMode, sError, bTemp);
	}


	CSphAutofile::~CSphAutofile()
	{
		Close();
	}


	int CSphAutofile::Open(const CSphString& sName, int iMode, CSphString& sError, bool bTemp)
	{
		assert(m_iFD == -1 && m_sFilename.IsEmpty());
		assert(!sName.IsEmpty());

#if USE_WINDOWS
		if (iMode == SPH_O_READ)
		{
			intptr_t tFD = (intptr_t)CreateFile(sName.cstr(), GENERIC_READ, FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
			m_iFD = _open_osfhandle(tFD, 0);
		}
		else
			m_iFD = ::open(sName.cstr(), iMode, 0644);
#else
		m_iFD = ::open(sName.cstr(), iMode, 0644);
#endif
		m_sFilename = sName; // not exactly sure why is this uncoditional. for error reporting later, i suppose

		if (m_iFD < 0)
			sError.SetSprintf("failed to open %s: %s", sName.cstr(), strerror(errno));
		else
		{
			m_bTemporary = bTemp; // only if we managed to actually open it
			m_bWouldTemporary = true; // if a shit happen - we could delete the file.
		}

		return m_iFD;
	}


	void CSphAutofile::Close()
	{
		if (m_iFD >= 0)
		{
			::close(m_iFD);
			if (m_bTemporary)
				::unlink(m_sFilename.cstr());
		}

		m_iFD = -1;
		m_sFilename = "";
		m_bTemporary = false;
		m_bWouldTemporary = false;
	}


	void CSphAutofile::SetTemporary()
	{
		m_bTemporary = m_bWouldTemporary;
	}


	const char* CSphAutofile::GetFilename() const
	{
		assert(m_sFilename.cstr());
		return m_sFilename.cstr();
	}


	SphOffset_t CSphAutofile::GetSize(SphOffset_t iMinSize, bool bCheckSizeT, CSphString& sError)
	{
		struct_stat st;
		if (stat(GetFilename(), &st) < 0)
		{
			sError.SetSprintf("failed to stat %s: %s", GetFilename(), strerror(errno));
			return -1;
		}
		if (st.st_size < iMinSize)
		{
			sError.SetSprintf("failed to load %s: bad size " INT64_FMT " (at least " INT64_FMT " bytes expected)",
				GetFilename(), (int64_t)st.st_size, (int64_t)iMinSize);
			return -1;
		}
		if (bCheckSizeT)
		{
			size_t uCheck = (size_t)st.st_size;
			if (st.st_size != SphOffset_t(uCheck))
			{
				sError.SetSprintf("failed to load %s: bad size " INT64_FMT " (out of size_t; 4 GB limit on 32-bit machine hit?)",
					GetFilename(), (int64_t)st.st_size);
				return -1;
			}
		}
		return st.st_size;
	}


	SphOffset_t CSphAutofile::GetSize()
	{
		CSphString sTmp;
		return GetSize(0, false, sTmp);
	}


	bool CSphAutofile::Read(void* pBuf, int64_t iCount, CSphString& sError)
	{
		int64_t iToRead = iCount;
		BYTE* pCur = (BYTE*)pBuf;
		while (iToRead > 0)
		{
			int64_t iToReadOnce = (m_pStat)
				? Min(iToRead, SPH_READ_PROGRESS_CHUNK)
				: Min(iToRead, SPH_READ_NOPROGRESS_CHUNK);
			int64_t iGot = STATS::sphRead(GetFD(), pCur, (size_t)iToReadOnce);

			if (iGot == -1)
			{
				// interrupted by a signal - try again
				if (errno == EINTR)
					continue;

				sError.SetSprintf("read error in %s (%s); " INT64_FMT " of " INT64_FMT " bytes read",
					GetFilename(), strerror(errno), iCount - iToRead, iCount);
				return false;
			}

			// EOF
			if (iGot == 0)
			{
				sError.SetSprintf("unexpected EOF in %s (%s); " INT64_FMT " of " INT64_FMT " bytes read",
					GetFilename(), strerror(errno), iCount - iToRead, iCount);
				return false;
			}

			iToRead -= iGot;
			pCur += iGot;

			if (m_pStat)
			{
				m_pStat->m_iBytes += iGot;
				m_pStat->Show(false);
			}
		}

		if (iToRead != 0)
		{
			sError.SetSprintf("read error in %s (%s); " INT64_FMT " of " INT64_FMT " bytes read",
				GetFilename(), strerror(errno), iCount - iToRead, iCount);
			return false;
		}
		return true;
	}


	void CSphAutofile::SetProgressCallback(CSphIndexProgress* pStat)
	{
		m_pStat = pStat;
	}


}