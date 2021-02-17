#include "neo/io/io_stats.h"

namespace NEO {
	namespace STATS {

		// whatever to collect IO stats
		static bool g_bCollectIOStats = false;
		static SphThreadKey_t g_tIOStatsTls;

		CSphIOStats::CSphIOStats()
			: m_iReadTime(0)
			, m_iReadOps(0)
			, m_iReadBytes(0)
			, m_iWriteTime(0)
			, m_iWriteOps(0)
			, m_iWriteBytes(0)
			, m_pPrev(NULL)
		{}


		CSphIOStats::~CSphIOStats()
		{
			Stop();
		}


		void CSphIOStats::Start()
		{
			if (!g_bCollectIOStats)
				return;

			m_pPrev = (CSphIOStats*)sphThreadGet(g_tIOStatsTls);
			sphThreadSet(g_tIOStatsTls, this);
			m_bEnabled = true;
		}

		void CSphIOStats::Stop()
		{
			if (!g_bCollectIOStats)
				return;

			m_bEnabled = false;
			sphThreadSet(g_tIOStatsTls, m_pPrev);
		}


		void CSphIOStats::Add(const CSphIOStats& b)
		{
			m_iReadTime += b.m_iReadTime;
			m_iReadOps += b.m_iReadOps;
			m_iReadBytes += b.m_iReadBytes;
			m_iWriteTime += b.m_iWriteTime;
			m_iWriteOps += b.m_iWriteOps;
			m_iWriteBytes += b.m_iWriteBytes;
		}

		//fwd dec
		//bool			sphCalcFileCRC32(const char* szFilename, DWORD& uCRC32);

		bool GetFileStats(const char* szFilename, CSphSavedFile& tInfo, CSphString* pError)
		{
			if (!szFilename || !*szFilename)
			{
				memset(&tInfo, 0, sizeof(tInfo));
				return true;
			}

			tInfo.m_sFilename = szFilename;

			struct_stat tStat;
			memset(&tStat, 0, sizeof(tStat));
			if (stat(szFilename, &tStat) < 0)
			{
				if (pError)
					*pError = strerror(errno);
				memset(&tStat, 0, sizeof(tStat));
				return false;
			}

			tInfo.m_uSize = tStat.st_size;
			tInfo.m_uCTime = tStat.st_ctime;
			tInfo.m_uMTime = tStat.st_mtime;

			DWORD uCRC32 = 0;
			if (!sphCalcFileCRC32(szFilename, uCRC32))
				return false;

			tInfo.m_uCRC32 = uCRC32;

			return true;
		}



		bool sphInitIOStats()
		{
			if (!sphThreadKeyCreate(&g_tIOStatsTls))
				return false;

			g_bCollectIOStats = true;
			return true;
		}

		void sphDoneIOStats()
		{
			sphThreadKeyDelete(g_tIOStatsTls);
			g_bCollectIOStats = false;
		}



		/*static*/ CSphIOStats* GetIOStats()
		{
			if (!g_bCollectIOStats)
				return NULL;

			CSphIOStats* pIOStats = (CSphIOStats*)sphThreadGet(g_tIOStatsTls);

			if (!pIOStats || !pIOStats->IsEnabled())
				return NULL;
			return pIOStats;
		}

		/*static*/ int64_t sphRead(int iFD, void* pBuf, size_t iCount)
		{
			CSphIOStats* pIOStats = GetIOStats();
			int64_t tmStart = 0;
			if (pIOStats)
				tmStart = sphMicroTimer();

			int64_t iRead = ::read(iFD, pBuf, iCount);

			if (pIOStats)
			{
				pIOStats->m_iReadTime += sphMicroTimer() - tmStart;
				pIOStats->m_iReadOps++;
				pIOStats->m_iReadBytes += (-1 == iRead) ? 0 : iCount;
			}

			return iRead;
		}
	}
}