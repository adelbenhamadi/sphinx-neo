#pragma once
#include "neo/int/types.h"
#include "neo/io/crc32.h"
#include "neo/io/file.h"

namespace NEO {
	namespace STATS {
		class CSphIOStats
		{
		public:
			int64_t		m_iReadTime;
			DWORD		m_iReadOps;
			int64_t		m_iReadBytes;
			int64_t		m_iWriteTime;
			DWORD		m_iWriteOps;
			int64_t		m_iWriteBytes;

			CSphIOStats();
			~CSphIOStats();

			void		Start();
			void		Stop();

			void		Add(const CSphIOStats& b);
			bool		IsEnabled() { return m_bEnabled; }

		private:
			bool		m_bEnabled;
			CSphIOStats* m_pPrev;
		};



		/// initialize IO statistics collecting
		bool			sphInitIOStats();

		/// clean up IO statistics collector
		void			sphDoneIOStats();

		/*static*/ CSphIOStats* GetIOStats();

		bool GetFileStats(const char* szFilename, CSphSavedFile& tInfo, CSphString* pError);

		// a tiny wrapper over ::read() which additionally performs IO stats update
		/*static*/ int64_t sphRead(int iFD, void* pBuf, size_t iCount);

	}
}