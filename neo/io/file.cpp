#include "neo/platform/compat.h"
#include "neo/io/file.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/io/crc32.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace NEO {
	

		CSphSavedFile::CSphSavedFile()
			: m_uSize(0)
			, m_uCTime(0)
			, m_uMTime(0)
			, m_uCRC32(0)
		{
		}


		CSphEmbeddedFiles::CSphEmbeddedFiles()
			: m_bEmbeddedSynonyms(false)
			, m_bEmbeddedStopwords(false)
			, m_bEmbeddedWordforms(false)
		{
		}


		void CSphEmbeddedFiles::Reset()
		{
			m_dSynonyms.Reset();
			m_dStopwordFiles.Reset();
			m_dStopwords.Reset();
			m_dWordforms.Reset();
			m_dWordformFiles.Reset();
		}


		void ReadFileInfo(CSphReader& tReader, const char* szFilename, CSphSavedFile& tFile, CSphString* sWarning)
		{
			tFile.m_uSize = tReader.GetOffset();
			tFile.m_uCTime = tReader.GetOffset();
			tFile.m_uMTime = tReader.GetOffset();
			tFile.m_uCRC32 = tReader.GetDword();
			tFile.m_sFilename = szFilename;

			if (szFilename && *szFilename && sWarning)
			{
				struct_stat tFileInfo;
				if (stat(szFilename, &tFileInfo) < 0)
					sWarning->SetSprintf("failed to stat %s: %s", szFilename, strerror(errno));
				else
				{
					DWORD uMyCRC32 = 0;
					if (!sphCalcFileCRC32(szFilename, uMyCRC32))
						sWarning->SetSprintf("failed to calculate CRC32 for %s", szFilename);
					else
						if (uMyCRC32 != tFile.m_uCRC32 || tFileInfo.st_size != tFile.m_uSize
							|| tFileInfo.st_ctime != tFile.m_uCTime || tFileInfo.st_mtime != tFile.m_uMTime)
							sWarning->SetSprintf("'%s' differs from the original", szFilename);
				}
			}
		}


		void WriteFileInfo(CSphWriter& tWriter, const CSphSavedFile& tInfo)
		{
			tWriter.PutOffset(tInfo.m_uSize);
			tWriter.PutOffset(tInfo.m_uCTime);
			tWriter.PutOffset(tInfo.m_uMTime);
			tWriter.PutDword(tInfo.m_uCRC32);
		}

	
}