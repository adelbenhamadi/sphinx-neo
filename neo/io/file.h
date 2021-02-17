#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/io/autofile.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"

namespace NEO {
	

		


		struct CSphSavedFile
		{
			CSphString			m_sFilename;
			SphOffset_t			m_uSize;
			SphOffset_t			m_uCTime;
			SphOffset_t			m_uMTime;
			DWORD				m_uCRC32;

			CSphSavedFile();
		};


		struct CSphEmbeddedFiles
		{
			bool						m_bEmbeddedSynonyms;
			bool						m_bEmbeddedStopwords;
			bool						m_bEmbeddedWordforms;
			CSphSavedFile				m_tSynonymFile;
			CSphVector<CSphString>		m_dSynonyms;
			CSphVector<CSphSavedFile>	m_dStopwordFiles;
			CSphVector<SphWordID_t>		m_dStopwords;
			CSphVector<CSphString>		m_dWordforms;
			CSphVector<CSphSavedFile>	m_dWordformFiles;

			CSphEmbeddedFiles();

			void						Reset();
		};

		void ReadFileInfo(CSphReader& tReader, const char* szFilename, CSphSavedFile& tFile, CSphString* sWarning);

		void WriteFileInfo(CSphWriter& tWriter, const CSphSavedFile& tInfo);

	
}