#include "neo/dict/dict.h"
#include "neo/io/autofile.h"
#include "neo/io/file.h"
#include "neo/dict/dict_keyword.h"
#include "neo/dict/dict_star.h"
#include "neo/dict/dict_traits.h"
#include "neo/dict/dict_exact.h"
#include "neo/core/global_idf.h"

namespace NEO {

	

	CSphDict* SetupDictionary(CSphDict* pDict, const CSphDictSettings& tSettings,
		const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex,
		CSphString& sError)
	{
		assert(pTokenizer);
		assert(pDict);

		pDict->Setup(tSettings);
		int iRet = pDict->SetMorphology(tSettings.m_sMorphology.cstr(), sError);
		if (iRet == CSphDict::ST_ERROR)
		{
			SafeDelete(pDict);
			return NULL;
		}

		if (pFiles && pFiles->m_bEmbeddedStopwords)
			pDict->LoadStopwords(pFiles->m_dStopwords);
		else
			pDict->LoadStopwords(tSettings.m_sStopwords.cstr(), pTokenizer);

		pDict->LoadWordforms(tSettings.m_dWordforms, pFiles && pFiles->m_bEmbeddedWordforms ? pFiles : NULL, pTokenizer, sIndex);

		return pDict;
	}


	void CSphDict::DictBegin(CSphAutofile&, CSphAutofile&, int, ThrottleState_t*) {}
	void CSphDict::DictEntry(const CSphDictEntry&) {}
	void CSphDict::DictEndEntries(SphOffset_t) {}
	bool CSphDict::DictEnd(DictHeader_t*, int, CSphString&, ThrottleState_t*) { return true; }
	bool CSphDict::DictIsError() const { return true; }



	void sphShutdownWordforms()
	{
		CSphVector<CSphSavedFile> dEmptyFiles;
		CSphDiskDictTraits::SweepWordformContainers(dEmptyFiles);
	}


	bool sphPrereadGlobalIDF(const CSphString& sPath, CSphString& sError)
	{
		g_tGlobalIDFLock.Lock();

		CSphGlobalIDF** ppGlobalIDF = g_hGlobalIDFs(sPath);
		bool bExpired = (ppGlobalIDF && *ppGlobalIDF && (*ppGlobalIDF)->Touch(sPath));

		if (!ppGlobalIDF || bExpired)
		{
			if (bExpired)
				sphLogDebug("Reloading global IDF (%s)", sPath.cstr());
			else
				sphLogDebug("Loading global IDF (%s)", sPath.cstr());

			// unlock while prereading
			g_tGlobalIDFLock.Unlock();

			CSphGlobalIDF* pGlobalIDF = new CSphGlobalIDF();
			if (!pGlobalIDF->Preread(sPath, sError))
			{
				SafeDelete(pGlobalIDF);
				return false;
			}

			// lock while updating
			g_tGlobalIDFLock.Lock();

			if (bExpired)
			{
				ppGlobalIDF = g_hGlobalIDFs(sPath);
				if (ppGlobalIDF)
				{
					CSphGlobalIDF* pOld = *ppGlobalIDF;
					*ppGlobalIDF = pGlobalIDF;
					SafeDelete(pOld);
				}
			}
			else
			{
				if (!g_hGlobalIDFs.Add(pGlobalIDF, sPath))
					SafeDelete(pGlobalIDF);
			}
		}

		g_tGlobalIDFLock.Unlock();

		return true;
	}


	void sphUpdateGlobalIDFs(const CSphVector<CSphString>& dFiles)
	{
		// delete unlisted entries
		g_tGlobalIDFLock.Lock();
		g_hGlobalIDFs.IterateStart();
		while (g_hGlobalIDFs.IterateNext())
		{
			const CSphString& sKey = g_hGlobalIDFs.IterateGetKey();
			if (!dFiles.Contains(sKey))
			{
				sphLogDebug("Unloading global IDF (%s)", sKey.cstr());
				SafeDelete(g_hGlobalIDFs.IterateGet());
				g_hGlobalIDFs.Delete(sKey);
			}
		}
		g_tGlobalIDFLock.Unlock();

		// load/rotate remaining entries
		CSphString sError;
		ARRAY_FOREACH(i, dFiles)
		{
			CSphString sPath = dFiles[i];
			if (!sphPrereadGlobalIDF(sPath, sError))
				sphLogDebug("Could not load global IDF (%s): %s", sPath.cstr(), sError.cstr());
		}
	}


	void sphShutdownGlobalIDFs()
	{
		CSphVector<CSphString> dEmptyFiles;
		sphUpdateGlobalIDFs(dEmptyFiles);
	}


	//////////////////////////////////////////////////////////////////////////


	void LoadDictionarySettings(CSphReader& tReader, CSphDictSettings& tSettings,
		CSphEmbeddedFiles& tEmbeddedFiles, DWORD uVersion, CSphString& sWarning)
	{
		if (uVersion < 9)
			return;

		tSettings.m_sMorphology = tReader.GetString();

		tEmbeddedFiles.m_bEmbeddedStopwords = false;
		if (uVersion >= 30)
		{
			tEmbeddedFiles.m_bEmbeddedStopwords = !!tReader.GetByte();
			if (tEmbeddedFiles.m_bEmbeddedStopwords)
			{
				int nStopwords = (int)tReader.GetDword();
				tEmbeddedFiles.m_dStopwords.Resize(nStopwords);
				ARRAY_FOREACH(i, tEmbeddedFiles.m_dStopwords)
					tEmbeddedFiles.m_dStopwords[i] = (SphWordID_t)tReader.UnzipOffset();
			}
		}

		tSettings.m_sStopwords = tReader.GetString();
		int nFiles = tReader.GetDword();

		CSphString sFile;
		tEmbeddedFiles.m_dStopwordFiles.Resize(nFiles);
		for (int i = 0; i < nFiles; i++)
		{
			sFile = tReader.GetString();
			ReadFileInfo(tReader, sFile.cstr(), tEmbeddedFiles.m_dStopwordFiles[i], tEmbeddedFiles.m_bEmbeddedSynonyms ? NULL : &sWarning);
		}

		tEmbeddedFiles.m_bEmbeddedWordforms = false;
		if (uVersion >= 30)
		{
			tEmbeddedFiles.m_bEmbeddedWordforms = !!tReader.GetByte();
			if (tEmbeddedFiles.m_bEmbeddedWordforms)
			{
				int nWordforms = (int)tReader.GetDword();
				tEmbeddedFiles.m_dWordforms.Resize(nWordforms);
				ARRAY_FOREACH(i, tEmbeddedFiles.m_dWordforms)
					tEmbeddedFiles.m_dWordforms[i] = tReader.GetString();
			}
		}

		if (uVersion >= 29)
			tSettings.m_dWordforms.Resize(tReader.GetDword());
		else
			tSettings.m_dWordforms.Resize(1);

		tEmbeddedFiles.m_dWordformFiles.Resize(tSettings.m_dWordforms.GetLength());
		ARRAY_FOREACH(i, tSettings.m_dWordforms)
		{
			tSettings.m_dWordforms[i] = tReader.GetString();
			ReadFileInfo(tReader, tSettings.m_dWordforms[i].cstr(),
				tEmbeddedFiles.m_dWordformFiles[i], tEmbeddedFiles.m_bEmbeddedWordforms ? NULL : &sWarning);
		}

		if (uVersion >= 13)
			tSettings.m_iMinStemmingLen = tReader.GetDword();

		tSettings.m_bWordDict = false; // default to crc for old indexes
		if (uVersion >= 21)
		{
			tSettings.m_bWordDict = (tReader.GetByte() != 0);
			if (!tSettings.m_bWordDict)
				sphWarning("dict=crc deprecated, use dict=keywords instead");
		}

		if (uVersion >= 36)
			tSettings.m_bStopwordsUnstemmed = (tReader.GetByte() != 0);

		if (uVersion >= 37)
			tSettings.m_sMorphFingerprint = tReader.GetString();
	}


	/// gets called from and MUST be in sync with RtIndex_t::SaveDiskHeader()!
	/// note that SaveDiskHeader() occasionaly uses some PREVIOUS format version!
	void SaveDictionarySettings(CSphWriter& tWriter, CSphDict* pDict, bool bForceWordDict, int iEmbeddedLimit)
	{
		assert(pDict);
		const CSphDictSettings& tSettings = pDict->GetSettings();

		tWriter.PutString(tSettings.m_sMorphology.cstr());
		const CSphVector <CSphSavedFile>& dSWFileInfos = pDict->GetStopwordsFileInfos();
		SphOffset_t uTotalSize = 0;
		ARRAY_FOREACH(i, dSWFileInfos)
			uTotalSize += dSWFileInfos[i].m_uSize;

		bool bEmbedStopwords = uTotalSize <= (SphOffset_t)iEmbeddedLimit;
		tWriter.PutByte(bEmbedStopwords ? 1 : 0);
		if (bEmbedStopwords)
			pDict->WriteStopwords(tWriter);

		tWriter.PutString(tSettings.m_sStopwords.cstr());
		tWriter.PutDword(dSWFileInfos.GetLength());
		ARRAY_FOREACH(i, dSWFileInfos)
		{
			tWriter.PutString(dSWFileInfos[i].m_sFilename.cstr());
			WriteFileInfo(tWriter, dSWFileInfos[i]);
		}

		const CSphVector <CSphSavedFile>& dWFFileInfos = pDict->GetWordformsFileInfos();
		uTotalSize = 0;
		ARRAY_FOREACH(i, dWFFileInfos)
			uTotalSize += dWFFileInfos[i].m_uSize;

		bool bEmbedWordforms = uTotalSize <= (SphOffset_t)iEmbeddedLimit;
		tWriter.PutByte(bEmbedWordforms ? 1 : 0);
		if (bEmbedWordforms)
			pDict->WriteWordforms(tWriter);

		tWriter.PutDword(dWFFileInfos.GetLength());
		ARRAY_FOREACH(i, dWFFileInfos)
		{
			tWriter.PutString(dWFFileInfos[i].m_sFilename.cstr());
			WriteFileInfo(tWriter, dWFFileInfos[i]);
		}

		tWriter.PutDword(tSettings.m_iMinStemmingLen);
		tWriter.PutByte(tSettings.m_bWordDict || bForceWordDict);
		tWriter.PutByte(tSettings.m_bStopwordsUnstemmed);
		tWriter.PutString(pDict->GetMorphDataFingerprint());
	}


}