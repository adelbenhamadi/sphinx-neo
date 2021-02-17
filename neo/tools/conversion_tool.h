#pragma once

#include "neo/platform//thread.h"
#include "neo/int/types.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/source/schema.h"
#include "neo/tokenizer/tokenizer.h"
#include "neo/tokenizer/tokenizer_settings.h"
#include "neo/index/index_settings.h"
#include "neo/dict/dict.h"
#include "neo/dict/dict_crc.h"
#include "neo/dict/dict_keyword.h"
#include "neo/dict/dict_reader.h"
#include "neo/core/die.h"
#include "neo/core/infix.h"
#include "neo/core/skip_list.h"
#include "neo/core/word_list.h"
#include "neo/tools/docinfo_transformer.h"
#include "neo/core/keyword_delta_writer.h"


#include "neo/sphinx/xrlp.h"

namespace NEO {

	static void CopyBytes(CSphWriter& wrTo, CSphReader& rdFrom, int iBytes)
	{
		const int BUFSIZE = 65536;
		BYTE* pBuf = new BYTE[BUFSIZE];

		int iCopied = 0;
		while (iCopied < iBytes)
		{
			int iToCopy = Min(iBytes - iCopied, BUFSIZE);
			rdFrom.GetBytes(pBuf, iToCopy);
			wrTo.PutBytes(pBuf, iToCopy);
			iCopied += iToCopy;
		}

		SafeDeleteArray(pBuf);
	}


	/// post-conversion chores
	/// rename the files, show elapsed time
	static void FinalizeUpgrade(const char** sRenames, const char* sBanner, const char* sPath, int64_t tmStart)
	{
		while (*sRenames)
		{
			CSphString sFrom, sTo;
			sFrom.SetSprintf("%s%s", sPath, sRenames[0]);
			sTo.SetSprintf("%s%s", sPath, sRenames[1]);
			sRenames += 2;

			if (::rename(sFrom.cstr(), sTo.cstr()))
				sphDie("%s: rename %s to %s failed: %s\n", sBanner,
					sFrom.cstr(), sTo.cstr(), strerror(errno));
		}

		// all done! yay
		int64_t tmWall = sphMicroTimer() - tmStart;
		fprintf(stdout, "%s: elapsed %d.%d sec\n", sBanner,
			(int)(tmWall / 1000000), (int)((tmWall / 100000) % 10));
		fprintf(stdout, "%s: done!\n", sBanner);
	}

	//////////////////////////////////////////////////////////////////////////
	// V.26 TO V.27 CONVERSION TOOL, INFIX BUILDER
	//////////////////////////////////////////////////////////////////////////

	void sphDictBuildInfixes(const char* sPath)
	{
		CSphString sFilename, sError;
		int64_t tmStart = sphMicroTimer();

		if_const(INDEX_FORMAT_VERSION != 27)
			sphDie("infix upgrade: only works in v.27 builds for now; get an older indextool or contact support");

		//////////////////////////////////////////////////
		// load (interesting parts from) the index header
		//////////////////////////////////////////////////

		CSphAutoreader rdHeader;
		sFilename.SetSprintf("%s.sph", sPath);
		if (!rdHeader.Open(sFilename.cstr(), sError))
			sphDie("infix upgrade: %s", sError.cstr());

		// version
		DWORD uHeader = rdHeader.GetDword();
		DWORD uVersion = rdHeader.GetDword();
		bool bUse64 = (rdHeader.GetDword() != 0);
		ESphDocinfo eDocinfo = (ESphDocinfo)rdHeader.GetDword();

		if (uHeader != INDEX_MAGIC_HEADER)
			sphDie("infix upgrade: invalid header file");
		if (uVersion < 21 || uVersion>26)
			sphDie("infix upgrade: got v.%d header, v.21 to v.26 required", uVersion);
		if (eDocinfo == SPH_DOCINFO_INLINE)
			sphDie("infix upgrade: docinfo=inline is not supported");

		CSphSchema tSchema;
		DictHeader_t tDictHeader;
		CSphSourceStats tStats;
		CSphIndexSettings tIndexSettings;
		CSphTokenizerSettings tTokenizerSettings;
		CSphDictSettings tDictSettings;
		CSphEmbeddedFiles tEmbeddedFiles;

		ReadSchema(rdHeader, tSchema, uVersion, eDocinfo == SPH_DOCINFO_INLINE);
		SphOffset_t iMinDocid = rdHeader.GetOffset();
		tDictHeader.m_iDictCheckpointsOffset = rdHeader.GetOffset();
		tDictHeader.m_iDictCheckpoints = rdHeader.GetDword();
		tDictHeader.m_iInfixCodepointBytes = 0;
		tDictHeader.m_iInfixBlocksOffset = 0;
		tDictHeader.m_iInfixBlocksWordsSize = 0;
		tStats.m_iTotalDocuments = rdHeader.GetDword();
		tStats.m_iTotalBytes = rdHeader.GetOffset();
		LoadIndexSettings(tIndexSettings, rdHeader, uVersion);
		if (!LoadTokenizerSettings(rdHeader, tTokenizerSettings, tEmbeddedFiles, uVersion, sError))
			sphDie("infix updrade: failed to load tokenizer settings: '%s'", sError.cstr());
		LoadDictionarySettings(rdHeader, tDictSettings, tEmbeddedFiles, uVersion, sError);
		int iKillListSize = rdHeader.GetDword();
		DWORD uMinMaxIndex = rdHeader.GetDword();

		if (rdHeader.GetErrorFlag())
			sphDie("infix upgrade: failed to parse header");
		rdHeader.Close();

		////////////////////
		// generate infixes
		////////////////////

		if (!tDictSettings.m_bWordDict)
			sphDie("infix upgrade: dict=keywords required");

		tIndexSettings.m_iMinPrefixLen = 0;
		tIndexSettings.m_iMinInfixLen = 2;

		ISphTokenizer* pTokenizer = ISphTokenizer::Create(tTokenizerSettings, &tEmbeddedFiles, sError);
		if (!pTokenizer)
			sphDie("infix upgrade: %s", sError.cstr());

		tDictHeader.m_iInfixCodepointBytes = pTokenizer->GetMaxCodepointLength();
		ISphInfixBuilder* pInfixer = sphCreateInfixBuilder(tDictHeader.m_iInfixCodepointBytes, &sError);
		if (!pInfixer)
			sphDie("infix upgrade: %s", sError.cstr());

		bool bHasMorphology = !tDictSettings.m_sMorphology.IsEmpty();
		// scan all dict entries, generate infixes
		// (in a separate block, so that tDictReader gets destroyed, and file closed)
		{
			CSphDictReader tDictReader;
			if (!tDictReader.Setup(sFilename.SetSprintf("%s.spi", sPath),
				tDictHeader.m_iDictCheckpointsOffset, tIndexSettings.m_eHitless, sError, true, &g_tThrottle, uVersion >= 31))
				sphDie("infix upgrade: %s", sError.cstr());
			while (tDictReader.Read())
			{
				const BYTE* sWord = tDictReader.GetWord();
				int iLen = strlen((const char*)sWord);
				pInfixer->AddWord(sWord, iLen, tDictReader.GetCheckpoint(), bHasMorphology);
			}
		}

		/////////////////////////////
		// write new dictionary file
		/////////////////////////////

		// ready to party
		// open all the cans!
		CSphAutofile tDict;
		tDict.Open(sFilename, SPH_O_READ, sError);

		CSphReader rdDict;
		rdDict.SetFile(tDict);
		rdDict.SeekTo(0, READ_NO_SIZE_HINT);

		CSphWriter wrDict;
		sFilename.SetSprintf("%s.spi.upgrade", sPath);
		if (!wrDict.OpenFile(sFilename, sError))
			sphDie("infix upgrade: failed to open %s", sFilename.cstr());

		// copy the keyword entries until checkpoints
		CopyBytes(wrDict, rdDict, (int)tDictHeader.m_iDictCheckpointsOffset);

		// write newly generated infix hash entries
		pInfixer->SaveEntries(wrDict);

		// copy checkpoints
		int iCheckpointsSize = (int)(tDict.GetSize() - tDictHeader.m_iDictCheckpointsOffset);
		tDictHeader.m_iDictCheckpointsOffset = wrDict.GetPos();
		CopyBytes(wrDict, rdDict, iCheckpointsSize);

		// write newly generated infix hash blocks
		tDictHeader.m_iInfixBlocksOffset = pInfixer->SaveEntryBlocks(wrDict);
		tDictHeader.m_iInfixBlocksWordsSize = pInfixer->GetBlocksWordsSize();
		if (tDictHeader.m_iInfixBlocksOffset > UINT_MAX) // FIXME!!! change to int64
			sphDie("INTERNAL ERROR: dictionary size " INT64_FMT " overflow at build infixes save", tDictHeader.m_iInfixBlocksOffset);


		// flush header
		// mostly for debugging convenience
		// primary storage is in the index wide header
		wrDict.PutBytes("dict-header", 11);
		wrDict.ZipInt(tDictHeader.m_iDictCheckpoints);
		wrDict.ZipOffset(tDictHeader.m_iDictCheckpointsOffset);
		wrDict.ZipInt(tDictHeader.m_iInfixCodepointBytes);
		wrDict.ZipInt((DWORD)tDictHeader.m_iInfixBlocksOffset);

		wrDict.CloseFile();
		if (wrDict.IsError())
			sphDie("infix upgrade: dictionary write error (out of space?)");

		if (rdDict.GetErrorFlag())
			sphDie("infix upgrade: dictionary read error");
		tDict.Close();

		////////////////////
		// write new header
		////////////////////

		assert(tDictSettings.m_bWordDict);
		CSphDict* pDict = sphCreateDictionaryKeywords(tDictSettings, &tEmbeddedFiles, pTokenizer, "$indexname", sError);
		if (!pDict)
			sphDie("infix upgrade: %s", sError.cstr());

		CSphWriter wrHeader;
		sFilename.SetSprintf("%s.sph.upgrade", sPath);
		if (!wrHeader.OpenFile(sFilename, sError))
			sphDie("infix upgrade: %s", sError.cstr());

		wrHeader.PutDword(INDEX_MAGIC_HEADER);
		wrHeader.PutDword(INDEX_FORMAT_VERSION);
		wrHeader.PutDword(bUse64);
		wrHeader.PutDword(eDocinfo);
		WriteSchema(wrHeader, tSchema);
		wrHeader.PutOffset(iMinDocid);
		wrHeader.PutOffset(tDictHeader.m_iDictCheckpointsOffset);
		wrHeader.PutDword(tDictHeader.m_iDictCheckpoints);
		wrHeader.PutByte(tDictHeader.m_iInfixCodepointBytes);
		wrHeader.PutDword((DWORD)tDictHeader.m_iInfixBlocksOffset);
		wrHeader.PutDword(tDictHeader.m_iInfixBlocksWordsSize);
		wrHeader.PutDword((DWORD)tStats.m_iTotalDocuments); // FIXME? we don't expect over 4G docs per just 1 local index
		wrHeader.PutOffset(tStats.m_iTotalBytes);
		SaveIndexSettings(wrHeader, tIndexSettings);
		SaveTokenizerSettings(wrHeader, pTokenizer, tIndexSettings.m_iEmbeddedLimit);
		SaveDictionarySettings(wrHeader, pDict, false, tIndexSettings.m_iEmbeddedLimit);
		wrHeader.PutDword(iKillListSize);
		wrHeader.PutDword(uMinMaxIndex);
		wrHeader.PutDword(0); // no field filter

		wrHeader.CloseFile();
		if (wrHeader.IsError())
			sphDie("infix upgrade: header write error (out of space?)");

		// all done!
		const char* sRenames[] = {
			".sph", ".sph.bak",
			".spi", ".spi.bak",
			".sph.upgrade", ".sph",
			".spi.upgrade", ".spi",
			NULL };
		FinalizeUpgrade(sRenames, "infix upgrade", sPath, tmStart);
	}

	//////////////////////////////////////////////////////////////////////////
	// V.12 TO V.31 CONVERSION TOOL, SKIPLIST BUILDER
	//////////////////////////////////////////////////////////////////////////

	struct EntrySkips_t
	{
		DWORD			m_uEntry;		//sequential index in dict
		SphOffset_t		m_iDoclist;		//doclist offset from dict
		int				m_iSkiplist;	//generated skiplist offset
	};

	void sphDictBuildSkiplists(const char* sPath)
	{
		CSphString sFilename, sError;
		int64_t tmStart = sphMicroTimer();

		if_const(INDEX_FORMAT_VERSION < 31 || INDEX_FORMAT_VERSION>35)
			sphDie("skiplists upgrade: ony works in v.31 to v.35 builds for now; get an older indextool or contact support");

		// load (interesting parts from) the index header
		CSphAutoreader rdHeader;
		sFilename.SetSprintf("%s.sph", sPath);
		if (!rdHeader.Open(sFilename.cstr(), sError))
			sphDie("skiplists upgrade: %s", sError.cstr());

		// version
		DWORD uHeader = rdHeader.GetDword();
		DWORD uVersion = rdHeader.GetDword();
		bool bUse64 = (rdHeader.GetDword() != 0);
		bool bConvertCheckpoints = (uVersion <= 21);
		ESphDocinfo eDocinfo = (ESphDocinfo)rdHeader.GetDword();
		const DWORD uLowestVersion = 12;

		if (bUse64 != USE_64BIT)
			sphDie("skiplists upgrade: USE_64BIT differs, index %s, binary %s",
				bUse64 ? "enabled" : "disabled", USE_64BIT ? "enabled" : "disabled");
		if (uHeader != INDEX_MAGIC_HEADER)
			sphDie("skiplists upgrade: invalid header file");
		if (uVersion < uLowestVersion)
			sphDie("skiplists upgrade: got v.%d header, v.%d to v.30 required", uVersion, uLowestVersion);
		if (eDocinfo == SPH_DOCINFO_INLINE)
			sphDie("skiplists upgrade: docinfo=inline is not supported yet");

		CSphSchema tSchema;
		DictHeader_t tDictHeader;
		CSphSourceStats tStats;
		CSphIndexSettings tIndexSettings;
		CSphTokenizerSettings tTokenizerSettings;
		CSphDictSettings tDictSettings;
		CSphEmbeddedFiles tEmbeddedFiles;

		ReadSchema(rdHeader, tSchema, uVersion, eDocinfo == SPH_DOCINFO_INLINE);
		SphOffset_t iMinDocid = rdHeader.GetOffset();
		tDictHeader.m_iDictCheckpointsOffset = rdHeader.GetOffset();
		tDictHeader.m_iDictCheckpoints = rdHeader.GetDword();
		tDictHeader.m_iInfixCodepointBytes = 0;
		tDictHeader.m_iInfixBlocksOffset = 0;
		if (uVersion >= 27)
		{
			tDictHeader.m_iInfixCodepointBytes = rdHeader.GetByte();
			tDictHeader.m_iInfixBlocksOffset = rdHeader.GetDword();
		}
		if (uVersion >= 34)
			tDictHeader.m_iInfixBlocksWordsSize = rdHeader.GetDword();

		tStats.m_iTotalDocuments = rdHeader.GetDword();
		tStats.m_iTotalBytes = rdHeader.GetOffset();
		LoadIndexSettings(tIndexSettings, rdHeader, uVersion);
		if (!LoadTokenizerSettings(rdHeader, tTokenizerSettings, tEmbeddedFiles, uVersion, sError))
			sphDie("skiplists upgrade: failed to load tokenizer settings: '%s'", sError.cstr());
		LoadDictionarySettings(rdHeader, tDictSettings, tEmbeddedFiles, uVersion, sError);
		int iKillListSize = rdHeader.GetDword();

		SphOffset_t uMinMaxIndex = 0;
		if (uVersion >= 33)
			uMinMaxIndex = rdHeader.GetOffset();
		else if (uVersion >= 20)
			uMinMaxIndex = rdHeader.GetDword();

		ISphFieldFilter* pFieldFilter = NULL;
		if (uVersion >= 28)
		{
			CSphFieldFilterSettings tFieldFilterSettings;
			LoadFieldFilterSettings(rdHeader, tFieldFilterSettings);
			if (tFieldFilterSettings.m_dRegexps.GetLength())
				pFieldFilter = sphCreateRegexpFilter(tFieldFilterSettings, sError);

			if (!sphSpawnRLPFilter(pFieldFilter, tIndexSettings, tTokenizerSettings, sPath, sError))
			{
				SafeDelete(pFieldFilter);
				sphDie("%s", sError.cstr());
			}
		}

		CSphFixedVector<uint64_t> dFieldLens(tSchema.m_dFields.GetLength());
		if (uVersion >= 35 && tIndexSettings.m_bIndexFieldLens)
			ARRAY_FOREACH(i, tSchema.m_dFields)
			dFieldLens[i] = rdHeader.GetOffset(); // FIXME? ideally 64bit even when off is 32bit..

		if (rdHeader.GetErrorFlag())
			sphDie("skiplists upgrade: failed to parse header");
		rdHeader.Close();

		//////////////////////
		// generate skiplists
		//////////////////////

		// keywords on disk might be in a different order than dictionary
		// and random accesses on a plain disk would be extremely slow
		// so we load the dictionary, sort by doclist offset
		// then we walk doclists, generate skiplists, sort back by entry number
		// then walk the disk dictionary again, lookup skiplist offset, and patch

		// load the dictionary
		CSphVector<EntrySkips_t> dSkips;
		const bool bWordDict = tDictSettings.m_bWordDict;

		CSphAutoreader rdDict;
		if (!rdDict.Open(sFilename.SetSprintf("%s.spi", sPath), sError))
			sphDie("skiplists upgrade: %s", sError.cstr());

		// compute actual keyword data length
		SphOffset_t iWordsEnd = tDictHeader.m_iDictCheckpointsOffset;
		if (bWordDict && tDictHeader.m_iInfixCodepointBytes)
		{
			rdDict.SeekTo(tDictHeader.m_iInfixBlocksOffset, 32); // need just 1 entry, 32 bytes should be ok
			rdDict.UnzipInt(); // skip block count
			int iInfixLen = rdDict.GetByte();
			rdDict.SkipBytes(iInfixLen);
			iWordsEnd = rdDict.UnzipInt() - strlen(g_sTagInfixEntries);
			rdDict.SeekTo(0, READ_NO_SIZE_HINT);
		}

		CSphDictReader* pReader = new CSphDictReader();
		pReader->Setup(&rdDict, iWordsEnd, tIndexSettings.m_eHitless, bWordDict, &g_tThrottle, uVersion >= 31);

		DWORD uEntry = 0;
		while (pReader->Read())
		{
			if (pReader->m_iDocs > SPH_SKIPLIST_BLOCK)
			{
				EntrySkips_t& t = dSkips.Add();
				t.m_uEntry = uEntry;
				t.m_iDoclist = pReader->m_iDoclistOffset;
				t.m_iSkiplist = -1;
			}
			if (++uEntry == 0)
				sphDie("skiplists upgrade: dictionaries over 4B entries are not supported yet!");
		}

		// sort by doclist offset
		dSkips.Sort(sphMemberLess(&EntrySkips_t::m_iDoclist));

		// walk doclists, create skiplists
		CSphAutoreader rdDocs;
		if (!rdDocs.Open(sFilename.SetSprintf("%s.spd", sPath), sError))
			sphDie("skiplists upgrade: %s", sError.cstr());

		CSphWriter wrSkips;
		if (!wrSkips.OpenFile(sFilename.SetSprintf("%s.spe.tmp", sPath), sError))
			sphDie("skiplists upgrade: failed to create %s", sFilename.cstr());
		wrSkips.PutByte(1);

		int iDone = -1;
		CSphVector<SkiplistEntry_t> dSkiplist;
		ARRAY_FOREACH(i, dSkips)
		{
			// seek to that keyword
			// OPTIMIZE? use length hint from dict too?
			rdDocs.SeekTo(dSkips[i].m_iDoclist, READ_NO_SIZE_HINT);

			// decode interesting bits of doclist
			SphDocID_t uDocid = SphDocID_t(iMinDocid);
			SphOffset_t uHitPosition = 0;
			DWORD uDocs = 0;

			for (;; )
			{
				// save current entry position
				SphOffset_t uPos = rdDocs.GetPos();

				// decode next entry
				SphDocID_t uDelta = rdDocs.UnzipDocid();
				if (!uDelta)
					break;

				// build skiplist, aka save decoder state as needed
				if ((uDocs & (SPH_SKIPLIST_BLOCK - 1)) == 0)
				{
					SkiplistEntry_t& t = dSkiplist.Add();
					t.m_iBaseDocid = uDocid;
					t.m_iOffset = uPos;
					t.m_iBaseHitlistPos = uHitPosition;
				}
				uDocs++;

				// do decode
				uDocid += uDelta; // track delta-encoded docid
				if (tIndexSettings.m_eHitFormat == SPH_HIT_FORMAT_INLINE)
				{
					DWORD uHits = rdDocs.UnzipInt();
					rdDocs.UnzipInt(); // skip hit field mask/data
					if (uHits == 1)
					{
						rdDocs.UnzipInt(); // skip inlined field id
					}
					else
					{
						uHitPosition += rdDocs.UnzipOffset(); // track delta-encoded hitlist offset
					}
				}
				else
				{
					uHitPosition += rdDocs.UnzipOffset(); // track delta-encoded hitlist offset
					rdDocs.UnzipInt(); // skip hit field mask/data
					rdDocs.UnzipInt(); // skip hit count
				}
			}

			// alright, we built it, so save it
			assert(uDocs > SPH_SKIPLIST_BLOCK);
			assert(dSkiplist.GetLength());

			dSkips[i].m_iSkiplist = (int)wrSkips.GetPos();
			SkiplistEntry_t tLast = dSkiplist[0];
			for (int j = 1; j < dSkiplist.GetLength(); j++)
			{
				const SkiplistEntry_t& t = dSkiplist[j];
				assert(t.m_iBaseDocid - tLast.m_iBaseDocid >= SPH_SKIPLIST_BLOCK);
				assert(t.m_iOffset - tLast.m_iOffset >= 4 * SPH_SKIPLIST_BLOCK);
				wrSkips.ZipOffset(t.m_iBaseDocid - tLast.m_iBaseDocid - SPH_SKIPLIST_BLOCK);
				wrSkips.ZipOffset(t.m_iOffset - tLast.m_iOffset - 4 * SPH_SKIPLIST_BLOCK);
				wrSkips.ZipOffset(t.m_iBaseHitlistPos - tLast.m_iBaseHitlistPos);
				tLast = t;
			}
			dSkiplist.Resize(0);

			// progress bar
			int iDone2 = (1 + i) * 100 / dSkips.GetLength();
			if (iDone2 != iDone)
			{
				iDone = iDone2;
				fprintf(stdout, "skiplists upgrade: building skiplists, %d%% done\r", iDone);
			}
		}
		fprintf(stdout, "skiplists upgrade: building skiplists, 100%% done\n");

		// finalize
		wrSkips.CloseFile();
		if (wrSkips.IsError())
			sphDie("skiplists upgrade: write error (out of space?)");
		if (rdDocs.GetErrorFlag())
			sphDie("skiplists upgrade: doclist read error: %s", rdDocs.GetErrorMessage().cstr());

		// sort by entry id again
		dSkips.Sort(sphMemberLess(&EntrySkips_t::m_uEntry));

		/////////////////////////////
		// write new dictionary file
		/////////////////////////////

		// converted dict writer
		CSphWriter wrDict;
		sFilename.SetSprintf("%s.spi.upgrade", sPath);
		if (!wrDict.OpenFile(sFilename, sError))
			sphDie("skiplists upgrade: failed to create %s", sFilename.cstr());
		wrDict.PutByte(1);

		// handy entry iterator
		// we will use this one to decode entries, and rdDict for other raw access
		pReader->Setup(&rdDict, iWordsEnd, tIndexSettings.m_eHitless, bWordDict, &g_tThrottle, uVersion >= 31);

		// we have to adjust some of the entries
		// thus we also have to recompute the offset in the checkpoints too
		//
		// infix hashes (if any) in dict=keywords refer to checkpoints by numbers
		// so infix data can simply be copied around

		// new checkpoints
		CSphVector<CSphWordlistCheckpoint> dNewCP;
		int iLastCheckpoint = 0;

		// skiplist lookup
		EntrySkips_t* pSkips = dSkips.Begin();

		// dict encoder state
		SphWordID_t uLastWordid = 0; // crc case
		SphOffset_t iLastDoclist = 0; // crc case
		CSphKeywordDeltaWriter tLastKeyword; // keywords case
		DWORD uWordCount = 0;

		// read old entries, write new entries
		while (pReader->Read())
		{
			// update or regenerate checkpoint
			if ((!bConvertCheckpoints && iLastCheckpoint != pReader->GetCheckpoint())
				|| (bConvertCheckpoints && (uWordCount % SPH_WORDLIST_CHECKPOINT) == 0))
			{
				// FIXME? GetCheckpoint() is for some reason 1-based
				if (uWordCount)
				{
					wrDict.ZipInt(0);
					if (bWordDict)
						wrDict.ZipInt(0);
					else
						wrDict.ZipOffset(pReader->m_iDoclistOffset - iLastDoclist);
				}
				uLastWordid = 0;
				iLastDoclist = 0;

				CSphWordlistCheckpoint& tCP = dNewCP.Add();
				if (bWordDict)
				{
					tCP.m_sWord = strdup((const char*)pReader->GetWord());
					tLastKeyword.Reset();
				}
				else
				{
					tCP.m_uWordID = pReader->m_uWordID;
				}
				tCP.m_iWordlistOffset = wrDict.GetPos();
				iLastCheckpoint = pReader->GetCheckpoint();
			}

			// resave entry
			if (bWordDict)
			{
				// keywords dict path
				const int iLen = strlen((const char*)pReader->GetWord());
				tLastKeyword.PutDelta(wrDict, pReader->GetWord(), iLen);
				wrDict.ZipOffset(pReader->m_iDoclistOffset);
				wrDict.ZipInt(pReader->m_iDocs);
				wrDict.ZipInt(pReader->m_iHits);
				if (pReader->m_iDocs >= DOCLIST_HINT_THRESH)
					wrDict.PutByte(pReader->m_iHint);
			}
			else
			{
				// crc dict path
				assert(pReader->m_uWordID > uLastWordid);
				assert(pReader->m_iDoclistOffset > iLastDoclist);
				wrDict.ZipOffset(pReader->m_uWordID - uLastWordid);
				wrDict.ZipOffset(pReader->m_iDoclistOffset - iLastDoclist);
				wrDict.ZipInt(pReader->m_iDocs);
				wrDict.ZipInt(pReader->m_iHits);
				uLastWordid = pReader->m_uWordID;
				iLastDoclist = pReader->m_iDoclistOffset;
			}

			// emit skiplist pointer
			if (pReader->m_iDocs > SPH_SKIPLIST_BLOCK)
			{
				// lots of checks
				if (uWordCount != pSkips->m_uEntry)
					sphDie("skiplist upgrade: internal error, entry mismatch (expected %d, got %d)",
						uWordCount, pSkips->m_uEntry);
				if (pReader->m_iDoclistOffset != pSkips->m_iDoclist)
					sphDie("skiplist upgrade: internal error, offset mismatch (expected %lld, got %lld)",
						INT64(pReader->m_iDoclistOffset), INT64(pSkips->m_iDoclist));
				if (pSkips->m_iSkiplist < 0)
					sphDie("skiplist upgrade: internal error, bad skiplist offset %d",
						pSkips->m_iSkiplist);

				// and a bit of work
				wrDict.ZipInt(pSkips->m_iSkiplist);
				pSkips++;
			}

			// next entry
			uWordCount++;
		}

		// finalize last keywords block
		wrDict.ZipInt(0);
		if (bWordDict)
			wrDict.ZipInt(0);
		else
			wrDict.ZipOffset(rdDocs.GetFilesize() - iLastDoclist);

		rdDocs.Close();
		SafeDelete(pReader);

		// copy infix hash entries, if any
		int iDeltaInfix = 0;
		if (bWordDict && tDictHeader.m_iInfixCodepointBytes)
		{
			if (iWordsEnd != rdDict.GetPos())
				sphDie("skiplist upgrade: internal error, infix hash position mismatch (expected=%lld, got=%lld)",
					INT64(iWordsEnd), INT64(rdDict.GetPos()));
			iDeltaInfix = (int)(wrDict.GetPos() - rdDict.GetPos());
			CopyBytes(wrDict, rdDict, (int)(tDictHeader.m_iDictCheckpointsOffset - iWordsEnd));
		}

		// write new checkpoints
		if (tDictHeader.m_iDictCheckpointsOffset != rdDict.GetPos())
			sphDie("skiplist upgrade: internal error, checkpoints position mismatch (expected=%lld, got=%lld)",
				INT64(tDictHeader.m_iDictCheckpointsOffset), INT64(rdDict.GetPos()));
		if (!bConvertCheckpoints && tDictHeader.m_iDictCheckpoints != dNewCP.GetLength())
			sphDie("skiplist upgrade: internal error, checkpoint count mismatch (old=%d, new=%d)",
				tDictHeader.m_iDictCheckpoints, dNewCP.GetLength());

		tDictHeader.m_iDictCheckpoints = dNewCP.GetLength();
		tDictHeader.m_iDictCheckpointsOffset = wrDict.GetPos();
		ARRAY_FOREACH(i, dNewCP)
		{
			if (bWordDict)
			{
				wrDict.PutString(dNewCP[i].m_sWord);
				SafeDeleteArray(dNewCP[i].m_sWord);
			}
			else
			{
				wrDict.PutOffset(dNewCP[i].m_uWordID);
			}
			wrDict.PutOffset(dNewCP[i].m_iWordlistOffset);
		}

		// update infix hash blocks, if any
		// (they store direct offsets to infix hash, which just got moved)
		if (bWordDict && tDictHeader.m_iInfixCodepointBytes)
		{
			rdDict.SeekTo(tDictHeader.m_iInfixBlocksOffset, READ_NO_SIZE_HINT);
			int iBlocks = rdDict.UnzipInt();

			wrDict.PutBytes(g_sTagInfixBlocks, strlen(g_sTagInfixBlocks));
			tDictHeader.m_iInfixBlocksOffset = wrDict.GetPos();
			if (tDictHeader.m_iInfixBlocksOffset > UINT_MAX) // FIXME!!! change to int64
				sphDie("INTERNAL ERROR: dictionary size " INT64_FMT " overflow at infix blocks save", wrDict.GetPos());

			wrDict.ZipInt(iBlocks);
			for (int i = 0; i < iBlocks; i++)
			{
				char sInfix[256];
				int iBytes = rdDict.GetByte();
				rdDict.GetBytes(sInfix, iBytes);
				wrDict.PutByte(iBytes);
				wrDict.PutBytes(sInfix, iBytes);
				wrDict.ZipInt(rdDict.UnzipInt() + iDeltaInfix);
			}
		}

		// emit new aux tail header
		if (bWordDict)
		{
			wrDict.PutBytes("dict-header", 11);
			wrDict.ZipInt(tDictHeader.m_iDictCheckpoints);
			wrDict.ZipOffset(tDictHeader.m_iDictCheckpointsOffset);
			wrDict.ZipInt(tDictHeader.m_iInfixCodepointBytes);
			wrDict.ZipInt((DWORD)tDictHeader.m_iInfixBlocksOffset);
		}

		wrDict.CloseFile();
		if (wrDict.IsError())
			sphDie("skiplists upgrade: dict write error (out of space?)");

		rdDict.Close();

		////////////////////
		// build min-max attribute index
		////////////////////

		bool bShuffleAttributes = false;
		if (uVersion < 20)
		{
			int iStride = DOCINFO_IDSIZE + tSchema.GetRowSize();
			int iEntrySize = sizeof(DWORD) * iStride;

			sFilename.SetSprintf("%s.spa", sPath);
			CSphAutofile rdDocinfo(sFilename.cstr(), SPH_O_READ, sError);
			if (rdDocinfo.GetFD() < 0)
				sphDie("skiplists upgrade: %s", sError.cstr());

			sFilename.SetSprintf("%s.spa.upgrade", sPath);
			CSphWriter wrDocinfo;
			if (!wrDocinfo.OpenFile(sFilename.cstr(), sError))
				sphDie("skiplists upgrade: %s", sError.cstr());

			CSphFixedVector<DWORD> dMva(0);
			CSphAutofile tMvaFile(sFilename.cstr(), SPH_O_READ, sError);
			if (tMvaFile.GetFD() >= 0 && tMvaFile.GetSize() > 0)
			{
				uint64_t uMvaSize = tMvaFile.GetSize();
				assert(uMvaSize / sizeof(DWORD) <= UINT_MAX);
				dMva.Reset((int)(uMvaSize / sizeof(DWORD)));
				tMvaFile.Read(dMva.Begin(), uMvaSize, sError);
			}
			tMvaFile.Close();

			int64_t iDocinfoSize = rdDocinfo.GetSize(iEntrySize, true, sError) / sizeof(CSphRowitem);
			assert(iDocinfoSize / iStride < UINT_MAX);
			int iRows = (int)(iDocinfoSize / iStride);

			AttrIndexBuilder_c tBuilder(tSchema);
			int64_t iMinMaxSize = tBuilder.GetExpectedSize(tStats.m_iTotalDocuments);
			if (iMinMaxSize > INT_MAX)
				sphDie("attribute files (.spa) over 128 GB are not supported");
			CSphFixedVector<CSphRowitem> dMinMax((int)iMinMaxSize);
			tBuilder.Prepare(dMinMax.Begin(), dMinMax.Begin() + dMinMax.GetLength()); // FIXME!!! for over INT_MAX blocks

			CSphFixedVector<CSphRowitem> dRow(iStride);

			uMinMaxIndex = 0;
			for (int i = 0; i < iRows; i++)
			{
				rdDocinfo.Read(dRow.Begin(), iStride * sizeof(CSphRowitem), sError);
				wrDocinfo.PutBytes(dRow.Begin(), iStride * sizeof(CSphRowitem));

				if (!tBuilder.Collect(dRow.Begin(), dMva.Begin(), dMva.GetLength(), sError, true))
					sphDie("skiplists upgrade: %s", sError.cstr());

				uMinMaxIndex += iStride;

				int iDone1 = (1 + i) * 100 / iRows;
				int iDone2 = (2 + i) * 100 / iRows;
				if (iDone1 != iDone2)
					fprintf(stdout, "skiplists upgrade: building attribute min-max, %d%% done\r", iDone1);
			}
			fprintf(stdout, "skiplists upgrade: building attribute min-max, 100%% done\n");

			tBuilder.FinishCollect();
			rdDocinfo.Close();

			wrDocinfo.PutBytes(dMinMax.Begin(), dMinMax.GetLength() * sizeof(CSphRowitem));
			wrDocinfo.CloseFile();
			if (wrDocinfo.IsError())
				sphDie("skiplists upgrade: attribute write error (out of space?)");

			bShuffleAttributes = true;
		}


		////////////////////
		// write new header
		////////////////////

		ISphTokenizer* pTokenizer = ISphTokenizer::Create(tTokenizerSettings, &tEmbeddedFiles, sError);
		if (!pTokenizer)
			sphDie("skiplists upgrade: %s", sError.cstr());

		CSphDict* pDict = bWordDict
			? sphCreateDictionaryKeywords(tDictSettings, &tEmbeddedFiles, pTokenizer, "$indexname", sError)
			: sphCreateDictionaryCRC(tDictSettings, &tEmbeddedFiles, pTokenizer, "$indexname", sError);
		if (!pDict)
			sphDie("skiplists upgrade: %s", sError.cstr());

		CSphWriter wrHeader;
		sFilename.SetSprintf("%s.sph.upgrade", sPath);
		if (!wrHeader.OpenFile(sFilename, sError))
			sphDie("skiplists upgrade: %s", sError.cstr());

		wrHeader.PutDword(INDEX_MAGIC_HEADER);
		wrHeader.PutDword(INDEX_FORMAT_VERSION);
		wrHeader.PutDword(bUse64);
		wrHeader.PutDword(eDocinfo);
		WriteSchema(wrHeader, tSchema);
		wrHeader.PutOffset(iMinDocid);
		wrHeader.PutOffset(tDictHeader.m_iDictCheckpointsOffset);
		wrHeader.PutDword(tDictHeader.m_iDictCheckpoints);
		wrHeader.PutByte(tDictHeader.m_iInfixCodepointBytes);
		wrHeader.PutDword((DWORD)tDictHeader.m_iInfixBlocksOffset);
		wrHeader.PutDword(tDictHeader.m_iInfixBlocksWordsSize);
		wrHeader.PutDword((DWORD)tStats.m_iTotalDocuments); // FIXME? we don't expect over 4G docs per just 1 local index
		wrHeader.PutOffset(tStats.m_iTotalBytes);
		SaveIndexSettings(wrHeader, tIndexSettings);
		SaveTokenizerSettings(wrHeader, pTokenizer, tIndexSettings.m_iEmbeddedLimit);
		SaveDictionarySettings(wrHeader, pDict, false, tIndexSettings.m_iEmbeddedLimit);
		wrHeader.PutDword(iKillListSize);
		wrHeader.PutOffset(uMinMaxIndex);
		SaveFieldFilterSettings(wrHeader, pFieldFilter);

		SafeDelete(pFieldFilter);

		// average field lengths
		if (tIndexSettings.m_bIndexFieldLens)
			ARRAY_FOREACH(i, tSchema.m_dFields)
			wrHeader.PutOffset(dFieldLens[i]);

		wrHeader.CloseFile();
		if (wrHeader.IsError())
			sphDie("skiplists upgrade: header write error (out of space?)");

		sFilename.SetSprintf("%s.sps", sPath);
		if (!sphIsReadable(sFilename.cstr(), NULL))
		{
			CSphWriter wrStrings;
			if (!wrStrings.OpenFile(sFilename, sError))
				sphDie("skiplists upgrade: %s", sError.cstr());

			wrStrings.PutByte(0);
			wrStrings.CloseFile();
			if (wrStrings.IsError())
				sphDie("skiplists upgrade: string write error (out of space?)");
		}

		// all done!
		const char* sRenames[] = {
			".spe.tmp", ".spe",
			".sph", ".sph.bak",
			".spi", ".spi.bak",
			".sph.upgrade", ".sph",
			".spi.upgrade", ".spi",
			bShuffleAttributes ? ".spa" : NULL, ".spa.bak",
			".spa.upgrade", ".spa",
		NULL };
		FinalizeUpgrade(sRenames, "skiplists upgrade", sPath, tmStart);
	}

}