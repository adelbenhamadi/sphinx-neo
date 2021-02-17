#include "neo/core/global_idf.h"
#include "neo/core/globals.h"
#include "neo/platform/compat.h"
#include "neo/int/types.h"
#include "neo/io/fnv64.h"
#include "neo/io/reader.h"
#include "neo/sphinx/xsearch.h"

namespace NEO {

	bool CSphGlobalIDF::Touch(const CSphString& sFilename)
	{
		// update m_uMTime, return true if modified
		struct_stat tStat;
		memset(&tStat, 0, sizeof(tStat));
		if (stat(sFilename.cstr(), &tStat) < 0)
			memset(&tStat, 0, sizeof(tStat));
		bool bModified = (m_uMTime != tStat.st_mtime);
		m_uMTime = tStat.st_mtime;
		return bModified;
	}


	bool CSphGlobalIDF::Preread(const CSphString& sFilename, CSphString& sError)
	{
		Touch(sFilename);

		CSphAutoreader tReader;
		if (!tReader.Open(sFilename, sError))
			return false;

		m_iTotalDocuments = tReader.GetOffset();
		const SphOffset_t iSize = tReader.GetFilesize() - sizeof(SphOffset_t);
		m_iTotalWords = iSize / sizeof(IDFWord_t);

		// allocate words cache
		CSphString sWarning;
		if (!m_pWords.Alloc(m_iTotalWords, sError))
			return false;

		// allocate lookup table if needed
		int iHashSize = (int)(U64C(1) << HASH_BITS);
		if (m_iTotalWords > iHashSize * 8)
		{
			if (!m_pHash.Alloc(iHashSize + 2, sError))
				return false;
		}

		// read file into memory (may exceed 2GB)
		const int iBlockSize = 10485760; // 10M block
		for (SphOffset_t iRead = 0; iRead < iSize && !sphInterrupted(); iRead += iBlockSize)
			tReader.GetBytes((BYTE*)m_pWords.GetWritePtr() + iRead, iRead + iBlockSize > iSize ? (int)(iSize - iRead) : iBlockSize);

		if (sphInterrupted())
			return false;

		// build lookup table
		if (m_pHash.GetLengthBytes())
		{
			int64_t* pHash = m_pHash.GetWritePtr();

			uint64_t uFirst = m_pWords[0].m_uWordID;
			uint64_t uRange = m_pWords[m_iTotalWords - 1].m_uWordID - uFirst;

			DWORD iShift = 0;
			while (uRange >= (U64C(1) << HASH_BITS))
			{
				iShift++;
				uRange >>= 1;
			}

			pHash[0] = iShift;
			pHash[1] = 0;
			DWORD uLastHash = 0;

			for (int64_t i = 1; i < m_iTotalWords; i++)
			{
				// check for interrupt (throttled for speed)
				if ((i & 0xffff) == 0 && sphInterrupted())
					return false;

				DWORD uHash = (DWORD)((m_pWords[i].m_uWordID - uFirst) >> iShift);

				if (uHash == uLastHash)
					continue;

				while (uLastHash < uHash)
					pHash[++uLastHash + 1] = i;

				uLastHash = uHash;
			}
			pHash[++uLastHash + 1] = m_iTotalWords;
		}
		return true;
	}


	DWORD CSphGlobalIDF::GetDocs(const CSphString& sWord) const
	{
		const char* s = sWord.cstr();

		// replace = to MAGIC_WORD_HEAD_NONSTEMMED for exact terms
		char sBuf[3 * SPH_MAX_WORD_LEN + 4];
		if (*s && *s == '=')
		{
			strncpy(sBuf, sWord.cstr(), sizeof(sBuf));
			sBuf[0] = MAGIC_WORD_HEAD_NONSTEMMED;
			s = sBuf;
		}

		uint64_t uWordID = sphFNV64(s);

		int64_t iStart = 0;
		int64_t iEnd = m_iTotalWords - 1;

		const IDFWord_t* pWords = (IDFWord_t*)m_pWords.GetWritePtr();

		if (m_pHash.GetLengthBytes())
		{
			uint64_t uFirst = pWords[0].m_uWordID;
			DWORD uHash = (DWORD)((uWordID - uFirst) >> m_pHash[0]);
			if (uHash > (U64C(1) << HASH_BITS))
				return 0;

			iStart = m_pHash[uHash + 1];
			iEnd = m_pHash[uHash + 2] - 1;
		}

		const IDFWord_t* pWord = sphBinarySearch(pWords + iStart, pWords + iEnd, bind(&IDFWord_t::m_uWordID), uWordID);
		return pWord ? pWord->m_iDocs : 0;
	}


	float CSphGlobalIDF::GetIDF(const CSphString& sWord, int64_t iDocsLocal, bool bPlainIDF)
	{
		const int64_t iDocs = Max(iDocsLocal, (int64_t)GetDocs(sWord));
		const int64_t iTotalClamped = Max(m_iTotalDocuments, iDocs);

		if (!iDocs)
			return 0.0f;

		if (bPlainIDF)
		{
			float fLogTotal = logf(float(1 + iTotalClamped));
			return logf(float(iTotalClamped - iDocs + 1) / float(iDocs))
				/ (2 * fLogTotal);
		}
		else
		{
			float fLogTotal = logf(float(1 + iTotalClamped));
			return logf(float(iTotalClamped) / float(iDocs))
				/ (2 * fLogTotal);
		}
	}


}