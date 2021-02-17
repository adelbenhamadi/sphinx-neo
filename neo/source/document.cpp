#include "neo/source/document.h"
#include "neo/core/globals.h"
#include "neo/utility/log.h"
#include "neo/tools/convert.h"
#include "neo/source/hitman.h"
#include "neo/dict/dict.h"
#include "neo/tokenizer/tokenizer.h"
#include "neo/utility/inline_misc.h"

namespace NEO {

	CSphSource_Document::CSphBuildHitsState_t::CSphBuildHitsState_t()
	{
		Reset();
	}

	CSphSource_Document::CSphBuildHitsState_t::~CSphBuildHitsState_t()
	{
		Reset();
	}

	void CSphSource_Document::CSphBuildHitsState_t::Reset()
	{
		m_bProcessingHits = false;
		m_bDocumentDone = false;
		m_dFields = NULL;
		m_dFieldLengths.Resize(0);
		m_iStartPos = 0;
		m_iHitPos = 0;
		m_iField = 0;
		m_iStartField = 0;
		m_iEndField = 0;
		m_iBuildLastStep = 1;

		ARRAY_FOREACH(i, m_dTmpFieldStorage)
			SafeDeleteArray(m_dTmpFieldStorage[i]);

		m_dTmpFieldStorage.Resize(0);
		m_dTmpFieldPtrs.Resize(0);
		m_dFiltered.Resize(0);
	}

	CSphSource_Document::CSphSource_Document(const char* sName)
		: CSphSource(sName)
		, m_pReadFileBuffer(NULL)
		, m_iReadFileBufferSize(256 * 1024)
		, m_iMaxFileBufferSize(2 * 1024 * 1024)
		, m_eOnFileFieldError(FFE_IGNORE_FIELD)
		, m_fpDumpRows(NULL)
		, m_iPlainFieldsLength(0)
		, m_pFieldLengthAttrs(NULL)
		, m_bIdsSorted(false)
		, m_iMaxHits(MAX_SOURCE_HITS)
	{
	}


	bool CSphSource_Document::IterateDocument(CSphString& sError)
	{
		assert(m_pTokenizer);
		assert(!m_tState.m_bProcessingHits);

		m_tHits.m_dData.Resize(0);

		m_tState.Reset();
		m_tState.m_iEndField = m_iPlainFieldsLength;
		m_tState.m_dFieldLengths.Resize(m_tState.m_iEndField);

		if (m_pFieldFilter)
		{
			m_tState.m_dTmpFieldPtrs.Resize(m_tState.m_iEndField);
			m_tState.m_dTmpFieldStorage.Resize(m_tState.m_iEndField);

			ARRAY_FOREACH(i, m_tState.m_dTmpFieldPtrs)
			{
				m_tState.m_dTmpFieldPtrs[i] = NULL;
				m_tState.m_dTmpFieldStorage[i] = NULL;
			}
		}

		m_dMva.Resize(1); // must not have zero offset

		// fetch next document
		for (;; )
		{
			m_tState.m_dFields = NextDocument(sError);
			if (m_tDocInfo.m_uDocID == 0)
				return true;

			const int* pFieldLengths = GetFieldLengths();
			for (int iField = 0; iField < m_tState.m_iEndField; iField++)
				m_tState.m_dFieldLengths[iField] = pFieldLengths[iField];

			// moved that here as docid==0 means eof for regular query
			// but joined might produce doc with docid==0 and breaks delta packing
			if (HasJoinedFields())
				m_dAllIds.Add(m_tDocInfo.m_uDocID);

			if (!m_tState.m_dFields)
				return false;

			// tricky bit
			// we can only skip document indexing from here, IterateHits() is too late
			// so in case the user chose to skip documents with file field problems
			// we need to check for those here
			if (m_eOnFileFieldError == FFE_SKIP_DOCUMENT || m_eOnFileFieldError == FFE_FAIL_INDEX)
			{
				bool bOk = true;
				for (int iField = 0; iField < m_tState.m_iEndField && bOk; iField++)
				{
					const BYTE* sFilename = m_tState.m_dFields[iField];
					if (m_tSchema.m_dFields[iField].m_bFilename)
						bOk &= CheckFileField(sFilename);

					if (!bOk && m_eOnFileFieldError == FFE_FAIL_INDEX)
					{
						sError.SetSprintf("error reading file field data (docid=" DOCID_FMT ", filename=%s)",
							m_tDocInfo.m_uDocID, sFilename);
						return false;
					}
				}
				if (!bOk && m_eOnFileFieldError == FFE_SKIP_DOCUMENT)
					continue;
			}

			if (m_pFieldFilter)
			{
				bool bHaveModifiedFields = false;
				for (int iField = 0; iField < m_tState.m_iEndField; iField++)
				{
					if (m_tSchema.m_dFields[iField].m_bFilename)
					{
						m_tState.m_dTmpFieldPtrs[iField] = m_tState.m_dFields[iField];
						continue;
					}

					CSphVector<BYTE> dFiltered;
					int iFilteredLen = m_pFieldFilter->Apply(m_tState.m_dFields[iField], m_tState.m_dFieldLengths[iField], dFiltered, false);
					if (iFilteredLen)
					{
						m_tState.m_dTmpFieldStorage[iField] = dFiltered.LeakData();
						m_tState.m_dTmpFieldPtrs[iField] = m_tState.m_dTmpFieldStorage[iField];
						m_tState.m_dFieldLengths[iField] = iFilteredLen;
						bHaveModifiedFields = true;
					}
					else
						m_tState.m_dTmpFieldPtrs[iField] = m_tState.m_dFields[iField];
				}

				if (bHaveModifiedFields)
					m_tState.m_dFields = (BYTE**)&(m_tState.m_dTmpFieldPtrs[0]);
			}

			// we're good
			break;
		}

		m_tStats.m_iTotalDocuments++;
		return true;
	}

	// HIT GENERATORS

	ISphHits* CSphSource_Document::IterateHits(CSphString& sError)
	{
		if (m_tState.m_bDocumentDone)
			return NULL;

		m_tHits.m_dData.Resize(0);

		BuildHits(sError, false);

		return &m_tHits;
	}


	bool CSphSource_Document::CheckFileField(const BYTE* sField)
	{
		CSphAutofile tFileSource;
		CSphString sError;

		if (tFileSource.Open((const char*)sField, SPH_O_READ, sError) == -1)
		{
			sphWarning("docid=" DOCID_FMT ": %s", m_tDocInfo.m_uDocID, sError.cstr());
			return false;
		}

		int64_t iFileSize = tFileSource.GetSize();
		if (iFileSize + 16 > m_iMaxFileBufferSize)
		{
			sphWarning("docid=" DOCID_FMT ": file '%s' too big for a field (size=" INT64_FMT ", max_file_field_buffer=%d)",
				m_tDocInfo.m_uDocID, (const char*)sField, iFileSize, m_iMaxFileBufferSize);
			return false;
		}

		return true;
	}

	/// returns file size on success, and replaces *ppField with a pointer to data
	/// returns -1 on failure (and emits a warning)
	int CSphSource_Document::LoadFileField(BYTE** ppField, CSphString& sError)
	{
		CSphAutofile tFileSource;

		BYTE* sField = *ppField;
		if (tFileSource.Open((const char*)sField, SPH_O_READ, sError) == -1)
		{
			sphWarning("docid=" DOCID_FMT ": %s", m_tDocInfo.m_uDocID, sError.cstr());
			return -1;
		}

		int64_t iFileSize = tFileSource.GetSize();
		if (iFileSize + 16 > m_iMaxFileBufferSize)
		{
			sphWarning("docid=" DOCID_FMT ": file '%s' too big for a field (size=" INT64_FMT ", max_file_field_buffer=%d)",
				m_tDocInfo.m_uDocID, (const char*)sField, iFileSize, m_iMaxFileBufferSize);
			return -1;
		}

		int iFieldBytes = (int)iFileSize;
		if (!iFieldBytes)
			return 0;

		int iBufSize = Max(m_iReadFileBufferSize, 1 << sphLog2(iFieldBytes + 15));
		if (m_iReadFileBufferSize < iBufSize)
			SafeDeleteArray(m_pReadFileBuffer);

		if (!m_pReadFileBuffer)
		{
			m_pReadFileBuffer = new char[iBufSize];
			m_iReadFileBufferSize = iBufSize;
		}

		if (!tFileSource.Read(m_pReadFileBuffer, iFieldBytes, sError))
		{
			sphWarning("docid=" DOCID_FMT ": read failed: %s", m_tDocInfo.m_uDocID, sError.cstr());
			return -1;
		}

		m_pReadFileBuffer[iFieldBytes] = '\0';

		*ppField = (BYTE*)m_pReadFileBuffer;
		return iFieldBytes;
	}


	bool AddFieldLens(CSphSchema& tSchema, bool bDynamic, CSphString& sError)
	{
		ARRAY_FOREACH(i, tSchema.m_dFields)
		{
			CSphColumnInfo tCol;
			tCol.m_sName.SetSprintf("%s_len", tSchema.m_dFields[i].m_sName.cstr());

			int iGot = tSchema.GetAttrIndex(tCol.m_sName.cstr());
			if (iGot >= 0)
			{
				if (tSchema.GetAttr(iGot).m_eAttrType == ESphAttr::SPH_ATTR_TOKENCOUNT)
				{
					// looks like we already added these
					assert(tSchema.GetAttr(iGot).m_sName == tCol.m_sName);
					return true;
				}

				sError.SetSprintf("attribute %s conflicts with index_field_lengths=1; remove it", tCol.m_sName.cstr());
				return false;
			}

			tCol.m_eAttrType = ESphAttr::SPH_ATTR_TOKENCOUNT;
			tSchema.AddAttr(tCol, bDynamic); // everything's dynamic at indexing time
		}
		return true;
	}


	bool CSphSource_Document::AddAutoAttrs(CSphString& sError)
	{
		// auto-computed length attributes
		if (m_bIndexFieldLens)
			return AddFieldLens(m_tSchema, true, sError);
		return true;
	}


	void CSphSource_Document::AllocDocinfo()
	{
		// tricky bit
		// with in-config schema, attr storage gets allocated in Setup() when source is initially created
		// so when this AddAutoAttrs() additionally changes the count, we have to change the number of attributes
		// but Reset() prohibits that, because that is usually a programming mistake, hence the Swap() dance
		CSphMatch tNew;
		tNew.Reset(m_tSchema.GetRowSize());
		Swap(m_tDocInfo, tNew);

		m_dStrAttrs.Resize(m_tSchema.GetAttrsCount());

		if (m_bIndexFieldLens && m_tSchema.GetAttrsCount() && m_tSchema.m_dFields.GetLength())
		{
			int iFirst = m_tSchema.GetAttrId_FirstFieldLen();
			assert(m_tSchema.GetAttr(iFirst).m_eAttrType == ESphAttr::SPH_ATTR_TOKENCOUNT);
			assert(m_tSchema.GetAttr(iFirst + m_tSchema.m_dFields.GetLength() - 1).m_eAttrType == ESphAttr::SPH_ATTR_TOKENCOUNT);

			m_pFieldLengthAttrs = m_tDocInfo.m_pDynamic + (m_tSchema.GetAttr(iFirst).m_tLocator.m_iBitOffset / 32);
		}
	}


	bool CSphSource_Document::BuildZoneHits(SphDocID_t uDocid, BYTE* sWord)
	{
		if (*sWord == MAGIC_CODE_SENTENCE || *sWord == MAGIC_CODE_PARAGRAPH || *sWord == MAGIC_CODE_ZONE)
		{
			m_tHits.AddHit(uDocid, m_pDict->GetWordID((BYTE*)MAGIC_WORD_SENTENCE), m_tState.m_iHitPos);

			if (*sWord == MAGIC_CODE_PARAGRAPH || *sWord == MAGIC_CODE_ZONE)
				m_tHits.AddHit(uDocid, m_pDict->GetWordID((BYTE*)MAGIC_WORD_PARAGRAPH), m_tState.m_iHitPos);

			if (*sWord == MAGIC_CODE_ZONE)
			{
				BYTE* pZone = (BYTE*)m_pTokenizer->GetBufferPtr();
				BYTE* pEnd = pZone;
				while (*pEnd && *pEnd != MAGIC_CODE_ZONE)
				{
					pEnd++;
				}

				if (*pEnd && *pEnd == MAGIC_CODE_ZONE)
				{
					*pEnd = '\0';
					m_tHits.AddHit(uDocid, m_pDict->GetWordID(pZone - 1), m_tState.m_iHitPos);
					m_pTokenizer->SetBufferPtr((const char*)pEnd + 1);
				}
			}

			m_tState.m_iBuildLastStep = 1;
			return true;
		}
		return false;
	}


	// track blended start and reset on not blended token
	static int TrackBlendedStart(const ISphTokenizer* pTokenizer, int iBlendedHitsStart, int iHitsCount)
	{
		iBlendedHitsStart = ((pTokenizer->TokenIsBlended() || pTokenizer->TokenIsBlendedPart()) ? iBlendedHitsStart : -1);
		if (pTokenizer->TokenIsBlended())
			iBlendedHitsStart = iHitsCount;

		return iBlendedHitsStart;
	}


#define BUILD_SUBSTRING_HITS_COUNT 4

	void CSphSource_Document::BuildSubstringHits(SphDocID_t uDocid, bool bPayload, ESphWordpart eWordpart, bool bSkipEndMarker)
	{
		bool bPrefixField = (eWordpart == SPH_WORDPART_PREFIX);
		bool bInfixMode = m_iMinInfixLen > 0;

		int iMinInfixLen = bPrefixField ? m_iMinPrefixLen : m_iMinInfixLen;
		if (!m_tState.m_bProcessingHits)
			m_tState.m_iBuildLastStep = 1;

		BYTE* sWord = NULL;
		BYTE sBuf[16 + 3 * SPH_MAX_WORD_LEN];

		int iIterHitCount = BUILD_SUBSTRING_HITS_COUNT;
		if (bPrefixField)
			iIterHitCount += SPH_MAX_WORD_LEN - m_iMinPrefixLen;
		else
			iIterHitCount += ((m_iMinInfixLen + SPH_MAX_WORD_LEN) * (SPH_MAX_WORD_LEN - m_iMinInfixLen) / 2);

		// FIELDEND_MASK at blended token stream should be set for HEAD token too
		int iBlendedHitsStart = -1;

		// index all infixes
		while ((m_iMaxHits == 0 || m_tHits.m_dData.GetLength() + iIterHitCount < m_iMaxHits)
			&& (sWord = m_pTokenizer->GetToken()) != NULL)
		{
			int iLastBlendedStart = TrackBlendedStart(m_pTokenizer, iBlendedHitsStart, m_tHits.Length());

			if (!bPayload)
			{
				HITMAN::AddPos(&m_tState.m_iHitPos, m_tState.m_iBuildLastStep + m_pTokenizer->GetOvershortCount() * m_iOvershortStep);
				if (m_pTokenizer->GetBoundary())
					HITMAN::AddPos(&m_tState.m_iHitPos, m_iBoundaryStep);
				m_tState.m_iBuildLastStep = 1;
			}

			if (BuildZoneHits(uDocid, sWord))
				continue;

			int iLen = m_pTokenizer->GetLastTokenLen();

			// always index full word (with magic head/tail marker(s))
			int iBytes = strlen((const char*)sWord);
			memcpy(sBuf + 1, sWord, iBytes);
			sBuf[iBytes + 1] = '\0';

			SphWordID_t uExactWordid = 0;
			if (m_bIndexExactWords)
			{
				sBuf[0] = MAGIC_WORD_HEAD_NONSTEMMED;
				uExactWordid = m_pDict->GetWordIDNonStemmed(sBuf);
			}

			sBuf[0] = MAGIC_WORD_HEAD;

			// stemmed word w/markers
			SphWordID_t iWord = m_pDict->GetWordIDWithMarkers(sBuf);
			if (!iWord)
			{
				m_tState.m_iBuildLastStep = m_iStopwordStep;
				continue;
			}

			if (m_bIndexExactWords)
				m_tHits.AddHit(uDocid, uExactWordid, m_tState.m_iHitPos);
			iBlendedHitsStart = iLastBlendedStart;
			m_tHits.AddHit(uDocid, iWord, m_tState.m_iHitPos);
			m_tState.m_iBuildLastStep = m_pTokenizer->TokenIsBlended() ? 0 : 1;

			// restore stemmed word
			int iStemmedLen = strlen((const char*)sBuf);
			sBuf[iStemmedLen - 1] = '\0';

			// stemmed word w/o markers
			if (strcmp((const char*)sBuf + 1, (const char*)sWord))
				m_tHits.AddHit(uDocid, m_pDict->GetWordID(sBuf + 1, iStemmedLen - 2, true), m_tState.m_iHitPos);

			// restore word
			memcpy(sBuf + 1, sWord, iBytes);
			sBuf[iBytes + 1] = MAGIC_WORD_TAIL;
			sBuf[iBytes + 2] = '\0';

			// if there are no infixes, that's it
			if (iMinInfixLen > iLen)
			{
				// index full word
				m_tHits.AddHit(uDocid, m_pDict->GetWordID(sWord), m_tState.m_iHitPos);
				continue;
			}

			// process all infixes
			int iMaxStart = bPrefixField ? 0 : (iLen - iMinInfixLen);

			BYTE* sInfix = sBuf + 1;

			for (int iStart = 0; iStart <= iMaxStart; iStart++)
			{
				BYTE* sInfixEnd = sInfix;
				for (int i = 0; i < iMinInfixLen; i++)
					sInfixEnd += m_pTokenizer->GetCodepointLength(*sInfixEnd);

				int iMaxSubLen = (iLen - iStart);
				if (m_iMaxSubstringLen)
					iMaxSubLen = Min(m_iMaxSubstringLen, iMaxSubLen);

				for (int i = iMinInfixLen; i <= iMaxSubLen; i++)
				{
					m_tHits.AddHit(uDocid, m_pDict->GetWordID(sInfix, sInfixEnd - sInfix, false), m_tState.m_iHitPos);

					// word start: add magic head
					if (bInfixMode && iStart == 0)
						m_tHits.AddHit(uDocid, m_pDict->GetWordID(sInfix - 1, sInfixEnd - sInfix + 1, false), m_tState.m_iHitPos);

					// word end: add magic tail
					if (bInfixMode && i == iLen - iStart)
						m_tHits.AddHit(uDocid, m_pDict->GetWordID(sInfix, sInfixEnd - sInfix + 1, false), m_tState.m_iHitPos);

					sInfixEnd += m_pTokenizer->GetCodepointLength(*sInfixEnd);
				}

				sInfix += m_pTokenizer->GetCodepointLength(*sInfix);
			}
		}

		m_tState.m_bProcessingHits = (sWord != NULL);

		// mark trailing hits
		// and compute fields lengths
		if (!bSkipEndMarker && !m_tState.m_bProcessingHits && m_tHits.Length())
		{
			CSphWordHit* pTail = const_cast <CSphWordHit*> (m_tHits.Last());

			if (m_pFieldLengthAttrs)
				m_pFieldLengthAttrs[HITMAN::GetField(pTail->m_uWordPos)] = HITMAN::GetPos(pTail->m_uWordPos);

			Hitpos_t uEndPos = pTail->m_uWordPos;
			if (iBlendedHitsStart >= 0)
			{
				assert(iBlendedHitsStart >= 0 && iBlendedHitsStart < m_tHits.Length());
				Hitpos_t uBlendedPos = (m_tHits.First() + iBlendedHitsStart)->m_uWordPos;
				uEndPos = Min(uEndPos, uBlendedPos);
			}

			// set end marker for all tail hits
			const CSphWordHit* pStart = m_tHits.First();
			while (pStart <= pTail && uEndPos <= pTail->m_uWordPos)
			{
				HITMAN::SetEndMarker(&pTail->m_uWordPos);
				pTail--;
			}
		}
	}


#define BUILD_REGULAR_HITS_COUNT 6

	void CSphSource_Document::BuildRegularHits(SphDocID_t uDocid, bool bPayload, bool bSkipEndMarker)
	{
		bool bWordDict = m_pDict->GetSettings().m_bWordDict;
		bool bGlobalPartialMatch = !bWordDict && (m_iMinPrefixLen > 0 || m_iMinInfixLen > 0);

		if (!m_tState.m_bProcessingHits)
			m_tState.m_iBuildLastStep = 1;

		BYTE* sWord = NULL;
		BYTE sBuf[16 + 3 * SPH_MAX_WORD_LEN];

		// FIELDEND_MASK at last token stream should be set for HEAD token too
		int iBlendedHitsStart = -1;

		// index words only
		while ((m_iMaxHits == 0 || m_tHits.m_dData.GetLength() + BUILD_REGULAR_HITS_COUNT < m_iMaxHits)
			&& (sWord = m_pTokenizer->GetToken()) != NULL)
		{
			int iLastBlendedStart = TrackBlendedStart(m_pTokenizer, iBlendedHitsStart, m_tHits.Length());

			if (!bPayload)
			{
				HITMAN::AddPos(&m_tState.m_iHitPos, m_tState.m_iBuildLastStep + m_pTokenizer->GetOvershortCount() * m_iOvershortStep);
				if (m_pTokenizer->GetBoundary())
					HITMAN::AddPos(&m_tState.m_iHitPos, m_iBoundaryStep);
			}

			if (BuildZoneHits(uDocid, sWord))
				continue;

			if (bGlobalPartialMatch)
			{
				int iBytes = strlen((const char*)sWord);
				memcpy(sBuf + 1, sWord, iBytes);
				sBuf[0] = MAGIC_WORD_HEAD;
				sBuf[iBytes + 1] = '\0';
				m_tHits.AddHit(uDocid, m_pDict->GetWordIDWithMarkers(sBuf), m_tState.m_iHitPos);
			}

			ESphTokenMorph eMorph = m_pTokenizer->GetTokenMorph();
			if (m_bIndexExactWords && eMorph != SPH_TOKEN_MORPH_GUESS)
			{
				int iBytes = strlen((const char*)sWord);
				memcpy(sBuf + 1, sWord, iBytes);
				sBuf[0] = MAGIC_WORD_HEAD_NONSTEMMED;
				sBuf[iBytes + 1] = '\0';
			}

			if (m_bIndexExactWords && eMorph == SPH_TOKEN_MORPH_ORIGINAL)
			{
				// can not use GetWordID here due to exception vs missed hit, ie
				// stemmed sWord hasn't got added to hit stream but might be added as exception to dictionary
				// that causes error at hit sorting phase \ dictionary HitblockPatch
				if (!m_pDict->GetSettings().m_bStopwordsUnstemmed)
					m_pDict->ApplyStemmers(sWord);

				if (!m_pDict->IsStopWord(sWord))
					m_tHits.AddHit(uDocid, m_pDict->GetWordIDNonStemmed(sBuf), m_tState.m_iHitPos);

				m_tState.m_iBuildLastStep = m_pTokenizer->TokenIsBlended() ? 0 : 1;
				continue;
			}

			SphWordID_t iWord = (eMorph == SPH_TOKEN_MORPH_GUESS)
				? m_pDict->GetWordIDNonStemmed(sWord) // tokenizer did morphology => dict must not stem
				: m_pDict->GetWordID(sWord); // tokenizer did not => stemmers can be applied
			if (iWord)
			{
#if 0
				if (HITMAN::GetPos(m_tState.m_iHitPos) == 1)
					printf("\n");
				printf("doc %d. pos %d. %s\n", uDocid, HITMAN::GetPos(m_tState.m_iHitPos), sWord);
#endif
				iBlendedHitsStart = iLastBlendedStart;
				m_tState.m_iBuildLastStep = m_pTokenizer->TokenIsBlended() ? 0 : 1;
				m_tHits.AddHit(uDocid, iWord, m_tState.m_iHitPos);
				if (m_bIndexExactWords && eMorph != SPH_TOKEN_MORPH_GUESS)
					m_tHits.AddHit(uDocid, m_pDict->GetWordIDNonStemmed(sBuf), m_tState.m_iHitPos);
			}
			else
				m_tState.m_iBuildLastStep = m_iStopwordStep;
		}

		m_tState.m_bProcessingHits = (sWord != NULL);

		// mark trailing hit
		// and compute field lengths
		if (!bSkipEndMarker && !m_tState.m_bProcessingHits && m_tHits.Length())
		{
			CSphWordHit* pTail = const_cast <CSphWordHit*> (m_tHits.Last());

			if (m_pFieldLengthAttrs)
				m_pFieldLengthAttrs[HITMAN::GetField(pTail->m_uWordPos)] = HITMAN::GetPos(pTail->m_uWordPos);

			Hitpos_t uEndPos = pTail->m_uWordPos;
			if (iBlendedHitsStart >= 0)
			{
				assert(iBlendedHitsStart >= 0 && iBlendedHitsStart < m_tHits.Length());
				Hitpos_t uBlendedPos = (m_tHits.First() + iBlendedHitsStart)->m_uWordPos;
				uEndPos = Min(uEndPos, uBlendedPos);
			}

			// set end marker for all tail hits
			const CSphWordHit* pStart = m_tHits.First();
			while (pStart <= pTail && uEndPos <= pTail->m_uWordPos)
			{
				HITMAN::SetEndMarker(&pTail->m_uWordPos);
				pTail--;
			}
		}
	}


	void CSphSource_Document::BuildHits(CSphString& sError, bool bSkipEndMarker)
	{
		SphDocID_t uDocid = m_tDocInfo.m_uDocID;

		for (; m_tState.m_iField < m_tState.m_iEndField; m_tState.m_iField++)
		{
			if (!m_tState.m_bProcessingHits)
			{
				// get that field
				BYTE* sField = m_tState.m_dFields[m_tState.m_iField - m_tState.m_iStartField];
				int iFieldBytes = m_tState.m_dFieldLengths[m_tState.m_iField - m_tState.m_iStartField];
				if (!sField || !(*sField) || !iFieldBytes)
					continue;

				// load files
				const BYTE* sTextToIndex;
				if (m_tSchema.m_dFields[m_tState.m_iField].m_bFilename)
				{
					LoadFileField(&sField, sError);
					sTextToIndex = sField;
					iFieldBytes = (int)strlen((char*)sField);
					if (m_pFieldFilter && iFieldBytes)
					{
						m_tState.m_dFiltered.Resize(0);
						int iFiltered = m_pFieldFilter->Apply(sTextToIndex, iFieldBytes, m_tState.m_dFiltered, false);
						if (iFiltered)
						{
							sTextToIndex = m_tState.m_dFiltered.Begin();
							iFieldBytes = iFiltered;
						}
					}
				}
				else
					sTextToIndex = sField;

				if (iFieldBytes <= 0)
					continue;

				// strip html
				if (m_pStripper)
				{
					m_pStripper->Strip((BYTE*)sTextToIndex);
					iFieldBytes = (int)strlen((char*)sTextToIndex);
				}

				// tokenize and build hits
				m_tStats.m_iTotalBytes += iFieldBytes;

				m_pTokenizer->BeginField(m_tState.m_iField);
				m_pTokenizer->SetBuffer((BYTE*)sTextToIndex, iFieldBytes);

				m_tState.m_iHitPos = HITMAN::Create(m_tState.m_iField, m_tState.m_iStartPos);
			}

			const CSphColumnInfo& tField = m_tSchema.m_dFields[m_tState.m_iField];

			if (tField.m_eWordpart != SPH_WORDPART_WHOLE)
				BuildSubstringHits(uDocid, tField.m_bPayload, tField.m_eWordpart, bSkipEndMarker);
			else
				BuildRegularHits(uDocid, tField.m_bPayload, bSkipEndMarker);

			if (m_tState.m_bProcessingHits)
				break;
		}

		m_tState.m_bDocumentDone = !m_tState.m_bProcessingHits;
	}

	////// mva 

	SphRange_t CSphSource_Document::IterateFieldMVAStart(int iAttr)
	{
		SphRange_t tRange;
		tRange.m_iStart = tRange.m_iLength = 0;

		if (iAttr < 0 || iAttr >= m_tSchema.GetAttrsCount())
			return tRange;

		const CSphColumnInfo& tMva = m_tSchema.GetAttr(iAttr);
		int uOff = MVA_DOWNSIZE(m_tDocInfo.GetAttr(tMva.m_tLocator));
		if (!uOff)
			return tRange;

		int iCount = m_dMva[uOff];
		assert(iCount);

		tRange.m_iStart = uOff + 1;
		tRange.m_iLength = iCount;

		return tRange;
	}


	int CSphSource_Document::ParseFieldMVA(CSphVector < DWORD >& dMva, const char* szValue, bool bMva64) const
	{
		if (!szValue)
			return 0;

		const char* pPtr = szValue;
		const char* pDigit = NULL;
		const int MAX_NUMBER_LEN = 64;
		char szBuf[MAX_NUMBER_LEN];

		assert(dMva.GetLength()); // must not have zero offset
		int uOff = dMva.GetLength();
		dMva.Add(0); // reserve value for count

		while (*pPtr)
		{
			if ((*pPtr >= '0' && *pPtr <= '9') || (bMva64 && *pPtr == '-'))
			{
				if (!pDigit)
					pDigit = pPtr;
			}
			else
			{
				if (pDigit)
				{
					if (pPtr - pDigit < MAX_NUMBER_LEN)
					{
						strncpy(szBuf, pDigit, pPtr - pDigit);
						szBuf[pPtr - pDigit] = '\0';
						if (!bMva64)
							dMva.Add(sphToDword(szBuf));
						else
							sphAddMva64(dMva, sphToInt64(szBuf));
					}

					pDigit = NULL;
				}
			}

			pPtr++;
		}

		if (pDigit)
		{
			if (!bMva64)
				dMva.Add(sphToDword(pDigit));
			else
				sphAddMva64(dMva, sphToInt64(pDigit));
		}

		int iCount = (size_t)dMva.GetLength() - uOff - 1;
		if (!iCount)
		{
			dMva.Pop(); // remove reserved value for count in case of 0 MVAs
			return 0;
		}
		else
		{
			dMva[uOff] = iCount;
			return uOff; // return offset to ( count, [value] )
		}
	}

	////////////////////////////////////



}