#include "neo/source/source_sv.h"
#include "neo/sphinx/xrlp.h"
#include "neo/tools/config_parser.h"
#include "neo/dict/dict.h"
#include "neo/source/schema_configurator.h"

namespace NEO {

	CSphSource* sphCreateSourceTSVpipe(const CSphConfigSection* pSource, FILE* pPipe, const char* sSourceName, bool bProxy)
	{
		CSphString sError;
		CSphSource_TSV* pTSV = CreateSourceWithProxy<CSphSource_TSV>(sSourceName, bProxy);
		if (!pTSV->Setup(*pSource, pPipe, sError))
		{
			SafeDelete(pTSV);
			fprintf(stdout, "ERROR: tsvpipe: %s", sError.cstr());
		}

		return pTSV;
	}


	CSphSource* sphCreateSourceCSVpipe(const CSphConfigSection* pSource, FILE* pPipe, const char* sSourceName, bool bProxy)
	{
		CSphString sError;
		const char* sDelimiter = pSource->GetStr("csvpipe_delimiter", "");
		CSphSource_CSV* pCSV = CreateSourceWithProxy<CSphSource_CSV>(sSourceName, bProxy);
		pCSV->SetDelimiter(sDelimiter);
		if (!pCSV->Setup(*pSource, pPipe, sError))
		{
			SafeDelete(pCSV);
			fprintf(stdout, "ERROR: csvpipe: %s", sError.cstr());
		}

		return pCSV;
	}


	CSphSource_BaseSV::CSphSource_BaseSV(const char* sName)
		: CSphSource_Document(sName)
		, m_dError(1024)
		, m_dColumnsLen(0)
		, m_dRemap(0)
		, m_dFields(0)
		, m_dFieldLengths(0)
		, m_iAutoCount(0)
	{
		m_iDataStart = 0;
		m_iBufUsed = 0;
	}


	CSphSource_BaseSV::~CSphSource_BaseSV()
	{
		Disconnect();
	}

	struct SortedRemapXSV_t : public RemapXSV_t
	{
		int m_iTag;
	};


	bool CSphSource_BaseSV::Setup(const CSphConfigSection& hSource, FILE* pPipe, CSphString& sError)
	{
		m_pFP = pPipe;
		m_tSchema.Reset();
		bool bWordDict = (m_pDict && m_pDict->GetSettings().m_bWordDict);

		if (!SetupSchema(hSource, bWordDict, sError))
			return false;

		if (!SourceCheckSchema(m_tSchema, sError))
			return false;

		int nFields = m_tSchema.m_dFields.GetLength();
		m_dFields.Reset(nFields);
		m_dFieldLengths.Reset(nFields);

		// build hash from schema names
		SmallStringHash_T<SortedRemapXSV_t> hSchema;
		SortedRemapXSV_t tElem;
		tElem.m_iTag = -1;
		tElem.m_iAttr = -1;
		ARRAY_FOREACH(i, m_tSchema.m_dFields)
		{
			tElem.m_iField = i;
			hSchema.Add(tElem, m_tSchema.m_dFields[i].m_sName);
		}
		tElem.m_iField = -1;
		for (int i = 0; i < m_tSchema.GetAttrsCount(); i++)
		{
			RemapXSV_t* pRemap = hSchema(m_tSchema.GetAttr(i).m_sName);
			if (pRemap)
			{
				pRemap->m_iAttr = i;
			}
			else
			{
				tElem.m_iAttr = i;
				hSchema.Add(tElem, m_tSchema.GetAttr(i).m_sName);
			}
		}

		// restore order for declared columns
		CSphString sColumn;
		hSource.IterateStart();
		while (hSource.IterateNext())
		{
			const CSphVariant* pVal = &hSource.IterateGet();
			while (pVal)
			{
				sColumn = pVal->strval();
				// uint attribute might have bit count that should by cut off from name
				const char* pColon = strchr(sColumn.cstr(), ':');
				if (pColon)
				{
					int iColon = pColon - sColumn.cstr();
					CSphString sTmp;
					sTmp.SetBinary(sColumn.cstr(), iColon);
					sColumn.SwapWith(sTmp);
				}

				// let's handle different char cases
				sColumn.ToLower();

				SortedRemapXSV_t* pColumn = hSchema(sColumn);
				assert(!pColumn || pColumn->m_iAttr >= 0 || pColumn->m_iField >= 0);
				assert(!pColumn || pColumn->m_iTag == -1);
				if (pColumn)
					pColumn->m_iTag = pVal->m_iTag;

				pVal = pVal->m_pNext;
			}
		}

		// fields + attributes + id - auto-generated
		m_dColumnsLen.Reset(hSchema.GetLength() + 1);
		m_dRemap.Reset(hSchema.GetLength() + 1);
		CSphFixedVector<SortedRemapXSV_t> dColumnsSorted(hSchema.GetLength());

		hSchema.IterateStart();
		for (int i = 0; hSchema.IterateNext(); i++)
		{
			assert(hSchema.IterateGet().m_iTag >= 0);
			dColumnsSorted[i] = hSchema.IterateGet();
		}

		sphSort(dColumnsSorted.Begin(), dColumnsSorted.GetLength(), bind(&SortedRemapXSV_t::m_iTag));

		// set remap incoming columns to fields \ attributes
		// doc_id dummy filler
		m_dRemap[0].m_iAttr = 0;
		m_dRemap[0].m_iField = 0;

		ARRAY_FOREACH(i, dColumnsSorted)
		{
			assert(!i || dColumnsSorted[i - 1].m_iTag < dColumnsSorted[i].m_iTag); // no duplicates allowed
			m_dRemap[i + 1] = dColumnsSorted[i];
		}

		return true;
	}


	bool CSphSource_BaseSV::Connect(CSphString& sError)
	{
		bool bWordDict = (m_pDict && m_pDict->GetSettings().m_bWordDict);
		ARRAY_FOREACH(i, m_tSchema.m_dFields)
		{
			CSphColumnInfo& tCol = m_tSchema.m_dFields[i];
			tCol.m_eWordpart = GetWordpart(tCol.m_sName.cstr(), bWordDict);
		}

		int iAttrs = m_tSchema.GetAttrsCount();
		if (!AddAutoAttrs(sError))
			return false;

		m_iAutoCount = m_tSchema.GetAttrsCount() - iAttrs;

		AllocDocinfo();

		m_tHits.m_dData.Reserve(m_iMaxHits);
		m_dBuf.Resize(DEFAULT_READ_BUFFER);
		m_dMva.Reserve(512);

		return true;
	}


	void CSphSource_BaseSV::Disconnect()
	{
		if (m_pFP)
		{
			fclose(m_pFP);
			m_pFP = NULL;
		}
		m_tHits.m_dData.Reset();
	}


	const char* CSphSource_BaseSV::DecorateMessage(const char* sTemplate, ...) const
	{
		va_list ap;
		va_start(ap, sTemplate);
		vsnprintf(m_dError.Begin(), m_dError.GetLength(), sTemplate, ap);
		va_end(ap);
		return m_dError.Begin();
	}

	static const BYTE g_dBOM[] = { 0xEF, 0xBB, 0xBF };

	bool CSphSource_BaseSV::IterateStart(CSphString& sError)
	{
		if (!m_tSchema.m_dFields.GetLength())
		{
			sError.SetSprintf("No fields in schema - will not index");
			return false;
		}

		m_iLine = 0;
		m_iDataStart = 0;

		// initial buffer update
		m_iBufUsed = fread(m_dBuf.Begin(), 1, m_dBuf.GetLength(), m_pFP);
		if (!m_iBufUsed)
		{
			sError.SetSprintf("source '%s': read error '%s'", m_tSchema.m_sName.cstr(), strerror(errno));
			return false;
		}
		m_iPlainFieldsLength = m_tSchema.m_dFields.GetLength();

		// space out BOM like xml-pipe does
		if (m_iBufUsed > (int)sizeof(g_dBOM) && memcmp(m_dBuf.Begin(), g_dBOM, sizeof(g_dBOM)) == 0)
			memset(m_dBuf.Begin(), ' ', sizeof(g_dBOM));
		return true;
	}

	BYTE** CSphSource_BaseSV::ReportDocumentError()
	{
		m_tDocInfo.m_uDocID = 1; // 0 means legal eof
		m_iDataStart = 0;
		m_iBufUsed = 0;
		return NULL;
	}


	BYTE** CSphSource_BaseSV::NextDocument(CSphString& sError)
	{
		ESphParseResult eRes = SplitColumns(sError);
		if (eRes == PARSING_FAILED)
			return ReportDocumentError();
		else if (eRes == DATA_OVER)
			return NULL;

		assert(eRes == GOT_DOCUMENT);

		// check doc_id
		if (!m_dColumnsLen[0])
		{
			sError.SetSprintf("source '%s': no doc_id found (line=%d)", m_tSchema.m_sName.cstr(), m_iLine);
			return ReportDocumentError();
		}

		// parse doc_id
		m_tDocInfo.m_uDocID = sphToDocid((const char*)&m_dBuf[m_iDocStart]);

		// check doc_id
		if (m_tDocInfo.m_uDocID == 0)
		{
			sError.SetSprintf("source '%s': invalid doc_id found (line=%d)", m_tSchema.m_sName.cstr(), m_iLine);
			return ReportDocumentError();
		}

		// parse column data
		int iOff = m_iDocStart + m_dColumnsLen[0] + 1; // skip docid and its trailing zero
		int iColumns = m_dRemap.GetLength();
		for (int iCol = 1; iCol < iColumns; iCol++)
		{
			// if+if for field-string attribute case
			const RemapXSV_t& tRemap = m_dRemap[iCol];

			// field column
			if (tRemap.m_iField != -1)
			{
				m_dFields[tRemap.m_iField] = m_dBuf.Begin() + iOff;
				m_dFieldLengths[tRemap.m_iField] = strlen((char*)m_dFields[tRemap.m_iField]);
			}

			// attribute column
			if (tRemap.m_iAttr != -1)
			{
				const CSphColumnInfo& tAttr = m_tSchema.GetAttr(tRemap.m_iAttr);
				const char* sVal = (const char*)m_dBuf.Begin() + iOff;

				switch (tAttr.m_eAttrType)
				{
				case ESphAttr::SPH_ATTR_STRING:
				case ESphAttr::SPH_ATTR_JSON:
					m_dStrAttrs[tRemap.m_iAttr] = sVal;
					m_tDocInfo.SetAttr(tAttr.m_tLocator, 0);
					break;

				case ESphAttr::SPH_ATTR_FLOAT:
					m_tDocInfo.SetAttrFloat(tAttr.m_tLocator, sphToFloat(sVal));
					break;

				case ESphAttr::SPH_ATTR_BIGINT:
					m_tDocInfo.SetAttr(tAttr.m_tLocator, sphToInt64(sVal));
					break;

				case ESphAttr::SPH_ATTR_UINT32SET:
				case ESphAttr::SPH_ATTR_INT64SET:
					m_tDocInfo.SetAttr(tAttr.m_tLocator, ParseFieldMVA(m_dMva, sVal, (tAttr.m_eAttrType == ESphAttr::SPH_ATTR_INT64SET)));
					break;

				case ESphAttr::SPH_ATTR_TOKENCOUNT:
					m_tDocInfo.SetAttr(tAttr.m_tLocator, 0);
					break;

				default:
					m_tDocInfo.SetAttr(tAttr.m_tLocator, sphToDword(sVal));
					break;
				}
			}

			iOff += m_dColumnsLen[iCol] + 1; // length of value plus null-terminator
		}

		m_iLine++;
		return m_dFields.Begin();
	}


	CSphSource_BaseSV::ESphParseResult CSphSource_TSV::SplitColumns(CSphString& sError)
	{
		int iColumns = m_dRemap.GetLength();
		int iCol = 0;
		int iColumnStart = m_iDataStart;
		BYTE* pData = m_dBuf.Begin() + m_iDataStart;
		const BYTE* pEnd = m_dBuf.Begin() + m_iBufUsed;
		m_iDocStart = m_iDataStart;

		for (;; )
		{
			if (iCol >= iColumns)
			{
				sError.SetSprintf("source '%s': too many columns found (found=%d, declared=%d, line=%d, docid=" DOCID_FMT ")",
					m_tSchema.m_sName.cstr(), iCol, iColumns + m_iAutoCount, m_iLine, m_tDocInfo.m_uDocID);
				return CSphSource_BaseSV::PARSING_FAILED;
			}

			// move to next control symbol
			while (pData < pEnd && *pData && *pData != '\t' && *pData != '\r' && *pData != '\n')
				pData++;

			if (pData < pEnd)
			{
				assert(*pData == '\t' || !*pData || *pData == '\r' || *pData == '\n');
				bool bNull = !*pData;
				bool bEOL = (*pData == '\r' || *pData == '\n');

				int iLen = pData - m_dBuf.Begin() - iColumnStart;
				assert(iLen >= 0);
				m_dColumnsLen[iCol] = iLen;
				*pData++ = '\0';
				iCol++;

				if (bNull)
				{
					// null terminated string found
					m_iDataStart = m_iBufUsed = 0;
					break;
				}
				else if (bEOL)
				{
					// end of document found
					// skip all EOL characters
					while (pData < pEnd && *pData && (*pData == '\r' || *pData == '\n'))
						pData++;
					break;
				}

				// column separator found
				iColumnStart = pData - m_dBuf.Begin();
				continue;
			}

			int iOff = pData - m_dBuf.Begin();

			// if there is space at the start, move data around
			// if not, resize the buffer
			if (m_iDataStart > 0)
			{
				memmove(m_dBuf.Begin(), m_dBuf.Begin() + m_iDataStart, m_iBufUsed - m_iDataStart);
				m_iBufUsed -= m_iDataStart;
				iOff -= m_iDataStart;
				iColumnStart -= m_iDataStart;
				m_iDataStart = 0;
				m_iDocStart = 0;
			}
			else if (m_iBufUsed == m_dBuf.GetLength())
			{
				m_dBuf.Resize(m_dBuf.GetLength() * 2);
			}

			// do read
			int iGot = fread(m_dBuf.Begin() + m_iBufUsed, 1, m_dBuf.GetLength() - m_iBufUsed, m_pFP);
			if (!iGot)
			{
				if (!iCol)
				{
					// normal file termination - no pending columns and documents
					m_iDataStart = m_iBufUsed = 0;
					m_tDocInfo.m_uDocID = 0;
					return CSphSource_BaseSV::DATA_OVER;
				}

				// error in case no data left in middle of data stream
				sError.SetSprintf("source '%s': read error '%s' (line=%d, docid=" DOCID_FMT ")",
					m_tSchema.m_sName.cstr(), strerror(errno), m_iLine, m_tDocInfo.m_uDocID);
				return CSphSource_BaseSV::PARSING_FAILED;
			}
			m_iBufUsed += iGot;

			// restored pointers after buffer resize
			pData = m_dBuf.Begin() + iOff;
			pEnd = m_dBuf.Begin() + m_iBufUsed;
		}

		// all columns presence check
		if (iCol != iColumns)
		{
			sError.SetSprintf("source '%s': not all columns found (found=%d, total=%d, line=%d, docid=" DOCID_FMT ")",
				m_tSchema.m_sName.cstr(), iCol, iColumns, m_iLine, m_tDocInfo.m_uDocID);
			return CSphSource_BaseSV::PARSING_FAILED;
		}

		// tail data
		assert(pData <= pEnd);
		m_iDataStart = pData - m_dBuf.Begin();
		return CSphSource_BaseSV::GOT_DOCUMENT;
	}


	bool CSphSource_TSV::SetupSchema(const CSphConfigSection& hSource, bool bWordDict, CSphString& sError)
	{
		bool bOk = true;
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_uint"), ESphAttr::SPH_ATTR_INTEGER, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_timestamp"), ESphAttr::SPH_ATTR_TIMESTAMP, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_bool"), ESphAttr::SPH_ATTR_BOOL, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_float"), ESphAttr::SPH_ATTR_FLOAT, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_bigint"), ESphAttr::SPH_ATTR_BIGINT, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_multi"), ESphAttr::SPH_ATTR_UINT32SET, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_multi_64"), ESphAttr::SPH_ATTR_INT64SET, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_string"), ESphAttr::SPH_ATTR_STRING, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_attr_json"), ESphAttr::SPH_ATTR_JSON, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("tsvpipe_field_string"), ESphAttr::SPH_ATTR_STRING, m_tSchema, sError);

		if (!bOk)
			return false;

		ConfigureFields(hSource("tsvpipe_field"), bWordDict, m_tSchema);
		ConfigureFields(hSource("tsvpipe_field_string"), bWordDict, m_tSchema);

		return true;
	}


	CSphSource_CSV::CSphSource_CSV(const char* sName)
		: CSphSource_BaseSV(sName)
	{
		m_iDelimiter = BYTE(',');
	}


	CSphSource_BaseSV::ESphParseResult CSphSource_CSV::SplitColumns(CSphString& sError)
	{
		int iColumns = m_dRemap.GetLength();
		int iCol = 0;
		int iColumnStart = m_iDataStart;
		int iQuotPrev = -1;
		int	iEscapeStart = -1;
		const BYTE* s = m_dBuf.Begin() + m_iDataStart; // parse this line
		BYTE* d = m_dBuf.Begin() + m_iDataStart; // do parsing in place
		const BYTE* pEnd = m_dBuf.Begin() + m_iBufUsed; // until we reach the end of current buffer
		m_iDocStart = m_iDataStart;
		bool bOnlySpace = true;
		bool bQuoted = false;
		bool bHasQuot = false;

		for (;; )
		{
			assert(d <= s);

			// move to next control symbol
			while (s < pEnd && *s && *s != m_iDelimiter && *s != '"' && *s != '\\' && *s != '\r' && *s != '\n')
			{
				bOnlySpace &= sphIsSpace(*s);
				*d++ = *s++;
			}

			if (s < pEnd)
			{
				assert(!*s || *s == m_iDelimiter || *s == '"' || *s == '\\' || *s == '\r' || *s == '\n');
				bool bNull = !*s;
				bool bEOL = (*s == '\r' || *s == '\n');
				bool bDelimiter = (*s == m_iDelimiter);
				bool bQuot = (*s == '"');
				bool bEscape = (*s == '\\');
				int iOff = s - m_dBuf.Begin();
				bool bEscaped = (iEscapeStart >= 0 && iEscapeStart + 1 == iOff);

				// escape symbol outside double quotation
				if (!bQuoted && !bDelimiter && (bEscape || bEscaped))
				{
					if (bEscaped) // next to escape symbol proceed as regular
					{
						*d++ = *s++;
					}
					else // escape just started
					{
						iEscapeStart = iOff;
						s++;
					}
					continue;
				}

				// double quote processing
				// [ " ... " ]
				// [ " ... "" ... " ]
				// [ " ... """ ]
				// [ " ... """" ... " ]
				// any symbol inside double quote proceed as regular
				// but quoted quote proceed as regular symbol
				if (bQuot)
				{
					if (bOnlySpace && iQuotPrev == -1)
					{
						// enable double quote
						bQuoted = true;
						bHasQuot = true;
					}
					else if (bQuoted)
					{
						// close double quote on 2st quote symbol
						bQuoted = false;
					}
					else if (bHasQuot && iQuotPrev != -1 && iQuotPrev + 1 == iOff)
					{
						// escaped quote found, re-enable double quote and copy symbol itself
						bQuoted = true;
						*d++ = '"';
					}
					else
					{
						*d++ = *s;
					}

					s++;
					iQuotPrev = iOff;
					continue;
				}

				if (bQuoted)
				{
					*d++ = *s++;
					continue;
				}

				int iLen = d - m_dBuf.Begin() - iColumnStart;
				assert(iLen >= 0);
				if (iCol < m_dColumnsLen.GetLength())
					m_dColumnsLen[iCol] = iLen;
				*d++ = '\0';
				s++;
				iCol++;

				if (bNull) // null terminated string found
				{
					m_iDataStart = m_iBufUsed = 0;
					break;
				}
				else if (bEOL) // end of document found
				{
					// skip all EOL characters
					while (s < pEnd && *s && (*s == '\r' || *s == '\n'))
						s++;
					break;
				}

				assert(bDelimiter);
				// column separator found
				iColumnStart = d - m_dBuf.Begin();
				bOnlySpace = true;
				bQuoted = false;
				bHasQuot = false;
				iQuotPrev = -1;
				continue;
			}

			/////////////////////
			// read in more data
			/////////////////////

			int iDstOff = s - m_dBuf.Begin();
			int iSrcOff = d - m_dBuf.Begin();

			// if there is space at the start, move data around
			// if not, resize the buffer
			if (m_iDataStart > 0)
			{
				memmove(m_dBuf.Begin(), m_dBuf.Begin() + m_iDataStart, m_iBufUsed - m_iDataStart);
				m_iBufUsed -= m_iDataStart;
				iDstOff -= m_iDataStart;
				iSrcOff -= m_iDataStart;
				iColumnStart -= m_iDataStart;
				if (iQuotPrev != -1)
					iQuotPrev -= m_iDataStart;
				iEscapeStart -= m_iDataStart;
				m_iDataStart = 0;
				m_iDocStart = 0;
			}
			else if (m_iBufUsed == m_dBuf.GetLength())
			{
				m_dBuf.Resize(m_dBuf.GetLength() * 2);
			}

			// do read
			int iGot = fread(m_dBuf.Begin() + m_iBufUsed, 1, m_dBuf.GetLength() - m_iBufUsed, m_pFP);
			if (!iGot)
			{
				if (!iCol)
				{
					// normal file termination - no pending columns and documents
					m_iDataStart = m_iBufUsed = 0;
					m_tDocInfo.m_uDocID = 0;
					return CSphSource_BaseSV::DATA_OVER;
				}

				if (iCol != iColumns)
				{
					sError.SetSprintf("source '%s': not all columns found (found=%d, total=%d, line=%d, docid=" DOCID_FMT ", error='%s')",
						m_tSchema.m_sName.cstr(), iCol, iColumns, m_iLine, m_tDocInfo.m_uDocID, strerror(errno));
				}
				else
				{
					// error in case no data left in middle of data stream
					sError.SetSprintf("source '%s': read error '%s' (line=%d, docid=" DOCID_FMT ")",
						m_tSchema.m_sName.cstr(), strerror(errno), m_iLine, m_tDocInfo.m_uDocID);
				}
				return CSphSource_BaseSV::PARSING_FAILED;
			}
			m_iBufUsed += iGot;

			// restore pointers because of the resize
			s = m_dBuf.Begin() + iDstOff;
			d = m_dBuf.Begin() + iSrcOff;
			pEnd = m_dBuf.Begin() + m_iBufUsed;
		}

		// all columns presence check
		if (iCol != iColumns)
		{
			sError.SetSprintf("source '%s': not all columns found (found=%d, total=%d, line=%d, docid=" DOCID_FMT ")",
				m_tSchema.m_sName.cstr(), iCol, iColumns, m_iLine, m_tDocInfo.m_uDocID);
			return CSphSource_BaseSV::PARSING_FAILED;
		}

		// tail data
		assert(s <= pEnd);
		m_iDataStart = s - m_dBuf.Begin();
		return CSphSource_BaseSV::GOT_DOCUMENT;
	}


	bool CSphSource_CSV::SetupSchema(const CSphConfigSection& hSource, bool bWordDict, CSphString& sError)
	{
		bool bOk = true;

		bOk &= ConfigureAttrs(hSource("csvpipe_attr_uint"), ESphAttr::SPH_ATTR_INTEGER, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_timestamp"), ESphAttr::SPH_ATTR_TIMESTAMP, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_bool"), ESphAttr::SPH_ATTR_BOOL, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_float"), ESphAttr::SPH_ATTR_FLOAT, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_bigint"), ESphAttr::SPH_ATTR_BIGINT, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_multi"), ESphAttr::SPH_ATTR_UINT32SET, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_multi_64"), ESphAttr::SPH_ATTR_INT64SET, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_string"), ESphAttr::SPH_ATTR_STRING, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_attr_json"), ESphAttr::SPH_ATTR_JSON, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("csvpipe_field_string"), ESphAttr::SPH_ATTR_STRING, m_tSchema, sError);

		if (!bOk)
			return false;

		ConfigureFields(hSource("csvpipe_field"), bWordDict, m_tSchema);
		ConfigureFields(hSource("csvpipe_field_string"), bWordDict, m_tSchema);

		return true;
	}


	void CSphSource_CSV::SetDelimiter(const char* sDelimiter)
	{
		if (sDelimiter && *sDelimiter)
			m_iDelimiter = *sDelimiter;
	}

}