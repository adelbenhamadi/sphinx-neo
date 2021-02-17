
#if USE_LIBEXPAT
#include "neo/source/source_xml_pipe2.h"
#include "neo/source/schema_configurator.h"
namespace NEO {

	CSphSource_XMLPipe2::CSphSource_XMLPipe2(const char* sName)
		: CSphSource_Document(sName)
		, m_pCurDocument(NULL)
		, m_pPipe(NULL)
		, m_iElementDepth(0)
		, m_pBuffer(NULL)
		, m_iBufferSize(1048576)
		, m_bRemoveParsed(false)
		, m_bInDocset(false)
		, m_bInSchema(false)
		, m_bInDocument(false)
		, m_bInKillList(false)
		, m_bInId(false)
		, m_bInIgnoredTag(false)
		, m_bFirstTagAfterDocset(false)
		, m_iKillListIterator(0)
		, m_iMVA(0)
		, m_iMVAIterator(0)
		, m_iCurField(-1)
		, m_iCurAttr(-1)
		, m_pParser(NULL)
		, m_iFieldBufferMax(65536)
		, m_pFieldBuffer(NULL)
		, m_iFieldBufferLen(0)
		, m_bFixupUTF8(false)
		, m_iReparseStart(0)
		, m_iReparseLen(0)
	{
	}


	CSphSource_XMLPipe2::~CSphSource_XMLPipe2()
	{
		Disconnect();
		SafeDeleteArray(m_pBuffer);
		SafeDeleteArray(m_pFieldBuffer);
		ARRAY_FOREACH(i, m_dParsedDocuments)
			SafeDelete(m_dParsedDocuments[i]);
	}


	void CSphSource_XMLPipe2::Disconnect()
	{
		if (m_pPipe)
		{
			pclose(m_pPipe);
			m_pPipe = NULL;
		}

		if (m_pParser)
		{
			sph_XML_ParserFree(m_pParser);
			m_pParser = NULL;
		}

		m_tHits.m_dData.Reset();
	}


	void CSphSource_XMLPipe2::Error(const char* sTemplate, ...)
	{
		if (!m_sError.IsEmpty())
			return;

		va_list ap;
		va_start(ap, sTemplate);
		m_sError = DecorateMessageVA(sTemplate, ap);
		va_end(ap);
	}


	const char* CSphSource_XMLPipe2::DecorateMessage(const char* sTemplate, ...) const
	{
		va_list ap;
		va_start(ap, sTemplate);
		const char* sRes = DecorateMessageVA(sTemplate, ap);
		va_end(ap);
		return sRes;
	}


	const char* CSphSource_XMLPipe2::DecorateMessageVA(const char* sTemplate, va_list ap) const
	{
		static char sBuf[1024];

		snprintf(sBuf, sizeof(sBuf), "source '%s': ", m_tSchema.m_sName.cstr());
		int iBufLen = strlen(sBuf);
		int iLeft = sizeof(sBuf) - iBufLen;
		char* szBufStart = sBuf + iBufLen;

		vsnprintf(szBufStart, iLeft, sTemplate, ap);
		iBufLen = strlen(sBuf);
		iLeft = sizeof(sBuf) - iBufLen;
		szBufStart = sBuf + iBufLen;

		if (m_pParser)
		{
			SphDocID_t uFailedID = 0;
			if (m_dParsedDocuments.GetLength())
				uFailedID = m_dParsedDocuments.Last()->m_uDocID;

			snprintf(szBufStart, iLeft, " (line=%d, pos=%d, docid=" DOCID_FMT ")",
				(int)sph_XML_GetCurrentLineNumber(m_pParser), (int)sph_XML_GetCurrentColumnNumber(m_pParser),
				uFailedID);
		}

		return sBuf;
	}


	bool CSphSource_XMLPipe2::Setup(int iFieldBufferMax, bool bFixupUTF8, FILE* pPipe, const CSphConfigSection& hSource, CSphString& sError)
	{
		assert(!m_pBuffer && !m_pFieldBuffer);

		m_pBuffer = new BYTE[m_iBufferSize];
		m_iFieldBufferMax = Max(iFieldBufferMax, 65536);
		m_pFieldBuffer = new BYTE[m_iFieldBufferMax];
		m_bFixupUTF8 = bFixupUTF8;
		m_pPipe = pPipe;
		m_tSchema.Reset();
		bool bWordDict = (m_pDict && m_pDict->GetSettings().m_bWordDict);
		bool bOk = true;

		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_uint"), ESphAttr::SPH_ATTR_INTEGER, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_timestamp"), ESphAttr::SPH_ATTR_TIMESTAMP, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_bool"), ESphAttr::SPH_ATTR_BOOL, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_float"), ESphAttr::SPH_ATTR_FLOAT, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_bigint"), ESphAttr::SPH_ATTR_BIGINT, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_multi"), ESphAttr::SPH_ATTR_UINT32SET, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_multi_64"), ESphAttr::SPH_ATTR_INT64SET, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_string"), ESphAttr::SPH_ATTR_STRING, m_tSchema, sError);
		bOk &= ConfigureAttrs(hSource("xmlpipe_attr_json"), ESphAttr::SPH_ATTR_JSON, m_tSchema, sError);

		bOk &= ConfigureAttrs(hSource("xmlpipe_field_string"), ESphAttr::SPH_ATTR_STRING, m_tSchema, sError);

		if (!bOk)
			return false;

		if (!SourceCheckSchema(m_tSchema, sError))
			return false;

		ConfigureFields(hSource("xmlpipe_field"), bWordDict, m_tSchema);
		ConfigureFields(hSource("xmlpipe_field_string"), bWordDict, m_tSchema);

		AllocDocinfo();
		return true;
	}


	bool CSphSource_XMLPipe2::Connect(CSphString& sError)
	{
		assert(m_pBuffer && m_pFieldBuffer);

		if_const(!InitDynamicExpat())
		{
			sError.SetSprintf("xmlpipe: failed to load libexpat library");
			return false;
		}

		ARRAY_FOREACH(i, m_tSchema.m_dFields)
		{
			CSphColumnInfo& tCol = m_tSchema.m_dFields[i];
			tCol.m_eWordpart = GetWordpart(tCol.m_sName.cstr(), m_pDict && m_pDict->GetSettings().m_bWordDict);
		}

		if (!AddAutoAttrs(sError))
			return false;
		AllocDocinfo();

		m_pParser = sph_XML_ParserCreate(NULL);
		if (!m_pParser)
		{
			sError.SetSprintf("xmlpipe: failed to create XML parser");
			return false;
		}

		sph_XML_SetUserData(m_pParser, this);
		sph_XML_SetElementHandler(m_pParser, xmlStartElement, xmlEndElement);
		sph_XML_SetCharacterDataHandler(m_pParser, xmlCharacters);

#if USE_LIBICONV
		sph_XML_SetUnknownEncodingHandler(m_pParser, xmlUnknownEncoding, NULL);
#endif

		m_dKillList.Reserve(1024);
		m_dKillList.Resize(0);

		m_bRemoveParsed = false;
		m_bInDocset = false;
		m_bInSchema = false;
		m_bInDocument = false;
		m_bInKillList = false;
		m_bInId = false;
		m_bFirstTagAfterDocset = false;
		m_iCurField = -1;
		m_iCurAttr = -1;
		m_iElementDepth = 0;

		m_dParsedDocuments.Reset();
		m_dDefaultAttrs.Reset();
		m_dInvalid.Reset();
		m_dWarned.Reset();

		m_dParsedDocuments.Reserve(1024);
		m_dParsedDocuments.Resize(0);

		m_iKillListIterator = 0;

		m_iMVA = 0;
		m_iMVAIterator = 0;

		m_sError = "";

		int iBytesRead = fread(m_pBuffer, 1, m_iBufferSize, m_pPipe);

		if (!ParseNextChunk(iBytesRead, sError))
			return false;

		m_dAttrToMVA.Resize(0);

		int iFieldMVA = 0;
		for (int i = 0; i < m_tSchema.GetAttrsCount(); i++)
		{
			const CSphColumnInfo& tCol = m_tSchema.GetAttr(i);
			if ((tCol.m_eAttrType == ESphAttr::SPH_ATTR_UINT32SET || tCol.m_eAttrType == ESphAttr::SPH_ATTR_INT64SET) && tCol.m_eSrc == SPH_ATTRSRC_FIELD)
				m_dAttrToMVA.Add(iFieldMVA++);
			else
				m_dAttrToMVA.Add(-1);
		}

		m_dFieldMVAs.Resize(iFieldMVA);
		ARRAY_FOREACH(i, m_dFieldMVAs)
			m_dFieldMVAs[i].Reserve(16);

		m_tHits.m_dData.Reserve(m_iMaxHits);

		return true;
	}


	bool CSphSource_XMLPipe2::ParseNextChunk(int iBufferLen, CSphString& sError)
	{
		if (!iBufferLen)
			return true;

		bool bLast = (iBufferLen != m_iBufferSize);

		m_iReparseLen = 0;
		if (m_bFixupUTF8)
		{
			BYTE* p = m_pBuffer;
			BYTE* pMax = m_pBuffer + iBufferLen;

			while (p < pMax)
			{
				BYTE v = *p;

				// fix control codes
				if (v < 0x20 && v != 0x0D && v != 0x0A)
				{
					*p++ = ' ';
					continue;
				}

				// accept ascii7 codes
				if (v < 128)
				{
					p++;
					continue;
				}

				// remove invalid start bytes
				if (v < 0xC2)
				{
					*p++ = ' ';
					continue;
				}

				// get and check byte count
				int iBytes = 0;
				while (v & 0x80)
				{
					iBytes++;
					v <<= 1;
				}
				if (iBytes < 2 || iBytes>3)
				{
					*p++ = ' ';
					continue;
				}

				// if we're on a boundary, save these few bytes for the future
				if (p + iBytes > pMax)
				{
					m_iReparseStart = (int)(p - m_pBuffer);
					m_iReparseLen = (int)(pMax - p);
					iBufferLen -= m_iReparseLen;
					break;
				}

				// otherwise (not a boundary), check them all
				int i = 1;
				int iVal = (v >> iBytes);
				for (; i < iBytes; i++)
				{
					if ((p[i] & 0xC0) != 0x80)
						break;
					iVal = (iVal << 6) + (p[i] & 0x3f);
				}

				if (i != iBytes // remove invalid sequences
					|| (iVal >= 0xd800 && iVal <= 0xdfff) // and utf-16 surrogate pairs
					|| (iBytes == 3 && iVal < 0x800) // and overlong 3-byte codes
					|| (iVal >= 0xfff0 && iVal <= 0xffff)) // and kinda-valid specials expat chokes on anyway
				{
					iBytes = i;
					for (i = 0; i < iBytes; i++)
						p[i] = ' ';
				}

				// only move forward by the amount of succesfully processed bytes!
				p += i;
			}
		}

		if (sph_XML_Parse(m_pParser, (const char*)m_pBuffer, iBufferLen, bLast) != XML_STATUS_OK)
		{
			SphDocID_t uFailedID = 0;
			if (m_dParsedDocuments.GetLength())
				uFailedID = m_dParsedDocuments.Last()->m_uDocID;

			sError.SetSprintf("source '%s': XML parse error: %s (line=%d, pos=%d, docid=" DOCID_FMT ")",
				m_tSchema.m_sName.cstr(), sph_XML_ErrorString(sph_XML_GetErrorCode(m_pParser)),
				(int)sph_XML_GetCurrentLineNumber(m_pParser), (int)sph_XML_GetCurrentColumnNumber(m_pParser),
				uFailedID);
			m_tDocInfo.m_uDocID = 1;
			return false;
		}

		if (!m_sError.IsEmpty())
		{
			sError = m_sError;
			m_tDocInfo.m_uDocID = 1;
			return false;
		}

		return true;
	}


	BYTE** CSphSource_XMLPipe2::NextDocument(CSphString& sError)
	{
		assert(m_pBuffer && m_pFieldBuffer);

		if (m_bRemoveParsed)
		{
			SafeDelete(m_dParsedDocuments[0]);
			m_dParsedDocuments.RemoveFast(0);
			m_bRemoveParsed = false;
		}

		int iReadResult = 0;

		while (m_dParsedDocuments.GetLength() == 0)
		{
			// saved bytes to the front!
			if (m_iReparseLen)
				memmove(m_pBuffer, m_pBuffer + m_iReparseStart, m_iReparseLen);

			// read more data
			iReadResult = fread(m_pBuffer + m_iReparseLen, 1, m_iBufferSize - m_iReparseLen, m_pPipe);
			if (iReadResult == 0)
				break;

			// and parse it
			if (!ParseNextChunk(iReadResult + m_iReparseLen, sError))
				return NULL;
		}

		while (m_dParsedDocuments.GetLength() != 0)
		{
			Document_t* pDocument = m_dParsedDocuments[0];
			int nAttrs = m_tSchema.GetAttrsCount();

			// docid
			m_tDocInfo.m_uDocID = VerifyID(pDocument->m_uDocID);
			if (m_tDocInfo.m_uDocID == 0)
			{
				SafeDelete(m_dParsedDocuments[0]);
				m_dParsedDocuments.RemoveFast(0);
				continue;
			}

			int iFirstFieldLenAttr = m_tSchema.GetAttrId_FirstFieldLen();
			int iLastFieldLenAttr = m_tSchema.GetAttrId_LastFieldLen();

			// attributes
			for (int i = 0; i < nAttrs; i++)
			{
				const CSphColumnInfo& tAttr = m_tSchema.GetAttr(i);

				// reset, and the value will be filled by IterateHits()
				if (i >= iFirstFieldLenAttr && i <= iLastFieldLenAttr)
				{
					assert(tAttr.m_eAttrType == ESphAttr::SPH_ATTR_TOKENCOUNT);
					m_tDocInfo.SetAttr(tAttr.m_tLocator, 0);
					continue;
				}

				const CSphString& sAttrValue = pDocument->m_dAttrs[i].IsEmpty() && m_dDefaultAttrs.GetLength()
					? m_dDefaultAttrs[i]
					: pDocument->m_dAttrs[i];

				if (tAttr.m_eAttrType == ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType == ESphAttr::SPH_ATTR_INT64SET)
				{
					m_tDocInfo.SetAttr(tAttr.m_tLocator, ParseFieldMVA(m_dMva, sAttrValue.cstr(), tAttr.m_eAttrType == ESphAttr::SPH_ATTR_INT64SET));
					continue;
				}

				switch (tAttr.m_eAttrType)
				{
				case ESphAttr::SPH_ATTR_STRING:
				case ESphAttr::SPH_ATTR_JSON:
					m_dStrAttrs[i] = sAttrValue.cstr();
					if (!m_dStrAttrs[i].cstr())
						m_dStrAttrs[i] = "";

					m_tDocInfo.SetAttr(tAttr.m_tLocator, 0);
					break;

				case ESphAttr::SPH_ATTR_FLOAT:
					m_tDocInfo.SetAttrFloat(tAttr.m_tLocator, sphToFloat(sAttrValue.cstr()));
					break;

				case ESphAttr::SPH_ATTR_BIGINT:
					m_tDocInfo.SetAttr(tAttr.m_tLocator, sphToInt64(sAttrValue.cstr()));
					break;

				default:
					m_tDocInfo.SetAttr(tAttr.m_tLocator, sphToDword(sAttrValue.cstr()));
					break;
				}
			}

			m_bRemoveParsed = true;

			int nFields = m_tSchema.m_dFields.GetLength();
			if (!nFields)
			{
				m_tDocInfo.m_uDocID = 0;
				return NULL;
			}

			m_dFieldPtrs.Resize(nFields);
			m_dFieldLengths.Resize(nFields);
			for (int i = 0; i < nFields; ++i)
			{
				m_dFieldPtrs[i] = pDocument->m_dFields[i].Begin();
				m_dFieldLengths[i] = pDocument->m_dFields[i].GetLength();

				// skip trailing zero
				if (m_dFieldLengths[i] && !m_dFieldPtrs[i][m_dFieldLengths[i] - 1])
					m_dFieldLengths[i]--;
			}

			return (BYTE**)&(m_dFieldPtrs[0]);
		}

		if (!iReadResult)
			m_tDocInfo.m_uDocID = 0;

		return NULL;
	}


	bool CSphSource_XMLPipe2::IterateKillListStart(CSphString&)
	{
		m_iKillListIterator = 0;
		return true;
	}


	bool CSphSource_XMLPipe2::IterateKillListNext(SphDocID_t& uDocId)
	{
		if (m_iKillListIterator >= m_dKillList.GetLength())
			return false;

		uDocId = m_dKillList[m_iKillListIterator++];
		return true;
	}

	enum EXMLElem
	{
		ELEM_DOCSET,
		ELEM_SCHEMA,
		ELEM_FIELD,
		ELEM_ATTR,
		ELEM_DOCUMENT,
		ELEM_KLIST,
		ELEM_NONE
	};

	static EXMLElem LookupElement(const char* szName)
	{
		if (szName[0] != 's')
			return ELEM_NONE;

		int iLen = strlen(szName);
		if (iLen >= 11 && iLen <= 15)
		{
			char iHash = (char)((iLen + szName[7]) & 15);
			switch (iHash)
			{
			case 1:		if (!strcmp(szName, "sphinx:docset"))		return ELEM_DOCSET;
			case 0:		if (!strcmp(szName, "sphinx:schema"))		return ELEM_SCHEMA;
			case 2:		if (!strcmp(szName, "sphinx:field"))		return ELEM_FIELD;
			case 12:	if (!strcmp(szName, "sphinx:attr"))		return ELEM_ATTR;
			case 3:		if (!strcmp(szName, "sphinx:document"))	return ELEM_DOCUMENT;
			case 10:	if (!strcmp(szName, "sphinx:killlist"))	return ELEM_KLIST;
			}
		}

		return ELEM_NONE;
	}

	void CSphSource_XMLPipe2::StartElement(const char* szName, const char** pAttrs)
	{
		EXMLElem ePos = LookupElement(szName);

		switch (ePos)
		{
		case ELEM_DOCSET:
			m_bInDocset = true;
			m_bFirstTagAfterDocset = true;
			return;

		case ELEM_SCHEMA:
		{
			if (!m_bInDocset || !m_bFirstTagAfterDocset)
			{
				Error("<sphinx:schema> is allowed immediately after <sphinx:docset> only");
				return;
			}

			if (m_tSchema.m_dFields.GetLength() > 0 || m_tSchema.GetAttrsCount() > 0)
			{
				sphWarn("%s", DecorateMessage("both embedded and configured schemas found; using embedded"));
				m_tSchema.Reset();
				CSphMatch tDocInfo;
				Swap(m_tDocInfo, tDocInfo);
			}

			m_bFirstTagAfterDocset = false;
			m_bInSchema = true;
		}
		return;

		case ELEM_FIELD:
		{
			if (!m_bInDocset || !m_bInSchema)
			{
				Error("<sphinx:field> is allowed inside <sphinx:schema> only");
				return;
			}

			const char** dAttrs = pAttrs;
			CSphColumnInfo Info;
			CSphString sDefault;
			bool bIsAttr = false;
			bool bWordDict = (m_pDict && m_pDict->GetSettings().m_bWordDict);

			while (dAttrs[0] && dAttrs[1] && dAttrs[0][0] && dAttrs[1][0])
			{
				if (!strcmp(*dAttrs, "name"))
				{
					AddFieldToSchema(dAttrs[1], bWordDict, m_tSchema);
					Info.m_sName = dAttrs[1];
				}
				else if (!strcmp(*dAttrs, "attr"))
				{
					bIsAttr = true;
					if (!strcmp(dAttrs[1], "string"))
						Info.m_eAttrType = ESphAttr::SPH_ATTR_STRING;
					else if (!strcmp(dAttrs[1], "json"))
						Info.m_eAttrType = ESphAttr::SPH_ATTR_JSON;

				}
				else if (!strcmp(*dAttrs, "default"))
					sDefault = dAttrs[1];

				dAttrs += 2;
			}

			if (bIsAttr)
			{
				if (CSphSchema::IsReserved(Info.m_sName.cstr()))
				{
					Error("%s is not a valid attribute name", Info.m_sName.cstr());
					return;
				}

				Info.m_iIndex = m_tSchema.GetAttrsCount();
				m_tSchema.AddAttr(Info, true); // all attributes are dynamic at indexing time
				m_dDefaultAttrs.Add(sDefault);
			}
		}
		return;

		case ELEM_ATTR:
		{
			if (!m_bInDocset || !m_bInSchema)
			{
				Error("<sphinx:attr> is allowed inside <sphinx:schema> only");
				return;
			}

			bool bError = false;
			CSphString sDefault;

			CSphColumnInfo Info;
			Info.m_eAttrType = ESphAttr::SPH_ATTR_INTEGER;

			const char** dAttrs = pAttrs;

			while (dAttrs[0] && dAttrs[1] && dAttrs[0][0] && dAttrs[1][0] && !bError)
			{
				if (!strcmp(*dAttrs, "name"))
					Info.m_sName = dAttrs[1];
				else if (!strcmp(*dAttrs, "bits"))
					Info.m_tLocator.m_iBitCount = strtol(dAttrs[1], NULL, 10);
				else if (!strcmp(*dAttrs, "default"))
					sDefault = dAttrs[1];
				else if (!strcmp(*dAttrs, "type"))
				{
					const char* szType = dAttrs[1];
					if (!strcmp(szType, "int"))				Info.m_eAttrType = ESphAttr::SPH_ATTR_INTEGER;
					else if (!strcmp(szType, "timestamp"))		Info.m_eAttrType = ESphAttr::SPH_ATTR_TIMESTAMP;
					else if (!strcmp(szType, "bool"))			Info.m_eAttrType = ESphAttr::SPH_ATTR_BOOL;
					else if (!strcmp(szType, "float"))			Info.m_eAttrType = ESphAttr::SPH_ATTR_FLOAT;
					else if (!strcmp(szType, "bigint"))		Info.m_eAttrType = ESphAttr::SPH_ATTR_BIGINT;
					else if (!strcmp(szType, "string"))		Info.m_eAttrType = ESphAttr::SPH_ATTR_STRING;
					else if (!strcmp(szType, "json"))			Info.m_eAttrType = ESphAttr::SPH_ATTR_JSON;
					else if (!strcmp(szType, "multi"))
					{
						Info.m_eAttrType = ESphAttr::SPH_ATTR_UINT32SET;
						Info.m_eSrc = SPH_ATTRSRC_FIELD;
					}
					else if (!strcmp(szType, "multi_64"))
					{
						Info.m_eAttrType = ESphAttr::SPH_ATTR_INT64SET;
						Info.m_eSrc = SPH_ATTRSRC_FIELD;
					}
					else
					{
						Error("unknown column type '%s'", szType);
						bError = true;
					}
				}

				dAttrs += 2;
			}

			if (!bError)
			{
				if (CSphSchema::IsReserved(Info.m_sName.cstr()))
				{
					Error("%s is not a valid attribute name", Info.m_sName.cstr());
					return;
				}

				Info.m_iIndex = m_tSchema.GetAttrsCount();
				m_tSchema.AddAttr(Info, true); // all attributes are dynamic at indexing time
				m_dDefaultAttrs.Add(sDefault);
			}
		}
		return;

		case ELEM_DOCUMENT:
		{
			if (!m_bInDocset || m_bInSchema)
				return DocumentError("<sphinx:schema>");

			if (m_bInKillList)
				return DocumentError("<sphinx:killlist>");

			if (m_bInDocument)
				return DocumentError("<sphinx:document>");

			if (m_tSchema.m_dFields.GetLength() == 0 && m_tSchema.GetAttrsCount() == 0)
			{
				Error("no schema configured, and no embedded schema found");
				return;
			}

			m_bInDocument = true;

			assert(!m_pCurDocument);
			m_pCurDocument = new Document_t;

			m_pCurDocument->m_uDocID = 0;
			m_pCurDocument->m_dFields.Resize(m_tSchema.m_dFields.GetLength());
			// for safety
			ARRAY_FOREACH(i, m_pCurDocument->m_dFields)
				m_pCurDocument->m_dFields[i].Add('\0');
			m_pCurDocument->m_dAttrs.Resize(m_tSchema.GetAttrsCount());

			if (pAttrs[0] && pAttrs[1] && pAttrs[0][0] && pAttrs[1][0])
				if (!strcmp(pAttrs[0], "id"))
					m_pCurDocument->m_uDocID = sphToDocid(pAttrs[1]);

			if (m_pCurDocument->m_uDocID == 0)
				Error("attribute 'id' required in <sphinx:document>");
		}
		return;

		case ELEM_KLIST:
		{
			if (!m_bInDocset || m_bInDocument || m_bInSchema)
			{
				Error("<sphinx:killlist> is not allowed inside <sphinx:schema> or <sphinx:document>");
				return;
			}

			m_bInKillList = true;
		}
		return;

		case ELEM_NONE: break; // avoid warning
		}

		if (m_bInKillList)
		{
			if (m_bInId)
			{
				m_iElementDepth++;
				return;
			}

			if (strcmp(szName, "id"))
			{
				Error("only 'id' is allowed inside <sphinx:killlist>");
				return;
			}

			m_bInId = true;

		}
		else if (m_bInDocument)
		{
			if (m_iCurField != -1 || m_iCurAttr != -1)
			{
				m_iElementDepth++;
				return;
			}

			for (int i = 0; i < m_tSchema.m_dFields.GetLength() && m_iCurField == -1; i++)
				if (m_tSchema.m_dFields[i].m_sName == szName)
					m_iCurField = i;

			m_iCurAttr = m_tSchema.GetAttrIndex(szName);

			if (m_iCurAttr != -1 || m_iCurField != -1)
				return;

			m_bInIgnoredTag = true;

			bool bInvalidFound = false;
			for (int i = 0; i < m_dInvalid.GetLength() && !bInvalidFound; i++)
				bInvalidFound = m_dInvalid[i] == szName;

			if (!bInvalidFound)
			{
				sphWarn("%s", DecorateMessage("unknown field/attribute '%s'; ignored", szName));
				m_dInvalid.Add(szName);
			}
		}
	}


	void CSphSource_XMLPipe2::EndElement(const char* szName)
	{
		m_bInIgnoredTag = false;

		EXMLElem ePos = LookupElement(szName);

		switch (ePos)
		{
		case ELEM_DOCSET:
			m_bInDocset = false;
			return;

		case ELEM_SCHEMA:
			m_bInSchema = false;
			AddAutoAttrs(m_sError);
			AllocDocinfo();
			return;

		case ELEM_DOCUMENT:
			m_bInDocument = false;
			if (m_pCurDocument)
				m_dParsedDocuments.Add(m_pCurDocument);
			m_pCurDocument = NULL;
			return;

		case ELEM_KLIST:
			m_bInKillList = false;
			return;

		case ELEM_FIELD: // avoid warnings
		case ELEM_ATTR:
		case ELEM_NONE: break;
		}

		if (m_bInKillList)
		{
			if (m_iElementDepth != 0)
			{
				m_iElementDepth--;
				return;
			}

			if (m_bInId)
			{
				m_pFieldBuffer[Min(m_iFieldBufferLen, m_iFieldBufferMax - 1)] = '\0';
				m_dKillList.Add(sphToDocid((const char*)m_pFieldBuffer));
				m_iFieldBufferLen = 0;
				m_bInId = false;
			}

		}
		else if (m_bInDocument && (m_iCurAttr != -1 || m_iCurField != -1))
		{
			if (m_iElementDepth != 0)
			{
				m_iElementDepth--;
				return;
			}

			if (m_iCurField != -1)
			{
				assert(m_pCurDocument);
				CSphVector<BYTE>& dBuf = m_pCurDocument->m_dFields[m_iCurField];

				dBuf.Last() = ' ';
				dBuf.Reserve(dBuf.GetLength() + m_iFieldBufferLen + 6); // 6 is a safety gap
				memcpy(dBuf.Begin() + dBuf.GetLength(), m_pFieldBuffer, m_iFieldBufferLen);
				dBuf.Resize(dBuf.GetLength() + m_iFieldBufferLen);
				dBuf.Add('\0');
			}
			if (m_iCurAttr != -1)
			{
				assert(m_pCurDocument);
				if (!m_pCurDocument->m_dAttrs[m_iCurAttr].IsEmpty())
					sphWarn("duplicate attribute node <%s> - using first value", m_tSchema.GetAttr(m_iCurAttr).m_sName.cstr());
				else
					m_pCurDocument->m_dAttrs[m_iCurAttr].SetBinary((char*)m_pFieldBuffer, m_iFieldBufferLen);
			}

			m_iFieldBufferLen = 0;

			m_iCurAttr = -1;
			m_iCurField = -1;
		}
	}


	void CSphSource_XMLPipe2::UnexpectedCharaters(const char* pCharacters, int iLen, const char* szComment)
	{
		const int MAX_WARNING_LENGTH = 64;

		bool bSpaces = true;
		for (int i = 0; i < iLen && bSpaces; i++)
			if (!sphIsSpace(pCharacters[i]))
				bSpaces = false;

		if (!bSpaces)
		{
			CSphString sWarning;
			sWarning.SetBinary(pCharacters, Min(iLen, MAX_WARNING_LENGTH));
			sphWarn("source '%s': unexpected string '%s' (line=%d, pos=%d) %s",
				m_tSchema.m_sName.cstr(), sWarning.cstr(),
				(int)sph_XML_GetCurrentLineNumber(m_pParser), (int)sph_XML_GetCurrentColumnNumber(m_pParser), szComment);
		}
	}


	void CSphSource_XMLPipe2::Characters(const char* pCharacters, int iLen)
	{
		if (m_bInIgnoredTag)
			return;

		if (!m_bInDocset)
		{
			UnexpectedCharaters(pCharacters, iLen, "outside of <sphinx:docset>");
			return;
		}

		if (!m_bInSchema && !m_bInDocument && !m_bInKillList)
		{
			UnexpectedCharaters(pCharacters, iLen, "outside of <sphinx:schema> and <sphinx:document>");
			return;
		}

		if (m_iCurAttr == -1 && m_iCurField == -1 && !m_bInKillList)
		{
			UnexpectedCharaters(pCharacters, iLen, m_bInDocument ? "inside <sphinx:document>" : (m_bInSchema ? "inside <sphinx:schema>" : ""));
			return;
		}

		if (iLen + m_iFieldBufferLen < m_iFieldBufferMax)
		{
			memcpy(m_pFieldBuffer + m_iFieldBufferLen, pCharacters, iLen);
			m_iFieldBufferLen += iLen;

		}
		else
		{
			const CSphString& sName = (m_iCurField != -1) ? m_tSchema.m_dFields[m_iCurField].m_sName : m_tSchema.GetAttr(m_iCurAttr).m_sName;

			bool bWarned = false;
			for (int i = 0; i < m_dWarned.GetLength() && !bWarned; i++)
				bWarned = m_dWarned[i] == sName;

			if (!bWarned)
			{
				sphWarn("source '%s': field/attribute '%s' length exceeds max length (line=%d, pos=%d, docid=" DOCID_FMT ")",
					m_tSchema.m_sName.cstr(), sName.cstr(),
					(int)sph_XML_GetCurrentLineNumber(m_pParser), (int)sph_XML_GetCurrentColumnNumber(m_pParser),
					m_pCurDocument->m_uDocID);

				m_dWarned.Add(sName);
			}
		}
	}

	CSphSource* sphCreateSourceXmlpipe2(const CSphConfigSection* pSource, FILE* pPipe, const char* szSourceName, int iMaxFieldLen, bool bProxy, CSphString& sError)
	{
		bool bUTF8 = pSource->GetInt("xmlpipe_fixup_utf8", 0) != 0;

		CSphSource_XMLPipe2* pXMLPipe = CreateSourceWithProxy<CSphSource_XMLPipe2>(szSourceName, bProxy);
		if (!pXMLPipe->Setup(iMaxFieldLen, bUTF8, pPipe, *pSource, sError))
			SafeDelete(pXMLPipe);

		return pXMLPipe;
	}

}

#endif