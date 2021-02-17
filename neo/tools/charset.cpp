#include "neo/tools/charset.h"

namespace NEO {
	const char* CSphCharsetDefinitionParser::GetLastError()
	{
		return m_bError ? m_sError : NULL;
	}


	bool CSphCharsetDefinitionParser::IsEof()
	{
		return (*m_pCurrent) == 0;
	}


	bool CSphCharsetDefinitionParser::CheckEof()
	{
		if (IsEof())
		{
			Error("unexpected end of line");
			return true;
		}
		else
		{
			return false;
		}
	}


	bool CSphCharsetDefinitionParser::Error(const char* sMessage)
	{
		char sErrorBuffer[32];
		strncpy(sErrorBuffer, m_pCurrent, sizeof(sErrorBuffer));
		sErrorBuffer[sizeof(sErrorBuffer) - 1] = '\0';

		snprintf(m_sError, sizeof(m_sError), "%s near '%s'",
			sMessage, sErrorBuffer);
		m_sError[sizeof(m_sError) - 1] = '\0';

		m_bError = true;
		return false;
	}


	int CSphCharsetDefinitionParser::HexDigit(int c)
	{
		if (c >= '0' && c <= '9') return c - '0';
		if (c >= 'a' && c <= 'f') return c - 'a' + 10;
		if (c >= 'A' && c <= 'F') return c - 'A' + 10;
		return 0;
	}


	void CSphCharsetDefinitionParser::SkipSpaces()
	{
		while ((*m_pCurrent) && isspace((BYTE)*m_pCurrent))
			m_pCurrent++;
	}


	int CSphCharsetDefinitionParser::ParseCharsetCode()
	{
		const char* p = m_pCurrent;
		int iCode = 0;

		if (p[0] == 'U' && p[1] == '+')
		{
			p += 2;
			while (isxdigit(*p))
			{
				iCode = iCode * 16 + HexDigit(*p++);
			}
			while (isspace(*p))
				p++;

		}
		else
		{
			if ((*(BYTE*)p) < 32 || (*(BYTE*)p) > 127)
			{
				Error("non-ASCII characters not allowed, use 'U+00AB' syntax");
				return -1;
			}

			iCode = *p++;
			while (isspace(*p))
				p++;
		}

		m_pCurrent = p;
		return iCode;
	}

	bool CSphCharsetDefinitionParser::AddRange(const CSphRemapRange& tRange, CSphVector<CSphRemapRange>& dRanges)
	{
		if (tRange.m_iRemapStart >= 0x20)
		{
			dRanges.Add(tRange);
			return true;
		}

		CSphString sError;
		sError.SetSprintf("dest range (U+%x) below U+20, not allowed", tRange.m_iRemapStart);
		Error(sError.cstr());
		return false;
	}


	struct CharsetAlias_t
	{
		CSphString					m_sName;
		int							m_iNameLen;
		CSphVector<CSphRemapRange>	m_dRemaps;
	};

	static CSphVector<CharsetAlias_t> g_dCharsetAliases;
	static const char* g_sDefaultCharsetAliases[] = { "english", "A..Z->a..z, a..z", "russian", "U+410..U+42F->U+430..U+44F, U+430..U+44F, U+401->U+451, U+451", NULL };

	bool sphInitCharsetAliasTable(CSphString& sError) // FIXME!!! move alias generation to config common section
	{
		g_dCharsetAliases.Reset();
		CSphCharsetDefinitionParser tParser;
		CSphVector<CharsetAlias_t> dAliases;

		for (int i = 0; g_sDefaultCharsetAliases[i]; i += 2)
		{
			CharsetAlias_t& tCur = dAliases.Add();
			tCur.m_sName = g_sDefaultCharsetAliases[i];
			tCur.m_iNameLen = tCur.m_sName.Length();

			if (!tParser.Parse(g_sDefaultCharsetAliases[i + 1], tCur.m_dRemaps))
			{
				sError = tParser.GetLastError();
				return false;
			}
		}

		g_dCharsetAliases.SwapData(dAliases);
		return true;
	}


	bool CSphCharsetDefinitionParser::Parse(const char* sConfig, CSphVector<CSphRemapRange>& dRanges)
	{
		m_pCurrent = sConfig;
		dRanges.Reset();

		// do parse
		while (*m_pCurrent)
		{
			SkipSpaces();
			if (IsEof())
				break;

			// check for stray comma
			if (*m_pCurrent == ',')
				return Error("stray ',' not allowed, use 'U+002C' instead");

			// alias
			bool bGotAlias = false;
			ARRAY_FOREACH_COND(i, g_dCharsetAliases, !bGotAlias)
			{
				const CharsetAlias_t& tCur = g_dCharsetAliases[i];
				bGotAlias = (strncmp(tCur.m_sName.cstr(), m_pCurrent, tCur.m_iNameLen) == 0 && (!m_pCurrent[tCur.m_iNameLen] || m_pCurrent[tCur.m_iNameLen] == ','));
				if (!bGotAlias)
					continue;

				// skip to next definition
				m_pCurrent += tCur.m_iNameLen;
				if (*m_pCurrent && *m_pCurrent == ',')
					m_pCurrent++;

				ARRAY_FOREACH(iDef, tCur.m_dRemaps)
				{
					if (!AddRange(tCur.m_dRemaps[iDef], dRanges))
						return false;
				}
			}
			if (bGotAlias)
				continue;

			// parse char code
			const char* pStart = m_pCurrent;
			int iStart = ParseCharsetCode();
			if (iStart < 0)
				return false;

			// stray char?
			if (!*m_pCurrent || *m_pCurrent == ',')
			{
				// stray char
				if (!AddRange(CSphRemapRange(iStart, iStart, iStart), dRanges))
					return false;

				if (IsEof())
					break;
				m_pCurrent++;
				continue;
			}

			// stray remap?
			if (m_pCurrent[0] == '-' && m_pCurrent[1] == '>')
			{
				// parse and add
				m_pCurrent += 2;
				int iDest = ParseCharsetCode();
				if (iDest < 0)
					return false;
				if (!AddRange(CSphRemapRange(iStart, iStart, iDest), dRanges))
					return false;

				// it's either end of line now, or must be followed by comma
				if (*m_pCurrent)
					if (*m_pCurrent++ != ',')
						return Error("syntax error");
				continue;
			}

			// range start?
			if (!(m_pCurrent[0] == '.' && m_pCurrent[1] == '.'))
				return Error("syntax error");
			m_pCurrent += 2;

			SkipSpaces();
			if (CheckEof())
				return false;

			// parse range end char code
			int iEnd = ParseCharsetCode();
			if (iEnd < 0)
				return false;
			if (iStart > iEnd)
			{
				m_pCurrent = pStart;
				return Error("range end less than range start");
			}

			// stray range?
			if (!*m_pCurrent || *m_pCurrent == ',')
			{
				if (!AddRange(CSphRemapRange(iStart, iEnd, iStart), dRanges))
					return false;

				if (IsEof())
					break;
				m_pCurrent++;
				continue;
			}

			// "checkerboard" range?
			if (m_pCurrent[0] == '/' && m_pCurrent[1] == '2')
			{
				for (int i = iStart; i < iEnd; i += 2)
				{
					if (!AddRange(CSphRemapRange(i, i, i + 1), dRanges))
						return false;
					if (!AddRange(CSphRemapRange(i + 1, i + 1, i + 1), dRanges))
						return false;
				}

				// skip "/2", expect ","
				m_pCurrent += 2;
				SkipSpaces();
				if (*m_pCurrent)
					if (*m_pCurrent++ != ',')
						return Error("expected end of line or ','");
				continue;
			}

			// remapped range?
			if (!(m_pCurrent[0] == '-' && m_pCurrent[1] == '>'))
				return Error("expected end of line, ',' or '-><char>'");
			m_pCurrent += 2;

			SkipSpaces();
			if (CheckEof())
				return false;

			// parse dest start
			const char* pRemapStart = m_pCurrent;
			int iRemapStart = ParseCharsetCode();
			if (iRemapStart < 0)
				return false;

			// expect '..'
			if (CheckEof())
				return false;
			if (!(m_pCurrent[0] == '.' && m_pCurrent[1] == '.'))
				return Error("expected '..'");
			m_pCurrent += 2;

			// parse dest end
			int iRemapEnd = ParseCharsetCode();
			if (iRemapEnd < 0)
				return false;

			// check dest range
			if (iRemapStart > iRemapEnd)
			{
				m_pCurrent = pRemapStart;
				return Error("dest range end less than dest range start");
			}

			// check for length mismatch
			if ((iRemapEnd - iRemapStart) != (iEnd - iStart))
			{
				m_pCurrent = pStart;
				return Error("dest range length must match src range length");
			}

			// remapped ok
			if (!AddRange(CSphRemapRange(iStart, iEnd, iRemapStart), dRanges))
				return false;

			if (IsEof())
				break;
			if (*m_pCurrent != ',')
				return Error("expected ','");
			m_pCurrent++;
		}

		dRanges.Sort();
		for (int i = 0; i < dRanges.GetLength() - 1; i++)
		{
			if (dRanges[i].m_iEnd >= dRanges[i + 1].m_iStart)
			{
				// FIXME! add an ambiguity check
				dRanges[i].m_iEnd = Max(dRanges[i].m_iEnd, dRanges[i + 1].m_iEnd);
				dRanges.Remove(i + 1);
				i--;
			}
		}

		return true;
	}


}