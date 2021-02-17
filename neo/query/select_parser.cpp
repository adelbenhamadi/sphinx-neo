#include "neo/query/select_parser.h"
#include "neo/source/column_info.h"
#include "neo/query/query.h"
#include "neo/query/query_item.h"
#include "neo/utility/inline_misc.h"

#ifdef CMAKE_GENERATED_GRAMMAR
#include "neo/sphinx/bissphinxselect.c"
#else

#include "neo/sphinx/yysphinxselect.c"

#endif

namespace NEO {

	int SelectParser_t::GetToken(YYSTYPE* lvalp)
	{
		// skip whitespace, check eof
		while (isspace(*m_pCur))
			m_pCur++;
		if (!*m_pCur)
			return 0;

		// begin working that token
		m_pLastTokenStart = m_pCur;
		lvalp->m_iStart = m_pCur - m_pStart;

		// check for constant
		if (isdigit(*m_pCur))
		{
			char* pEnd = NULL;
			double fDummy; // to avoid gcc unused result warning
			fDummy = strtod(m_pCur, &pEnd);
			fDummy *= 2; // to avoid gcc unused variable warning

			m_pCur = pEnd;
			lvalp->m_iEnd = m_pCur - m_pStart;
			return SEL_TOKEN;
		}

		// check for token
		if (sphIsAttr(m_pCur[0]) || (m_pCur[0] == '@' && sphIsAttr(m_pCur[1]) && !isdigit(m_pCur[1])))
		{
			m_pCur++;
			while (sphIsAttr(*m_pCur)) m_pCur++;
			lvalp->m_iEnd = m_pCur - m_pStart;

#define LOC_CHECK(_str,_len,_ret) \
			if ( lvalp->m_iEnd==_len+lvalp->m_iStart && strncasecmp ( m_pStart+lvalp->m_iStart, _str, _len )==0 ) return _ret;

			LOC_CHECK("ID", 2, SEL_ID);
			LOC_CHECK("AS", 2, SEL_AS);
			LOC_CHECK("OR", 2, TOK_OR);
			LOC_CHECK("AND", 3, TOK_AND);
			LOC_CHECK("NOT", 3, TOK_NOT);
			LOC_CHECK("DIV", 3, TOK_DIV);
			LOC_CHECK("MOD", 3, TOK_MOD);
			LOC_CHECK("AVG", 3, SEL_AVG);
			LOC_CHECK("MIN", 3, SEL_MIN);
			LOC_CHECK("MAX", 3, SEL_MAX);
			LOC_CHECK("SUM", 3, SEL_SUM);
			LOC_CHECK("GROUP_CONCAT", 12, SEL_GROUP_CONCAT);
			LOC_CHECK("GROUPBY", 7, SEL_GROUPBY);
			LOC_CHECK("COUNT", 5, SEL_COUNT);
			LOC_CHECK("DISTINCT", 8, SEL_DISTINCT);
			LOC_CHECK("WEIGHT", 6, SEL_WEIGHT);
			LOC_CHECK("OPTION", 6, SEL_OPTION);
			LOC_CHECK("IS", 2, TOK_IS);
			LOC_CHECK("NULL", 4, TOK_NULL);
			LOC_CHECK("FOR", 3, TOK_FOR);
			LOC_CHECK("IN", 2, TOK_FUNC_IN);
			LOC_CHECK("RAND", 4, TOK_FUNC_RAND);

#undef LOC_CHECK

			return SEL_TOKEN;
		}

		// check for equality checks
		lvalp->m_iEnd = 1 + lvalp->m_iStart;
		switch (*m_pCur)
		{
		case '<':
			m_pCur++;
			if (*m_pCur == '>') { m_pCur++; lvalp->m_iEnd++; return TOK_NE; }
			if (*m_pCur == '=') { m_pCur++; lvalp->m_iEnd++; return TOK_LTE; }
			return '<';

		case '>':
			m_pCur++;
			if (*m_pCur == '=') { m_pCur++; lvalp->m_iEnd++; return TOK_GTE; }
			return '>';

		case '=':
			m_pCur++;
			if (*m_pCur == '=') { m_pCur++; lvalp->m_iEnd++; }
			return TOK_EQ;

		case '\'':
		{
			const char cEnd = *m_pCur;
			for (const char* s = m_pCur + 1; *s; s++)
			{
				if (*s == cEnd && s - 1 >= m_pCur && *(s - 1) != '\\')
				{
					m_pCur = s + 1;
					return TOK_CONST_STRING;
				}
			}
			return -1;
		}
		}

		// check for comment begin/end
		if (m_pCur[0] == '/' && m_pCur[1] == '*')
		{
			m_pCur += 2;
			lvalp->m_iEnd += 1;
			return SEL_COMMENT_OPEN;
		}
		if (m_pCur[0] == '*' && m_pCur[1] == '/')
		{
			m_pCur += 2;
			lvalp->m_iEnd += 1;
			return SEL_COMMENT_CLOSE;
		}

		// return char as a token
		return *m_pCur++;
	}

	void SelectParser_t::AutoAlias(CSphQueryItem& tItem, YYSTYPE* pStart, YYSTYPE* pEnd)
	{
		if (pStart && pEnd)
		{
			tItem.m_sAlias.SetBinary(m_pStart + pStart->m_iStart, pEnd->m_iEnd - pStart->m_iStart);
			sphColumnToLowercase(const_cast<char*>(tItem.m_sAlias.cstr())); // as in SqlParser_c
		}
		else
			tItem.m_sAlias = tItem.m_sExpr;
	}

	void SelectParser_t::AddItem(YYSTYPE* pExpr, ESphAggrFunc eAggrFunc, YYSTYPE* pStart, YYSTYPE* pEnd)
	{
		CSphQueryItem& tItem = m_pQuery->m_dItems.Add();
		tItem.m_sExpr.SetBinary(m_pStart + pExpr->m_iStart, pExpr->m_iEnd - pExpr->m_iStart);
		sphColumnToLowercase(const_cast<char*>(tItem.m_sExpr.cstr()));
		tItem.m_eAggrFunc = eAggrFunc;
		AutoAlias(tItem, pStart, pEnd);
	}

	void SelectParser_t::AddItem(const char* pToken, YYSTYPE* pStart, YYSTYPE* pEnd)
	{
		CSphQueryItem& tItem = m_pQuery->m_dItems.Add();
		tItem.m_sExpr = pToken;
		tItem.m_eAggrFunc = SPH_AGGR_NONE;
		sphColumnToLowercase(const_cast<char*>(tItem.m_sExpr.cstr()));
		AutoAlias(tItem, pStart, pEnd);
	}

	void SelectParser_t::AliasLastItem(YYSTYPE* pAlias)
	{
		if (pAlias)
		{
			CSphQueryItem& tItem = m_pQuery->m_dItems.Last();
			tItem.m_sAlias.SetBinary(m_pStart + pAlias->m_iStart, pAlias->m_iEnd - pAlias->m_iStart);
			tItem.m_sAlias.ToLower();
		}
	}

	bool SelectParser_t::IsTokenEqual(YYSTYPE* pTok, const char* sRef)
	{
		int iLen = strlen(sRef);
		if (iLen != (pTok->m_iEnd - pTok->m_iStart))
			return false;
		return strncasecmp(m_pStart + pTok->m_iStart, sRef, iLen) == 0;
	}

	void SelectParser_t::AddOption(YYSTYPE* pOpt, YYSTYPE* pVal)
	{
		if (IsTokenEqual(pOpt, "reverse_scan"))
		{
			if (IsTokenEqual(pVal, "1"))
				m_pQuery->m_bReverseScan = true;
		}
		else if (IsTokenEqual(pOpt, "sort_method"))
		{
			if (IsTokenEqual(pVal, "kbuffer"))
				m_pQuery->m_bSortKbuffer = true;
		}
		else if (IsTokenEqual(pOpt, "max_predicted_time"))
		{
			char szNumber[256];
			int iLen = pVal->m_iEnd - pVal->m_iStart;
			assert(iLen < (int)sizeof(szNumber));
			strncpy(szNumber, m_pStart + pVal->m_iStart, iLen);
			int64_t iMaxPredicted = strtoull(szNumber, NULL, 10);
			m_pQuery->m_iMaxPredictedMsec = int(iMaxPredicted > INT_MAX ? INT_MAX : iMaxPredicted);
		}
	}


	int yylex(YYSTYPE* lvalp, SelectParser_t* pParser)
	{
		return pParser->GetToken(lvalp);
	}

	void yyerror(SelectParser_t* pParser, const char* sMessage)
	{
		pParser->m_sParserError.SetSprintf("%s near '%s'", sMessage, pParser->m_pLastTokenStart);
	}



}