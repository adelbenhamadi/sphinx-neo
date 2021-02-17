#pragma once
#include "neo/int/types.h"
#include "neo/query/query_item.h"
#include "neo/source/enums.h"
#include "neo/utility/inline_misc.h"


namespace NEO {

	class CSphQuery;


	struct SelectBounds_t
	{
		int		m_iStart;
		int		m_iEnd;
	};
#define YYSTYPE NEO::SelectBounds_t
	class SelectParser_t;

#ifdef CMAKE_GENERATED_GRAMMAR
#include "neo/sphinx/bissphinxselect.h"
#else
#include "neo/sphinx/yysphinxselect.h"
#endif


	class SelectParser_t
	{
	public:
		int				GetToken(YYSTYPE* lvalp);
		void			AddItem(YYSTYPE* pExpr, ESphAggrFunc eAggrFunc = SPH_AGGR_NONE, YYSTYPE* pStart = NULL, YYSTYPE* pEnd = NULL);
		void			AddItem(const char* pToken, YYSTYPE* pStart = NULL, YYSTYPE* pEnd = NULL);
		void			AliasLastItem(YYSTYPE* pAlias);
		void			AddOption(YYSTYPE* pOpt, YYSTYPE* pVal);

	private:
		void			AutoAlias(CSphQueryItem& tItem, YYSTYPE* pStart, YYSTYPE* pEnd);
		bool			IsTokenEqual(YYSTYPE* pTok, const char* sRef);

	public:
		CSphString		m_sParserError;
		const char* m_pLastTokenStart;

		const char* m_pStart;
		const char* m_pCur;

		CSphQuery* m_pQuery;
	};

	int yylex(YYSTYPE* lvalp, SelectParser_t* pParser);

	void yyerror(SelectParser_t* pParser, const char* sMessage);



}