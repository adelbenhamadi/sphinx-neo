#pragma once
#include "neo/int/types.h"
#include "neo/tools/lower_caser.h"

namespace NEO {
	/// parser to build lowercaser from textual config
	class CSphCharsetDefinitionParser
	{
	public:
		CSphCharsetDefinitionParser() : m_bError(false) {}
		bool				Parse(const char* sConfig, CSphVector<CSphRemapRange>& dRanges);
		const char* GetLastError();

	protected:
		bool				m_bError;
		char				m_sError[1024];
		const char* m_pCurrent;

		bool				Error(const char* sMessage);
		void				SkipSpaces();
		bool				IsEof();
		bool				CheckEof();
		int					HexDigit(int c);
		int					ParseCharsetCode();
		bool				AddRange(const CSphRemapRange& tRange, CSphVector<CSphRemapRange>& dRanges);
	};



	bool			sphInitCharsetAliasTable(CSphString& sError);


}