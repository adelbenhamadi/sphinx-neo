#pragma once
#include "neo/int/types.h"

namespace NEO {

	struct CSphTokenizerSettings
	{
		int					m_iType;
		CSphString			m_sCaseFolding;
		int					m_iMinWordLen;
		CSphString			m_sSynonymsFile;
		CSphString			m_sBoundary;
		CSphString			m_sIgnoreChars;
		int					m_iNgramLen;
		CSphString			m_sNgramChars;
		CSphString			m_sBlendChars;
		CSphString			m_sBlendMode;
		CSphString			m_sIndexingPlugin;	///< this tokenizer wants an external plugin to process its raw output

		CSphTokenizerSettings();
	};


}