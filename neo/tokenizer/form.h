#pragma once
#include "neo/int/types.h"
#include "neo/utility/hash.h"

namespace NEO {

	struct CSphNormalForm
	{
		CSphString				m_sForm;
		int						m_iLengthCP;
	};

	struct CSphMultiform
	{
		int								m_iFileId;
		CSphTightVector<CSphNormalForm>	m_dNormalForm;
		CSphTightVector<CSphString>		m_dTokens;
	};


	struct CSphMultiforms
	{
		int							m_iMinTokens;
		int							m_iMaxTokens;
		CSphVector<CSphMultiform*>	m_pForms;		// OPTIMIZE? blobify?
	};


	struct CSphMultiformContainer
	{
		CSphMultiformContainer() : m_iMaxTokens(0) {}

		int						m_iMaxTokens;
		typedef CSphOrderedHash < CSphMultiforms*, CSphString, CSphStrHashFunc, 131072 > CSphMultiformHash;
		CSphMultiformHash	m_Hash;
	};

}
