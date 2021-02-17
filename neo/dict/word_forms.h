#pragma once
#include "neo/int/types.h"
#include "neo/utility/hash.h"


namespace NEO {

	//fwd dec
	struct CSphStoredNF;
	struct CSphSavedFile;
	struct CSphMultiformContainer;
	struct CSphStrHashFunc;

	/// wordforms container
	struct CSphWordforms
	{
		int							m_iRefCount;
		CSphVector<CSphSavedFile>	m_dFiles;
		uint64_t					m_uTokenizerFNV;
		CSphString					m_sIndexName;
		bool						m_bHavePostMorphNF;
		CSphVector <CSphStoredNF>	m_dNormalForms;
		CSphMultiformContainer* m_pMultiWordforms;
		CSphOrderedHash < int, CSphString, CSphStrHashFunc, 1048576 >	m_dHash;

		CSphWordforms();
		~CSphWordforms();

		bool						IsEqual(const CSphVector<CSphSavedFile>& dFiles);
		bool						ToNormalForm(BYTE* pWord, bool bBefore, bool bOnlyCheck) const;
	};


}