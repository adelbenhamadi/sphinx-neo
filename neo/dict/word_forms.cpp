#include "neo/dict/word_forms.h"
#include "neo/int/types.h"
#include "neo/tokenizer/form.h"
#include "neo/io/file.h"
#include "neo/dict/dict.h"

namespace NEO {

	CSphWordforms::CSphWordforms()
		: m_iRefCount(0)
		, m_uTokenizerFNV(0)
		, m_bHavePostMorphNF(false)
		, m_pMultiWordforms(NULL)
	{
	}


	CSphWordforms::~CSphWordforms()
	{
		if (m_pMultiWordforms)
		{
			m_pMultiWordforms->m_Hash.IterateStart();
			while (m_pMultiWordforms->m_Hash.IterateNext())
			{
				CSphMultiforms* pWordforms = m_pMultiWordforms->m_Hash.IterateGet();
				ARRAY_FOREACH(i, pWordforms->m_pForms)
					SafeDelete(pWordforms->m_pForms[i]);

				SafeDelete(pWordforms);
			}

			SafeDelete(m_pMultiWordforms);
		}
	}


	bool CSphWordforms::IsEqual(const CSphVector<CSphSavedFile>& dFiles)
	{
		if (m_dFiles.GetLength() != dFiles.GetLength())
			return false;

		ARRAY_FOREACH(i, m_dFiles)
		{
			const CSphSavedFile& tF1 = m_dFiles[i];
			const CSphSavedFile& tF2 = dFiles[i];
			if (tF1.m_sFilename != tF2.m_sFilename || tF1.m_uCRC32 != tF2.m_uCRC32 || tF1.m_uSize != tF2.m_uSize ||
				tF1.m_uCTime != tF2.m_uCTime || tF1.m_uMTime != tF2.m_uMTime)
				return false;
		}

		return true;
	}


	bool CSphWordforms::ToNormalForm(BYTE* pWord, bool bBefore, bool bOnlyCheck) const
	{
		int* pIndex = m_dHash((char*)pWord);
		if (!pIndex)
			return false;

		if (*pIndex < 0 || *pIndex >= m_dNormalForms.GetLength())
			return false;

		if (bBefore == m_dNormalForms[*pIndex].m_bAfterMorphology)
			return false;

		if (m_dNormalForms[*pIndex].m_sWord.IsEmpty())
			return false;

		if (bOnlyCheck)
			return true;

		strcpy((char*)pWord, m_dNormalForms[*pIndex].m_sWord.cstr()); // NOLINT
		return true;
	}


}