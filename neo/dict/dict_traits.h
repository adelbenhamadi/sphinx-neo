#pragma once
#include "dict.h"

namespace NEO {

	/// dict traits
	class CSphDictTraits : public CSphDict
	{
	public:
		explicit			CSphDictTraits(CSphDict* pDict) : m_pDict(pDict) { assert(m_pDict); }

		virtual void		LoadStopwords(const char* sFiles, const ISphTokenizer* pTokenizer) { m_pDict->LoadStopwords(sFiles, pTokenizer); }
		virtual void		LoadStopwords(const CSphVector<SphWordID_t>& dStopwords) { m_pDict->LoadStopwords(dStopwords); }
		virtual void		WriteStopwords(CSphWriter& tWriter) { m_pDict->WriteStopwords(tWriter); }
		virtual bool		LoadWordforms(const CSphVector<CSphString>& dFiles, const CSphEmbeddedFiles* pEmbedded, const ISphTokenizer* pTokenizer, const char* sIndex) { return m_pDict->LoadWordforms(dFiles, pEmbedded, pTokenizer, sIndex); }
		virtual void		WriteWordforms(CSphWriter& tWriter) { m_pDict->WriteWordforms(tWriter); }
		virtual int			SetMorphology(const char* szMorph, CSphString& sMessage) { return m_pDict->SetMorphology(szMorph, sMessage); }

		virtual SphWordID_t	GetWordID(const BYTE* pWord, int iLen, bool bFilterStops) { return m_pDict->GetWordID(pWord, iLen, bFilterStops); }
		virtual SphWordID_t GetWordID(BYTE* pWord);
		virtual SphWordID_t	GetWordIDNonStemmed(BYTE* pWord) { return m_pDict->GetWordIDNonStemmed(pWord); }

		virtual void		Setup(const CSphDictSettings&) {}
		virtual const CSphDictSettings& GetSettings() const { return m_pDict->GetSettings(); }
		virtual const CSphVector <CSphSavedFile>& GetStopwordsFileInfos() { return m_pDict->GetStopwordsFileInfos(); }
		virtual const CSphVector <CSphSavedFile>& GetWordformsFileInfos() { return m_pDict->GetWordformsFileInfos(); }
		virtual const CSphMultiformContainer* GetMultiWordforms() const { return m_pDict->GetMultiWordforms(); }
		virtual const CSphWordforms* GetWordforms() { return m_pDict->GetWordforms(); }

		virtual bool		IsStopWord(const BYTE* pWord) const { return m_pDict->IsStopWord(pWord); }
		virtual uint64_t	GetSettingsFNV() const { return m_pDict->GetSettingsFNV(); }

	protected:
		CSphDict* m_pDict;
	};


}