#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/dict/dict_entry.h"
#include "neo/dict/dict_crc.h"

namespace NEO {

	class CRtDictKeywords : public ISphRtDictWraper
	{
	private:
		CSphDict* m_pBase;
		CSphOrderedHash<int, CSphString, CSphStrHashFunc, 8192>	m_hKeywords;
		CSphVector<BYTE>		m_dPackedKeywords;

		CSphString				m_sWarning;
		int						m_iKeywordsOverrun;
		CSphString				m_sWord; // For allocation reuse.

	public:
		explicit CRtDictKeywords(CSphDict* pBase)
			: m_pBase(pBase)
			, m_iKeywordsOverrun(0)
		{
			m_dPackedKeywords.Add(0); // avoid zero offset at all costs
		}
		virtual ~CRtDictKeywords() {}

		virtual SphWordID_t GetWordID(BYTE* pWord)
		{
			SphWordID_t uCRC = m_pBase->GetWordID(pWord);
			if (uCRC)
				return AddKeyword(pWord);
			else
				return 0;
		}

		virtual SphWordID_t GetWordIDWithMarkers(BYTE* pWord)
		{
			SphWordID_t uCRC = m_pBase->GetWordIDWithMarkers(pWord);
			if (uCRC)
				return AddKeyword(pWord);
			else
				return 0;
		}

		virtual SphWordID_t GetWordIDNonStemmed(BYTE* pWord)
		{
			SphWordID_t uCRC = m_pBase->GetWordIDNonStemmed(pWord);
			if (uCRC)
				return AddKeyword(pWord);
			else
				return 0;
		}

		virtual SphWordID_t GetWordID(const BYTE* pWord, int iLen, bool bFilterStops)
		{
			SphWordID_t uCRC = m_pBase->GetWordID(pWord, iLen, bFilterStops);
			if (uCRC)
				return AddKeyword(pWord);
			else
				return 0;
		}

		virtual const BYTE* GetPackedKeywords() { return m_dPackedKeywords.Begin(); }
		virtual int GetPackedLen() { return m_dPackedKeywords.GetLength(); }
		virtual void ResetKeywords()
		{
			m_dPackedKeywords.Resize(0);
			m_dPackedKeywords.Add(0); // avoid zero offset at all costs
			m_hKeywords.Reset();
		}

		SphWordID_t AddKeyword(const BYTE* pWord)
		{
			int iLen = strlen((const char*)pWord);
			// stemmer might squeeze out the word
			if (!iLen)
				return 0;

			// fix of very long word (zones)
			if (iLen >= (SPH_MAX_WORD_LEN * 3))
			{
				int iClippedLen = SPH_MAX_WORD_LEN * 3;
				m_sWord.SetBinary((const char*)pWord, iClippedLen);
				if (m_iKeywordsOverrun)
				{
					m_sWarning.SetSprintf("word overrun buffer, clipped!!! clipped='%s', length=%d(%d)", m_sWord.cstr(), iClippedLen, iLen);
				}
				else
				{
					m_sWarning.SetSprintf(", clipped='%s', length=%d(%d)", m_sWord.cstr(), iClippedLen, iLen);
				}
				iLen = iClippedLen;
				m_iKeywordsOverrun++;
			}
			else
			{
				m_sWord.SetBinary((const char*)pWord, iLen);
			}

			int* pOff = m_hKeywords(m_sWord);
			if (pOff)
			{
				return *pOff;
			}

			int iOff = m_dPackedKeywords.GetLength();
			m_dPackedKeywords.Resize(iOff + iLen + 1);
			m_dPackedKeywords[iOff] = (BYTE)(iLen & 0xFF);
			memcpy(m_dPackedKeywords.Begin() + iOff + 1, pWord, iLen);

			m_hKeywords.Add(iOff, m_sWord);

			return iOff;
		}

		virtual void LoadStopwords(const char* sFiles, const ISphTokenizer* pTokenizer) { m_pBase->LoadStopwords(sFiles, pTokenizer); }
		virtual void LoadStopwords(const CSphVector<SphWordID_t>& dStopwords) { m_pBase->LoadStopwords(dStopwords); }
		virtual void WriteStopwords(CSphWriter& tWriter) { m_pBase->WriteStopwords(tWriter); }
		virtual bool LoadWordforms(const CSphVector<CSphString>& dFiles, const CSphEmbeddedFiles* pEmbedded, const ISphTokenizer* pTokenizer, const char* sIndex) { return m_pBase->LoadWordforms(dFiles, pEmbedded, pTokenizer, sIndex); }
		virtual void WriteWordforms(CSphWriter& tWriter) { m_pBase->WriteWordforms(tWriter); }
		virtual int SetMorphology(const char* szMorph, CSphString& sMessage) { return m_pBase->SetMorphology(szMorph, sMessage); }
		virtual void Setup(const CSphDictSettings& tSettings) { m_pBase->Setup(tSettings); }
		virtual const CSphDictSettings& GetSettings() const { return m_pBase->GetSettings(); }
		virtual const CSphVector <CSphSavedFile>& GetStopwordsFileInfos() { return m_pBase->GetStopwordsFileInfos(); }
		virtual const CSphVector <CSphSavedFile>& GetWordformsFileInfos() { return m_pBase->GetWordformsFileInfos(); }
		virtual const CSphMultiformContainer* GetMultiWordforms() const { return m_pBase->GetMultiWordforms(); }
		virtual bool IsStopWord(const BYTE* pWord) const { return m_pBase->IsStopWord(pWord); }
		virtual const char* GetLastWarning() const { return m_iKeywordsOverrun ? m_sWarning.cstr() : NULL; }
		virtual void ResetWarning() { m_iKeywordsOverrun = 0; }
		virtual uint64_t GetSettingsFNV() const { return m_pBase->GetSettingsFNV(); }
	};


	ISphRtDictWraper* sphCreateRtKeywordsDictionaryWrapper(CSphDict* pBase);




}