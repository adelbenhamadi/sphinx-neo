#pragma once
#include "neo/index/enums.h"
#include "neo/core/globals.h"

namespace NEO {

	struct ISphSubstringPayload;

	struct SphExpanded_t
	{
		int m_iNameOff;
		int m_iDocs;
		int m_iHits;
	};

	class ISphWordlist
	{
	public:
		struct Args_t : public ISphNoncopyable
		{
			CSphVector<SphExpanded_t>	m_dExpanded;
			const bool					m_bPayload;
			int							m_iExpansionLimit;
			const bool					m_bHasMorphology;
			const ESphHitless			m_eHitless;

			ISphSubstringPayload* m_pPayload;
			int							m_iTotalDocs;
			int							m_iTotalHits;
			const void* m_pIndexData;

			Args_t(bool bPayload, int iExpansionLimit, bool bHasMorphology, ESphHitless eHitless, const void* pIndexData);
			~Args_t();
			void AddExpanded(const BYTE* sWord, int iLen, int iDocs, int iHits);
			const char* GetWordExpanded(int iIndex) const;

		private:
			CSphVector<char> m_sBuf;
		};

		virtual ~ISphWordlist() {}
		virtual void GetPrefixedWords(const char* sSubstring, int iSubLen, const char* sWildcard, Args_t& tArgs) const = 0;
		virtual void GetInfixedWords(const char* sSubstring, int iSubLen, const char* sWildcard, Args_t& tArgs) const = 0;
	};


}