#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/int/scoped_pointer.h"
#include "neo/io//buffer.h"
#include "neo/dict/dict_header.h"
#include "neo/dict/dict_entry.h"
#include "neo/core/iwordlist.h"
#include "neo/index/enums.h"
#include "neo/utility/inline_misc.h"

#include "neo/sphinxint.h"

namespace NEO {

	extern const char* g_sTagInfixBlocks;
	extern const char* g_sTagInfixEntries;



	//fwd dec
	struct ISphSubstringPayload;
	struct CSphWordlistCheckpoint;
	struct ISphCheckpointReader;
	struct InfixBlock_t;



	// !COMMIT eliminate this, move it to proper dict impls
	class CWordlist : public ISphWordlist, public DictHeader_t, public ISphWordlistSuggest
	{
	public:
		// !COMMIT slow data
		CSphMappedBuffer<BYTE>						m_tBuf;					//my cache
		CSphFixedVector<CSphWordlistCheckpoint>		m_dCheckpoints;			//checkpoint offsets
		CSphVector<InfixBlock_t>					m_dInfixBlocks;
		CSphFixedVector<BYTE>						m_pWords;				//arena for checkpoint's words
		BYTE* m_pInfixBlocksWords;	//arena for infix checkpoint's words

		SphOffset_t									m_iWordsEnd;			//end of wordlist
		bool										m_bHaveSkips;			//whether there are skiplists
		CSphScopedPtr<ISphCheckpointReader>			m_tMapedCpReader;

	public:
		CWordlist();
		~CWordlist();
		void								Reset();
		bool								Preread(const char* sName, DWORD uVersion, bool bWordDict, CSphString& sError);

		const CSphWordlistCheckpoint* FindCheckpoint(const char* sWord, int iWordLen, SphWordID_t iWordID, bool bStarMode) const;
		bool								GetWord(const BYTE* pBuf, SphWordID_t iWordID, CSphDictEntry& tWord) const;

		const BYTE* AcquireDict(const CSphWordlistCheckpoint* pCheckpoint) const;
		virtual void						GetPrefixedWords(const char* sSubstring, int iSubLen, const char* sWildcard, Args_t& tArgs) const;
		virtual void						GetInfixedWords(const char* sSubstring, int iSubLen, const char* sWildcard, Args_t& tArgs) const;

		virtual void						SuffixGetChekpoints(const SuggestResult_t& tRes, const char* sSuffix, int iLen, CSphVector<DWORD>& dCheckpoints) const;
		virtual void						SetCheckpoint(SuggestResult_t& tRes, DWORD iCP) const;
		virtual bool						ReadNextWord(SuggestResult_t& tRes, DictWord_t& tWord) const;

		void								DebugPopulateCheckpoints();

	private:
		bool								m_bWordDict;
	};

	struct CSphWordlistCheckpoint
	{
		union
		{
			SphWordID_t		m_uWordID;
			const char* m_sWord;
		};
		SphOffset_t			m_iWordlistOffset;
	};

	struct ISphCheckpointReader
	{
		ISphCheckpointReader() {}
		virtual ~ISphCheckpointReader() {}
		virtual const BYTE* ReadEntry(const BYTE* pBuf, CSphWordlistCheckpoint& tCP) const = 0;
		int m_iSrcStride;
	};



	template<bool WORD_WIDE, bool OFFSET_WIDE>
	struct CheckpointReader_T : public ISphCheckpointReader
	{
		CheckpointReader_T()
		{
			m_iSrcStride = 0;
			if_const(WORD_WIDE)
				m_iSrcStride += sizeof(SphOffset_t);
		else
		m_iSrcStride += sizeof(DWORD);

		if_const(OFFSET_WIDE)
			m_iSrcStride += sizeof(SphOffset_t);
		else
		m_iSrcStride += sizeof(DWORD);
		}

		const BYTE* ReadEntry(const BYTE* pBuf, CSphWordlistCheckpoint& tCP) const
		{
			if_const(WORD_WIDE)
			{
				tCP.m_uWordID = (SphWordID_t)sphUnalignedRead(*(SphOffset_t*)pBuf);
				pBuf += sizeof(SphOffset_t);
			}
 else
		{
		tCP.m_uWordID = sphGetDword(pBuf);
		pBuf += sizeof(DWORD);
		}

		if_const(OFFSET_WIDE)
		{
			tCP.m_iWordlistOffset = sphUnalignedRead(*(SphOffset_t*)pBuf);
			pBuf += sizeof(SphOffset_t);
		}
 else
		{
		tCP.m_iWordlistOffset = sphGetDword(pBuf);
		pBuf += sizeof(DWORD);
		}

		return pBuf;
		}
	};

}