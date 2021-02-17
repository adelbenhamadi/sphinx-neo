#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/int/throttle_state.h"
#include "neo/io/reader.h"
#include "neo/index/enums.h"

namespace NEO {

	// !COMMIT eliminate this, move to dict (or at least couple with CWordlist)
	class CSphDictReader
	{
	public:
		// current word
		SphWordID_t		m_uWordID;
		SphOffset_t		m_iDoclistOffset;
		int				m_iDocs;
		int				m_iHits;
		bool			m_bHasHitlist;
		int				m_iHint;

	private:
		ESphHitless		m_eHitless;
		CSphAutoreader	m_tMyReader;
		CSphReader* m_pReader;
		SphOffset_t		m_iMaxPos;

		bool			m_bWordDict;
		char			m_sWord[MAX_KEYWORD_BYTES];

		int				m_iCheckpoint;
		bool			m_bHasSkips;

	public:
		CSphDictReader()
			: m_uWordID(0)
			, m_iDoclistOffset(0)
			, m_iHint(0)
			, m_iMaxPos(0)
			, m_bWordDict(true)
			, m_iCheckpoint(1)
			, m_bHasSkips(false)
		{
			m_sWord[0] = '\0';
		}

		bool Setup(const CSphString& sFilename, SphOffset_t iMaxPos, ESphHitless eHitless,
			CSphString& sError, bool bWordDict, ThrottleState_t* pThrottle, bool bHasSkips);


		void Setup(CSphReader* pReader, SphOffset_t iMaxPos, ESphHitless eHitless, bool bWordDict, ThrottleState_t* pThrottle, bool bHasSkips);


		bool Read();


		int CmpWord(const CSphDictReader& tOther) const;


		BYTE* GetWord() const { return (BYTE*)m_sWord; }

		int GetCheckpoint() const { return m_iCheckpoint; }
	};


}