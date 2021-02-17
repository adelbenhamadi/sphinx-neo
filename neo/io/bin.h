#pragma once
#include "neo/io/enums.h"
#include "neo/index/enums.h"
#include "neo/core/globals.h"
#include "neo/int/aggregate_hit.h"
#include "neo/int/throttle_state.h"
#include "neo/io/io.h"

namespace NEO {

	/// bin, block input buffer
	struct CSphBin
	{
		static const int	MIN_SIZE = 8192;
		static const int	WARN_SIZE = 262144;

	protected:
		ESphHitless			m_eMode;
		int					m_iSize;

		BYTE* m_dBuffer;
		BYTE* m_pCurrent;
		int					m_iLeft;
		int					m_iDone;
		ESphBinState		m_eState;
		bool				m_bWordDict;
		bool				m_bError;	// FIXME? sort of redundant, but states are a mess

		CSphAggregateHit	m_tHit;									//currently decoded hit
		BYTE				m_sKeyword[MAX_KEYWORD_BYTES];	//currently decoded hit keyword (in keywords dict mode)

#ifndef NDEBUG
		SphWordID_t			m_iLastWordID;
		BYTE				m_sLastKeyword[MAX_KEYWORD_BYTES];
#endif

		int					m_iFile;		//my file
		SphOffset_t* m_pFilePos;		//shared current offset in file
		ThrottleState_t* m_pThrottle;

	public:
		SphOffset_t			m_iFilePos;		//my current offset in file
		int					m_iFileLeft;	//how much data is still unread from the file

	public:
		explicit 			CSphBin(ESphHitless eMode = SPH_HITLESS_NONE, bool bWordDict = false);
		~CSphBin();

		static int			CalcBinSize(int iMemoryLimit, int iBlocks, const char* sPhase, bool bWarn = true);
		void				Init(int iFD, SphOffset_t* pSharedOffset, const int iBinSize);

		SphWordID_t			ReadVLB();
		int					ReadByte();
		ESphBinRead			ReadBytes(void* pDest, int iBytes);
		int					ReadHit(CSphAggregateHit* pHit, int iRowitems, CSphRowitem* pRowitems);

		DWORD				UnzipInt();
		SphOffset_t			UnzipOffset();

		bool				IsEOF() const;
		bool				IsDone() const;
		bool				IsError() const { return m_bError; }
		ESphBinRead			Precache();
		void				SetThrottle(ThrottleState_t* pState) { m_pThrottle = pState; }
	};

}