#pragma once
#include "neo/int/types.h"
#include "neo/int/aggregate_hit.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/core/skip_list.h"
#include "neo/dict/dict.h"
#include "neo/dict/dict_header.h"
#include "neo/dict/dict_entry.h"
#include "neo/index/index_settings.h"

namespace NEO {


	class CSphHitBuilder
	{
	public:
		CSphHitBuilder(const CSphIndexSettings& tSettings, const CSphVector<SphWordID_t>& dHitless, bool bMerging, int iBufSize, CSphDict* pDict, CSphString* sError);
		~CSphHitBuilder() {}

		bool	CreateIndexFiles(const char* sDocName, const char* sHitName, const char* sSkipName, bool bInplace, int iWriteBuffer, CSphAutofile& tHit, SphOffset_t* pSharedOffset);
		void	HitReset();
		void	cidxHit(CSphAggregateHit* pHit, const CSphRowitem* pAttrs);
		bool	cidxDone(int iMemLimit, int iMinInfixLen, int iMaxCodepointLen, DictHeader_t* pDictHeader);
		int		cidxWriteRawVLB(int fd, CSphWordHit* pHit, int iHits, DWORD* pDocinfo, int iDocinfos, int iStride);

		SphOffset_t		GetHitfilePos() const { return m_wrHitlist.GetPos(); }
		void			CloseHitlist() { m_wrHitlist.CloseFile(); }
		bool			IsError() const { return (m_pDict->DictIsError() || m_wrDoclist.IsError() || m_wrHitlist.IsError()); }
		void			SetMin(const CSphRowitem* pDynamic, int iDynamic);
		void			HitblockBegin() { m_pDict->HitblockBegin(); }
		bool			IsWordDict() const { return m_pDict->GetSettings().m_bWordDict; }
		void			SetThrottle(ThrottleState_t* pState) { m_pThrottle = pState; }

	private:
		void	DoclistBeginEntry(SphDocID_t uDocid, const DWORD* pAttrs);
		void	DoclistEndEntry(Hitpos_t uLastPos);
		void	DoclistEndList();

		CSphWriter					m_wrDoclist;			//wordlist writer
		CSphWriter					m_wrHitlist;			//hitlist writer
		CSphWriter					m_wrSkiplist;			//skiplist writer
		CSphFixedVector<BYTE>		m_dWriteBuffer;			//my write buffer (for temp files)
		ThrottleState_t* m_pThrottle;

		CSphFixedVector<CSphRowitem>	m_dMinRow;

		CSphAggregateHit			m_tLastHit;				//hitlist entry
		Hitpos_t					m_iPrevHitPos;			//previous hit position
		bool						m_bGotFieldEnd;
		BYTE						m_sLastKeyword[MAX_KEYWORD_BYTES];

		const CSphVector<SphWordID_t>& m_dHitlessWords;
		CSphDict* m_pDict;
		CSphString* m_pLastError;

		SphOffset_t					m_iLastHitlistPos;		//doclist entry
		SphOffset_t					m_iLastHitlistDelta;	//doclist entry
		FieldMask_t					m_dLastDocFields;		//doclist entry
		DWORD						m_uLastDocHits;			//doclist entry

		CSphDictEntry				m_tWord;				//dictionary entry

		ESphHitFormat				m_eHitFormat;
		ESphHitless					m_eHitless;
		bool						m_bMerging;

		CSphVector<SkiplistEntry_t>	m_dSkiplist;
	};

}