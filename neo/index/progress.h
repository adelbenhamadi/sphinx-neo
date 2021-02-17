#pragma once
#include "neo/int/types.h"

namespace NEO {

	/// progress info
	struct CSphIndexProgress
	{
		enum  Phase_e
		{
			PHASE_COLLECT,				///< document collection phase
			PHASE_SORT,					///< final sorting phase
			PHASE_COLLECT_MVA,			///< multi-valued attributes collection phase
			PHASE_SORT_MVA,				///< multi-valued attributes collection phase
			PHASE_MERGE,				///< index merging

			PHASE_PREREAD,				///< searchd startup, prereading data
			PHASE_PRECOMPUTE			///< searchd startup, indexing attributes
		};

		Phase_e			m_ePhase;		///< current indexing phase

		int64_t			m_iDocuments;	///< PHASE_COLLECT: documents collected so far
		int64_t			m_iBytes;		///< PHASE_COLLECT: bytes collected so far;
										///< PHASE_PREREAD: bytes read so far;
		int64_t			m_iBytesTotal;	///< PHASE_PREREAD: total bytes to read;

		int64_t			m_iAttrs;		///< PHASE_COLLECT_MVA, PHASE_SORT_MVA: attrs processed so far
		int64_t			m_iAttrsTotal;	///< PHASE_SORT_MVA: attrs total

		SphOffset_t		m_iHits;		///< PHASE_SORT: hits sorted so far
		SphOffset_t		m_iHitsTotal;	///< PHASE_SORT: hits total

		int				m_iWords;		///< PHASE_MERGE: words merged so far

		int				m_iDone;		///< generic percent, 0..1000 range

		typedef void (*IndexingProgress_fn) (const CSphIndexProgress* pStat, bool bPhaseEnd);
		IndexingProgress_fn m_fnProgress;

		CSphIndexProgress()
			: m_ePhase(Phase_e::PHASE_COLLECT)
			, m_iDocuments(0)
			, m_iBytes(0)
			, m_iBytesTotal(0)
			, m_iAttrs(0)
			, m_iAttrsTotal(0)
			, m_iHits(0)
			, m_iHitsTotal(0)
			, m_iWords(0)
			, m_fnProgress(NULL)
			,m_iDone(0)
		{}

		/// builds a message to print
		/// WARNING, STATIC BUFFER, NON-REENTRANT
		const char* BuildMessage() const;

		void Show(bool bPhaseEnd) const;
	};

}