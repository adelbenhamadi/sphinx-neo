#include "neo/index/progress.h"
#include "neo/platform/compat.h"

namespace NEO {

	static inline float GetPercent(int64_t a, int64_t b)
	{
		if (b == 0)
			return 100.0f;

		int64_t r = a * 100000 / b;
		return float(r) / 1000;
	}


	const char* CSphIndexProgress::BuildMessage() const
	{
		static char sBuf[256];
		switch (m_ePhase)
		{
		case PHASE_COLLECT:
			snprintf(sBuf, sizeof(sBuf), "collected " INT64_FMT " docs, %.1f MB", m_iDocuments,
				float(m_iBytes) / 1000000.0f);
			break;

		case PHASE_SORT:
			snprintf(sBuf, sizeof(sBuf), "sorted %.1f Mhits, %.1f%% done", float(m_iHits) / 1000000,
				GetPercent(m_iHits, m_iHitsTotal));
			break;

		case PHASE_COLLECT_MVA:
			snprintf(sBuf, sizeof(sBuf), "collected " INT64_FMT " attr values", m_iAttrs);
			break;

		case PHASE_SORT_MVA:
			snprintf(sBuf, sizeof(sBuf), "sorted %.1f Mvalues, %.1f%% done", float(m_iAttrs) / 1000000,
				GetPercent(m_iAttrs, m_iAttrsTotal));
			break;

		case PHASE_MERGE:
			snprintf(sBuf, sizeof(sBuf), "merged %.1f Kwords", float(m_iWords) / 1000);
			break;

		case PHASE_PREREAD:
			snprintf(sBuf, sizeof(sBuf), "read %.1f of %.1f MB, %.1f%% done",
				float(m_iBytes) / 1000000.0f, float(m_iBytesTotal) / 1000000.0f,
				GetPercent(m_iBytes, m_iBytesTotal));
			break;

		case PHASE_PRECOMPUTE:
			snprintf(sBuf, sizeof(sBuf), "indexing attributes, %d.%d%% done", m_iDone / 10, m_iDone % 10);
			break;

		default:
			assert(0 && "internal error: unhandled progress phase");
			snprintf(sBuf, sizeof(sBuf), "(progress-phase-%d)", m_ePhase);
			break;
		}

		sBuf[sizeof(sBuf) - 1] = '\0';
		return sBuf;
	}


	void CSphIndexProgress::Show(bool bPhaseEnd) const
	{
		if (m_fnProgress)
			m_fnProgress(this, bPhaseEnd);
	}

}