#pragma once

namespace NEO {

	struct MappedCheckpoint_fn : public ISphNoncopyable
	{
		const CSphWordlistCheckpoint* m_pDstStart;
		const BYTE* m_pSrcStart;
		const ISphCheckpointReader* m_pReader;

		MappedCheckpoint_fn(const CSphWordlistCheckpoint* pDstStart, const BYTE* pSrcStart, const ISphCheckpointReader* pReader)
			: m_pDstStart(pDstStart)
			, m_pSrcStart(pSrcStart)
			, m_pReader(pReader)
		{}

		CSphWordlistCheckpoint operator() (const CSphWordlistCheckpoint* pCP) const
		{
			assert(m_pDstStart <= pCP);
			const BYTE* pCur = (pCP - m_pDstStart) * m_pReader->m_iSrcStride + m_pSrcStart;
			CSphWordlistCheckpoint tEntry;
			m_pReader->ReadEntry(pCur, tEntry);
			return tEntry;
		}
	};

}