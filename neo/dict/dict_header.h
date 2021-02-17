#pragma once

namespace NEO {
	// dictionary header
	struct DictHeader_t
	{
		int				m_iDictCheckpoints;			//how many dict checkpoints (keyword blocks) are there
		SphOffset_t		m_iDictCheckpointsOffset;	//dict checkpoints file position

		int				m_iInfixCodepointBytes;		//max bytes per infix codepoint (0 means no infixes)
		int64_t			m_iInfixBlocksOffset;		//infix blocks file position (stored as unsigned 32bit int as keywords dictionary is pretty small)
		int				m_iInfixBlocksWordsSize;	//infix checkpoints size

		DictHeader_t()
			: m_iDictCheckpoints(0)
			, m_iDictCheckpointsOffset(0)
			, m_iInfixCodepointBytes(0)
			, m_iInfixBlocksOffset(0)
			, m_iInfixBlocksWordsSize(0)
		{}
	};

}