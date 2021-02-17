#pragma once

namespace NEO {

	struct CSphDictSettings
	{
		CSphString		m_sMorphology;
		CSphString		m_sStopwords;
		CSphVector<CSphString> m_dWordforms;
		int				m_iMinStemmingLen;
		bool			m_bWordDict;
		bool			m_bStopwordsUnstemmed;
		CSphString		m_sMorphFingerprint;		///< not used for creation; only for a check when loading

		CSphDictSettings()
			: m_iMinStemmingLen(1)
			, m_bWordDict(true)
			, m_bStopwordsUnstemmed(false)
		{}
	};

}