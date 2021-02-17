#pragma once
#include "neo/int/types.h"
#include "neo/tokenizer/enums.h"
#include "neo/source/base.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"


namespace NEO {

	struct CSphIndexSettings : public CSphSourceSettings
	{
		ESphDocinfo		m_eDocinfo;
		ESphHitFormat	m_eHitFormat;
		bool			m_bHtmlStrip;
		CSphString		m_sHtmlIndexAttrs;
		CSphString		m_sHtmlRemoveElements;
		CSphString		m_sZones;
		ESphHitless		m_eHitless;
		CSphString		m_sHitlessFiles;
		bool			m_bVerbose;
		int				m_iEmbeddedLimit;

		ESphBigram				m_eBigramIndex;
		CSphString				m_sBigramWords;
		CSphVector<CSphString>	m_dBigramWords;

		DWORD			m_uAotFilterMask;		///< lemmatize_XX_all forces us to transform queries on the index level too
		ESphRLPFilter	m_eChineseRLP;			///< chinese RLP filter
		CSphString		m_sRLPContext;			///< path to RLP context file

		CSphString		m_sIndexTokenFilter;	///< indexing time token filter spec string (pretty useless for disk, vital for RT)

		CSphIndexSettings()
			: m_eDocinfo(SPH_DOCINFO_NONE)
			, m_eHitFormat(SPH_HIT_FORMAT_PLAIN)
			, m_bHtmlStrip(false)
			, m_eHitless(SPH_HITLESS_NONE)
			, m_bVerbose(false)
			, m_iEmbeddedLimit(0)
			, m_eBigramIndex(SPH_BIGRAM_NONE)
			, m_uAotFilterMask(0)
			, m_eChineseRLP(SPH_RLP_NONE)
		{
		};
	};



	void LoadIndexSettings(CSphIndexSettings& tSettings, CSphReader& tReader, DWORD uVersion);

	void SaveIndexSettings(CSphWriter& tWriter, const CSphIndexSettings& tSettings);


}