#include "neo/index/index_settings.h"


namespace NEO {

	void LoadIndexSettings(CSphIndexSettings& tSettings, CSphReader& tReader, DWORD uVersion)
	{
		if (uVersion >= 8)
		{
			tSettings.m_iMinPrefixLen = tReader.GetDword();
			tSettings.m_iMinInfixLen = tReader.GetDword();

		}
		else if (uVersion >= 6)
		{
			bool bPrefixesOnly = (tReader.GetByte() != 0);
			tSettings.m_iMinPrefixLen = tReader.GetDword();
			tSettings.m_iMinInfixLen = 0;
			if (!bPrefixesOnly)
				Swap(tSettings.m_iMinPrefixLen, tSettings.m_iMinInfixLen);
		}

		if (uVersion >= 38)
			tSettings.m_iMaxSubstringLen = tReader.GetDword();

		if (uVersion >= 9)
		{
			tSettings.m_bHtmlStrip = !!tReader.GetByte();
			tSettings.m_sHtmlIndexAttrs = tReader.GetString();
			tSettings.m_sHtmlRemoveElements = tReader.GetString();
		}

		if (uVersion >= 12)
			tSettings.m_bIndexExactWords = !!tReader.GetByte();

		if (uVersion >= 18)
			tSettings.m_eHitless = (ESphHitless)tReader.GetDword();

		if (uVersion >= 19)
			tSettings.m_eHitFormat = (ESphHitFormat)tReader.GetDword();
		else // force plain format for old indices
			tSettings.m_eHitFormat = SPH_HIT_FORMAT_PLAIN;

		if (uVersion >= 21)
			tSettings.m_bIndexSP = !!tReader.GetByte();

		if (uVersion >= 22)
		{
			tSettings.m_sZones = tReader.GetString();
			if (uVersion < 25 && !tSettings.m_sZones.IsEmpty())
				tSettings.m_sZones.SetSprintf("%s*", tSettings.m_sZones.cstr());
		}

		if (uVersion >= 23)
		{
			tSettings.m_iBoundaryStep = (int)tReader.GetDword();
			tSettings.m_iStopwordStep = (int)tReader.GetDword();
		}

		if (uVersion >= 28)
			tSettings.m_iOvershortStep = (int)tReader.GetDword();

		if (uVersion >= 30)
			tSettings.m_iEmbeddedLimit = (int)tReader.GetDword();

		if (uVersion >= 32)
		{
			tSettings.m_eBigramIndex = (ESphBigram)tReader.GetByte();
			tSettings.m_sBigramWords = tReader.GetString();
		}

		if (uVersion >= 35)
			tSettings.m_bIndexFieldLens = (tReader.GetByte() != 0);

		if (uVersion >= 39)
		{
			tSettings.m_eChineseRLP = (ESphRLPFilter)tReader.GetByte();
			tSettings.m_sRLPContext = tReader.GetString();
		}

		if (uVersion >= 41)
			tSettings.m_sIndexTokenFilter = tReader.GetString();
	}

	void SaveIndexSettings(CSphWriter& tWriter, const CSphIndexSettings& tSettings)
	{
		tWriter.PutDword(tSettings.m_iMinPrefixLen);
		tWriter.PutDword(tSettings.m_iMinInfixLen);
		tWriter.PutDword(tSettings.m_iMaxSubstringLen);
		tWriter.PutByte(tSettings.m_bHtmlStrip ? 1 : 0);
		tWriter.PutString(tSettings.m_sHtmlIndexAttrs.cstr());
		tWriter.PutString(tSettings.m_sHtmlRemoveElements.cstr());
		tWriter.PutByte(tSettings.m_bIndexExactWords ? 1 : 0);
		tWriter.PutDword(tSettings.m_eHitless);
		tWriter.PutDword(tSettings.m_eHitFormat);
		tWriter.PutByte(tSettings.m_bIndexSP);
		tWriter.PutString(tSettings.m_sZones);
		tWriter.PutDword(tSettings.m_iBoundaryStep);
		tWriter.PutDword(tSettings.m_iStopwordStep);
		tWriter.PutDword(tSettings.m_iOvershortStep);
		tWriter.PutDword(tSettings.m_iEmbeddedLimit);
		tWriter.PutByte(tSettings.m_eBigramIndex);
		tWriter.PutString(tSettings.m_sBigramWords);
		tWriter.PutByte(tSettings.m_bIndexFieldLens);
		tWriter.PutByte(tSettings.m_eChineseRLP);
		tWriter.PutString(tSettings.m_sRLPContext);
		tWriter.PutString(tSettings.m_sIndexTokenFilter);
	}

}