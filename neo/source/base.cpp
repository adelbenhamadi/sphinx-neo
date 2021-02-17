#include "neo/source/base.h"
#include "neo/source/schema.h"
#include "neo/tools/html_stripper.h"
#include "neo/dict/dict.h"
#include "neo/tokenizer/tokenizer.h"

namespace NEO {


	void LoadFieldFilterSettings(CSphReader& tReader, CSphFieldFilterSettings& tFieldFilterSettings)
	{
		int nRegexps = tReader.GetDword();
		if (!nRegexps)
			return;

		tFieldFilterSettings.m_dRegexps.Resize(nRegexps);
		ARRAY_FOREACH(i, tFieldFilterSettings.m_dRegexps)
			tFieldFilterSettings.m_dRegexps[i] = tReader.GetString();

		tReader.GetByte(); // deprecated utf-8 flag
	}


	void SaveFieldFilterSettings(CSphWriter& tWriter, ISphFieldFilter* pFieldFilter)
	{
		if (!pFieldFilter)
		{
			tWriter.PutDword(0);
			return;
		}

		CSphFieldFilterSettings tSettings;
		pFieldFilter->GetSettings(tSettings);

		tWriter.PutDword(tSettings.m_dRegexps.GetLength());
		ARRAY_FOREACH(i, tSettings.m_dRegexps)
			tWriter.PutString(tSettings.m_dRegexps[i]);

		tWriter.PutByte(1); // deprecated utf8 flag
	}

	///////////////////////

	CSphSourceSettings::CSphSourceSettings()
		: m_iMinPrefixLen(0)
		, m_iMinInfixLen(0)
		, m_iMaxSubstringLen(0)
		, m_iBoundaryStep(0)
		, m_bIndexExactWords(false)
		, m_iOvershortStep(1)
		, m_iStopwordStep(1)
		, m_bIndexSP(false)
		, m_bIndexFieldLens(false)
	{}


	ESphWordpart CSphSourceSettings::GetWordpart(const char* sField, bool bWordDict)
	{
		if (bWordDict)
			return SPH_WORDPART_WHOLE;

		bool bPrefix = (m_iMinPrefixLen > 0) && (m_dPrefixFields.GetLength() == 0 || m_dPrefixFields.Contains(sField));
		bool bInfix = (m_iMinInfixLen > 0) && (m_dInfixFields.GetLength() == 0 || m_dInfixFields.Contains(sField));

		assert(!(bPrefix && bInfix)); // no field must be marked both prefix and infix
		if (bPrefix)
			return SPH_WORDPART_PREFIX;
		if (bInfix)
			return SPH_WORDPART_INFIX;
		return SPH_WORDPART_WHOLE;
	}

	//////////////////////


	ISphFieldFilter::ISphFieldFilter()
		: m_pParent(NULL)
	{
	}


	ISphFieldFilter::~ISphFieldFilter()
	{
		SafeDelete(m_pParent);
	}


	void ISphFieldFilter::SetParent(ISphFieldFilter* pParent)
	{
		SafeDelete(m_pParent);
		m_pParent = pParent;
	}



#if USE_RE2
	class CSphFieldRegExps : public ISphFieldFilter
	{
	public:
		CSphFieldRegExps(bool bCloned);
		virtual					~CSphFieldRegExps();

		virtual	int				Apply(const BYTE* sField, int iLength, CSphVector<BYTE>& dStorage, bool);
		virtual	void			GetSettings(CSphFieldFilterSettings& tSettings) const;
		ISphFieldFilter* Clone();

		bool					AddRegExp(const char* sRegExp, CSphString& sError);

	private:
		struct RegExp_t
		{
			CSphString	m_sFrom;
			CSphString	m_sTo;

			RE2* m_pRE2;
		};

		CSphVector<RegExp_t>	m_dRegexps;
		bool					m_bCloned;
	};


	CSphFieldRegExps::CSphFieldRegExps(bool bCloned)
		: m_bCloned(bCloned)
	{
	}


	CSphFieldRegExps::~CSphFieldRegExps()
	{
		if (!m_bCloned)
		{
			ARRAY_FOREACH(i, m_dRegexps)
				SafeDelete(m_dRegexps[i].m_pRE2);
		}
	}


	int CSphFieldRegExps::Apply(const BYTE* sField, int iLength, CSphVector<BYTE>& dStorage, bool)
	{
		dStorage.Resize(0);
		if (!sField || !*sField)
			return 0;

		bool bReplaced = false;
		std::string sRe2 = (iLength ? std::string((char*)sField, iLength) : (char*)sField);
		ARRAY_FOREACH(i, m_dRegexps)
		{
			assert(m_dRegexps[i].m_pRE2);
			bReplaced |= (RE2::GlobalReplace(&sRe2, *m_dRegexps[i].m_pRE2, m_dRegexps[i].m_sTo.cstr()) > 0);
		}

		if (!bReplaced)
			return 0;

		int iDstLen = sRe2.length();
		dStorage.Resize(iDstLen + 4); // string SAFETY_GAP
		strncpy((char*)dStorage.Begin(), sRe2.c_str(), dStorage.GetLength());
		return iDstLen;
	}


	void CSphFieldRegExps::GetSettings(CSphFieldFilterSettings& tSettings) const
	{
		tSettings.m_dRegexps.Resize(m_dRegexps.GetLength());
		ARRAY_FOREACH(i, m_dRegexps)
			tSettings.m_dRegexps[i].SetSprintf("%s => %s", m_dRegexps[i].m_sFrom.cstr(), m_dRegexps[i].m_sTo.cstr());
	}


	bool CSphFieldRegExps::AddRegExp(const char* sRegExp, CSphString& sError)
	{
		if (m_bCloned)
			return false;

		const char sSplitter[] = "=>";
		const char* sSplit = strstr(sRegExp, sSplitter);
		if (!sSplit)
		{
			sError = "mapping token (=>) not found";
			return false;
		}
		else if (strstr(sSplit + strlen(sSplitter), sSplitter))
		{
			sError = "mapping token (=>) found more than once";
			return false;
		}

		m_dRegexps.Resize(m_dRegexps.GetLength() + 1);
		RegExp_t& tRegExp = m_dRegexps.Last();
		tRegExp.m_sFrom.SetBinary(sRegExp, sSplit - sRegExp);
		tRegExp.m_sTo = sSplit + strlen(sSplitter);
		tRegExp.m_sFrom.Trim();
		tRegExp.m_sTo.Trim();

		RE2::Options tOptions;
		tOptions.set_utf8(true);
		tRegExp.m_pRE2 = new RE2(tRegExp.m_sFrom.cstr(), tOptions);

		std::string sRE2Error;
		if (!tRegExp.m_pRE2->CheckRewriteString(tRegExp.m_sTo.cstr(), &sRE2Error))
		{
			sError.SetSprintf("\"%s => %s\" is not a valid mapping: %s", tRegExp.m_sFrom.cstr(), tRegExp.m_sTo.cstr(), sRE2Error.c_str());
			SafeDelete(tRegExp.m_pRE2);
			m_dRegexps.Remove(m_dRegexps.GetLength() - 1);
			return false;
		}

		return true;
	}


	ISphFieldFilter* CSphFieldRegExps::Clone()
	{
		ISphFieldFilter* pClonedParent = NULL;
		if (m_pParent)
			pClonedParent = m_pParent->Clone();

		CSphFieldRegExps* pCloned = new CSphFieldRegExps(true);
		pCloned->m_dRegexps = m_dRegexps;

		return pCloned;
	}
#endif


#if USE_RE2
	ISphFieldFilter* sphCreateRegexpFilter(const CSphFieldFilterSettings& tFilterSettings, CSphString& sError)
	{
		CSphFieldRegExps* pFilter = new CSphFieldRegExps(false);
		ARRAY_FOREACH(i, tFilterSettings.m_dRegexps)
			pFilter->AddRegExp(tFilterSettings.m_dRegexps[i].cstr(), sError);

		return pFilter;
	}
#else
	ISphFieldFilter* sphCreateRegexpFilter(const CSphFieldFilterSettings&, CSphString&)
	{
		return NULL;
	}
#endif


	//////////////////////

	CSphSource::CSphSource(const char* sName)
		: m_pTokenizer(NULL)
		, m_pDict(NULL)
		, m_pFieldFilter(NULL)
		, m_tSchema(sName)
		, m_pStripper(NULL)
		, m_iNullIds(0)
		, m_iMaxIds(0)
	{
	}


	CSphSource::~CSphSource()
	{
		SafeDelete(m_pStripper);
	}


	void CSphSource::SetDict(CSphDict* pDict)
	{
		assert(pDict);
		m_pDict = pDict;
	}


	const CSphSourceStats& CSphSource::GetStats()
	{
		return m_tStats;
	}


	bool CSphSource::SetStripHTML(const char* sExtractAttrs, const char* sRemoveElements,
		bool bDetectParagraphs, const char* sZones, CSphString& sError)
	{
		if (!m_pStripper)
			m_pStripper = new CSphHTMLStripper(true);

		if (!m_pStripper->SetIndexedAttrs(sExtractAttrs, sError))
			return false;

		if (!m_pStripper->SetRemovedElements(sRemoveElements, sError))
			return false;

		if (bDetectParagraphs)
			m_pStripper->EnableParagraphs();

		if (!m_pStripper->SetZones(sZones, sError))
			return false;

		return true;
	}


	void CSphSource::SetFieldFilter(ISphFieldFilter* pFilter)
	{
		m_pFieldFilter = pFilter;
	}

	void CSphSource::SetTokenizer(ISphTokenizer* pTokenizer)
	{
		assert(pTokenizer);
		m_pTokenizer = pTokenizer;
	}


	bool CSphSource::UpdateSchema(CSphSchema* pInfo, CSphString& sError)
	{
		assert(pInfo);

		// fill it
		if (pInfo->m_dFields.GetLength() == 0 && pInfo->GetAttrsCount() == 0)
		{
			*pInfo = m_tSchema;
			return true;
		}

		// check it
		return m_tSchema.CompareTo(*pInfo, sError);
	}


	void CSphSource::Setup(const CSphSourceSettings& tSettings)
	{
		m_iMinPrefixLen = Max(tSettings.m_iMinPrefixLen, 0);
		m_iMinInfixLen = Max(tSettings.m_iMinInfixLen, 0);
		m_iMaxSubstringLen = Max(tSettings.m_iMaxSubstringLen, 0);
		m_iBoundaryStep = Max(tSettings.m_iBoundaryStep, -1);
		m_bIndexExactWords = tSettings.m_bIndexExactWords;
		m_iOvershortStep = Min(Max(tSettings.m_iOvershortStep, 0), 1);
		m_iStopwordStep = Min(Max(tSettings.m_iStopwordStep, 0), 1);
		m_bIndexSP = tSettings.m_bIndexSP;
		m_dPrefixFields = tSettings.m_dPrefixFields;
		m_dInfixFields = tSettings.m_dInfixFields;
		m_bIndexFieldLens = tSettings.m_bIndexFieldLens;
	}


	SphDocID_t CSphSource::VerifyID(SphDocID_t uID)
	{
		if (uID == 0)
		{
			m_iNullIds++;
			return 0;
		}

		if (uID == DOCID_MAX)
		{
			m_iMaxIds++;
			return 0;
		}

		return uID;
	}


	ISphHits* CSphSource::IterateJoinedHits(CSphString&)
	{
		static ISphHits dDummy;
		m_tDocInfo.m_uDocID = 0; // pretend that's an eof
		return &dDummy;
	}


}