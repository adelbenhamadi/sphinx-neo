#pragma once
#include "neo/int/types.h"
#include "neo/tokenizer/tokenizer.h"
#include "neo/dict/dict.h"
#include "neo/dict/dict_exact.h"
#include "neo/tools/html_stripper.h"
#include "neo/int/scoped_pointer.h"

namespace NEO {

	/// snippet setupper
	/// used by searchd and SNIPPET() function in exprs
	/// should probably be refactored as a single function
	/// a precursor to sphBuildExcerpts() call
	class SnippetContext_t : ISphNoncopyable
	{
	private:
		CSphScopedPtr<CSphDict> m_tDictCloned;
		CSphScopedPtr<CSphDict> m_tExactDict;

	public:
		CSphDict* m_pDict;
		CSphScopedPtr<ISphTokenizer> m_tTokenizer;
		CSphScopedPtr<CSphHTMLStripper> m_tStripper;
		ISphTokenizer* m_pQueryTokenizer;
		XQQuery_t m_tExtQuery;
		DWORD m_eExtQuerySPZ;

		SnippetContext_t()
			: m_tDictCloned(NULL)
			, m_tExactDict(NULL)
			, m_pDict(NULL)
			, m_tTokenizer(NULL)
			, m_tStripper(NULL)
			, m_pQueryTokenizer(NULL)
			, m_eExtQuerySPZ(SPH_SPZ_NONE)
		{
		}

		~SnippetContext_t()
		{
			SafeDelete(m_pQueryTokenizer);
		}

		static CSphDict* SetupExactDict(const CSphIndexSettings& tSettings, CSphScopedPtr<CSphDict>& tExact, CSphDict* pDict)
		{
			// handle index_exact_words
			if (!tSettings.m_bIndexExactWords)
				return pDict;

			tExact = new CSphDictExact(pDict);
			return tExact.Ptr();
		}

		static DWORD CollectQuerySPZ(const XQNode_t* pNode)
		{
			if (!pNode)
				return SPH_SPZ_NONE;

			DWORD eSPZ = SPH_SPZ_NONE;
			if (pNode->GetOp() == SPH_QUERY_SENTENCE)
				eSPZ |= SPH_SPZ_SENTENCE;
			else if (pNode->GetOp() == SPH_QUERY_PARAGRAPH)
				eSPZ |= SPH_SPZ_PARAGRAPH;

			ARRAY_FOREACH(i, pNode->m_dChildren)
				eSPZ |= CollectQuerySPZ(pNode->m_dChildren[i]);

			return eSPZ;
		}

		static bool SetupStripperSPZ(const CSphIndexSettings& tSettings, const ExcerptQuery_t& q,
			bool bSetupSPZ, CSphScopedPtr<CSphHTMLStripper>& tStripper, ISphTokenizer* pTokenizer,
			CSphString& sError)
		{
			if (bSetupSPZ &&
				(!pTokenizer->EnableSentenceIndexing(sError) || !pTokenizer->EnableZoneIndexing(sError)))
			{
				return false;
			}


			if (q.m_sStripMode == "strip" || q.m_sStripMode == "retain"
				|| (q.m_sStripMode == "index" && tSettings.m_bHtmlStrip))
			{
				// don't strip HTML markup in 'retain' mode - proceed zones only
				tStripper = new CSphHTMLStripper(q.m_sStripMode != "retain");

				if (q.m_sStripMode == "index")
				{
					if (
						!tStripper->SetIndexedAttrs(tSettings.m_sHtmlIndexAttrs.cstr(), sError) ||
						!tStripper->SetRemovedElements(tSettings.m_sHtmlRemoveElements.cstr(), sError))
					{
						sError.SetSprintf("HTML stripper config error: %s", sError.cstr());
						return false;
					}
				}

				if (bSetupSPZ)
				{
					tStripper->EnableParagraphs();
				}

				// handle zone(s) in special mode only when passage_boundary enabled
				if (bSetupSPZ && !tStripper->SetZones(tSettings.m_sZones.cstr(), sError))
				{
					sError.SetSprintf("HTML stripper config error: %s", sError.cstr());
					return false;
				}
			}

			return true;
		}

		bool Setup(const CSphIndex* pIndex, const ExcerptQuery_t& tSettings, CSphString& sError)
		{
			assert(pIndex);
			CSphScopedPtr<CSphDict> tDictCloned(NULL);
			m_pDict = pIndex->GetDictionary();
			if (m_pDict->HasState())
				m_tDictCloned = m_pDict = m_pDict->Clone();

			// AOT tokenizer works only with query mode
			if (pIndex->GetSettings().m_uAotFilterMask &&
				(!tSettings.m_bHighlightQuery || tSettings.m_bExactPhrase))
			{
				if (!tSettings.m_bHighlightQuery)
					sError.SetSprintf("failed to setup AOT with query_mode=0, use query_mode=1");
				else
					sError.SetSprintf("failed to setup AOT with exact_phrase, use phrase search operator with query_mode=1");
				return false;
			}

			// OPTIMIZE! do a lightweight indexing clone here
			if (tSettings.m_bHighlightQuery && pIndex->GetSettings().m_uAotFilterMask)
				m_tTokenizer = sphAotCreateFilter(pIndex->GetTokenizer()->Clone(SPH_CLONE_INDEX), m_pDict, pIndex->GetSettings().m_bIndexExactWords, pIndex->GetSettings().m_uAotFilterMask);
			else
				m_tTokenizer = pIndex->GetTokenizer()->Clone(SPH_CLONE_INDEX);

			m_pQueryTokenizer = NULL;
			if (tSettings.m_bHighlightQuery || tSettings.m_bExactPhrase)
			{
				m_pQueryTokenizer = pIndex->GetQueryTokenizer()->Clone(SPH_CLONE_QUERY_LIGHTWEIGHT);
			}
			else
			{
				// legacy query mode should handle exact form modifier and star wildcard
				m_pQueryTokenizer = pIndex->GetTokenizer()->Clone(SPH_CLONE_INDEX);
				if (pIndex->IsStarDict())
				{
					m_pQueryTokenizer->AddPlainChar('*');
					m_pQueryTokenizer->AddPlainChar('?');
					m_pQueryTokenizer->AddPlainChar('%');
				}
				if (pIndex->GetSettings().m_bIndexExactWords)
					m_pQueryTokenizer->AddPlainChar('=');
			}

			// setup exact dictionary if needed
			m_pDict = SetupExactDict(pIndex->GetSettings(), m_tExactDict, m_pDict);

			if (tSettings.m_bHighlightQuery)
			{
				// OPTIMIZE? double lightweight clone here? but then again it's lightweight
				if (!sphParseExtendedQuery(m_tExtQuery, tSettings.m_sWords.cstr(), NULL, m_pQueryTokenizer,
					&pIndex->GetMatchSchema(), m_pDict, pIndex->GetSettings()))
				{
					sError = m_tExtQuery.m_sParseError;
					return false;
				}
				if (m_tExtQuery.m_pRoot)
					m_tExtQuery.m_pRoot->ClearFieldMask();

				m_eExtQuerySPZ = SPH_SPZ_NONE;
				m_eExtQuerySPZ |= CollectQuerySPZ(m_tExtQuery.m_pRoot);
				if (m_tExtQuery.m_dZones.GetLength())
					m_eExtQuerySPZ |= SPH_SPZ_ZONE;

				if (pIndex->GetSettings().m_uAotFilterMask)
					TransformAotFilter(m_tExtQuery.m_pRoot, m_pDict->GetWordforms(), pIndex->GetSettings());
			}

			bool bSetupSPZ = (tSettings.m_ePassageSPZ != SPH_SPZ_NONE || m_eExtQuerySPZ != SPH_SPZ_NONE ||
				(tSettings.m_sStripMode == "retain" && tSettings.m_bHighlightQuery));

			if (!SetupStripperSPZ(pIndex->GetSettings(), tSettings, bSetupSPZ, m_tStripper, m_tTokenizer.Ptr(), sError))
				return false;

			return true;
		}
	};

}