#include "neo/sphinxint.h"
#include "neo/platform//thread.h"
#include "neo/query/node_cache.h"
#include "neo/int/scoped_pointer.h"
#include "neo/core/infix.h"

#include "neo/sphinx/xstemmer.h"
#include "neo/sphinx/xquery.h"

namespace NEO {

	int sphDictCmp(const char* pStr1, int iLen1, const char* pStr2, int iLen2)
	{
		assert(pStr1 && pStr2);
		assert(iLen1 && iLen2);
		const int iCmpLen = Min(iLen1, iLen2);
		return memcmp(pStr1, pStr2, iCmpLen);
	}

	int sphDictCmpStrictly(const char* pStr1, int iLen1, const char* pStr2, int iLen2)
	{
		assert(pStr1 && pStr2);
		assert(iLen1 && iLen2);
		const int iCmpLen = Min(iLen1, iLen2);
		const int iCmpRes = memcmp(pStr1, pStr2, iCmpLen);
		return iCmpRes == 0 ? iLen1 - iLen2 : iCmpRes;
	}


	void sphBuildNGrams(const char* sWord, int iLen, char cDelimiter, CSphVector<char>& dNGrams)
	{
		int dOff[SPH_MAX_WORD_LEN + 1];
		int iCodepoints = BuildUtf8Offsets(sWord, iLen, dOff, sizeof(dOff));
		if (iCodepoints < 3)
			return;

		dNGrams.Reserve(iLen * 3);
		for (int iChar = 0; iChar <= iCodepoints - 3; iChar++)
		{
			int iStart = dOff[iChar];
			int iEnd = dOff[iChar + 3];
			int iGramLen = iEnd - iStart;

			char* sDst = dNGrams.AddN(iGramLen + 1);
			memcpy(sDst, sWord + iStart, iGramLen);
			sDst[iGramLen] = cDelimiter;
		}
		// n-grams split by delimiter
		// however it's still null terminated
		dNGrams.Last() = '\0';
	}


	template <typename T>
	int sphLevenshtein(const T* sWord1, int iLen1, const T* sWord2, int iLen2)
	{
		if (!iLen1)
			return iLen2;
		if (!iLen2)
			return iLen1;

		int dTmp[3 * SPH_MAX_WORD_LEN + 1]; // FIXME!!! remove extra length after utf8->codepoints conversion

		for (int i = 0; i <= iLen2; i++)
			dTmp[i] = i;

		for (int i = 0; i < iLen1; i++)
		{
			dTmp[0] = i + 1;
			int iWord1 = sWord1[i];
			int iDist = i;

			for (int j = 0; j < iLen2; j++)
			{
				int iDistNext = dTmp[j + 1];
				dTmp[j + 1] = (iWord1 == sWord2[j] ? iDist : (1 + Min(Min(iDist, iDistNext), dTmp[j])));
				iDist = iDistNext;
			}
		}

		return dTmp[iLen2];
	}


	int sphLevenshtein(const char* sWord1, int iLen1, const char* sWord2, int iLen2)
	{
		return sphLevenshtein<char>(sWord1, iLen1, sWord2, iLen2);
	}

	int sphLevenshtein(const int* sWord1, int iLen1, const int* sWord2, int iLen2)
	{
		return sphLevenshtein<int>(sWord1, iLen1, sWord2, iLen2);
	}


	uint64_t sphCalcLocatorHash(const CSphAttrLocator& tLoc, uint64_t uPrevHash)
	{
		uint64_t uHash = sphFNV64(&tLoc.m_bDynamic, sizeof(tLoc.m_bDynamic), uPrevHash);
		uHash = sphFNV64(&tLoc.m_iBitCount, sizeof(tLoc.m_iBitCount), uHash);
		return sphFNV64(&tLoc.m_iBitOffset, sizeof(tLoc.m_iBitOffset), uHash);
	}

	uint64_t sphCalcExprDepHash(const char* szTag, ISphExpr* pExpr, const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable)
	{
		uint64_t uHash = sphFNV64(szTag, strlen(szTag), uPrevHash);
		return sphCalcExprDepHash(pExpr, tSorterSchema, uHash, bDisable);
	}

	uint64_t sphCalcExprDepHash(ISphExpr* pExpr, const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable)
	{
		CSphVector<int> dCols;
		pExpr->Command(SPH_EXPR_GET_DEPENDENT_COLS, &dCols);

		uint64_t uHash = uPrevHash;
		ARRAY_FOREACH(i, dCols)
		{
			const CSphColumnInfo& tCol = tSorterSchema.GetAttr(dCols[i]);
			if (tCol.m_pExpr.Ptr())
			{
				// one more expression
				uHash = tCol.m_pExpr->GetHash(tSorterSchema, uHash, bDisable);
				if (bDisable)
					return 0;
			}
			else
			{
				uHash = sphCalcLocatorHash(tCol.m_tLocator, uHash); // plain column, add locator to hash
			}
		}

		return uHash;
	}

	/////////////////


	// fix MSVC 2005 fuckup, template DoGetKeywords() just above somehow resets forScope
#if USE_WINDOWS
#pragma conform(forScope,on)
#endif


	static int sphQueryHeightCalc(const XQNode_t* pNode)
	{
		if (!pNode->m_dChildren.GetLength())
		{
			// exception, pre-cached OR of tiny (rare) keywords is just one node
			if (pNode->GetOp() == SPH_QUERY_OR)
			{
#ifndef NDEBUG
				// sanity checks
				// this node must be only created for a huge OR of tiny expansions
				assert(pNode->m_dWords.GetLength());
				ARRAY_FOREACH(i, pNode->m_dWords)
				{
					assert(pNode->m_dWords[i].m_iAtomPos == pNode->m_dWords[0].m_iAtomPos);
					assert(pNode->m_dWords[i].m_bExpanded);
				}
#endif
				return 1;
			}
			return pNode->m_dWords.GetLength();
		}

		if (pNode->GetOp() == SPH_QUERY_BEFORE)
			return 1;

		int iMaxChild = 0;
		int iHeight = 0;
		ARRAY_FOREACH(i, pNode->m_dChildren)
		{
			int iBottom = sphQueryHeightCalc(pNode->m_dChildren[i]);
			int iTop = pNode->m_dChildren.GetLength() - i - 1;
			if (iBottom + iTop >= iMaxChild + iHeight)
			{
				iMaxChild = iBottom;
				iHeight = iTop;
			}
		}

		return iMaxChild + iHeight;
	}

	bool sphHasExpandableWildcards(const char* sWord)
	{
		const char* pCur = sWord;
		int iWilds = 0;

		for (; *pCur; pCur++)
			if (sphIsWild(*pCur))
				iWilds++;

		int iLen = pCur - sWord;

		return (iWilds && iWilds < iLen);
	}

	bool sphExpandGetWords(const char* sWord, const ExpansionContext_t& tCtx, ISphWordlist::Args_t& tWordlist)
	{
		if (!sphIsWild(*sWord) || tCtx.m_iMinInfixLen == 0)
		{
			// do prefix expansion
			// remove exact form modifier, if any
			const char* sPrefix = sWord;
			if (*sPrefix == '=')
				sPrefix++;

			// skip leading wildcards
			// (in case we got here on non-infixed index path)
			const char* sWildcard = sPrefix;
			while (sphIsWild(*sPrefix))
			{
				sPrefix++;
				sWildcard++;
			}

			// compute non-wildcard prefix length
			int iPrefix = 0;
			for (const char* s = sPrefix; *s && !sphIsWild(*s); s++)
				iPrefix++;

			// do not expand prefixes under min length
			int iMinLen = Max(tCtx.m_iMinPrefixLen, tCtx.m_iMinInfixLen);
			if (iPrefix < iMinLen)
				return false;

			// prefix expansion should work on nonstemmed words only
			char sFixed[MAX_KEYWORD_BYTES];
			if (tCtx.m_bHasMorphology)
			{
				sFixed[0] = MAGIC_WORD_HEAD_NONSTEMMED;
				memcpy(sFixed + 1, sPrefix, iPrefix);
				sPrefix = sFixed;
				iPrefix++;
			}

			tCtx.m_pWordlist->GetPrefixedWords(sPrefix, iPrefix, sWildcard, tWordlist);

		}
		else
		{
			// do infix expansion
			assert(sphIsWild(*sWord));
			assert(tCtx.m_iMinInfixLen > 0);

			// find the longest substring of non-wildcards
			const char* sMaxInfix = NULL;
			int iMaxInfix = 0;
			int iCur = 0;

			for (const char* s = sWord; *s; s++)
			{
				if (sphIsWild(*s))
				{
					iCur = 0;
				}
				else if (++iCur > iMaxInfix)
				{
					sMaxInfix = s - iCur + 1;
					iMaxInfix = iCur;
				}
			}

			// do not expand infixes under min_infix_len
			if (iMaxInfix < tCtx.m_iMinInfixLen)
				return false;

			// ignore heading star
			tCtx.m_pWordlist->GetInfixedWords(sMaxInfix, iMaxInfix, sWord, tWordlist);
		}

		return true;
	}

	ExpansionContext_t::ExpansionContext_t()
		: m_pWordlist(NULL)
		, m_pBuf(NULL)
		, m_pResult(NULL)
		, m_iMinPrefixLen(0)
		, m_iMinInfixLen(0)
		, m_iExpansionLimit(0)
		, m_bHasMorphology(false)
		, m_bMergeSingles(false)
		, m_pPayloads(NULL)
		, m_eHitless(SPH_HITLESS_NONE)
		, m_pIndexData(NULL)
	{}

	// transform the (A B) NEAR C into A NEAR B NEAR C
	static void TransformNear(XQNode_t** ppNode)
	{
		XQNode_t*& pNode = *ppNode;
		if (pNode->GetOp() == SPH_QUERY_NEAR)
		{
			assert(pNode->m_dWords.GetLength() == 0);
			CSphVector<XQNode_t*> dArgs;
			int iStartFrom;

			// transform all (A B C) NEAR D into A NEAR B NEAR C NEAR D
			do
			{
				dArgs.Reset();
				iStartFrom = 0;
				ARRAY_FOREACH(i, pNode->m_dChildren)
				{
					XQNode_t* pChild = pNode->m_dChildren[i]; //shortcut
					if (pChild->GetOp() == SPH_QUERY_AND && pChild->m_dChildren.GetLength() > 0)
					{
						ARRAY_FOREACH(j, pChild->m_dChildren)
						{
							if (j == 0 && iStartFrom == 0)
							{
								// we will remove the node anyway, so just replace it with 1-st child instead
								pNode->m_dChildren[i] = pChild->m_dChildren[j];
								pNode->m_dChildren[i]->m_pParent = pNode;
								iStartFrom = i + 1;
							}
							else
							{
								dArgs.Add(pChild->m_dChildren[j]);
							}
						}
						pChild->m_dChildren.Reset();
						SafeDelete(pChild);
					}
					else if (iStartFrom != 0)
					{
						dArgs.Add(pChild);
					}
				}

				if (iStartFrom != 0)
				{
					pNode->m_dChildren.Resize(iStartFrom + dArgs.GetLength());
					ARRAY_FOREACH(i, dArgs)
					{
						pNode->m_dChildren[i + iStartFrom] = dArgs[i];
						pNode->m_dChildren[i + iStartFrom]->m_pParent = pNode;
					}
				}
			} while (iStartFrom != 0);
		}

		ARRAY_FOREACH(i, pNode->m_dChildren)
			TransformNear(&pNode->m_dChildren[i]);
	}


	/// tag excluded keywords (rvals to operator NOT)
	static void TagExcluded(XQNode_t* pNode, bool bNot)
	{
		if (pNode->GetOp() == SPH_QUERY_ANDNOT)
		{
			assert(pNode->m_dChildren.GetLength() == 2);
			assert(pNode->m_dWords.GetLength() == 0);
			TagExcluded(pNode->m_dChildren[0], bNot);
			TagExcluded(pNode->m_dChildren[1], !bNot);

		}
		else if (pNode->m_dChildren.GetLength())
		{
			// FIXME? check if this works okay with "virtually plain" stuff?
			ARRAY_FOREACH(i, pNode->m_dChildren)
				TagExcluded(pNode->m_dChildren[i], bNot);
		}
		else
		{
			// tricky bit
			// no assert on length here and that is intended
			// we have fully empty nodes (0 children, 0 words) sometimes!
			ARRAY_FOREACH(i, pNode->m_dWords)
				pNode->m_dWords[i].m_bExcluded = bNot;
		}
	}


	/// optimize phrase queries if we have bigrams
	static void TransformBigrams(XQNode_t* pNode, const CSphIndexSettings& tSettings)
	{
		assert(tSettings.m_eBigramIndex != SPH_BIGRAM_NONE);
		assert(tSettings.m_eBigramIndex == SPH_BIGRAM_ALL || tSettings.m_dBigramWords.GetLength());

		if (pNode->GetOp() != SPH_QUERY_PHRASE)
		{
			ARRAY_FOREACH(i, pNode->m_dChildren)
				TransformBigrams(pNode->m_dChildren[i], tSettings);
			return;
		}

		CSphBitvec bmRemove;
		bmRemove.Init(pNode->m_dWords.GetLength());

		for (int i = 0; i < pNode->m_dWords.GetLength() - 1; i++)
		{
			// check whether this pair was indexed
			bool bBigram = false;
			switch (tSettings.m_eBigramIndex)
			{
			case SPH_BIGRAM_NONE:
				break;
			case SPH_BIGRAM_ALL:
				bBigram = true;
				break;
			case SPH_BIGRAM_FIRSTFREQ:
				bBigram = tSettings.m_dBigramWords.BinarySearch(pNode->m_dWords[i].m_sWord) != NULL;
				break;
			case SPH_BIGRAM_BOTHFREQ:
				bBigram =
					(tSettings.m_dBigramWords.BinarySearch(pNode->m_dWords[i].m_sWord) != NULL) &&
					(tSettings.m_dBigramWords.BinarySearch(pNode->m_dWords[i + 1].m_sWord) != NULL);
				break;
			}
			if (!bBigram)
				continue;

			// replace the pair with a bigram keyword
			// FIXME!!! set phrase weight for this "word" here
			pNode->m_dWords[i].m_sWord.SetSprintf("%s%c%s",
				pNode->m_dWords[i].m_sWord.cstr(),
				MAGIC_WORD_BIGRAM,
				pNode->m_dWords[i + 1].m_sWord.cstr());

			// only mark for removal now, we will sweep later
			// so that [a b c] would convert to ["a b" "b c"], not just ["a b" c]
			bmRemove.BitClear(i);
			bmRemove.BitSet(i + 1);
		}

		// remove marked words
		int iOut = 0;
		ARRAY_FOREACH(i, pNode->m_dWords)
			if (!bmRemove.BitGet(i))
				pNode->m_dWords[iOut++] = pNode->m_dWords[i];
		pNode->m_dWords.Resize(iOut);

		// fixup nodes that are not real phrases any more
		if (pNode->m_dWords.GetLength() == 1)
			pNode->SetOp(SPH_QUERY_AND);
	}

	static XQNode_t* CloneKeyword(const XQNode_t* pNode)
	{
		assert(pNode);

		XQNode_t* pRes = new XQNode_t(pNode->m_dSpec);
		pRes->m_dWords = pNode->m_dWords;
		return pRes;
	}

	/// create a node from a set of lemmas
	/// WARNING, tKeyword might or might not be pointing to pNode->m_dWords[0]
	/// Called from the daemon side (searchd) in time of query
	static void TransformAotFilterKeyword(XQNode_t* pNode, const XQKeyword_t& tKeyword, const CSphWordforms* pWordforms, const CSphIndexSettings& tSettings)
	{
		assert(pNode->m_dWords.GetLength() <= 1);
		assert(pNode->m_dChildren.GetLength() == 0);

		XQNode_t* pExact = NULL;
		if (pWordforms)
		{
			// do a copy, because patching in place is not an option
			// short => longlonglong wordform mapping would crash
			// OPTIMIZE? forms that are not found will (?) get looked up again in the dict
			char sBuf[MAX_KEYWORD_BYTES];
			strncpy(sBuf, tKeyword.m_sWord.cstr(), sizeof(sBuf));
			if (pWordforms->ToNormalForm((BYTE*)sBuf, true, false))
			{
				if (!pNode->m_dWords.GetLength())
					pNode->m_dWords.Add(tKeyword);
				pNode->m_dWords[0].m_sWord = sBuf;
				pNode->m_dWords[0].m_bMorphed = true;
				return;
			}
		}

		CSphVector<CSphString> dLemmas;
		DWORD uLangMask = tSettings.m_uAotFilterMask;
		for (int i = AOT_LANGS::AOT_BEGIN; i < AOT_LANGS::AOT_LENGTH; ++i)
		{
			if (uLangMask & (1UL << i))
			{
				if (i == AOT_LANGS::AOT_RU)
					sphAotLemmatizeRu(dLemmas, (BYTE*)tKeyword.m_sWord.cstr());
				else if (i == AOT_LANGS::AOT_DE)
					sphAotLemmatizeDe(dLemmas, (BYTE*)tKeyword.m_sWord.cstr());
				else
					sphAotLemmatize(dLemmas, (BYTE*)tKeyword.m_sWord.cstr(), i);
			}
		}

		// post-morph wordforms
		if (pWordforms && pWordforms->m_bHavePostMorphNF)
		{
			char sBuf[MAX_KEYWORD_BYTES];
			ARRAY_FOREACH(i, dLemmas)
			{
				strncpy(sBuf, dLemmas[i].cstr(), sizeof(sBuf));
				if (pWordforms->ToNormalForm((BYTE*)sBuf, false, false))
					dLemmas[i] = sBuf;
			}
		}

		if (dLemmas.GetLength() && tSettings.m_bIndexExactWords)
		{
			pExact = CloneKeyword(pNode);
			if (!pExact->m_dWords.GetLength())
				pExact->m_dWords.Add(tKeyword);

			pExact->m_dWords[0].m_sWord.SetSprintf("=%s", tKeyword.m_sWord.cstr());
			pExact->m_pParent = pNode;
		}

		if (!pExact && dLemmas.GetLength() <= 1)
		{
			// zero or one lemmas, update node in-place
			if (!pNode->m_dWords.GetLength())
				pNode->m_dWords.Add(tKeyword);
			if (dLemmas.GetLength())
			{
				pNode->m_dWords[0].m_sWord = dLemmas[0];
				pNode->m_dWords[0].m_bMorphed = true;
			}
		}
		else
		{
			// multiple lemmas, create an OR node
			pNode->SetOp(SPH_QUERY_OR);
			ARRAY_FOREACH(i, dLemmas)
			{
				pNode->m_dChildren.Add(new XQNode_t(pNode->m_dSpec));
				pNode->m_dChildren.Last()->m_pParent = pNode;
				XQKeyword_t& tLemma = pNode->m_dChildren.Last()->m_dWords.Add();
				tLemma.m_sWord = dLemmas[i];
				tLemma.m_iAtomPos = tKeyword.m_iAtomPos;
				tLemma.m_bFieldStart = tKeyword.m_bFieldStart;
				tLemma.m_bFieldEnd = tKeyword.m_bFieldEnd;
				tLemma.m_bMorphed = true;
			}
			pNode->m_dWords.Reset();
			if (pExact)
				pNode->m_dChildren.Add(pExact);
		}
	}


	/// AOT morph guesses transform
	/// replaces tokens with their respective morph guesses subtrees
	/// used in lemmatize_ru_all morphology processing mode that can generate multiple guesses
	/// in other modes, there is always exactly one morph guess, and the dictionary handles it
	/// Called from the daemon side (searchd)
	void TransformAotFilter(XQNode_t* pNode, const CSphWordforms* pWordforms, const CSphIndexSettings& tSettings)
	{
		// case one, regular operator (and empty nodes)
		ARRAY_FOREACH(i, pNode->m_dChildren)
			TransformAotFilter(pNode->m_dChildren[i], pWordforms, tSettings);
		if (pNode->m_dChildren.GetLength() || pNode->m_dWords.GetLength() == 0)
			return;

		// case two, operator on a bag of words
		// FIXME? check phrase vs expand_keywords vs lemmatize_ru_all?
		if (pNode->m_dWords.GetLength()
			&& (pNode->GetOp() == SPH_QUERY_PHRASE || pNode->GetOp() == SPH_QUERY_PROXIMITY || pNode->GetOp() == SPH_QUERY_QUORUM))
		{
			assert(pNode->m_dWords.GetLength());

			ARRAY_FOREACH(i, pNode->m_dWords)
			{
				XQNode_t* pNew = new XQNode_t(pNode->m_dSpec);
				pNew->m_pParent = pNode;
				pNew->m_iAtomPos = pNode->m_dWords[i].m_iAtomPos;
				pNode->m_dChildren.Add(pNew);
				TransformAotFilterKeyword(pNew, pNode->m_dWords[i], pWordforms, tSettings);
			}

			pNode->m_dWords.Reset();
			pNode->m_bVirtuallyPlain = true;
			return;
		}

		// case three, plain old single keyword
		assert(pNode->m_dWords.GetLength() == 1);
		TransformAotFilterKeyword(pNode, pNode->m_dWords[0], pWordforms, tSettings);
	}


	// transform the "one two three"/1 quorum into one|two|three (~40% faster)
	static void TransformQuorum(XQNode_t** ppNode)
	{
		XQNode_t*& pNode = *ppNode;

		// recurse non-quorum nodes
		if (pNode->GetOp() != SPH_QUERY_QUORUM)
		{
			ARRAY_FOREACH(i, pNode->m_dChildren)
				TransformQuorum(&pNode->m_dChildren[i]);
			return;
		}

		// skip quorums with thresholds other than 1
		if (pNode->m_iOpArg != 1)
			return;

		// transform quorums with a threshold of 1 only
		assert(pNode->GetOp() == SPH_QUERY_QUORUM && pNode->m_dChildren.GetLength() == 0);
		CSphVector<XQNode_t*> dArgs;
		ARRAY_FOREACH(i, pNode->m_dWords)
		{
			XQNode_t* pAnd = new XQNode_t(pNode->m_dSpec);
			pAnd->m_dWords.Add(pNode->m_dWords[i]);
			dArgs.Add(pAnd);
		}
		pNode->m_dWords.Reset();
		pNode->SetOp(SPH_QUERY_OR, dArgs);
	}


	void sphTransformExtendedQuery(XQNode_t** ppNode, const CSphIndexSettings& tSettings, bool bHasBooleanOptimization, const ISphKeywordsStat* pKeywords)
	{
		TransformQuorum(ppNode);
		(*ppNode)->Check(true);
		TransformNear(ppNode);
		(*ppNode)->Check(true);
		if (tSettings.m_eBigramIndex != SPH_BIGRAM_NONE)
			TransformBigrams(*ppNode, tSettings);
		TagExcluded(*ppNode, false);
		(*ppNode)->Check(true);

		// boolean optimization
		if (bHasBooleanOptimization)
			sphOptimizeBoolean(ppNode, pKeywords);
	}


	//////////////////////////////////////////



#define SPH_EXTNODE_STACK_SIZE 160

	bool sphCheckQueryHeight(const XQNode_t* pRoot, CSphString& sError)
	{
		int iHeight = 0;
		if (pRoot)
			iHeight = sphQueryHeightCalc(pRoot);

		int64_t iQueryStack = sphGetStackUsed() + iHeight * SPH_EXTNODE_STACK_SIZE;
		bool bValid = (g_iThreadStackSize >= iQueryStack);
		if (!bValid)
			sError.SetSprintf("query too complex, not enough stack (thread_stack=%dK or higher required)",
				(int)((iQueryStack + 1024 - (iQueryStack % 1024)) / 1024));
		return bValid;
	}


	static XQNode_t* ExpandKeyword(XQNode_t* pNode, const CSphIndexSettings& tSettings)
	{
		assert(pNode);

		XQNode_t* pExpand = new XQNode_t(pNode->m_dSpec);
		pExpand->SetOp(SPH_QUERY_OR, pNode);

		if (tSettings.m_iMinInfixLen > 0)
		{
			assert(pNode->m_dChildren.GetLength() == 0);
			assert(pNode->m_dWords.GetLength() == 1);
			XQNode_t* pInfix = CloneKeyword(pNode);
			pInfix->m_dWords[0].m_sWord.SetSprintf("*%s*", pNode->m_dWords[0].m_sWord.cstr());
			pInfix->m_pParent = pExpand;
			pExpand->m_dChildren.Add(pInfix);
		}
		else if (tSettings.m_iMinPrefixLen > 0)
		{
			assert(pNode->m_dChildren.GetLength() == 0);
			assert(pNode->m_dWords.GetLength() == 1);
			XQNode_t* pPrefix = CloneKeyword(pNode);
			pPrefix->m_dWords[0].m_sWord.SetSprintf("%s*", pNode->m_dWords[0].m_sWord.cstr());
			pPrefix->m_pParent = pExpand;
			pExpand->m_dChildren.Add(pPrefix);
		}

		if (tSettings.m_bIndexExactWords)
		{
			assert(pNode->m_dChildren.GetLength() == 0);
			assert(pNode->m_dWords.GetLength() == 1);
			XQNode_t* pExact = CloneKeyword(pNode);
			pExact->m_dWords[0].m_sWord.SetSprintf("=%s", pNode->m_dWords[0].m_sWord.cstr());
			pExact->m_pParent = pExpand;
			pExpand->m_dChildren.Add(pExact);
		}

		return pExpand;
	}

	XQNode_t* sphQueryExpandKeywords(XQNode_t* pNode, const CSphIndexSettings& tSettings)
	{
		// only if expansion makes sense at all
		if (tSettings.m_iMinInfixLen <= 0 && tSettings.m_iMinPrefixLen <= 0 && !tSettings.m_bIndexExactWords)
			return pNode;

		// process children for composite nodes
		if (pNode->m_dChildren.GetLength())
		{
			ARRAY_FOREACH(i, pNode->m_dChildren)
			{
				pNode->m_dChildren[i] = sphQueryExpandKeywords(pNode->m_dChildren[i], tSettings);
				pNode->m_dChildren[i]->m_pParent = pNode;
			}
			return pNode;
		}

		// if that's a phrase/proximity node, create a very special, magic phrase/proximity node
		if (pNode->GetOp() == SPH_QUERY_PHRASE || pNode->GetOp() == SPH_QUERY_PROXIMITY || pNode->GetOp() == SPH_QUERY_QUORUM)
		{
			assert(pNode->m_dWords.GetLength() > 1);
			ARRAY_FOREACH(i, pNode->m_dWords)
			{
				XQNode_t* pWord = new XQNode_t(pNode->m_dSpec);
				pWord->m_dWords.Add(pNode->m_dWords[i]);
				pNode->m_dChildren.Add(ExpandKeyword(pWord, tSettings));
				pNode->m_dChildren.Last()->m_iAtomPos = pNode->m_dWords[i].m_iAtomPos;
				pNode->m_dChildren.Last()->m_pParent = pNode;
			}
			pNode->m_dWords.Reset();
			pNode->m_bVirtuallyPlain = true;
			return pNode;
		}

		// skip empty plain nodes
		if (pNode->m_dWords.GetLength() <= 0)
			return pNode;

		// process keywords for plain nodes
		assert(pNode->m_dWords.GetLength() == 1);

		XQKeyword_t& tKeyword = pNode->m_dWords[0];
		if (tKeyword.m_sWord.Begins("=")
			|| tKeyword.m_sWord.Begins("*")
			|| tKeyword.m_sWord.Ends("*"))
		{
			return pNode;
		}

		// do the expansion
		return ExpandKeyword(pNode, tSettings);
	}


	struct BinaryNode_t
	{
		int m_iLo;
		int m_iHi;
	};

	static void BuildExpandedTree(const XQKeyword_t& tRootWord, ISphWordlist::Args_t& dWordSrc, XQNode_t* pRoot)
	{
		assert(dWordSrc.m_dExpanded.GetLength());
		pRoot->m_dWords.Reset();

		// build a binary tree from all the other expansions
		CSphVector<BinaryNode_t> dNodes;
		dNodes.Reserve(dWordSrc.m_dExpanded.GetLength());

		XQNode_t* pCur = pRoot;

		dNodes.Add();
		dNodes.Last().m_iLo = 0;
		dNodes.Last().m_iHi = (dWordSrc.m_dExpanded.GetLength() - 1);

		while (dNodes.GetLength())
		{
			BinaryNode_t tNode = dNodes.Pop();
			if (tNode.m_iHi < tNode.m_iLo)
			{
				pCur = pCur->m_pParent;
				continue;
			}

			int iMid = (tNode.m_iLo + tNode.m_iHi) / 2;
			dNodes.Add();
			dNodes.Last().m_iLo = tNode.m_iLo;
			dNodes.Last().m_iHi = iMid - 1;
			dNodes.Add();
			dNodes.Last().m_iLo = iMid + 1;
			dNodes.Last().m_iHi = tNode.m_iHi;

			if (pCur->m_dWords.GetLength())
			{
				assert(pCur->m_dWords.GetLength() == 1);
				XQNode_t* pTerm = CloneKeyword(pRoot);
				Swap(pTerm->m_dWords, pCur->m_dWords);
				pCur->m_dChildren.Add(pTerm);
				pTerm->m_pParent = pCur;
			}

			XQNode_t* pChild = CloneKeyword(pRoot);
			pChild->m_dWords.Add(tRootWord);
			pChild->m_dWords.Last().m_sWord = dWordSrc.GetWordExpanded(iMid);
			pChild->m_dWords.Last().m_bExpanded = true;
			pChild->m_bNotWeighted = pRoot->m_bNotWeighted;

			pChild->m_pParent = pCur;
			pCur->m_dChildren.Add(pChild);
			pCur->SetOp(SPH_QUERY_OR);

			pCur = pChild;
		}
	}


	/// do wildcard expansion for keywords dictionary
	/// (including prefix and infix expansion)
	XQNode_t* sphExpandXQNode(XQNode_t* pNode, ExpansionContext_t& tCtx)
	{
		assert(pNode);
		assert(tCtx.m_pResult);

		// process children for composite nodes
		if (pNode->m_dChildren.GetLength())
		{
			ARRAY_FOREACH(i, pNode->m_dChildren)
			{
				pNode->m_dChildren[i] = sphExpandXQNode(pNode->m_dChildren[i], tCtx);
				pNode->m_dChildren[i]->m_pParent = pNode;
			}
			return pNode;
		}

		// if that's a phrase/proximity node, create a very special, magic phrase/proximity node
		if (pNode->GetOp() == SPH_QUERY_PHRASE || pNode->GetOp() == SPH_QUERY_PROXIMITY || pNode->GetOp() == SPH_QUERY_QUORUM)
		{
			assert(pNode->m_dWords.GetLength() > 1);
			ARRAY_FOREACH(i, pNode->m_dWords)
			{
				XQNode_t* pWord = new XQNode_t(pNode->m_dSpec);
				pWord->m_dWords.Add(pNode->m_dWords[i]);
				pNode->m_dChildren.Add(sphExpandXQNode(pWord, tCtx));
				pNode->m_dChildren.Last()->m_iAtomPos = pNode->m_dWords[i].m_iAtomPos;
				pNode->m_dChildren.Last()->m_pParent = pNode;

				// tricky part
				// current node may have field/zone limits attached
				// normally those get pushed down during query parsing
				// but here we create nodes manually and have to push down limits too
				pWord->CopySpecs(pNode);
			}
			pNode->m_dWords.Reset();
			pNode->m_bVirtuallyPlain = true;
			return pNode;
		}

		// skip empty plain nodes
		if (pNode->m_dWords.GetLength() <= 0)
			return pNode;

		// process keywords for plain nodes
		assert(pNode->m_dChildren.GetLength() == 0);
		assert(pNode->m_dWords.GetLength() == 1);

		// check the wildcards
		const char* sFull = pNode->m_dWords[0].m_sWord.cstr();

		// no wildcards, or just wildcards? do not expand
		if (!sphHasExpandableWildcards(sFull))
			return pNode;

		bool bUseTermMerge = (tCtx.m_bMergeSingles && pNode->m_dSpec.m_dZones.GetLength() == 0);
		ISphWordlist::Args_t tWordlist(bUseTermMerge, tCtx.m_iExpansionLimit, tCtx.m_bHasMorphology, tCtx.m_eHitless, tCtx.m_pIndexData);

		if (!sphExpandGetWords(sFull, tCtx, tWordlist))
		{
			tCtx.m_pResult->m_sWarning.SetSprintf("Query word length is less than min %s length. word: '%s' ", (tCtx.m_iMinInfixLen > 0 ? "infix" : "prefix"), sFull);
			return pNode;
		}

		// no real expansions?
		// mark source word as expanded to prevent warning on terms mismatch in statistics
		if (!tWordlist.m_dExpanded.GetLength() && !tWordlist.m_pPayload)
		{
			tCtx.m_pResult->AddStat(pNode->m_dWords.Begin()->m_sWord, 0, 0);
			pNode->m_dWords.Begin()->m_bExpanded = true;
			return pNode;
		}

		// copy the original word (iirc it might get overwritten),
		const XQKeyword_t tRootWord = pNode->m_dWords[0];
		tCtx.m_pResult->AddStat(tRootWord.m_sWord, tWordlist.m_iTotalDocs, tWordlist.m_iTotalHits);

		// and build a binary tree of all the expansions
		if (tWordlist.m_dExpanded.GetLength())
		{
			BuildExpandedTree(tRootWord, tWordlist, pNode);
		}

		if (tWordlist.m_pPayload)
		{
			ISphSubstringPayload* pPayload = tWordlist.m_pPayload;
			tWordlist.m_pPayload = NULL;
			tCtx.m_pPayloads->Add(pPayload);

			if (pNode->m_dWords.GetLength())
			{
				// all expanded fit to single payload
				pNode->m_dWords.Begin()->m_bExpanded = true;
				pNode->m_dWords.Begin()->m_pPayload = pPayload;
			}
			else
			{
				// payload added to expanded binary tree
				assert(pNode->GetOp() == SPH_QUERY_OR);
				assert(pNode->m_dChildren.GetLength());

				XQNode_t* pSubstringNode = new XQNode_t(pNode->m_dSpec);
				pSubstringNode->SetOp(SPH_QUERY_OR);

				XQKeyword_t tSubstringWord = tRootWord;
				tSubstringWord.m_bExpanded = true;
				tSubstringWord.m_pPayload = pPayload;
				pSubstringNode->m_dWords.Add(tSubstringWord);

				pNode->m_dChildren.Add(pSubstringNode);
				pSubstringNode->m_pParent = pNode;
			}
		}

		return pNode;
	}

	////////////////////////////

	//suggestion stuff: need to be moved elsewhere!

	// sort by distance(uLen) desc, checkpoint index(uOff) asc
	struct CmpHistogram_fn
	{
		inline bool IsLess(const Slice_t& a, const Slice_t& b) const
		{
			return (a.m_uLen > b.m_uLen || (a.m_uLen == b.m_uLen && a.m_uOff < b.m_uOff));
		}
	};

	bool SuggestResult_t::SetWord(const char* sWord, const ISphTokenizer* pTok, bool bUseLastWord)
	{
		CSphScopedPtr<ISphTokenizer> pTokenizer(pTok->Clone(ESphTokenizerClone::SPH_CLONE_QUERY_LIGHTWEIGHT));
		pTokenizer->SetBuffer((BYTE*)sWord, strlen(sWord));

		const BYTE* pToken = pTokenizer->GetToken();
		for (; pToken != NULL; )
		{
			m_sWord = (const char*)pToken;
			if (!bUseLastWord)
				break;

			if (pTokenizer->TokenIsBlended())
				pTokenizer->SkipBlended();
			pToken = pTokenizer->GetToken();
		}


		m_iLen = m_sWord.Length();
		m_iCodepoints = DecodeUtf8((const BYTE*)m_sWord.cstr(), m_dCodepoints);
		m_bUtf8 = (m_iCodepoints != m_iLen);

		bool bValidWord = (m_iCodepoints >= 3);
		if (bValidWord)
			sphBuildNGrams(m_sWord.cstr(), m_iLen, '\0', m_dTrigrams);

		return bValidWord;
	}

	void SuggestResult_t::Flattern(int iLimit)
	{
		int iCount = Min(m_dMatched.GetLength(), iLimit);
		m_dMatched.Resize(iCount);
	}

	struct SliceInt_t
	{
		int		m_iOff;
		int		m_iEnd;
	};

	static void SuggestGetChekpoints(const ISphWordlistSuggest* pWordlist, int iInfixCodepointBytes, const CSphVector<char>& dTrigrams, CSphVector<Slice_t>& dCheckpoints, SuggestResult_t& tStats)
	{
		CSphVector<DWORD> dWordCp; // FIXME!!! add mask that trigram matched
		// v1 - current index, v2 - end index
		CSphVector<SliceInt_t> dMergeIters;

		int iReserveLen = 0;
		int iLastLen = 0;
		const char* sTrigram = dTrigrams.Begin();
		const char* sTrigramEnd = sTrigram + dTrigrams.GetLength();
		for (;; )
		{
			int iTrigramLen = strlen(sTrigram);
			int iInfixLen = sphGetInfixLength(sTrigram, iTrigramLen, iInfixCodepointBytes);

			// count how many checkpoint we will get
			iReserveLen = Max(iReserveLen,(int) dWordCp.GetLength() - iLastLen);
			iLastLen = dWordCp.GetLength();

			dMergeIters.Add().m_iOff = dWordCp.GetLength();
			pWordlist->SuffixGetChekpoints(tStats, sTrigram, iInfixLen, dWordCp);

			sTrigram += iTrigramLen + 1;
			if (sTrigram >= sTrigramEnd)
				break;

			if (sphInterrupted())
				return;
		}
		if (!dWordCp.GetLength())
			return;

		for (int i = 0; i < dMergeIters.GetLength() - 1; i++)
		{
			dMergeIters[i].m_iEnd = dMergeIters[i + 1].m_iOff;
		}
		dMergeIters.Last().m_iEnd = dWordCp.GetLength();

		// v1 - checkpoint index, v2 - checkpoint count
		dCheckpoints.Reserve(iReserveLen);
		dCheckpoints.Resize(0);

		// merge sorting of already ordered checkpoints
		for (;; )
		{
			DWORD iMinCP = UINT_MAX;
			DWORD iMinIndex = UINT_MAX;
			ARRAY_FOREACH(i, dMergeIters)
			{
				const SliceInt_t& tElem = dMergeIters[i];
				if (tElem.m_iOff < tElem.m_iEnd && dWordCp[tElem.m_iOff] < iMinCP)
				{
					iMinIndex = i;
					iMinCP = dWordCp[tElem.m_iOff];
				}
			}

			if (iMinIndex == UINT_MAX)
				break;

			if (dCheckpoints.GetLength() == 0 || iMinCP != dCheckpoints.Last().m_uOff)
			{
				dCheckpoints.Add().m_uOff = iMinCP;
				dCheckpoints.Last().m_uLen = 1;
			}
			else
			{
				dCheckpoints.Last().m_uLen++;
			}

			assert(iMinIndex != UINT_MAX && iMinCP != UINT_MAX);
			assert(dMergeIters[iMinIndex].m_iOff < dMergeIters[iMinIndex].m_iEnd);
			dMergeIters[iMinIndex].m_iOff++;
		}
		dCheckpoints.Sort(CmpHistogram_fn());
	}


	struct CmpSuggestOrder_fn
	{
		bool IsLess(const SuggestWord_t& a, const SuggestWord_t& b)
		{
			if (a.m_iDistance == b.m_iDistance)
				return a.m_iDocs > b.m_iDocs;

			return a.m_iDistance < b.m_iDistance;
		}
	};

	void SuggestMergeDocs(CSphVector<SuggestWord_t>& dMatched)
	{
		if (!dMatched.GetLength())
			return;

		dMatched.Sort(bind(&SuggestWord_t::m_iNameHash));

		int iSrc = 1;
		int iDst = 1;
		while (iSrc < dMatched.GetLength())
		{
			if (dMatched[iDst - 1].m_iNameHash == dMatched[iSrc].m_iNameHash)
			{
				dMatched[iDst - 1].m_iDocs += dMatched[iSrc].m_iDocs;
				iSrc++;
			}
			else
			{
				dMatched[iDst++] = dMatched[iSrc++];
			}
		}

		dMatched.Resize(iDst);
	}

	template <bool SINGLE_BYTE_CHAR>
	void SuggestMatchWords(const ISphWordlistSuggest* pWordlist, const CSphVector<Slice_t>& dCheckpoints, const SuggestArgs_t& tArgs, SuggestResult_t& tRes)
	{
		// walk those checkpoints, check all their words

		const int iMinWordLen = (tArgs.m_iDeltaLen > 0 ? Max(0, tRes.m_iCodepoints - tArgs.m_iDeltaLen) : -1);
		const int iMaxWordLen = (tArgs.m_iDeltaLen > 0 ? tRes.m_iCodepoints + tArgs.m_iDeltaLen : INT_MAX);

		CSphHash<int> dHashTrigrams;
		const char* s = tRes.m_dTrigrams.Begin();
		const char* sEnd = s + tRes.m_dTrigrams.GetLength();
		while (s < sEnd)
		{
			dHashTrigrams.Add(sphCRC32(s), 1);
			while (*s) s++;
			s++;
		}
		int dCharOffset[SPH_MAX_WORD_LEN + 1];
		int dDictWordCodepoints[SPH_MAX_WORD_LEN];

		const int iQLen = Max(tArgs.m_iQueueLen, tArgs.m_iLimit);
		const int iRejectThr = tArgs.m_iRejectThr;
		int iQueueRejected = 0;
		int iLastBad = 0;
		bool bSorted = true;
		const bool bMergeWords = tRes.m_bMergeWords;
		const bool bHasExactDict = tRes.m_bHasExactDict;
		const int iMaxEdits = tArgs.m_iMaxEdits;
		const bool bNonCharAllowed = tArgs.m_bNonCharAllowed;
		tRes.m_dMatched.Reserve(iQLen * 2);
		CmpSuggestOrder_fn fnCmp;

		ARRAY_FOREACH(i, dCheckpoints)
		{
			DWORD iCP = dCheckpoints[i].m_uOff;
			pWordlist->SetCheckpoint(tRes, iCP);

			ISphWordlistSuggest::DictWord_t tWord;
			while (pWordlist->ReadNextWord(tRes, tWord))
			{
				const char* sDictWord = tWord.m_sWord;
				int iDictWordLen = tWord.m_iLen;
				int iDictCodepoints = iDictWordLen;

				// for stemmer \ lematizer suggest should match only original words
				if (bHasExactDict && sDictWord[0] != MAGIC_WORD_HEAD_NONSTEMMED)
					continue;

				if (bHasExactDict)
				{
					// skip head MAGIC_WORD_HEAD_NONSTEMMED char
					sDictWord++;
					iDictWordLen--;
					iDictCodepoints--;
				}

				if_const(SINGLE_BYTE_CHAR)
				{
					if (iDictWordLen <= iMinWordLen || iDictWordLen >= iMaxWordLen)
						continue;
				}

				int iChars = 0;

				const BYTE* s = (const BYTE*)sDictWord;
				const BYTE* sEnd = s + iDictWordLen;
				bool bGotNonChar = false;
				while (!bGotNonChar && s < sEnd)
				{
					dCharOffset[iChars] = s - (const BYTE*)sDictWord;
					int iCode = sphUTF8Decode(s);
					if (!bNonCharAllowed)
						bGotNonChar = (iCode < 'A' || (iCode > 'Z' && iCode < 'a')); // skip words with any numbers or special characters

					if_const(!SINGLE_BYTE_CHAR)
					{
						dDictWordCodepoints[iChars] = iCode;
					}
					iChars++;
				}
				dCharOffset[iChars] = s - (const BYTE*)sDictWord;
				iDictCodepoints = iChars;

				if_const(!SINGLE_BYTE_CHAR)
				{
					if (iDictCodepoints <= iMinWordLen || iDictCodepoints >= iMaxWordLen)
						continue;
				}

				// skip word in case of non char symbol found
				if (bGotNonChar)
					continue;

				// FIXME!!! should we skip in such cases
				// utf8 reference word			!=	single byte dictionary word
				// single byte reference word	!=	utf8 dictionary word

				bool bGotMatch = false;
				for (int iChar = 0; iChar <= iDictCodepoints - 3 && !bGotMatch; iChar++)
				{
					int iStart = dCharOffset[iChar];
					int iEnd = dCharOffset[iChar + 3];
					bGotMatch = (dHashTrigrams.Find(sphCRC32(sDictWord + iStart, iEnd - iStart)) != NULL);
				}

				// skip word in case of no trigrams matched
				if (!bGotMatch)
					continue;

				int iDist = INT_MAX;
				if_const(SINGLE_BYTE_CHAR)
					iDist = sphLevenshtein(tRes.m_sWord.cstr(), tRes.m_iLen, sDictWord, iDictWordLen);
				else
					iDist = sphLevenshtein(tRes.m_dCodepoints, tRes.m_iCodepoints, dDictWordCodepoints, iDictCodepoints);

				// skip word in case of too many edits
				if (iDist > iMaxEdits)
					continue;

				SuggestWord_t tElem;
				tElem.m_iNameOff = tRes.m_dBuf.GetLength();
				tElem.m_iLen = iDictWordLen;
				tElem.m_iDistance = iDist;
				tElem.m_iDocs = tWord.m_iDocs;

				// store in k-buffer up to 2*QLen words
				if (!iLastBad || fnCmp.IsLess(tElem, tRes.m_dMatched[iLastBad]))
				{
					if (bMergeWords)
						tElem.m_iNameHash = sphCRC32(sDictWord, iDictWordLen);

					tRes.m_dMatched.Add(tElem);
					BYTE* sWord = tRes.m_dBuf.AddN(iDictWordLen + 1);
					memcpy(sWord, sDictWord, iDictWordLen);
					sWord[iDictWordLen] = '\0';
					iQueueRejected = 0;
					bSorted = false;
				}
				else
				{
					iQueueRejected++;
				}

				// sort k-buffer in case of threshold overflow
				if (tRes.m_dMatched.GetLength() > iQLen * 2)
				{
					if (bMergeWords)
						SuggestMergeDocs(tRes.m_dMatched);
					int iTotal = tRes.m_dMatched.GetLength();
					tRes.m_dMatched.Sort(CmpSuggestOrder_fn());
					bSorted = true;

					// there might be less than necessary elements after merge operation
					if (iTotal > iQLen)
					{
						iQueueRejected += iTotal - iQLen;
						tRes.m_dMatched.Resize(iQLen);
					}
					iLastBad = tRes.m_dMatched.GetLength() - 1;
				}
			}

			if (sphInterrupted())
				break;

			// stop dictionary unpacking in case queue rejects a lot of matched words
			if (iQueueRejected && iQueueRejected > iQLen * iRejectThr)
				break;
		}

		// sort at least once or any unsorted
		if (!bSorted)
		{
			if (bMergeWords)
				SuggestMergeDocs(tRes.m_dMatched);
			tRes.m_dMatched.Sort(CmpSuggestOrder_fn());
		}
	}


	void sphGetSuggest(const ISphWordlistSuggest* pWordlist, int iInfixCodepointBytes, const SuggestArgs_t& tArgs, SuggestResult_t& tRes)
	{
		assert(pWordlist);

		CSphVector<Slice_t> dCheckpoints;
		SuggestGetChekpoints(pWordlist, iInfixCodepointBytes, tRes.m_dTrigrams, dCheckpoints, tRes);
		if (!dCheckpoints.GetLength())
			return;

		if (tRes.m_bUtf8)
			SuggestMatchWords<false>(pWordlist, dCheckpoints, tArgs, tRes);
		else
			SuggestMatchWords<true>(pWordlist, dCheckpoints, tArgs, tRes);

		if (sphInterrupted())
			return;

		tRes.Flattern(tArgs.m_iLimit);
	}


	// all indexes should produce same terms for same query
	void SphWordStatChecker_t::Set(const SmallStringHash_T<CSphQueryResultMeta::WordStat_t>& hStat)
	{
		m_dSrcWords.Reserve(hStat.GetLength());
		hStat.IterateStart();
		while (hStat.IterateNext())
		{
			m_dSrcWords.Add(sphFNV64(hStat.IterateGetKey().cstr()));
		}
		m_dSrcWords.Sort();
	}


	void SphWordStatChecker_t::DumpDiffer(const SmallStringHash_T<CSphQueryResultMeta::WordStat_t>& hStat, const char* sIndex, CSphString& sWarning) const
	{
		if (!m_dSrcWords.GetLength())
			return;

		CSphStringBuilder tWarningBuilder;
		hStat.IterateStart();
		while (hStat.IterateNext())
		{
			uint64_t uHash = sphFNV64(hStat.IterateGetKey().cstr());
			if (!m_dSrcWords.BinarySearch(uHash))
			{
				if (!tWarningBuilder.Length())
				{
					if (sIndex)
						tWarningBuilder.Appendf("index '%s': ", sIndex);

					tWarningBuilder.Appendf("query word(s) mismatch: %s", hStat.IterateGetKey().cstr());
				}
				else
				{
					tWarningBuilder.Appendf(", %s", hStat.IterateGetKey().cstr());
				}
			}
		}

		if (tWarningBuilder.Length())
			sWarning = tWarningBuilder.cstr();
	}

}