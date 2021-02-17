#include "neo/query/node_cache.h"
#include "neo/query/extra.h"
#include "neo/query/ext_term.h"
#include "neo/platform/thread.h"
#include "neo/dict/dict.h"
#include "neo/query/field_mask.h"
#include "neo/index/ft_index.h"
#include "neo/core/globals.h"

namespace NEO {

ExtNode_i::ExtNode_i()
	: m_iAtomPos(0)
	, m_iStride(0)
	, m_pDocinfo(NULL)
{
	m_dDocs[0].m_uDocid = DOCID_MAX;
	m_dHits[0].m_uDocid = DOCID_MAX;
}


 ISphQword* CreateQueryWord(const XQKeyword_t& tWord, const ISphQwordSetup& tSetup, CSphDict* pZonesDict )
{
	BYTE sTmp[3 * SPH_MAX_WORD_LEN + 16];
	strncpy((char*)sTmp, tWord.m_sWord.cstr(), sizeof(sTmp));
	sTmp[sizeof(sTmp) - 1] = '\0';

	ISphQword* pWord = tSetup.QwordSpawn(tWord);
	pWord->m_sWord = tWord.m_sWord;
	CSphDict* pDict = pZonesDict ? pZonesDict : tSetup.m_pDict;
	pWord->m_uWordID = tWord.m_bMorphed
		? pDict->GetWordIDNonStemmed(sTmp)
		: pDict->GetWordID(sTmp);
	pWord->m_sDictWord = (char*)sTmp;
	pWord->m_bExpanded = tWord.m_bExpanded;
	tSetup.QwordSetup(pWord);

	if (tWord.m_bFieldStart && tWord.m_bFieldEnd)	pWord->m_iTermPos = TERM_POS_FIELD_STARTEND;
	else if (tWord.m_bFieldStart)					pWord->m_iTermPos = TERM_POS_FIELD_START;
	else if (tWord.m_bFieldEnd)					pWord->m_iTermPos = TERM_POS_FIELD_END;
	else											pWord->m_iTermPos = TERM_POS_NONE;

	pWord->m_fBoost = tWord.m_fBoost;
	pWord->m_iAtomPos = tWord.m_iAtomPos;
	return pWord;
}


ExtNode_i* ExtNode_i::Create(const XQKeyword_t& tWord, const XQNode_t* pNode, const ISphQwordSetup& tSetup)
{
	return Create(CreateQueryWord(tWord, tSetup), pNode, tSetup);
}

ExtNode_i* ExtNode_i::Create(ISphQword* pQword, const XQNode_t* pNode, const ISphQwordSetup& tSetup)
{
	assert(pQword);

	if (pNode->m_dSpec.m_iFieldMaxPos)
		pQword->m_iTermPos = TERM_POS_FIELD_LIMIT;

	if (pNode->m_dSpec.m_dZones.GetLength())
		pQword->m_iTermPos = TERM_POS_ZONES;

	if (!pQword->m_bHasHitlist)
	{
		if (tSetup.m_pWarning && pQword->m_iTermPos != TERM_POS_NONE)
			tSetup.m_pWarning->SetSprintf("hitlist unavailable, position limit ignored");
		return new ExtTermHitless_c(pQword, pNode->m_dSpec.m_dFieldMask, tSetup, pNode->m_bNotWeighted);
	}
	switch (pQword->m_iTermPos)
	{
	case TERM_POS_FIELD_STARTEND:	return new ExtTermPos_c<TERM_POS_FIELD_STARTEND>(pQword, pNode, tSetup);
	case TERM_POS_FIELD_START:		return new ExtTermPos_c<TERM_POS_FIELD_START>(pQword, pNode, tSetup);
	case TERM_POS_FIELD_END:		return new ExtTermPos_c<TERM_POS_FIELD_END>(pQword, pNode, tSetup);
	case TERM_POS_FIELD_LIMIT:		return new ExtTermPos_c<TERM_POS_FIELD_LIMIT>(pQword, pNode, tSetup);
	case TERM_POS_ZONES:			return new ExtTermPos_c<TERM_POS_ZONES>(pQword, pNode, tSetup);
	default:						return new ExtTerm_c(pQword, pNode->m_dSpec.m_dFieldMask, tSetup, pNode->m_bNotWeighted);
	}
}



CSphQueryNodeCache::CSphQueryNodeCache(int iCells, int iMaxCachedDocs, int iMaxCachedHits)
{
	m_pPool = NULL;
	if (iCells > 0 && iMaxCachedHits > 0 && iMaxCachedDocs > 0)
	{
		m_pPool = new NodeCacheContainer_t[iCells];
		for (int i = 0; i < iCells; i++)
			m_pPool[i].m_pNodeCache = this;
	}
	m_iMaxCachedDocs = iMaxCachedDocs / sizeof(ExtDoc_t);
	m_iMaxCachedHits = iMaxCachedHits / sizeof(ExtHit_t);
}

CSphQueryNodeCache::~CSphQueryNodeCache()
{
	SafeDeleteArray(m_pPool);
}

ExtNode_i* CSphQueryNodeCache::CreateProxy(ExtNode_i* pChild, const XQNode_t* pRawChild, const ISphQwordSetup& tSetup)
{
	if (m_iMaxCachedDocs <= 0 || m_iMaxCachedHits <= 0)
		return pChild;

	assert(pRawChild);
	return m_pPool[pRawChild->GetOrder()].CreateCachedWrapper(pChild, pRawChild, tSetup);
}


ExtNode_i* NodeCacheContainer_t::CreateCachedWrapper(ExtNode_i* pChild, const XQNode_t* pRawChild, const ISphQwordSetup& tSetup)
{
	if (!m_StateOk)
		return pChild;

	// wow! we have a virgin!
	if (!m_Docs.GetLength())
	{
		m_iRefCount = pRawChild->GetCount();
		m_pSetup = &tSetup;
	}
	return new ExtNodeCached_t(this, pChild);
}


bool NodeCacheContainer_t::WarmupCache(ExtNode_i* pChild, int iQwords)
{
	assert(pChild);
	assert(m_pSetup);

	m_iAtomPos = pChild->m_iAtomPos;
	const ExtDoc_t* pChunk = pChild->GetDocsChunk();
	int iStride = 0;

	if (pChunk && pChunk->m_pDocinfo)
		iStride = pChild->m_iStride;

	while (pChunk)
	{
		const ExtDoc_t* pChunkHits = pChunk;
		bool iHasDocs = false;
		for (; pChunk->m_uDocid != DOCID_MAX; pChunk++)
		{
			m_Docs.Add(*pChunk);
			// exclude number or Qwords from FIDF
			m_Docs.Last().m_fTFIDF *= iQwords;
			m_pNodeCache->m_iMaxCachedDocs--;
			if (iStride > 0)
			{
				// since vector will relocate the data on resize, do NOT fill new m_pDocinfo right now
				int iLen = m_InlineAttrs.GetLength();
				m_InlineAttrs.Resize(iLen + iStride);
				memcpy(&m_InlineAttrs[iLen], pChunk->m_pDocinfo, iStride * sizeof(CSphRowitem));
			}
			iHasDocs = true;
		}

		const ExtHit_t* pHits = NULL;
		if (iHasDocs)
		{
			while ((pHits = pChild->GetHitsChunk(pChunkHits)) != NULL)
			{
				for (; pHits->m_uDocid != DOCID_MAX; pHits++)
				{
					m_Hits.Add(*pHits);
					m_pNodeCache->m_iMaxCachedHits--;
				}
			}
		}

		// too many values, stop caching
		if (m_pNodeCache->m_iMaxCachedDocs < 0 || m_pNodeCache->m_iMaxCachedHits < 0)
		{
			Invalidate();
			pChild->Reset(*m_pSetup);
			m_pSetup = NULL;
			return false;
		}
		pChunk = pChild->GetDocsChunk();
	}

	if (iStride)
		ARRAY_FOREACH(i, m_Docs)
		m_Docs[i].m_pDocinfo = &m_InlineAttrs[i * iStride];

	m_Docs.Add().m_uDocid = DOCID_MAX;
	m_Hits.Add().m_uDocid = DOCID_MAX;
	pChild->Reset(*m_pSetup);
	m_pSetup = NULL;
	return true;
}


void NodeCacheContainer_t::Invalidate()
{
	m_pNodeCache->m_iMaxCachedDocs += m_Docs.GetLength();
	m_pNodeCache->m_iMaxCachedHits += m_Docs.GetLength();
	m_Docs.Reset();
	m_InlineAttrs.Reset();
	m_Hits.Reset();
	m_StateOk = false;
}


void ExtNodeCached_t::StepForwardToHitsFor(SphDocID_t uDocId)
{
	assert(m_pNode);
	assert(m_pNode->m_StateOk);

	CSphVector<ExtHit_t>& dHits = m_pNode->m_Hits;

	int iEnd = dHits.GetLength() - 1;
	if (m_iHitIndex >= iEnd)
		return;
	if (dHits[m_iHitIndex].m_uDocid == uDocId)
		return;

	// binary search for lower (most left) bound of the subset of values
	int iHitIndex = m_iHitIndex; // http://blog.gamedeff.com/?p=12
	while (iEnd - iHitIndex > 1)
	{
		if (uDocId<dHits[iHitIndex].m_uDocid || uDocId>dHits[iEnd].m_uDocid)
		{
			m_iHitIndex = -1;
			return;
		}
		int iMid = iHitIndex + (iEnd - iHitIndex) / 2;
		if (dHits[iMid].m_uDocid >= uDocId)
			iEnd = iMid;
		else
			iHitIndex = iMid;
	}
	m_iHitIndex = iEnd;
}

const ExtDoc_t* ExtNodeCached_t::GetDocsChunk()
{
	if (!m_pNode || !m_pChild)
		return NULL;

	if (!m_pNode->m_StateOk)
		return m_pChild->GetDocsChunk();

	if (m_iMaxTimer > 0 && sphMicroTimer() >= m_iMaxTimer)
	{
		if (m_pWarning)
			*m_pWarning = "query time exceeded max_query_time";
		return NULL;
	}

	int iDoc = Min(m_iDocIndex + MAX_DOCS - 1,(int) m_pNode->m_Docs.GetLength() - 1) - m_iDocIndex;
	memcpy(&m_dDocs[0], &m_pNode->m_Docs[m_iDocIndex], sizeof(ExtDoc_t) * iDoc);
	m_iDocIndex += iDoc;

	// funny trick based on the formula of FIDF calculation.
	for (int i = 0; i < iDoc; i++)
		m_dDocs[i].m_fTFIDF /= m_iQwords;

	return ReturnDocsChunk(iDoc, "cached");
}

const ExtHit_t* ExtNodeCached_t::GetHitsChunk(const ExtDoc_t* pMatched)
{
	if (!m_pNode || !m_pChild)
		return NULL;

	if (!m_pNode->m_StateOk)
		return m_pChild->GetHitsChunk(pMatched);

	if (!pMatched)
		return NULL;

	SphDocID_t uFirstMatch = pMatched->m_uDocid;

	// aim to the right document
	ExtDoc_t* pDoc = m_pHitDoc;
	m_pHitDoc = NULL;

	if (!pDoc)
	{
		// if we already emitted hits for this matches block, do not do that again
		if (uFirstMatch == m_uHitsOverFor)
			return NULL;

		// find match
		pDoc = m_dDocs;
		do
		{
			while (pDoc->m_uDocid < pMatched->m_uDocid) pDoc++;
			if (pDoc->m_uDocid == DOCID_MAX)
			{
				m_uHitsOverFor = uFirstMatch;
				return NULL; // matched docs block is over for me, gimme another one
			}

			while (pMatched->m_uDocid < pDoc->m_uDocid) pMatched++;
			if (pMatched->m_uDocid == DOCID_MAX)
			{
				m_uHitsOverFor = uFirstMatch;
				return NULL; // matched doc block did not yet begin for me, gimme another one
			}
		} while (pDoc->m_uDocid != pMatched->m_uDocid);

		// setup hitlist reader
		StepForwardToHitsFor(pDoc->m_uDocid);
	}

	// hit emission
	int iHit = 0;
	while (iHit < ExtNode_i::MAX_HITS - 1)
	{
		// get next hit
		ExtHit_t& tCachedHit = m_pNode->m_Hits[m_iHitIndex];
		if (tCachedHit.m_uDocid == DOCID_MAX)
			break;
		if (tCachedHit.m_uDocid != pDoc->m_uDocid)
		{
			// no more hits; get next acceptable document
			pDoc++;
			do
			{
				while (pDoc->m_uDocid < pMatched->m_uDocid) pDoc++;
				if (pDoc->m_uDocid == DOCID_MAX) { pDoc = NULL; break; } // matched docs block is over for me, gimme another one

				while (pMatched->m_uDocid < pDoc->m_uDocid) pMatched++;
				if (pMatched->m_uDocid == DOCID_MAX) { pDoc = NULL; break; } // matched doc block did not yet begin for me, gimme another one
			} while (pDoc->m_uDocid != pMatched->m_uDocid);

			if (!pDoc)
				break;
			assert(pDoc->m_uDocid == pMatched->m_uDocid);

			// setup hitlist reader
			StepForwardToHitsFor(pDoc->m_uDocid);
			continue;
		}
		m_iHitIndex++;
		m_dHits[iHit] = tCachedHit;
		m_dHits[iHit].m_uQuerypos = (WORD)(tCachedHit.m_uQuerypos + m_iAtomPos - m_pNode->m_iAtomPos);
		iHit++;
	}

	m_pHitDoc = pDoc;
	if (iHit == 0 || iHit < MAX_HITS - 1)
		m_uHitsOverFor = uFirstMatch;

	assert(iHit >= 0 && iHit < MAX_HITS);
	m_dHits[iHit].m_uDocid = DOCID_MAX;
	return (iHit != 0) ? m_dHits : NULL;
}

/////////////////////////

template < TermPosFilter_e T, class ExtBase >
const ExtDoc_t* ExtConditional<T, ExtBase>::GetDocsChunk()
{
	SphDocID_t uSkipID = m_uLastID;
	// fetch more docs if needed
	if (!m_pRawDocs)
	{
		m_pRawDocs = ExtBase::GetDocsChunk();
		if (!m_pRawDocs)
			return NULL;

		m_pRawDoc = m_pRawDocs;
		m_pRawHit = NULL;
		uSkipID = 0;
	}

	// filter the hits, and build the documents list
	int iMyDoc = 0;
	int iMyHit = 0;

	const ExtDoc_t* pDoc = m_pRawDoc; // just a shortcut
	const ExtHit_t* pHit = m_pRawHit;
	SphDocID_t uLastID = m_uLastID = 0;

	CSphRowitem* pDocinfo = ExtBase::m_pDocinfo;
	for (;; )
	{
		// try to fetch more hits for current raw docs block if we're out
		if (!pHit || pHit->m_uDocid == DOCID_MAX)
			pHit = ExtBase::GetHitsChunk(m_pRawDocs);

		// did we touch all the hits we had? if so, we're fully done with
		// current raw docs block, and should start a new one
		if (!pHit)
		{
			m_pRawDocs = ExtBase::GetDocsChunk();
			if (!m_pRawDocs) // no more incoming documents? bail
				break;

			pDoc = m_pRawDocs;
			pHit = NULL;
			continue;
		}

		// skip all tail hits hits from documents below or same ID as uSkipID

		// scan until next acceptable hit
		while (pHit->m_uDocid < pDoc->m_uDocid || (uSkipID && pHit->m_uDocid <= uSkipID)) // skip leftovers
			pHit++;

		while ((pHit->m_uDocid != DOCID_MAX || (uSkipID && pHit->m_uDocid <= uSkipID)) && !t_Acceptor::IsAcceptableHit(pHit)) // skip unneeded hits
			pHit++;

		if (pHit->m_uDocid == DOCID_MAX || (uSkipID && pHit->m_uDocid <= uSkipID)) // check for eof
			continue;

		// find and emit new document
		while (pDoc->m_uDocid < pHit->m_uDocid) pDoc++; // FIXME? unsafe in broken cases
		assert(pDoc->m_uDocid == pHit->m_uDocid);
		assert(iMyDoc < ExtBase::MAX_DOCS - 1);

		if (uLastID != pDoc->m_uDocid)
			CopyExtDoc(m_dMyDocs[iMyDoc++], *pDoc, &pDocinfo, ExtBase::m_iStride);
		uLastID = pDoc->m_uDocid;


		// current hit is surely acceptable.
		m_dMyHits[iMyHit++] = *(pHit++);
		// copy acceptable hits for this document
		while (iMyHit < ExtBase::MAX_HITS - 1 && pHit->m_uDocid == uLastID)
		{
			if (t_Acceptor::IsAcceptableHit(pHit))
			{
				m_dMyHits[iMyHit++] = *pHit;
			}
			pHit++;
		}

		if (iMyHit == ExtBase::MAX_HITS - 1)
		{
			// there is no more space for acceptable hits; but further calls to GetHits() *might* produce some
			// we need to memorize the trailing document id
			m_uLastID = uLastID;
			break;
		}
	}

	m_pRawDoc = pDoc;
	m_pRawHit = pHit;

	assert(iMyDoc >= 0 && iMyDoc < ExtBase::MAX_DOCS);
	assert(iMyHit >= 0 && iMyHit < ExtBase::MAX_DOCS);

	m_dMyDocs[iMyDoc].m_uDocid = DOCID_MAX;
	m_dMyHits[iMyHit].m_uDocid = DOCID_MAX;
	m_eState = COPY_FILTERED;

	PrintDocsChunk(iMyDoc, ExtBase::m_iAtomPos, m_dMyDocs, "cond", this);

	return iMyDoc ? m_dMyDocs : NULL;
}


template < TermPosFilter_e T, class ExtBase >
const ExtHit_t* ExtConditional<T, ExtBase>::GetHitsChunk(const ExtDoc_t* pDocs)
{
	const ExtDoc_t* pStart = pDocs;
	if (m_eState == COPY_DONE)
	{
		// this request completed in full
		if (m_uDoneFor == pDocs->m_uDocid || !m_uDoneFor || m_uHitStartDocid == pDocs->m_uDocid)
			return NULL;

		// old request completed in full, but we have a new hits subchunk request now
		// even though there were no new docs requests in the meantime!
		m_eState = COPY_FILTERED;
		if (m_uDoneFor && m_uDoneFor != DOCID_MAX)
		{
			while (pDocs->m_uDocid != DOCID_MAX && pDocs->m_uDocid <= m_uDoneFor)
				pDocs++;
		}
	}
	m_uDoneFor = pDocs->m_uDocid;

	// regular case
	// copy hits for requested docs from my hits to filtered hits, and return those
	int iFilteredHits = 0;

	if (m_eState == COPY_FILTERED)
	{
		const ExtHit_t* pMyHit = m_dMyHits;
		for (;; )
		{
			// skip hits that the caller is not interested in
			while (pMyHit->m_uDocid < pDocs->m_uDocid)
				pMyHit++;

			// out of acceptable hits?
			if (pMyHit->m_uDocid == DOCID_MAX)
			{
				// do we have a trailing document? if yes, we should also copy trailing hits
				m_eState = m_uLastID ? COPY_TRAILING : COPY_DONE;
				break;
			}

			// skip docs that i do not have
			while (pDocs->m_uDocid < pMyHit->m_uDocid)
				pDocs++;

			// out of requested docs? over and out
			if (pDocs->m_uDocid == DOCID_MAX)
			{
				m_eState = COPY_DONE;
				break;
			}

			// copy matching hits
			while (iFilteredHits < ExtBase::MAX_HITS - 1 && pDocs->m_uDocid == pMyHit->m_uDocid)
			{
				m_dFilteredHits[iFilteredHits++] = *pMyHit++;
			}

			// paranoid check that we're not out of bounds
			assert(iFilteredHits <= ExtBase::MAX_HITS - 1 && pDocs->m_uDocid != pMyHit->m_uDocid);
		}
	}

	// trailing hits case
	// my hits did not have enough space, so we should pass raw hits for the last doc
	while (m_eState == COPY_TRAILING && m_uLastID && iFilteredHits < ExtBase::MAX_HITS - 1)
	{
		// where do we stand?
		if (!m_pRawHit || m_pRawHit->m_uDocid == DOCID_MAX)
			m_pRawHit = ExtBase::GetHitsChunk(m_pRawDocs);

		// no more hits for current chunk
		if (!m_pRawHit)
		{
			m_eState = COPY_DONE;
			break;
		}

		// copy while we can
		while (m_pRawHit->m_uDocid == m_uLastID && iFilteredHits < ExtBase::MAX_HITS - 1)
		{
			if (t_Acceptor::IsAcceptableHit(m_pRawHit))
			{
				m_dFilteredHits[iFilteredHits++] = *m_pRawHit;
			}
			m_pRawHit++;
		}

		// raise the flag for future calls if trailing hits are over
		if (m_pRawHit->m_uDocid != m_uLastID && m_pRawHit->m_uDocid != DOCID_MAX)
			m_eState = COPY_DONE;

		// in any case, this chunk is over
		break;
	}

	m_uDoneFor = pDocs->m_uDocid;
	m_uHitStartDocid = 0;
	if (m_uDoneFor == DOCID_MAX && pDocs - 1 >= pStart)
	{
		m_uDoneFor = (pDocs - 1)->m_uDocid;
		m_uHitStartDocid = pStart->m_uDocid;
	}
	if (iFilteredHits && m_dFilteredHits[iFilteredHits - 1].m_uDocid > m_uDoneFor)
		m_uDoneFor = m_dFilteredHits[iFilteredHits - 1].m_uDocid;

	PrintHitsChunk(iFilteredHits, ExtBase::m_iAtomPos, m_dFilteredHits, "cond", this);

	m_dFilteredHits[iFilteredHits].m_uDocid = DOCID_MAX;
	return iFilteredHits ? m_dFilteredHits : NULL;
}

template < TermPosFilter_e T, class ExtBase >
ExtConditional<T, ExtBase>::ExtConditional(ISphQword* pQword, const XQNode_t* pNode, const ISphQwordSetup& tSetup)
	: BufferedNode_c()
	, ExtBase()
	, t_Acceptor(pQword, pNode, tSetup)
{
	ExtBase::AllocDocinfo(tSetup);
}

template < TermPosFilter_e T, class ExtBase >
void ExtConditional<T, ExtBase>::Reset(const ISphQwordSetup& tSetup)
{
	BufferedNode_c::Reset();
	ExtBase::Reset(tSetup);
	TermAcceptor_c<T>::Reset();
}


/////////////////////////


/// skips hits until their docids are less than the given limit
static inline void SkipHitsLtDocid(const ExtHit_t* (*ppHits), SphDocID_t uMatch, ExtNode_i* pNode, const ExtDoc_t* pDocs)
{
	for (;; )
	{
		const ExtHit_t* pHit = *ppHits;
		if (!pHit || pHit->m_uDocid == DOCID_MAX)
		{
			pHit = *ppHits = pNode->GetHitsChunk(pDocs); // OPTIMIZE? use that max?
			if (!pHit)
				return;
		}

		while (pHit->m_uDocid < uMatch)
			pHit++;

		*ppHits = pHit;
		if (pHit->m_uDocid != DOCID_MAX)
			return;
	}
}


/// skips hits within current document while their position is less or equal than the given limit
/// returns true if a matching hit (with big enough position, and in current document) was found
/// returns false otherwise
static inline bool SkipHitsLtePos(const ExtHit_t* (*ppHits), Hitpos_t uPos, ExtNode_i* pNode, const ExtDoc_t* pDocs)
{
	SphDocID_t uDocid = (*ppHits)->m_uDocid;
	for (;; )
	{
		const ExtHit_t* pHit = *ppHits;
		if (!pHit || pHit->m_uDocid == DOCID_MAX)
		{
			pHit = *ppHits = pNode->GetHitsChunk(pDocs); // OPTIMIZE? use that max?
			if (!pHit)
				return false;
		}

		while (pHit->m_uDocid == uDocid && pHit->m_uHitpos <= uPos)
			pHit++;

		*ppHits = pHit;
		if (pHit->m_uDocid != DOCID_MAX)
			return (pHit->m_uDocid == uDocid);
	}
}



inline void ExtTerm_c::Init(ISphQword* pQword, const FieldMask_t& dFields, const ISphQwordSetup& tSetup, bool bNotWeighted)
{
	m_pQword = pQword;
	m_pWarning = tSetup.m_pWarning;
	m_bNotWeighted = bNotWeighted;
	m_iAtomPos = pQword->m_iAtomPos;
	m_pLastChecked = m_dDocs;
	m_uMatchChecked = 0;
	m_bTail = false;
	m_dQueriedFields = dFields;
	m_bHasWideFields = false;
	if (tSetup.m_pIndex && tSetup.m_pIndex->GetMatchSchema().m_dFields.GetLength() > 32)
		for (int i = 1; i < FieldMask_t::SIZE && !m_bHasWideFields; i++)
			if (m_dQueriedFields[i])
				m_bHasWideFields = true;
	m_iMaxTimer = tSetup.m_iMaxTimer;
	m_pStats = tSetup.m_pStats;
	m_pNanoBudget = m_pStats ? m_pStats->m_pNanoBudget : NULL;
	AllocDocinfo(tSetup);
}

ExtTerm_c::ExtTerm_c(ISphQword* pQword, const ISphQwordSetup& tSetup)
	: m_pQword(pQword)
	, m_pWarning(tSetup.m_pWarning)
	, m_bNotWeighted(true)
{
	m_iAtomPos = pQword->m_iAtomPos;
	m_pLastChecked = m_dDocs;
	m_uMatchChecked = 0;
	m_bTail = false;
	m_dQueriedFields.SetAll();
	m_bHasWideFields = tSetup.m_pIndex && (tSetup.m_pIndex->GetMatchSchema().m_dFields.GetLength() > 32);
	m_iMaxTimer = tSetup.m_iMaxTimer;
	m_pStats = tSetup.m_pStats;
	m_pNanoBudget = m_pStats ? m_pStats->m_pNanoBudget : NULL;
	AllocDocinfo(tSetup);
}

ExtTerm_c::ExtTerm_c(ISphQword* pQword, const FieldMask_t& dFields, const ISphQwordSetup& tSetup, bool bNotWeighted)
{
	Init(pQword, dFields, tSetup, bNotWeighted);
}

void ExtTerm_c::Reset(const ISphQwordSetup& tSetup)
{
	m_pLastChecked = m_dDocs;
	m_uMatchChecked = 0;
	m_bTail = false;
	m_iMaxTimer = tSetup.m_iMaxTimer;
	m_pQword->Reset();
	tSetup.QwordSetup(m_pQword);
}

int ExtTerm_c::GetQwords(ExtQwordsHash_t& hQwords)
{
	m_fIDF = 0.0f;

	ExtQword_t* pQword = hQwords(m_pQword->m_sWord);
	if (!m_bNotWeighted && pQword && !pQword->m_bExcluded)
		pQword->m_iQueryPos = Min(pQword->m_iQueryPos, m_pQword->m_iAtomPos);

	if (m_bNotWeighted || pQword)
		return m_pQword->m_bExcluded ? -1 : m_pQword->m_iAtomPos;

	m_fIDF = -1.0f;
	ExtQword_t tInfo;
	tInfo.m_sWord = m_pQword->m_sWord;
	tInfo.m_sDictWord = m_pQword->m_sDictWord;
	tInfo.m_iDocs = m_pQword->m_iDocs;
	tInfo.m_iHits = m_pQword->m_iHits;
	tInfo.m_iQueryPos = m_pQword->m_iAtomPos;
	tInfo.m_fIDF = -1.0f; // suppress gcc 4.2.3 warning
	tInfo.m_fBoost = m_pQword->m_fBoost;
	tInfo.m_bExpanded = m_pQword->m_bExpanded;
	tInfo.m_bExcluded = m_pQword->m_bExcluded;
	hQwords.Add(tInfo, m_pQword->m_sWord);
	return m_pQword->m_bExcluded ? -1 : m_pQword->m_iAtomPos;
}

void ExtTerm_c::SetQwordsIDF(const ExtQwordsHash_t& hQwords)
{
	if (m_fIDF < 0.0f)
	{
		assert(hQwords(m_pQword->m_sWord));
		m_fIDF = hQwords(m_pQword->m_sWord)->m_fIDF;
	}
}

void ExtTerm_c::GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const
{
	if (m_bNotWeighted || m_pQword->m_bExcluded)
		return;

	ExtQword_t& tQword = hQwords[m_pQword->m_sWord];

	TermPos_t& tPos = dTermDupes.Add();
	tPos.m_uAtomPos = (WORD)m_pQword->m_iAtomPos;
	tPos.m_uQueryPos = (WORD)tQword.m_iQueryPos;
}


const ExtDoc_t* ExtTerm_c::GetDocsChunk()
{
	m_pLastChecked = m_dDocs;
	m_bTail = false;

	if (!m_pQword->m_iDocs)
		return NULL;

	// max_query_time
	if (m_iMaxTimer > 0 && sphMicroTimer() >= m_iMaxTimer)
	{
		if (m_pWarning)
			*m_pWarning = "query time exceeded max_query_time";
		return NULL;
	}

	// max_predicted_time
	if (m_pNanoBudget && *m_pNanoBudget < 0)
	{
		if (m_pWarning)
			*m_pWarning = "predicted query time exceeded max_predicted_time";
		return NULL;
	}

	// interrupt by sitgerm
	if (m_bInterruptNow)
	{
		if (m_pWarning)
			*m_pWarning = "Server shutdown in progress";
		return NULL;
	}

	int iDoc = 0;
	CSphRowitem* pDocinfo = m_pDocinfo;
	while (iDoc < MAX_DOCS - 1)
	{
		const CSphMatch& tMatch = m_pQword->GetNextDoc(pDocinfo);
		if (!tMatch.m_uDocID)
		{
			m_pQword->m_iDocs = 0;
			break;
		}

		if (!m_bHasWideFields)
		{
			// fields 0-31 can be quickly checked right here, right now
			if (!(m_pQword->m_dQwordFields.GetMask32() & m_dQueriedFields.GetMask32()))
				continue;
		}
		else
		{
			// fields 32+ need to be checked with CollectHitMask() and stuff
			m_pQword->CollectHitMask();
			bool bHasSameFields = false;
			for (int i = 0; i < FieldMask_t::SIZE && !bHasSameFields; i++)
				bHasSameFields = (m_pQword->m_dQwordFields[i] & m_dQueriedFields[i]) != 0;
			if (!bHasSameFields)
				continue;
		}

		ExtDoc_t& tDoc = m_dDocs[iDoc++];
		tDoc.m_uDocid = tMatch.m_uDocID;
		tDoc.m_pDocinfo = pDocinfo;
		tDoc.m_uHitlistOffset = m_pQword->m_iHitlistPos;
		tDoc.m_uDocFields = m_pQword->m_dQwordFields.GetMask32() & m_dQueriedFields.GetMask32(); // OPTIMIZE: only needed for phrase node
		tDoc.m_fTFIDF = float(m_pQword->m_uMatchHits) / float(m_pQword->m_uMatchHits + SPH_BM25_K1) * m_fIDF;
		pDocinfo += m_iStride;
	}

	if (m_pStats)
		m_pStats->m_iFetchedDocs += iDoc;
	if (m_pNanoBudget)
		*m_pNanoBudget -= g_iPredictorCostDoc * iDoc;

	return ReturnDocsChunk(iDoc, "term");
}

const ExtHit_t* ExtTerm_c::GetHitsChunk(const ExtDoc_t* pMatched)
{
	if (!pMatched)
		return NULL;

	// aim to the right document
	ExtDoc_t* pDoc = m_pLastChecked;
	assert(pDoc && pDoc >= m_dDocs && pDoc < m_dDocs + MAX_DOCS);

	if (!m_bTail)
	{
		// if we already emitted hits for this matches block, do not do that again
		if (pMatched->m_uDocid == m_uMatchChecked)
			return NULL;

		// find match
		m_uMatchChecked = pMatched->m_uDocid;
		do
		{
			while (pDoc->m_uDocid < pMatched->m_uDocid) pDoc++;
			m_pLastChecked = pDoc;
			if (pDoc->m_uDocid == DOCID_MAX)
				return NULL; // matched docs block is over for me, gimme another one

			while (pMatched->m_uDocid < pDoc->m_uDocid) pMatched++;
			if (pMatched->m_uDocid == DOCID_MAX)
				return NULL; // matched doc block did not yet begin for me, gimme another one
		} while (pDoc->m_uDocid != pMatched->m_uDocid);

		// setup hitlist reader
		m_pQword->SeekHitlist(pDoc->m_uHitlistOffset);
	}

	// hit emission
	int iHit = 0;
	m_bTail = false;
	while (iHit < MAX_HITS - 1)
	{
		// get next hit
		Hitpos_t uHit = m_pQword->GetNextHit();
		if (uHit == EMPTY_HIT)
		{
			// no more hits; get next acceptable document
			pDoc++;
			m_pLastChecked = pDoc;

			do
			{
				while (pDoc->m_uDocid < pMatched->m_uDocid) pDoc++;
				m_pLastChecked = pDoc;
				if (pDoc->m_uDocid == DOCID_MAX) { pDoc = NULL; break; } // matched docs block is over for me, gimme another one

				while (pMatched->m_uDocid < pDoc->m_uDocid) pMatched++;
				if (pMatched->m_uDocid == DOCID_MAX) { pDoc = NULL; break; } // matched doc block did not yet begin for me, gimme another one
			} while (pDoc->m_uDocid != pMatched->m_uDocid);

			if (!pDoc)
				break;
			assert(pDoc->m_uDocid == pMatched->m_uDocid);

			// setup hitlist reader
			m_pQword->SeekHitlist(pDoc->m_uHitlistOffset);
			continue;
		}

		if (!(m_dQueriedFields.Test(HITMAN::GetField(uHit))))
			continue;

		ExtHit_t& tHit = m_dHits[iHit++];
		tHit.m_uDocid = pDoc->m_uDocid;
		tHit.m_uHitpos = uHit;
		tHit.m_uQuerypos = (WORD)m_iAtomPos; // assume less that 64K words per query
		tHit.m_uWeight = tHit.m_uMatchlen = tHit.m_uSpanlen = 1;
	}

	if (iHit == MAX_HITS - 1)
		m_bTail = true;

	if (m_pStats)
		m_pStats->m_iFetchedHits += iHit;
	if (m_pNanoBudget)
		*m_pNanoBudget -= g_iPredictorCostHit * iHit;

	return ReturnHitsChunk(iHit, "term", false);
}


/// Immediately interrupt current operation
void sphInterruptNow()
{
	ExtTerm_c::m_bInterruptNow = true;
}

bool sphInterrupted()
{
	return ExtTerm_c::m_bInterruptNow;
}

volatile bool ExtTerm_c::m_bInterruptNow = false;




const ExtHit_t* ExtTermHitless_c::GetHitsChunk(const ExtDoc_t* pMatched)
{
	if (!pMatched)
		return NULL;

	// aim to the right document
	ExtDoc_t* pDoc = m_pLastChecked;
	assert(pDoc && pDoc >= m_dDocs && pDoc < m_dDocs + MAX_DOCS);

	if (!m_bTail)
	{
		// if we already emitted hits for this matches block, do not do that again
		if (pMatched->m_uDocid == m_uMatchChecked)
			return NULL;

		// find match
		m_uMatchChecked = pMatched->m_uDocid;
		do
		{
			while (pDoc->m_uDocid < pMatched->m_uDocid) pDoc++;
			m_pLastChecked = pDoc;
			if (pDoc->m_uDocid == DOCID_MAX)
				return NULL; // matched docs block is over for me, gimme another one

			while (pMatched->m_uDocid < pDoc->m_uDocid) pMatched++;
			if (pMatched->m_uDocid == DOCID_MAX)
				return NULL; // matched doc block did not yet begin for me, gimme another one
		} while (pDoc->m_uDocid != pMatched->m_uDocid);

		m_uFieldPos = 0;
	}

	// hit emission
	int iHit = 0;
	m_bTail = false;
	DWORD uMaxFields = SPH_MAX_FIELDS;
	if (!m_bHasWideFields)
	{
		uMaxFields = 0;
		DWORD uFields = pDoc->m_uDocFields;
		while (uFields) // count up to highest bit, max value is 32
		{
			uFields >>= 1;
			uMaxFields++;
		}
	}
	for (;; )
	{
		if ((m_uFieldPos < 32 && (pDoc->m_uDocFields & (1 << m_uFieldPos))) // not necessary
			&& m_dQueriedFields.Test(m_uFieldPos))
		{
			// emit hit
			ExtHit_t& tHit = m_dHits[iHit++];
			tHit.m_uDocid = pDoc->m_uDocid;
			tHit.m_uHitpos = HITMAN::Create(m_uFieldPos, -1);
			tHit.m_uQuerypos = (WORD)m_iAtomPos;
			tHit.m_uWeight = tHit.m_uMatchlen = tHit.m_uSpanlen = 1;

			if (iHit == MAX_HITS - 1)
				break;
		}

		if (m_uFieldPos < uMaxFields - 1)
		{
			m_uFieldPos++;
			continue;
		}

		// field mask is empty, get next document
		pDoc++;
		m_pLastChecked = pDoc;
		do
		{
			while (pDoc->m_uDocid < pMatched->m_uDocid) pDoc++;
			m_pLastChecked = pDoc;
			if (pDoc->m_uDocid == DOCID_MAX) { pDoc = NULL; break; } // matched docs block is over for me, gimme another one

			while (pMatched->m_uDocid < pDoc->m_uDocid) pMatched++;
			if (pMatched->m_uDocid == DOCID_MAX) { pDoc = NULL; break; } // matched doc block did not yet begin for me, gimme another one
		} while (pDoc->m_uDocid != pMatched->m_uDocid);

		if (!pDoc)
			break;

		m_uFieldPos = 0;
	}

	if (iHit == MAX_HITS - 1)
		m_bTail = true;

	assert(iHit >= 0 && iHit < MAX_HITS);
	m_dHits[iHit].m_uDocid = DOCID_MAX;
	return (iHit != 0) ? m_dHits : NULL;
}

////////////////////////////////


ExtUnit_c::ExtUnit_c(ExtNode_i* pFirst, ExtNode_i* pSecond, const FieldMask_t& uFields,
	const ISphQwordSetup& tSetup, const char* sUnit)
{
	m_pArg1 = pFirst;
	m_pArg2 = pSecond;

	XQKeyword_t tDot;
	tDot.m_sWord = sUnit;
	m_pDot = new ExtTerm_c(CreateQueryWord(tDot, tSetup), uFields, tSetup, true);

	m_uHitsOverFor = 0;
	m_uTailDocid = 0;
	m_uTailSentenceEnd = 0;

	m_pDocs1 = NULL;
	m_pDocs2 = NULL;
	m_pDotDocs = NULL;
	m_pDoc1 = NULL;
	m_pDoc2 = NULL;
	m_pDotDoc = NULL;
	m_pHit1 = NULL;
	m_pHit2 = NULL;
	m_pDotHit = NULL;
	m_dMyHits[0].m_uDocid = DOCID_MAX;
}


ExtUnit_c::~ExtUnit_c()
{
	SafeDelete(m_pArg1);
	SafeDelete(m_pArg2);
	SafeDelete(m_pDot);
}


void ExtUnit_c::Reset(const ISphQwordSetup& tSetup)
{
	m_pArg1->Reset(tSetup);
	m_pArg2->Reset(tSetup);
	m_pDot->Reset(tSetup);

	m_uHitsOverFor = 0;
	m_uTailDocid = 0;
	m_uTailSentenceEnd = 0;

	m_pDocs1 = NULL;
	m_pDocs2 = NULL;
	m_pDotDocs = NULL;
	m_pDoc1 = NULL;
	m_pDoc2 = NULL;
	m_pDotDoc = NULL;
	m_pHit1 = NULL;
	m_pHit2 = NULL;
	m_pDotHit = NULL;
	m_dMyHits[0].m_uDocid = DOCID_MAX;
}


int ExtUnit_c::GetQwords(ExtQwordsHash_t& hQwords)
{
	int iMax1 = m_pArg1->GetQwords(hQwords);
	int iMax2 = m_pArg2->GetQwords(hQwords);
	return Max(iMax1, iMax2);
}


void ExtUnit_c::SetQwordsIDF(const ExtQwordsHash_t& hQwords)
{
	m_pArg1->SetQwordsIDF(hQwords);
	m_pArg2->SetQwordsIDF(hQwords);
}

void ExtUnit_c::GetTerms(const ExtQwordsHash_t& hQwords, CSphVector<TermPos_t>& dTermDupes) const
{
	m_pArg1->GetTerms(hQwords, dTermDupes);
	m_pArg2->GetTerms(hQwords, dTermDupes);
}

uint64_t ExtUnit_c::GetWordID() const
{
	uint64_t dHash[2];
	dHash[0] = m_pArg1->GetWordID();
	dHash[1] = m_pArg2->GetWordID();
	return sphFNV64(dHash, sizeof(dHash));
}


int ExtUnit_c::FilterHits(int iMyHit, DWORD uSentenceEnd, SphDocID_t uDocid, int* pDoc)
{
	while (iMyHit < MAX_HITS - 1)
	{
		if (uSentenceEnd)
		{
			// we're in a matched sentence state
			// copy hits until next dot
			bool bValid1 = m_pHit1 && m_pHit1->m_uDocid == uDocid && m_pHit1->m_uHitpos < uSentenceEnd;
			bool bValid2 = m_pHit2 && m_pHit2->m_uDocid == uDocid && m_pHit2->m_uHitpos < uSentenceEnd;

			if (!bValid1 && !bValid2)
			{
				// no more hits in this sentence
				uSentenceEnd = 0;
				if (m_pHit1 && m_pHit2 && m_pHit1->m_uDocid == uDocid && m_pHit2->m_uDocid == uDocid)
					continue; // no more in-sentence hits, but perhaps more sentences in this document
				else
					break; // document is over
			}

			// register document as matching
			if (pDoc)
			{
				ExtDoc_t& tDoc = m_dDocs[(*pDoc)++];
				tDoc.m_uDocid = m_pDoc1->m_uDocid;
				tDoc.m_uDocFields = m_pDoc1->m_uDocFields | m_pDoc2->m_uDocFields; // non necessary
				tDoc.m_uHitlistOffset = -1;
				tDoc.m_fTFIDF = m_pDoc1->m_fTFIDF + m_pDoc2->m_fTFIDF;
				tDoc.m_pDocinfo = NULL; // no inline support, sorry
				pDoc = NULL; // just once
			}

			if (bValid1 && (!bValid2 || m_pHit1->m_uHitpos < m_pHit2->m_uHitpos))
			{
				m_dMyHits[iMyHit++] = *m_pHit1++;
				if (m_pHit1->m_uDocid == DOCID_MAX)
					m_pHit1 = m_pArg1->GetHitsChunk(m_pDocs1);

			}
			else
			{
				m_dMyHits[iMyHit++] = *m_pHit2++;
				if (m_pHit2->m_uDocid == DOCID_MAX)
					m_pHit2 = m_pArg2->GetHitsChunk(m_pDocs2);
			}

		}
		else
		{
			// no sentence matched yet
			// let's check the next hit pair
			assert(m_pHit1->m_uDocid == uDocid);
			assert(m_pHit2->m_uDocid == uDocid);
			assert(m_pDotHit->m_uDocid == uDocid);

			// our current hit pair locations
			DWORD uMin = Min(m_pHit1->m_uHitpos, m_pHit2->m_uHitpos);
			DWORD uMax = Max(m_pHit1->m_uHitpos, m_pHit2->m_uHitpos);

			// skip all dots beyond the min location
			if (!SkipHitsLtePos(&m_pDotHit, uMin, m_pDot, m_pDotDocs))
			{
				// we have a match!
				// moreover, no more dots past min location in current document
				// copy hits until next document
				uSentenceEnd = UINT_MAX;
				continue;
			}

			// does the first post-pair-start dot separate our hit pair?
			if (m_pDotHit->m_uHitpos < uMax)
			{
				// yes, got an "A dot B" case
				// rewind candidate hits past this dot, break if current document is over
				if (!SkipHitsLtePos(&m_pHit1, m_pDotHit->m_uHitpos, m_pArg1, m_pDocs1))
					break;
				if (!SkipHitsLtePos(&m_pHit2, m_pDotHit->m_uHitpos, m_pArg2, m_pDocs2))
					break;
				continue;

			}
			else
			{
				// we have a match!
				// copy hits until next dot
				if (!SkipHitsLtePos(&m_pDotHit, uMax, m_pDot, m_pDotDocs))
					uSentenceEnd = UINT_MAX; // correction, no next dot, so make it "next document"
				else
					uSentenceEnd = m_pDotHit->m_uHitpos;
				assert(uSentenceEnd);
			}
		}
	}

	m_uTailSentenceEnd = uSentenceEnd; // just in case tail hits loop will happen
	return iMyHit;
}

void ExtUnit_c::SkipTailHits()
{
	m_uTailDocid = 0;
	m_pDoc1++;
	m_pDoc2++;
}


const ExtDoc_t* ExtUnit_c::GetDocsChunk()
{
	// SENTENCE operator is essentially AND on steroids
	// that also takes relative dot positions into account
	//
	// when document matches both args but not the dot, it degenerates into AND
	// we immediately lookup and copy matching document hits anyway, though
	// this is suboptimal (because these hits might never be required at all)
	// but this is expected to be rare case, so let's keep code simple
	//
	// when document matches both args and the dot, we need to filter the hits
	// only those left/right pairs that are not (!) separated by a dot should match

	int iDoc = 0;
	int iMyHit = 0;

	if (m_uTailDocid)
		SkipTailHits();

	while (iMyHit < MAX_HITS - 1)
	{
		// fetch more candidate docs, if needed
		if (!m_pDoc1 || m_pDoc1->m_uDocid == DOCID_MAX)
		{
			m_pDoc1 = m_pDocs1 = m_pArg1->GetDocsChunk();
			if (!m_pDoc1)
				break; // node is over
		}

		if (!m_pDoc2 || m_pDoc2->m_uDocid == DOCID_MAX)
		{
			m_pDoc2 = m_pDocs2 = m_pArg2->GetDocsChunk();
			if (!m_pDoc2)
				break; // node is over
		}

		// find next candidate match
		while (m_pDoc1->m_uDocid != m_pDoc2->m_uDocid && m_pDoc1->m_uDocid != DOCID_MAX && m_pDoc2->m_uDocid != DOCID_MAX)
		{
			while (m_pDoc1->m_uDocid < m_pDoc2->m_uDocid && m_pDoc2->m_uDocid != DOCID_MAX)
				m_pDoc1++;
			while (m_pDoc1->m_uDocid > m_pDoc2->m_uDocid && m_pDoc1->m_uDocid != DOCID_MAX)
				m_pDoc2++;
		}

		// got our candidate that matches AND?
		SphDocID_t uDocid = m_pDoc1->m_uDocid;
		if (m_pDoc1->m_uDocid == DOCID_MAX || m_pDoc2->m_uDocid == DOCID_MAX)
			continue;

		// yes, now fetch more dots docs, if needed
		// note how NULL is accepted here, "A and B but no dots" case is valid!
		if (!m_pDotDoc || m_pDotDoc->m_uDocid == DOCID_MAX)
			m_pDotDoc = m_pDotDocs = m_pDot->GetDocsChunk();

		// skip preceding docs
		while (m_pDotDoc && m_pDotDoc->m_uDocid < uDocid)
		{
			while (m_pDotDoc->m_uDocid < uDocid)
				m_pDotDoc++;

			if (m_pDotDoc->m_uDocid == DOCID_MAX)
				m_pDotDoc = m_pDotDocs = m_pDot->GetDocsChunk();
		}

		// we will need document hits on both routes below
		SkipHitsLtDocid(&m_pHit1, uDocid, m_pArg1, m_pDocs1);
		SkipHitsLtDocid(&m_pHit2, uDocid, m_pArg2, m_pDocs2);
		assert(m_pHit1 && m_pHit1->m_uDocid == uDocid);
		assert(m_pHit2 && m_pHit2->m_uDocid == uDocid);

		DWORD uSentenceEnd = 0;
		if (!m_pDotDoc || m_pDotDoc->m_uDocid != uDocid)
		{
			// no dots in current document?
			// just copy all hits until next document
			uSentenceEnd = UINT_MAX;

		}
		else
		{
			// got both hits and dots
			// rewind to relevant dots hits, then do sentence boundary detection
			SkipHitsLtDocid(&m_pDotHit, uDocid, m_pDot, m_pDotDocs);
		}

		// do those hits
		iMyHit = FilterHits(iMyHit, uSentenceEnd, uDocid, &iDoc);

		// out of matching hits buffer? gotta return docs chunk now, then
		if (iMyHit == MAX_HITS - 1)
		{
			// mark a possibility of some trailing hits for current dot, if any
			if ((m_pHit1 && m_pHit1->m_uDocid == uDocid) || (m_pHit2 && m_pHit2->m_uDocid == uDocid))
			{
				m_uTailDocid = uDocid; // yep, do check that tail
			}
			else
			{
				SkipTailHits(); // nope, both hit lists are definitely over
			}

			return ReturnDocsChunk(iDoc, iMyHit);
		}

		// all hits copied; do the next candidate
		m_pDoc1++;
		m_pDoc2++;
	}

	return ReturnDocsChunk(iDoc, iMyHit);
}


const ExtHit_t* ExtUnit_c::GetHitsChunk(const ExtDoc_t* pDocs)
{
	SphDocID_t uFirstMatch = pDocs->m_uDocid;

	// current hits chunk already returned
	if (m_uHitsOverFor == uFirstMatch)
	{
		// and there are no trailing hits? bail
		if (!m_uTailDocid)
			return NULL;

		// and there might be trailing hits for the last document? try and loop them
		int iMyHit = FilterHits(0, m_uTailSentenceEnd, m_uTailDocid, NULL);
		if (!iMyHit)
		{
			// no trailing hits were there actually
			m_uTailDocid = 0;
			m_pDoc1++;
			m_pDoc2++;
			return NULL;
		}

		// ok, we got some trailing hits!
		// check whether we might have even more
		if (!(iMyHit == MAX_HITS - 1 && m_pHit1 && m_pHit1->m_uDocid == m_uTailDocid && m_pHit2 && m_pHit2->m_uDocid == m_uTailDocid))
		{
			// nope, both hit lists are definitely over now
			m_uTailDocid = 0;
			m_pDoc1++;
			m_pDoc2++;
		}

		// return those trailing hits
		assert(iMyHit < MAX_HITS);
		m_dMyHits[iMyHit].m_uDocid = DOCID_MAX;
		return m_dMyHits;
	}

	// no hits returned yet; do return them
	int iHit = 0;
	ExtHit_t* pMyHit = m_dMyHits;

	for (;; )
	{
		// skip filtered hits until next requested document
		while (pMyHit->m_uDocid != pDocs->m_uDocid)
		{
			while (pDocs->m_uDocid < pMyHit->m_uDocid && pDocs->m_uDocid != DOCID_MAX)
				pDocs++;
			if (pDocs->m_uDocid == DOCID_MAX)
				break;
			while (pMyHit->m_uDocid < pDocs->m_uDocid)
				pMyHit++;
		}

		// out of hits
		if (pMyHit->m_uDocid == DOCID_MAX || pDocs->m_uDocid == DOCID_MAX)
		{
			// there still might be trailing hits
			// if so, they will be handled on next entry
			m_uHitsOverFor = uFirstMatch;
			if (pDocs->m_uDocid == DOCID_MAX && m_uTailDocid)
				SkipTailHits();

			break;
		}

		// copy
		while (pMyHit->m_uDocid == pDocs->m_uDocid)
			m_dHits[iHit++] = *pMyHit++;

		if (pMyHit->m_uDocid == DOCID_MAX)
		{
			m_uHitsOverFor = uFirstMatch;
			break;
		}
	}

	assert(iHit >= 0 && iHit < MAX_HITS);
	m_dHits[iHit].m_uDocid = DOCID_MAX;
	return (iHit != 0) ? m_dHits : NULL;
}


}