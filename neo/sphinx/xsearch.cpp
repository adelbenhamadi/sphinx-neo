#include "neo/sphinx/xsearch.h"
#include "neo/sphinx/xquery.h"
#include "neo/sphinxint.h"
//#include "neo/sphinxexcerpt.h"




#include "neo/index/enums.h"
#include "neo/index/ft_index.h"
#include "neo/core/generic.h"
#include "neo/io/fnv64.h"
#include "neo/io/crc32.h"
#include "neo/core/iextra.h"
#include "neo/core/ranker.h"
#include "neo/query/extra.h"
#include "neo/query/ext_term.h"
#include "neo/query/node_cache.h"
#include "neo/query/iqword.h"
#include "neo/core/match.h"
#include "neo/core/match_engine.h"
#include "neo/core/generic.h"

#include <math.h>



namespace NEO {

struct AtomPosQWord_fn
{
	bool operator () ( ISphQword * ) const { return true; }
};

struct AtomPosExtNode_fn
{
	bool operator () ( ExtNode_i * pNode ) const { return !pNode->GotHitless(); }
};

template <typename T, typename NODE_CHECK>
int CountAtomPos ( const CSphVector<T *> & dNodes, const NODE_CHECK & fnCheck )
{
	if ( dNodes.GetLength()<2 )
		return dNodes.GetLength();

	int iMinPos = INT_MAX;
	int iMaxPos = 0;
	ARRAY_FOREACH ( i, dNodes )
	{
		T * pNode = dNodes[i];
		if ( fnCheck ( pNode ) )
		{
			iMinPos = Min ( pNode->m_iAtomPos, iMinPos );
			iMaxPos = Max ( pNode->m_iAtomPos, iMaxPos );
		}
	}
	if ( iMinPos==INT_MAX )
		return 0;

	CSphBitvec dAtomPos ( iMaxPos - iMinPos + 1 );
	ARRAY_FOREACH ( i, dNodes )
	{
		if ( fnCheck ( dNodes[i] ) )
			dAtomPos.BitSet ( dNodes[i]->m_iAtomPos - iMinPos );
	}

	return dAtomPos.BitCount();
}


template < typename T >
static ExtNode_i * CreateMultiNode ( const XQNode_t * pQueryNode, const ISphQwordSetup & tSetup, bool bNeedsHitlist )
{
	///////////////////////////////////
	// virtually plain (expanded) node
	///////////////////////////////////
	assert ( pQueryNode );
	if ( pQueryNode->m_dChildren.GetLength() )
	{
		CSphVector<ExtNode_i *> dNodes;
		CSphVector<ExtNode_i *> dTerms;
		ARRAY_FOREACH ( i, pQueryNode->m_dChildren )
		{
			ExtNode_i * pTerm = ExtNode_i::Create ( pQueryNode->m_dChildren[i], tSetup );
			assert ( !pTerm || pTerm->m_iAtomPos>=0 );
			if ( pTerm )
			{
				if ( !pTerm->GotHitless() )
					dNodes.Add ( pTerm );
				else
					dTerms.Add ( pTerm );
			}
		}

		int iAtoms = CountAtomPos ( dNodes, AtomPosExtNode_fn() );
		if ( iAtoms<2 )
		{
			ARRAY_FOREACH ( i, dNodes )
				SafeDelete ( dNodes[i] );
			ARRAY_FOREACH ( i, dTerms )
				SafeDelete ( dTerms[i] );
			if ( tSetup.m_pWarning )
				tSetup.m_pWarning->SetSprintf ( "can't create phrase node, hitlists unavailable (hitlists=%d, nodes=%d)", iAtoms, pQueryNode->m_dChildren.GetLength() );
			return NULL;
		}

		// FIXME! tricky combo again
		// quorum+expand used KeywordsEqual() path to drill down until actual nodes
		ExtNode_i * pResult = new T ( dNodes, *pQueryNode, tSetup );

		// AND result with the words that had no hitlist
		if ( dTerms.GetLength () )
		{
			pResult = new ExtAnd_c ( pResult, dTerms[0], tSetup );
			for ( int i=1; i<dTerms.GetLength (); i++ )
				pResult = new ExtAnd_c ( pResult, dTerms[i], tSetup );
		}

		if ( pQueryNode->GetCount() )
			return tSetup.m_pNodeCache->CreateProxy ( pResult, pQueryNode, tSetup );

		return pResult;
	}

	//////////////////////
	// regular plain node
	//////////////////////

	ExtNode_i * pResult = NULL;
	CSphVector<ISphQword *> dQwordsHit;	// have hits
	CSphVector<ISphQword *> dQwords;	// don't have hits

	// partition phrase words
	const CSphVector<XQKeyword_t> & dWords = pQueryNode->m_dWords;
	ARRAY_FOREACH ( i, dWords )
	{
		ISphQword * pWord = CreateQueryWord ( dWords[i], tSetup );
		if ( pWord->m_bHasHitlist || !bNeedsHitlist )
			dQwordsHit.Add ( pWord );
		else
			dQwords.Add ( pWord );
	}

	// see if we can create the node
	int iAtoms = CountAtomPos ( dQwordsHit, AtomPosQWord_fn() );
	if ( iAtoms<2 )
	{
		ARRAY_FOREACH ( i, dQwords )
			SafeDelete ( dQwords[i] );
		ARRAY_FOREACH ( i, dQwordsHit )
			SafeDelete ( dQwordsHit[i] );
		if ( tSetup.m_pWarning )
			tSetup.m_pWarning->SetSprintf ( "can't create phrase node, hitlists unavailable (hitlists=%d, nodes=%d)",
				iAtoms, dWords.GetLength() );
		return NULL;

	} else
	{
		// at least two words have hitlists, creating phrase node
		assert ( pQueryNode->m_dWords.GetLength() );
		assert ( pQueryNode->GetOp()==SPH_QUERY_PHRASE || pQueryNode->GetOp()==SPH_QUERY_PROXIMITY || pQueryNode->GetOp()==SPH_QUERY_QUORUM );

		// create nodes
		CSphVector<ExtNode_i *> dNodes;
		ARRAY_FOREACH ( i, dQwordsHit )
		{
			dNodes.Add ( ExtNode_i::Create ( dQwordsHit[i], pQueryNode, tSetup ) );
			dNodes.Last()->m_iAtomPos = dQwordsHit[i]->m_iAtomPos;
		}

		pResult = new T ( dNodes, *pQueryNode, tSetup );
	}

	// AND result with the words that had no hitlist
	if ( dQwords.GetLength() )
	{
		ExtNode_i * pNode = ExtNode_i::Create ( dQwords[0], pQueryNode, tSetup );
		for ( int i=1; i<dQwords.GetLength(); i++ )
			pNode = new ExtAnd_c ( pNode, ExtNode_i::Create ( dQwords[i], pQueryNode, tSetup ), tSetup );
		pResult = new ExtAnd_c ( pResult, pNode, tSetup );
	}

	if ( pQueryNode->GetCount() )
		return tSetup.m_pNodeCache->CreateProxy ( pResult, pQueryNode, tSetup );

	return pResult;
}

static ExtNode_i * CreateOrderNode ( const XQNode_t * pNode, const ISphQwordSetup & tSetup )
{
	if ( pNode->m_dChildren.GetLength()<2 )
	{
		if ( tSetup.m_pWarning )
			tSetup.m_pWarning->SetSprintf ( "order node requires at least two children" );
		return NULL;
	}

	CSphVector<ExtNode_i *> dChildren;
	ARRAY_FOREACH ( i, pNode->m_dChildren )
	{
		ExtNode_i * pChild = ExtNode_i::Create ( pNode->m_dChildren[i], tSetup );
		if ( !pChild || pChild->GotHitless() )
		{
			if ( tSetup.m_pWarning )
				tSetup.m_pWarning->SetSprintf ( "failed to create order node, hitlist unavailable" );
			ARRAY_FOREACH ( j, dChildren )
				SafeDelete ( dChildren[j] );
			return NULL;
		}
		dChildren.Add ( pChild );
	}
	ExtNode_i * pResult = new ExtOrder_c ( dChildren, tSetup );

	if ( pNode->GetCount() )
		return tSetup.m_pNodeCache->CreateProxy ( pResult, pNode, tSetup );

	return pResult;
}

//////////////////////////////////////////////////////////////////////////

ExtPayload_c::ExtPayload_c ( const XQNode_t * pNode, const ISphQwordSetup & tSetup )
{
	// sanity checks
	// this node must be only created for a huge OR of tiny expansions
	assert ( pNode->m_dWords.GetLength()==1 );
	assert ( tSetup.m_eDocinfo!=SPH_DOCINFO_INLINE );
	assert ( pNode->m_dWords.Begin()->m_pPayload );
	assert ( pNode->m_dSpec.m_dZones.GetLength()==0 && !pNode->m_dSpec.m_bZoneSpan );

	(XQKeyword_t &)m_tWord = *pNode->m_dWords.Begin();
	m_dFieldMask = pNode->m_dSpec.m_dFieldMask;
	m_iAtomPos = m_tWord.m_iAtomPos;

	BYTE sTmpWord [ 3*SPH_MAX_WORD_LEN + 4 ];
	// our little stemming buffer (morphology aware dictionary might need to change the keyword)
	strncpy ( (char*)sTmpWord, m_tWord.m_sWord.cstr(), sizeof(sTmpWord) );
	sTmpWord[sizeof(sTmpWord)-1] = '\0';

	// setup keyword disk reader
	m_tWord.m_uWordID = tSetup.m_pDict->GetWordID ( sTmpWord );
	m_tWord.m_sDictWord = (const char *)sTmpWord;
	m_tWord.m_fIDF = -1.0f;
	m_tWord.m_iDocs = 0;
	m_tWord.m_iHits = 0;

	m_pWarning = tSetup.m_pWarning;
	m_iMaxTimer = tSetup.m_iMaxTimer;

	PopulateCache ( tSetup, true );
}


void ExtPayload_c::PopulateCache ( const ISphQwordSetup & tSetup, bool bFillStat )
{
	ISphQword * pQword = tSetup.QwordSpawn ( m_tWord );
	pQword->m_sWord = m_tWord.m_sWord;
	pQword->m_uWordID = m_tWord.m_uWordID;
	pQword->m_sDictWord = m_tWord.m_sDictWord;
	pQword->m_bExpanded = true;

	bool bOk = tSetup.QwordSetup ( pQword );

	// setup keyword idf and stats
	if ( bFillStat )
	{
		m_tWord.m_iDocs = pQword->m_iDocs;
		m_tWord.m_iHits = pQword->m_iHits;
	}
	m_dCache.Reserve ( Max ( pQword->m_iHits, pQword->m_iDocs ) );

	// read and cache all docs and hits
	if ( bOk )
		for ( ;; )
	{
		const CSphMatch & tMatch = pQword->GetNextDoc ( NULL );
		if ( !tMatch.m_uDocID )
			break;

		pQword->SeekHitlist ( pQword->m_iHitlistPos );
		for ( Hitpos_t uHit = pQword->GetNextHit(); uHit!=EMPTY_HIT; uHit = pQword->GetNextHit() )
		{
			// apply field limits
			if ( !m_dFieldMask.Test ( HITMAN::GetField(uHit) ) )
				continue;

			// FIXME!!! apply zone limits too

			// apply field-start/field-end modifiers
			if ( m_tWord.m_bFieldStart && HITMAN::GetPos(uHit)!=1 )
				continue;
			if ( m_tWord.m_bFieldEnd && !HITMAN::IsEnd(uHit) )
				continue;

			// ok, this hit works, copy it
			ExtPayloadEntry_t & tEntry = m_dCache.Add ();
			tEntry.m_uDocid = tMatch.m_uDocID;
			tEntry.m_uHitpos = uHit;
		}
	}

	m_dCache.Sort();
	if ( bFillStat && m_dCache.GetLength() )
	{
		// there might be duplicate documents, but not hits, lets recalculate docs count
		// FIXME!!! that not work for RT index - get rid of ExtPayload_c and move PopulateCache code to index specific QWord
		SphDocID_t uLastDoc = m_dCache.Begin()->m_uDocid;
		const ExtPayloadEntry_t * pCur = m_dCache.Begin() + 1;
		const ExtPayloadEntry_t * pEnd = m_dCache.Begin() + m_dCache.GetLength();
		int iDocsTotal = 1;
		while ( pCur!=pEnd )
		{
			iDocsTotal += ( uLastDoc!=pCur->m_uDocid );
			uLastDoc = pCur->m_uDocid;
			pCur++;
		}
		m_tWord.m_iDocs = iDocsTotal;
	}

	// reset iterators
	m_iCurDocsEnd = 0;
	m_iCurHit = 0;

	// dismissed
	SafeDelete ( pQword );
}


void ExtPayload_c::Reset ( const ISphQwordSetup & tSetup )
{
	m_iMaxTimer = tSetup.m_iMaxTimer;
	m_dCache.Resize ( 0 );
	PopulateCache ( tSetup, false );
}


const ExtDoc_t * ExtPayload_c::GetDocsChunk()
{
	m_iCurHit = m_iCurDocsEnd;
	if ( m_iCurDocsEnd>=m_dCache.GetLength() )
		return NULL;

	// max_query_time
	if ( m_iMaxTimer>0 && sphMicroTimer()>=m_iMaxTimer )
	{
		if ( m_pWarning )
			*m_pWarning = "query time exceeded max_query_time";
		return NULL;
	}

	// interrupt by sitgerm
	if ( ExtTerm_c::m_bInterruptNow )
	{
		if ( m_pWarning )
			*m_pWarning = "Server shutdown in progress";
		return NULL;
	}

	int iDoc = 0;
	int iEnd = m_iCurDocsEnd; // shortcut, and vs2005 optimization
	while ( iDoc<MAX_DOCS-1 && iEnd<m_dCache.GetLength() )
	{
		SphDocID_t uDocid = m_dCache[iEnd].m_uDocid;

		ExtDoc_t & tDoc = m_dDocs[iDoc++];
		tDoc.m_uDocid = uDocid;
		tDoc.m_pDocinfo = NULL; // no country for old inline men
		tDoc.m_uDocFields = 0;
		tDoc.m_uHitlistOffset = 0;

		int iHitStart = iEnd;
		while ( iEnd<m_dCache.GetLength() && m_dCache[iEnd].m_uDocid==uDocid )
		{
			tDoc.m_uDocFields |= 1<< ( HITMAN::GetField ( m_dCache[iEnd].m_uHitpos ) );
			iEnd++;
		}

		int iHits = iEnd - iHitStart;
		tDoc.m_fTFIDF = float(iHits) / float(SPH_BM25_K1+iHits) * m_tWord.m_fIDF;
	}
	m_iCurDocsEnd = iEnd;

	return ReturnDocsChunk ( iDoc, "payload" );
}


const ExtHit_t * ExtPayload_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	if ( m_iCurHit>=m_iCurDocsEnd )
		return NULL;

	int iHit = 0;
	while ( pDocs->m_uDocid!=DOCID_MAX )
	{
		// skip rejected documents
		while ( m_iCurHit<m_iCurDocsEnd && m_dCache[m_iCurHit].m_uDocid<pDocs->m_uDocid )
			m_iCurHit++;
		if ( m_iCurHit>=m_iCurDocsEnd )
			break;

		// skip non-matching documents
		SphDocID_t uDocid = m_dCache[m_iCurHit].m_uDocid;
		if ( pDocs->m_uDocid<uDocid )
		{
			while ( pDocs->m_uDocid<uDocid )
				pDocs++;
			if ( pDocs->m_uDocid!=uDocid )
				continue;
		}

		// copy accepted documents
		while ( m_iCurHit<m_iCurDocsEnd && m_dCache[m_iCurHit].m_uDocid==pDocs->m_uDocid && iHit<MAX_HITS-1 )
		{
			ExtHit_t & tHit = m_dHits[iHit++];
			tHit.m_uDocid = m_dCache[m_iCurHit].m_uDocid;
			tHit.m_uHitpos = m_dCache[m_iCurHit].m_uHitpos;
			tHit.m_uQuerypos = (WORD) m_tWord.m_iAtomPos;
			tHit.m_uWeight = tHit.m_uMatchlen = tHit.m_uSpanlen = 1;
			m_iCurHit++;
		}
		if ( m_iCurHit>=m_iCurDocsEnd || iHit>=MAX_HITS-1 )
			break;
	}

	assert ( iHit>=0 && iHit<MAX_HITS );
	m_dHits[iHit].m_uDocid = DOCID_MAX;
	return ( iHit!=0 ) ? m_dHits : NULL;
}


int ExtPayload_c::GetQwords ( ExtQwordsHash_t & hQwords )
{
	int iMax = -1;
	ExtQword_t tQword;
	tQword.m_sWord = m_tWord.m_sWord;
	tQword.m_sDictWord = m_tWord.m_sDictWord;
	tQword.m_iDocs = m_tWord.m_iDocs;
	tQword.m_iHits = m_tWord.m_iHits;
	tQword.m_fIDF = -1.0f;
	tQword.m_fBoost = m_tWord.m_fBoost;
	tQword.m_iQueryPos = m_tWord.m_iAtomPos;
	tQword.m_bExpanded = true;
	tQword.m_bExcluded = m_tWord.m_bExcluded;

	hQwords.Add ( tQword, m_tWord.m_sWord );
	if ( !m_tWord.m_bExcluded )
		iMax = Max ( iMax, m_tWord.m_iAtomPos );

	return iMax;
}


void ExtPayload_c::SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
{
	// pull idfs
	if ( m_tWord.m_fIDF<0.0f )
	{
		assert ( hQwords ( m_tWord.m_sWord ) );
		m_tWord.m_fIDF = hQwords ( m_tWord.m_sWord )->m_fIDF;
	}
}

void ExtPayload_c::GetTerms ( const ExtQwordsHash_t & hQwords, CSphVector<TermPos_t> & dTermDupes ) const
{
	if ( m_tWord.m_bExcluded )
		return;

	ExtQword_t & tQword = hQwords[ m_tWord.m_sWord ];

	TermPos_t & tPos = dTermDupes.Add();
	tPos.m_uAtomPos = (WORD)m_tWord.m_iAtomPos;
	tPos.m_uQueryPos = (WORD)tQword.m_iQueryPos;
}

//////////////////////////////////////////////////////////////////////////

ExtNode_i * ExtNode_i::Create ( const XQNode_t * pNode, const ISphQwordSetup & tSetup )
{
	// empty node?
	if ( pNode->IsEmpty() )
		return NULL;

	if ( pNode->m_dWords.GetLength() || pNode->m_bVirtuallyPlain )
	{
		const int iWords = pNode->m_bVirtuallyPlain
			? pNode->m_dChildren.GetLength()
			: pNode->m_dWords.GetLength();

		if ( iWords==1 )
		{
			if ( pNode->m_dWords.Begin()->m_bExpanded && pNode->m_dWords.Begin()->m_pPayload )
				return new ExtPayload_c ( pNode, tSetup );

			if ( pNode->m_bVirtuallyPlain )
				return Create ( pNode->m_dChildren[0], tSetup );
			else
				return Create ( pNode->m_dWords[0], pNode, tSetup );
		}

		switch ( pNode->GetOp() )
		{
			case SPH_QUERY_PHRASE:
				return CreateMultiNode<ExtPhrase_c> ( pNode, tSetup, true );

			case SPH_QUERY_PROXIMITY:
				return CreateMultiNode<ExtProximity_c> ( pNode, tSetup, true );

			case SPH_QUERY_NEAR:
				return CreateMultiNode<ExtMultinear_c> ( pNode, tSetup, true );

			case SPH_QUERY_QUORUM:
			{
				assert ( pNode->m_dWords.GetLength()==0 || pNode->m_dChildren.GetLength()==0 );
				int iQuorumCount = pNode->m_dWords.GetLength()+pNode->m_dChildren.GetLength();
				int iThr = ExtQuorum_c::GetThreshold ( *pNode, iQuorumCount );
				bool bOrOperator = false;
				if ( iThr>=iQuorumCount )
				{
					// threshold is too high
					if ( tSetup.m_pWarning && !pNode->m_bPercentOp )
						tSetup.m_pWarning->SetSprintf ( "quorum threshold too high (words=%d, thresh=%d); replacing quorum operator with AND operator",
							iQuorumCount, pNode->m_iOpArg );

				} else if ( iQuorumCount>256 )
				{
					// right now quorum can only handle 256 words
					if ( tSetup.m_pWarning )
						tSetup.m_pWarning->SetSprintf ( "too many words (%d) for quorum; replacing with an AND", iQuorumCount );
				} else if ( iThr==1 )
				{
					bOrOperator = true;
				} else // everything is ok; create quorum node
				{
					return CreateMultiNode<ExtQuorum_c> ( pNode, tSetup, false );
				}

				// couldn't create quorum, make an AND node instead
				CSphVector<ExtNode_i*> dTerms;
				dTerms.Reserve ( iQuorumCount );

				ARRAY_FOREACH ( i, pNode->m_dWords )
					dTerms.Add ( Create ( pNode->m_dWords[i], pNode, tSetup ) );

				ARRAY_FOREACH ( i, pNode->m_dChildren )
					dTerms.Add ( Create ( pNode->m_dChildren[i], tSetup ) );

				// make not simple, but optimized AND node.
				dTerms.Sort ( ExtNodeTF_fn() );

				ExtNode_i * pCur = dTerms[0];
				for ( int i=1; i<dTerms.GetLength(); i++ )
				{
					if ( !bOrOperator )
						pCur = new ExtAnd_c ( pCur, dTerms[i], tSetup );
					else
						pCur = new ExtOr_c ( pCur, dTerms[i], tSetup );
				}

				if ( pNode->GetCount() )
					return tSetup.m_pNodeCache->CreateProxy ( pCur, pNode, tSetup );
				return pCur;
			}
			default:
				assert ( 0 && "unexpected plain node type" );
				return NULL;
		}

	} else
	{
		int iChildren = pNode->m_dChildren.GetLength ();
		assert ( iChildren>0 );

		// special case, operator BEFORE
		if ( pNode->GetOp ()==SPH_QUERY_BEFORE )
		{
			// before operator can not handle ZONESPAN
			bool bZoneSpan = ARRAY_ANY ( bZoneSpan, pNode->m_dChildren, pNode->m_dChildren[_any]->m_dSpec.m_bZoneSpan );
			if ( bZoneSpan && tSetup.m_pWarning )
				tSetup.m_pWarning->SetSprintf ( "BEFORE operator is incompatible with ZONESPAN, ZONESPAN ignored" );
			return CreateOrderNode ( pNode, tSetup );
		}

		// special case, AND over terms (internally reordered for speed)
		bool bAndTerms = ( pNode->GetOp()==SPH_QUERY_AND );
		bool bZonespan = true;
		bool bZonespanChecked = false;
		for ( int i=0; i<iChildren && bAndTerms; i++ )
		{
			const XQNode_t * pChildren = pNode->m_dChildren[i];
			bAndTerms = ( pChildren->m_dWords.GetLength()==1 );
			bZonespan &= pChildren->m_dSpec.m_bZoneSpan;
			if ( !bZonespan )
				break;
			bZonespanChecked = true;
		}
		bZonespan &= bZonespanChecked;

		if ( bAndTerms )
		{
			// create eval-tree terms from query-tree nodes
			CSphVector<ExtNode_i*> dTerms;
			for ( int i=0; i<iChildren; i++ )
			{
				const XQNode_t * pChild = pNode->m_dChildren[i];
				ExtNode_i * pTerm = ExtNode_i::Create ( pChild, tSetup );
				if ( pTerm )
					dTerms.Add ( pTerm );
			}

			// sort them by frequency, to speed up AND matching
			dTerms.Sort ( ExtNodeTF_fn() );

			// create the right eval-tree node
			ExtNode_i * pCur = dTerms[0];
			for ( int i=1; i<dTerms.GetLength(); i++ )
				if ( bZonespan )
					pCur = new ExtAndZonespan_c ( pCur, dTerms[i], tSetup, pNode->m_dChildren[0] );
				else
					pCur = new ExtAnd_c ( pCur, dTerms[i], tSetup );

			// zonespan has Extra data which is not (yet?) covered by common-node optimizations,
			// so we need to avoid those for zonespan
			if ( !bZonespan && pNode->GetCount() )
				return tSetup.m_pNodeCache->CreateProxy ( pCur, pNode, tSetup );

			return pCur;
		}

		// Multinear and phrase could be also non-plain, so here is the second entry for it.
		if ( pNode->GetOp()==SPH_QUERY_NEAR )
			return CreateMultiNode<ExtMultinear_c> ( pNode, tSetup, true );
		if ( pNode->GetOp()==SPH_QUERY_PHRASE )
			return CreateMultiNode<ExtPhrase_c> ( pNode, tSetup, true );

		// generic create
		ExtNode_i * pCur = NULL;
		for ( int i=0; i<iChildren; i++ )
		{
			ExtNode_i * pNext = ExtNode_i::Create ( pNode->m_dChildren[i], tSetup );
			if ( !pNext ) continue;
			if ( !pCur )
			{
				pCur = pNext;
				continue;
			}
			switch ( pNode->GetOp() )
			{
				case SPH_QUERY_OR:			pCur = new ExtOr_c ( pCur, pNext, tSetup ); break;
				case SPH_QUERY_MAYBE:		pCur = new ExtMaybe_c ( pCur, pNext, tSetup ); break;
				case SPH_QUERY_AND:			pCur = new ExtAnd_c ( pCur, pNext, tSetup ); break;
				case SPH_QUERY_ANDNOT:		pCur = new ExtAndNot_c ( pCur, pNext, tSetup ); break;
				case SPH_QUERY_SENTENCE:	pCur = new ExtUnit_c ( pCur, pNext, pNode->m_dSpec.m_dFieldMask, tSetup, MAGIC_WORD_SENTENCE ); break;
				case SPH_QUERY_PARAGRAPH:	pCur = new ExtUnit_c ( pCur, pNext, pNode->m_dSpec.m_dFieldMask, tSetup, MAGIC_WORD_PARAGRAPH ); break;
				default:					assert ( 0 && "internal error: unhandled op in ExtNode_i::Create()" ); break;
			}
		}
		if ( pCur && pNode->GetCount() )
			return tSetup.m_pNodeCache->CreateProxy ( pCur, pNode, tSetup );
		return pCur;
	}
}


//////////////////////////////////////////////////////////////////////////


inline bool TermAcceptor_c<TERM_POS_FIELD_LIMIT>::IsAcceptableHit ( const ExtHit_t * pHit ) const
{
	return HITMAN::GetPos ( pHit->m_uHitpos )<=m_iMaxFieldPos;
}

template<>
inline bool TermAcceptor_c<TERM_POS_FIELD_START>::IsAcceptableHit ( const ExtHit_t * pHit ) const
{
	return HITMAN::GetPos ( pHit->m_uHitpos )==1;
}

template<>
inline bool TermAcceptor_c<TERM_POS_FIELD_END>::IsAcceptableHit ( const ExtHit_t * pHit ) const
{
	return HITMAN::IsEnd ( pHit->m_uHitpos );
}

template<>
inline bool TermAcceptor_c<TERM_POS_FIELD_STARTEND>::IsAcceptableHit ( const ExtHit_t * pHit ) const
{
	return HITMAN::GetPos ( pHit->m_uHitpos )==1 && HITMAN::IsEnd ( pHit->m_uHitpos );
}

inline bool TermAcceptor_c<TERM_POS_ZONES>::IsAcceptableHit ( const ExtHit_t * pHit ) const
{
	assert ( m_pZoneChecker );

	if ( m_uLastZonedId!=pHit->m_uDocid )
		m_iCheckFrom = 0;
	m_uLastZonedId = pHit->m_uDocid;

	// only check zones that actually match this document
	for ( int i=m_iCheckFrom; i<m_dZones.GetLength(); i++ )
	{
		SphZoneHit_e eState = m_pZoneChecker->IsInZone ( m_dZones[i], pHit, NULL );
		switch ( eState )
		{
			case SPH_ZONE_FOUND:
				return true;
			case SPH_ZONE_NO_DOCUMENT:
				Swap ( m_dZones[i], m_dZones[m_iCheckFrom] );
				m_iCheckFrom++;
				break;
			default:
				break;
		}
	}
	return false;
}


//////////////////////////////////////////////////////////////////////////

ExtTwofer_c::ExtTwofer_c ( ExtNode_i * pFirst, ExtNode_i * pSecond, const ISphQwordSetup & tSetup )
{
	Init ( pFirst, pSecond, tSetup );
}

inline void	ExtTwofer_c::Init ( ExtNode_i * pLeft, ExtNode_i * pRight, const ISphQwordSetup & tSetup )
{
	m_pLeft = pLeft;
	m_pRight = pRight;
	m_pCurHitL = NULL;
	m_pCurHitR = NULL;
	m_pCurDocL = NULL;
	m_pCurDocR = NULL;
	m_uNodePosL = 0;
	m_uNodePosR = 0;
	m_uMatchedDocid = 0;
	m_iAtomPos = ( pLeft && pLeft->m_iAtomPos ) ? pLeft->m_iAtomPos : 0;
	if ( pRight && pRight->m_iAtomPos && pRight->m_iAtomPos<m_iAtomPos && m_iAtomPos!=0 )
		m_iAtomPos = pRight->m_iAtomPos;
	AllocDocinfo ( tSetup );
}

ExtTwofer_c::~ExtTwofer_c ()
{
	SafeDelete ( m_pLeft );
	SafeDelete ( m_pRight );
}

void ExtTwofer_c::Reset ( const ISphQwordSetup & tSetup )
{
	m_pLeft->Reset ( tSetup );
	m_pRight->Reset ( tSetup );
	m_pCurHitL = NULL;
	m_pCurHitR = NULL;
	m_pCurDocL = NULL;
	m_pCurDocR = NULL;
	m_uMatchedDocid = 0;
}

int ExtTwofer_c::GetQwords ( ExtQwordsHash_t & hQwords )
{
	int iMax1 = m_pLeft->GetQwords ( hQwords );
	int iMax2 = m_pRight->GetQwords ( hQwords );
	return Max ( iMax1, iMax2 );
}

void ExtTwofer_c::SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
{
	m_pLeft->SetQwordsIDF ( hQwords );
	m_pRight->SetQwordsIDF ( hQwords );
}

void ExtTwofer_c::GetTerms ( const ExtQwordsHash_t & hQwords, CSphVector<TermPos_t> & dTermDupes ) const
{
	m_pLeft->GetTerms ( hQwords, dTermDupes );
	m_pRight->GetTerms ( hQwords, dTermDupes );
}

//////////////////////////////////////////////////////////////////////////
struct CmpAndHitReverse_fn
{
	inline bool IsLess(const ExtHit_t& a, const ExtHit_t& b) const
	{
		return (a.m_uDocid < b.m_uDocid || (a.m_uDocid == b.m_uDocid && a.m_uHitpos < b.m_uHitpos) || (a.m_uDocid == b.m_uDocid && a.m_uHitpos == b.m_uHitpos && a.m_uQuerypos > b.m_uQuerypos));
	}
};

const ExtDoc_t * ExtAnd_c::GetDocsChunk()
{
	const ExtDoc_t * pCurL = m_pCurDocL;
	const ExtDoc_t * pCurR = m_pCurDocR;

	int iDoc = 0;
	CSphRowitem * pDocinfo = m_pDocinfo;
	for ( ;; )
	{
		// if any of the pointers is empty, *and* there is no data yet, process next child chunk
		// if there is data, we can't advance, because child hitlist offsets would be lost
		if ( !pCurL || !pCurR )
		{
			if ( iDoc!=0 )
				break;

			if ( !pCurL )
			{
				if ( pCurR && pCurR->m_uDocid!=DOCID_MAX )
					m_pLeft->HintDocid ( pCurR->m_uDocid );
				pCurL = m_pLeft->GetDocsChunk();
			}
			if ( !pCurR )
			{
				if ( pCurL && pCurL->m_uDocid!=DOCID_MAX )
					m_pRight->HintDocid ( pCurL->m_uDocid );
				pCurR = m_pRight->GetDocsChunk();
			}
			if ( !pCurL || !pCurR )
			{
				m_pCurDocL = NULL;
				m_pCurDocR = NULL;
				return NULL;
			}
		}

		// find common matches
		assert ( pCurL && pCurR );
		while ( iDoc<MAX_DOCS-1 )
		{
			// find next matching docid
			while ( pCurL->m_uDocid < pCurR->m_uDocid ) pCurL++;
			if ( pCurL->m_uDocid==DOCID_MAX ) { pCurL = NULL; break; }

			while ( pCurR->m_uDocid < pCurL->m_uDocid ) pCurR++;
			if ( pCurR->m_uDocid==DOCID_MAX ) { pCurR = NULL; break; }

			if ( pCurL->m_uDocid!=pCurR->m_uDocid ) continue;

			// emit it
			ExtDoc_t & tDoc = m_dDocs[iDoc++];
			tDoc.m_uDocid = pCurL->m_uDocid;
			tDoc.m_uDocFields = pCurL->m_uDocFields | pCurR->m_uDocFields; // not necessary
			tDoc.m_uHitlistOffset = -1;
			tDoc.m_fTFIDF = pCurL->m_fTFIDF + pCurR->m_fTFIDF;
			CopyExtDocinfo ( tDoc, *pCurL, &pDocinfo, m_iStride );

			// skip it
			pCurL++; if ( pCurL->m_uDocid==DOCID_MAX ) pCurL = NULL;
			pCurR++; if ( pCurR->m_uDocid==DOCID_MAX ) pCurR = NULL;
			if ( !pCurL || !pCurR ) break;
		}
	}

	m_pCurDocL = pCurL;
	m_pCurDocR = pCurR;

	return ReturnDocsChunk ( iDoc, "and" );
}


const ExtHit_t * ExtAnd_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	const ExtHit_t * pCurL = m_pCurHitL;
	const ExtHit_t * pCurR = m_pCurHitR;

	if ( m_uMatchedDocid < pDocs->m_uDocid )
		m_uMatchedDocid = 0;

	int iHit = 0;
	WORD uNodePos0 = m_uNodePosL;
	WORD uNodePos1 = m_uNodePosR;
	while ( iHit<MAX_HITS-1 )
	{
		// emit hits, while possible
		if ( m_uMatchedDocid!=0
			&& m_uMatchedDocid!=DOCID_MAX
			&& ( ( pCurL && pCurL->m_uDocid==m_uMatchedDocid ) || ( pCurR && pCurR->m_uDocid==m_uMatchedDocid ) ) )
		{
			// merge, while possible
			if ( pCurL && pCurR && pCurL->m_uDocid==m_uMatchedDocid && pCurR->m_uDocid==m_uMatchedDocid )
				while ( iHit<MAX_HITS-1 )
			{
				if ( ( pCurL->m_uHitpos < pCurR->m_uHitpos )
					|| ( pCurL->m_uHitpos==pCurR->m_uHitpos && pCurL->m_uQuerypos<=pCurR->m_uQuerypos ) )
				{
					m_dHits[iHit] = *pCurL++;
					if ( uNodePos0!=0 )
						m_dHits[iHit++].m_uNodepos = uNodePos0;
					else
						iHit++;
					if ( pCurL->m_uDocid!=m_uMatchedDocid )
						break;
				} else
				{
					m_dHits[iHit] = *pCurR++;
					if ( uNodePos1!=0 )
						m_dHits[iHit++].m_uNodepos = uNodePos1;
					else
						iHit++;
					if ( pCurR->m_uDocid!=m_uMatchedDocid )
						break;
				}
			}

			// copy tail, while possible, unless the other child is at the end of a hit block
			if ( pCurL && pCurL->m_uDocid==m_uMatchedDocid && !( pCurR && pCurR->m_uDocid==DOCID_MAX ) )
			{
				while ( pCurL->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
				{
					m_dHits[iHit] = *pCurL++;
					if ( uNodePos0!=0 )
						m_dHits[iHit++].m_uNodepos = uNodePos0;
					else
						iHit++;
				}
			}
			if ( pCurR && pCurR->m_uDocid==m_uMatchedDocid && !( pCurL && pCurL->m_uDocid==DOCID_MAX ) )
			{
				while ( pCurR->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
				{
					m_dHits[iHit] = *pCurR++;
					if ( uNodePos1!=0 )
						m_dHits[iHit++].m_uNodepos = uNodePos1;
					else
						iHit++;
				}
			}
		}

		// move on
		if ( ( pCurL && pCurL->m_uDocid!=m_uMatchedDocid && pCurL->m_uDocid!=DOCID_MAX )
			&& ( pCurR && pCurR->m_uDocid!=m_uMatchedDocid && pCurR->m_uDocid!=DOCID_MAX ) )
				m_uMatchedDocid = 0;

		// warmup if needed
		if ( !pCurL || pCurL->m_uDocid==DOCID_MAX ) pCurL = m_pLeft->GetHitsChunk ( pDocs );
		if ( !pCurR || pCurR->m_uDocid==DOCID_MAX ) pCurR = m_pRight->GetHitsChunk ( pDocs );

		// one of the hitlists is over
		if ( !pCurL || !pCurR )
		{
			// if one is over, we might still need to copy the other one. otherwise, skip it
			if ( ( pCurL && pCurL->m_uDocid==m_uMatchedDocid ) || ( pCurR && pCurR->m_uDocid==m_uMatchedDocid ) )
				continue;

			if ( pCurL )
				while ( ( pCurL = m_pLeft->GetHitsChunk ( pDocs ) )!=NULL );
			if ( pCurR )
				while ( ( pCurR = m_pRight->GetHitsChunk ( pDocs ) )!=NULL );

			if ( !pCurL && !pCurR )
				break; // both are over, we're done
		}

		// find matching doc
		assert ( pCurR && pCurL );
		while ( !m_uMatchedDocid )
		{
			while ( pCurL->m_uDocid < pCurR->m_uDocid ) pCurL++;
			if ( pCurL->m_uDocid==DOCID_MAX ) break;

			while ( pCurR->m_uDocid < pCurL->m_uDocid ) pCurR++;
			if ( pCurR->m_uDocid==DOCID_MAX ) break;

			if ( pCurL->m_uDocid==pCurR->m_uDocid ) m_uMatchedDocid = pCurL->m_uDocid;
		}
	}

	m_pCurHitL = pCurL;
	m_pCurHitR = pCurR;

	if ( iHit && m_bQPosReverse )
		sphSort ( m_dHits, iHit, CmpAndHitReverse_fn() );

	return ReturnHitsChunk ( iHit, "and", m_bQPosReverse );
}

//////////////////////////////////////////////////////////////////////////

bool ExtAndZonespanned_c::IsSameZonespan ( const ExtHit_t * pHit1, const ExtHit_t * pHit2 ) const
{
	ARRAY_FOREACH ( i, m_dZones )
	{
		int iSpan1, iSpan2;
		if ( m_pZoneChecker->IsInZone ( m_dZones[i], pHit1, &iSpan1 )==SPH_ZONE_FOUND && m_pZoneChecker->IsInZone ( m_dZones[i], pHit2, &iSpan2 )==SPH_ZONE_FOUND )
		{
			assert ( iSpan1>=0 && iSpan2>=0 );
			if ( iSpan1==iSpan2 )
				return true;
		}
	}
	return false;
}

const ExtHit_t * ExtAndZonespanned_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	const ExtHit_t * pCurL = m_pCurHitL;
	const ExtHit_t * pCurR = m_pCurHitR;

	if ( m_uMatchedDocid < pDocs->m_uDocid )
		m_uMatchedDocid = 0;

	int iHit = 0;
	WORD uNodePos0 = m_uNodePosL;
	WORD uNodePos1 = m_uNodePosR;
	while ( iHit<MAX_HITS-1 )
	{
		// emit hits, while possible
		if ( m_uMatchedDocid!=0
			&& m_uMatchedDocid!=DOCID_MAX
			&& ( ( pCurL && pCurL->m_uDocid==m_uMatchedDocid ) || ( pCurR && pCurR->m_uDocid==m_uMatchedDocid ) ) )
		{
			// merge, while possible
			if ( pCurL && pCurR && pCurL->m_uDocid==m_uMatchedDocid && pCurR->m_uDocid==m_uMatchedDocid )
				while ( iHit<MAX_HITS-1 )
				{
					if ( ( pCurL->m_uHitpos < pCurR->m_uHitpos )
						|| ( pCurL->m_uHitpos==pCurR->m_uHitpos && pCurL->m_uQuerypos<=pCurR->m_uQuerypos ) )
					{
						if ( IsSameZonespan ( pCurL, pCurR ) )
						{
							m_dHits[iHit] = *pCurL;
							if ( uNodePos0!=0 )
								m_dHits[iHit].m_uNodepos = uNodePos0;
							iHit++;
						}
						pCurL++;
						if ( pCurL->m_uDocid!=m_uMatchedDocid )
							break;
					} else
					{
						if ( IsSameZonespan ( pCurL, pCurR ) )
						{
							m_dHits[iHit] = *pCurR;
							if ( uNodePos1!=0 )
								m_dHits[iHit].m_uNodepos = uNodePos1;
							iHit++;
						}
						pCurR++;
						if ( pCurR->m_uDocid!=m_uMatchedDocid )
							break;
					}
				}

			// our special GetDocsChunk made the things so simply, that we doesn't need to care about tail hits at all.
			// copy tail, while possible, unless the other child is at the end of a hit block
			if ( pCurL && pCurL->m_uDocid==m_uMatchedDocid && !( pCurR && pCurR->m_uDocid==DOCID_MAX ) )
			{
				while ( pCurL->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
				{
					pCurL++;
				}
			}
			if ( pCurR && pCurR->m_uDocid==m_uMatchedDocid && !( pCurL && pCurL->m_uDocid==DOCID_MAX ) )
			{
				while ( pCurR->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
				{
					pCurR++;
				}
			}
		}

		// move on
		if ( ( pCurL && pCurL->m_uDocid!=m_uMatchedDocid && pCurL->m_uDocid!=DOCID_MAX )
			&& ( pCurR && pCurR->m_uDocid!=m_uMatchedDocid && pCurR->m_uDocid!=DOCID_MAX ) )
			m_uMatchedDocid = 0;

		// warmup if needed
		if ( !pCurL || pCurL->m_uDocid==DOCID_MAX )
		{
			pCurL = m_pLeft->GetHitsChunk ( pDocs );
		}
		if ( !pCurR || pCurR->m_uDocid==DOCID_MAX )
		{
			pCurR = m_pRight->GetHitsChunk ( pDocs );
		}

		// one of the hitlists is over
		if ( !pCurL || !pCurR )
		{
			if ( !pCurL && !pCurR ) break; // both are over, we're done

			// one is over, but we still need to copy the other one
			m_uMatchedDocid = pCurL ? pCurL->m_uDocid : pCurR->m_uDocid;
			assert ( m_uMatchedDocid!=DOCID_MAX );
			continue;
		}

		// find matching doc
		assert ( pCurR && pCurL );
		while ( !m_uMatchedDocid )
		{
			while ( pCurL->m_uDocid < pCurR->m_uDocid ) pCurL++;
			if ( pCurL->m_uDocid==DOCID_MAX ) break;

			while ( pCurR->m_uDocid < pCurL->m_uDocid ) pCurR++;
			if ( pCurR->m_uDocid==DOCID_MAX ) break;

			if ( pCurL->m_uDocid==pCurR->m_uDocid ) m_uMatchedDocid = pCurL->m_uDocid;
		}
	}

	m_pCurHitL = pCurL;
	m_pCurHitR = pCurR;

	if ( iHit && m_bQPosReverse )
		sphSort ( m_dHits, iHit, CmpAndHitReverse_fn() );

	return ReturnHitsChunk ( iHit, "and-zonespan", m_bQPosReverse );
}

//////////////////////////////////////////////////////////////////////////

const ExtDoc_t * ExtOr_c::GetDocsChunk()
{
	const ExtDoc_t * pCurL = m_pCurDocL;
	const ExtDoc_t * pCurR = m_pCurDocR;

	DWORD uTouched = 0;
	int iDoc = 0;
	CSphRowitem * pDocinfo = m_pDocinfo;
	while ( iDoc<MAX_DOCS-1 )
	{
		// if any of the pointers is empty, and not touched yet, advance
		if ( !pCurL || pCurL->m_uDocid==DOCID_MAX )
		{
			if ( uTouched & 1 ) break; // it was touched, so we can't advance, because child hitlist offsets would be lost
			pCurL = m_pLeft->GetDocsChunk();
		}
		if ( !pCurR || pCurR->m_uDocid==DOCID_MAX )
		{
			if ( uTouched & 2 ) break; // it was touched, so we can't advance, because child hitlist offsets would be lost
			pCurR = m_pRight->GetDocsChunk();
		}

		// check if we're over
		if ( !pCurL && !pCurR ) break;

		// merge lists while we can, copy tail while if we can not
		if ( pCurL && pCurR )
		{
			// merge lists if we have both of them
			while ( iDoc<MAX_DOCS-1 )
			{
				// copy min docids from 1st child
				while ( pCurL->m_uDocid < pCurR->m_uDocid && iDoc<MAX_DOCS-1 )
				{
					CopyExtDoc ( m_dDocs[iDoc++], *pCurL++, &pDocinfo, m_iStride );
					uTouched |= 1;
				}
				if ( pCurL->m_uDocid==DOCID_MAX ) { pCurL = NULL; break; }

				// copy min docids from 2nd child
				while ( pCurR->m_uDocid < pCurL->m_uDocid && iDoc<MAX_DOCS-1 )
				{
					CopyExtDoc ( m_dDocs[iDoc++], *pCurR++, &pDocinfo, m_iStride );
					uTouched |= 2;
				}
				if ( pCurR->m_uDocid==DOCID_MAX ) { pCurR = NULL; break; }

				// copy min docids from both children
				assert ( pCurL->m_uDocid && pCurL->m_uDocid!=DOCID_MAX );
				assert ( pCurR->m_uDocid && pCurR->m_uDocid!=DOCID_MAX );

				while ( pCurL->m_uDocid==pCurR->m_uDocid && pCurL->m_uDocid!=DOCID_MAX && iDoc<MAX_DOCS-1 )
				{
					m_dDocs[iDoc] = *pCurL;
					m_dDocs[iDoc].m_uDocFields = pCurL->m_uDocFields | pCurR->m_uDocFields; // not necessary
					m_dDocs[iDoc].m_fTFIDF = pCurL->m_fTFIDF + pCurR->m_fTFIDF;
					CopyExtDocinfo ( m_dDocs[iDoc], *pCurL, &pDocinfo, m_iStride );
					iDoc++;
					pCurL++;
					pCurR++;
					uTouched |= 3;
				}
				if ( pCurL->m_uDocid==DOCID_MAX ) { pCurL = NULL; break; }
				if ( pCurR->m_uDocid==DOCID_MAX ) { pCurR = NULL; break; }
			}
		} else
		{
			// copy tail if we don't have both lists
			const ExtDoc_t * pList = pCurL ? pCurL : pCurR;
			if ( pList->m_uDocid!=DOCID_MAX && iDoc<MAX_DOCS-1 )
			{
				while ( pList->m_uDocid!=DOCID_MAX && iDoc<MAX_DOCS-1 )
					CopyExtDoc ( m_dDocs[iDoc++], *pList++, &pDocinfo, m_iStride );
				uTouched |= pCurL ? 1 : 2;
			}

			if ( pList->m_uDocid==DOCID_MAX ) pList = NULL;
			if ( pCurL )
				pCurL = pList;
			else
				pCurR = pList;
		}
	}

	m_pCurDocL = pCurL;
	m_pCurDocR = pCurR;

	return ReturnDocsChunk ( iDoc, "or" );
}

const ExtHit_t * ExtOr_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	const ExtHit_t * pCurL = m_pCurHitL;
	const ExtHit_t * pCurR = m_pCurHitR;

	int iHit = 0;
	while ( iHit<MAX_HITS-1 )
	{
		// emit hits, while possible
		if ( m_uMatchedDocid!=0
			&& m_uMatchedDocid!=DOCID_MAX
			&& ( ( pCurL && pCurL->m_uDocid==m_uMatchedDocid ) || ( pCurR && pCurR->m_uDocid==m_uMatchedDocid ) ) )
		{
			// merge, while possible
			if ( pCurL && pCurR && pCurL->m_uDocid==m_uMatchedDocid && pCurR->m_uDocid==m_uMatchedDocid )
				while ( iHit<MAX_HITS-1 )
			{
				if ( ( pCurL->m_uHitpos<pCurR->m_uHitpos ) ||
					( pCurL->m_uHitpos==pCurR->m_uHitpos && pCurL->m_uQuerypos<=pCurR->m_uQuerypos ) )
				{
					m_dHits[iHit++] = *pCurL++;
					if ( pCurL->m_uDocid!=m_uMatchedDocid )
						break;
				} else
				{
					m_dHits[iHit++] = *pCurR++;
					if ( pCurR->m_uDocid!=m_uMatchedDocid )
						break;
				}
			}

			// a pretty tricky bit
			// one of the nodes might have run out of current hits chunk (rather hits at all)
			// so we need to get the next hits chunk NOW, check for that condition, and keep merging
			// simply going to tail hits copying is incorrect, it could copy in wrong order
			// example, word A, pos 1, 2, 3, hit chunk ends, 4, 5, 6, word B, pos 7, 8, 9
			if ( !pCurL || pCurL->m_uDocid==DOCID_MAX )
			{
				pCurL = m_pLeft->GetHitsChunk ( pDocs );
				if ( pCurL && pCurL->m_uDocid==m_uMatchedDocid )
					continue;
			}
			if ( !pCurR || pCurR->m_uDocid==DOCID_MAX )
			{
				pCurR = m_pRight->GetHitsChunk ( pDocs );
				if ( pCurR && pCurR->m_uDocid==m_uMatchedDocid )
					continue;
			}

			// copy tail, while possible
			if ( pCurL && pCurL->m_uDocid==m_uMatchedDocid )
			{
				while ( pCurL->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
					m_dHits[iHit++] = *pCurL++;
			} else
			{
				assert ( pCurR && pCurR->m_uDocid==m_uMatchedDocid );
				while ( pCurR->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
					m_dHits[iHit++] = *pCurR++;
			}
		}

		// move on
		if ( ( pCurL && pCurL->m_uDocid!=m_uMatchedDocid ) && ( pCurR && pCurR->m_uDocid!=m_uMatchedDocid ) )
			m_uMatchedDocid = 0;

		// warmup if needed
		if ( !pCurL || pCurL->m_uDocid==DOCID_MAX ) pCurL = m_pLeft->GetHitsChunk ( pDocs );
		if ( !pCurR || pCurR->m_uDocid==DOCID_MAX ) pCurR = m_pRight->GetHitsChunk ( pDocs );
		if ( !pCurL && !pCurR )
			break;

		m_uMatchedDocid = ( pCurL && pCurR )
			? Min ( pCurL->m_uDocid, pCurR->m_uDocid )
			: ( pCurL ? pCurL->m_uDocid : pCurR->m_uDocid );
	}

	m_pCurHitL = pCurL;
	m_pCurHitR = pCurR;

	return ReturnHitsChunk ( iHit, "or", false );
}

//////////////////////////////////////////////////////////////////////////

// returns documents from left subtree only
//
// each call returns only one document and rewinds docs in rhs to look for the
// same docID as in lhs
//
// we do this to return hits from rhs too which we need to affect match rank
const ExtDoc_t * ExtMaybe_c::GetDocsChunk()
{
	const ExtDoc_t * pCurL = m_pCurDocL;
	const ExtDoc_t * pCurR = m_pCurDocR;
	int iDoc = 0;

	// try to get next doc from lhs
	if ( !pCurL || pCurL->m_uDocid==DOCID_MAX )
		pCurL = m_pLeft->GetDocsChunk();

	// we have nothing to do if there is no doc from lhs
	if ( pCurL )
	{
		// look for same docID in rhs
		do
		{
			if ( !pCurR || pCurR->m_uDocid==DOCID_MAX )
				pCurR = m_pRight->GetDocsChunk();
			else if ( pCurR->m_uDocid<pCurL->m_uDocid )
				++pCurR;
		} while ( pCurR && pCurR->m_uDocid<pCurL->m_uDocid );

		m_dDocs [ iDoc ] = *pCurL;
		// alter doc like with | fulltext operator if we have it both in lhs and rhs
		if ( pCurR && pCurL->m_uDocid==pCurR->m_uDocid )
		{
			m_dDocs [ iDoc ].m_uDocFields |= pCurR->m_uDocFields;
			m_dDocs [ iDoc ].m_fTFIDF += pCurR->m_fTFIDF;
		}
		++pCurL;
		++iDoc;
	}

	m_pCurDocL = pCurL;
	m_pCurDocR = pCurR;

	return ReturnDocsChunk ( iDoc, "maybe" );
}

//////////////////////////////////////////////////////////////////////////

ExtAndNot_c::ExtAndNot_c ( ExtNode_i * pFirst, ExtNode_i * pSecond, const ISphQwordSetup & tSetup )
	: ExtTwofer_c ( pFirst, pSecond, tSetup )
	, m_bPassthrough ( false )
{
}

const ExtDoc_t * ExtAndNot_c::GetDocsChunk()
{
	// if reject-list is over, simply pass through to accept-list
	if ( m_bPassthrough )
		return m_pLeft->GetDocsChunk();

	// otherwise, do some removals
	const ExtDoc_t * pCurL = m_pCurDocL;
	const ExtDoc_t * pCurR = m_pCurDocR;

	int iDoc = 0;
	CSphRowitem * pDocinfo = m_pDocinfo;
	while ( iDoc<MAX_DOCS-1 )
	{
		// pull more docs from accept, if needed
		if ( !pCurL || pCurL->m_uDocid==DOCID_MAX )
		{
			// there were matches; we can not pull more because that'd fuckup hitlists
			if ( iDoc )
				break;

			// no matches so far; go pull
			pCurL = m_pLeft->GetDocsChunk();
			if ( !pCurL )
				break;
		}

		// pull more docs from reject, if nedeed
		if ( !pCurR || pCurR->m_uDocid==DOCID_MAX )
			pCurR = m_pRight->GetDocsChunk();

		// if there's nothing to filter against, simply copy leftovers
		if ( !pCurR )
		{
			assert ( pCurL );
			while ( pCurL->m_uDocid!=DOCID_MAX && iDoc<MAX_DOCS-1 )
				CopyExtDoc ( m_dDocs[iDoc++], *pCurL++, &pDocinfo, m_iStride );

			if ( pCurL->m_uDocid==DOCID_MAX )
				m_bPassthrough = true;

			break;
		}

		// perform filtering
		assert ( pCurL );
		assert ( pCurR );
		for ( ;; )
		{
			assert ( iDoc<MAX_DOCS-1 );
			assert ( pCurL->m_uDocid!=DOCID_MAX );
			assert ( pCurR->m_uDocid!=DOCID_MAX );

			// copy accepted until min rejected id
			while ( pCurL->m_uDocid < pCurR->m_uDocid && iDoc<MAX_DOCS-1 )
				CopyExtDoc ( m_dDocs[iDoc++], *pCurL++, &pDocinfo, m_iStride );
			if ( pCurL->m_uDocid==DOCID_MAX || iDoc==MAX_DOCS-1 ) break;

			// skip rejected until min accepted id
			while ( pCurR->m_uDocid < pCurL->m_uDocid ) pCurR++;
			if ( pCurR->m_uDocid==DOCID_MAX ) break;

			// skip both while ids match
			while ( pCurL->m_uDocid==pCurR->m_uDocid && pCurL->m_uDocid!=DOCID_MAX )
			{
				pCurL++;
				pCurR++;
			}
			if ( pCurL->m_uDocid==DOCID_MAX || pCurR->m_uDocid==DOCID_MAX ) break;
		}
	}

	m_pCurDocL = pCurL;
	m_pCurDocR = pCurR;

	return ReturnDocsChunk ( iDoc, "andnot" );
}

const ExtHit_t * ExtAndNot_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	return m_pLeft->GetHitsChunk ( pDocs );
}

void ExtAndNot_c::Reset ( const ISphQwordSetup & tSetup )
{
	m_bPassthrough = false;
	ExtTwofer_c::Reset ( tSetup );
}

//////////////////////////////////////////////////////////////////////////

ExtNWayT::ExtNWayT ( const CSphVector<ExtNode_i *> & dNodes, const ISphQwordSetup & tSetup )
	: m_pNode ( NULL )
	, m_pDocs ( NULL )
	, m_pHits ( NULL )
	, m_pDoc ( NULL )
	, m_pHit ( NULL )
	, m_pMyDoc ( NULL )
	, m_pMyHit ( NULL )
	, m_uLastDocID ( 0 )
	, m_uMatchedDocid ( 0 )
	, m_uHitsOverFor ( 0 )
{
	assert ( dNodes.GetLength()>1 );
	m_iAtomPos = dNodes[0]->m_iAtomPos;
	m_dMyHits[0].m_uDocid = DOCID_MAX;
	AllocDocinfo ( tSetup );
}

ExtNWayT::~ExtNWayT ()
{
	SafeDelete ( m_pNode );
}

void ExtNWayT::Reset ( const ISphQwordSetup & tSetup )
{
	m_pNode->Reset ( tSetup );
	m_pDocs = NULL;
	m_pHits = NULL;
	m_pDoc = NULL;
	m_pHit = NULL;
	m_pMyDoc = NULL;
	m_pMyHit = NULL;
	m_uLastDocID = 0;
	m_uMatchedDocid = 0;
	m_uHitsOverFor = 0;
	m_dMyHits[0].m_uDocid = DOCID_MAX;
}

int ExtNWayT::GetQwords ( ExtQwordsHash_t & hQwords )
{
	assert ( m_pNode );
	return m_pNode->GetQwords ( hQwords );
}

void ExtNWayT::SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
{
	assert ( m_pNode );
	m_pNode->SetQwordsIDF ( hQwords );
}

void ExtNWayT::GetTerms ( const ExtQwordsHash_t & hQwords, CSphVector<TermPos_t> & dTermDupes ) const
{
	assert ( m_pNode );
	m_pNode->GetTerms ( hQwords, dTermDupes );
}

uint64_t ExtNWayT::GetWordID() const
{
	assert ( m_pNode );
	return m_pNode->GetWordID();
}

template < class FSM >
const ExtDoc_t * ExtNWay_c<FSM>::GetDocsChunk()
{
	// initial warmup
	if ( !m_pDoc )
	{
		if ( !m_pDocs ) m_pDocs = m_pNode->GetDocsChunk();
		if ( !m_pDocs ) return NULL; // no more docs
		m_pDoc = m_pDocs;
	}

	// shortcuts
	const ExtDoc_t * pDoc = m_pDoc;
	const ExtHit_t * pHit = m_pHit;

	FSM::ResetFSM();

	// skip leftover hits
	while ( m_uLastDocID )
	{
		if ( !pHit || pHit->m_uDocid==DOCID_MAX )
		{
			pHit = m_pHits = m_pNode->GetHitsChunk ( m_pDocs );
			if ( !pHit )
				break;
		}

		while ( pHit->m_uDocid==m_uLastDocID )
			pHit++;

		if ( pHit->m_uDocid!=DOCID_MAX && pHit->m_uDocid!=m_uLastDocID )
			m_uLastDocID = 0;
	}

	// search for matches
	int iDoc = 0;
	int iHit = 0;
	CSphRowitem * pDocinfo = m_pDocinfo;
	while ( iHit<MAX_HITS-1 )
	{
		// out of hits?
		if ( !pHit || pHit->m_uDocid==DOCID_MAX )
		{
			// grab more hits
			pHit = m_pHits = m_pNode->GetHitsChunk ( m_pDocs );
			if ( m_pHits ) continue;

			m_uMatchedDocid = 0;

			// no more hits for current docs chunk; grab more docs
			pDoc = m_pDocs = m_pNode->GetDocsChunk();
			if ( !m_pDocs ) break;

			// we got docs, there must be hits
			pHit = m_pHits = m_pNode->GetHitsChunk ( m_pDocs );
			assert ( pHit );
			continue;
		}

		// check if the incoming hit is out of bounds, or affects min pos
		if ( pHit->m_uDocid!=m_uMatchedDocid )
		{
			m_uMatchedDocid = pHit->m_uDocid;
			FSM::ResetFSM();
			continue;
		}

		if ( FSM::HitFSM ( pHit, &m_dMyHits[iHit] ) )
		{
			// emit document, if it's new
			if ( pHit->m_uDocid!=m_uLastDocID )
			{
				assert ( pDoc->m_uDocid<=pHit->m_uDocid );
				while ( pDoc->m_uDocid < pHit->m_uDocid ) pDoc++;
				assert ( pDoc->m_uDocid==pHit->m_uDocid );

				m_dDocs[iDoc].m_uDocid = pHit->m_uDocid;
				m_dDocs[iDoc].m_uDocFields = 1<< ( HITMAN::GetField ( pHit->m_uHitpos ) ); // non necessary
				m_dDocs[iDoc].m_uHitlistOffset = -1;
				m_dDocs[iDoc].m_fTFIDF = pDoc->m_fTFIDF;
				CopyExtDocinfo ( m_dDocs[iDoc], *pDoc, &pDocinfo, m_iStride );
				iDoc++;
				m_uLastDocID = pHit->m_uDocid;
			}
			iHit++;
		}

		// go on
		pHit++;
	}

	// reset current positions for hits chunk getter
	m_pMyDoc = m_dDocs;
	m_pMyHit = m_dMyHits;

	// save shortcuts
	m_pDoc = pDoc;
	m_pHit = pHit;

	assert ( iHit>=0 && iHit<MAX_HITS );
	m_dMyHits[iHit].m_uDocid = DOCID_MAX; // end marker
	m_uMatchedDocid = 0;

	return ReturnDocsChunk ( iDoc, "nway" );
}

template < class FSM >
const ExtHit_t * ExtNWay_c<FSM>::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	// if we already emitted hits for this matches block, do not do that again
	SphDocID_t uFirstMatch = pDocs->m_uDocid;
	if ( uFirstMatch==m_uHitsOverFor )
		return NULL;

	// shortcuts
	const ExtDoc_t * pMyDoc = m_pMyDoc;
	const ExtHit_t * pMyHit = m_pMyHit;

	if ( !pMyDoc )
		return NULL;
	assert ( pMyHit );

	// filter and copy hits from m_dMyHits
	int iHit = 0;
	while ( iHit<MAX_HITS-1 )
	{
		// pull next doc if needed
		if ( !m_uMatchedDocid )
		{
			do
			{
				while ( pMyDoc->m_uDocid < pDocs->m_uDocid ) pMyDoc++;
				if ( pMyDoc->m_uDocid==DOCID_MAX ) break;

				while ( pDocs->m_uDocid < pMyDoc->m_uDocid ) pDocs++;
				if ( pDocs->m_uDocid==DOCID_MAX ) break;
			} while ( pDocs->m_uDocid!=pMyDoc->m_uDocid );

			if ( pDocs->m_uDocid!=pMyDoc->m_uDocid )
			{
				assert ( pMyDoc->m_uDocid==DOCID_MAX || pDocs->m_uDocid==DOCID_MAX );
				break;
			}

			assert ( pDocs->m_uDocid==pMyDoc->m_uDocid );
			assert ( pDocs->m_uDocid!=0 );
			assert ( pDocs->m_uDocid!=DOCID_MAX );

			m_uMatchedDocid = pDocs->m_uDocid;
		}

		// skip until we have to
		while ( pMyHit->m_uDocid < m_uMatchedDocid ) pMyHit++;

		// copy while we can
		if ( pMyHit->m_uDocid!=DOCID_MAX )
		{
			assert ( pMyHit->m_uDocid==m_uMatchedDocid );
			assert ( m_uMatchedDocid!=0 && m_uMatchedDocid!=DOCID_MAX );

			while ( pMyHit->m_uDocid==m_uMatchedDocid && iHit<MAX_HITS-1 )
				m_dHits[iHit++] = *pMyHit++;
		}

		// handle different end conditions
		if ( pMyHit->m_uDocid!=m_uMatchedDocid && pMyHit->m_uDocid!=DOCID_MAX )
		{
			// it's simply next document in the line; switch to it
			m_uMatchedDocid = 0;
			pMyDoc++;

		} else if ( pMyHit->m_uDocid==DOCID_MAX && !m_pHit )
		{
			// it's the end
			break;

		} else if ( pMyHit->m_uDocid==DOCID_MAX && m_pHit && iHit<MAX_HITS-1 )
		{
			// the trickiest part; handle the end of my hitlist chunk
			// The doclist chunk was built from it; so it must be the end of doclist as well.
			// Covered by test 114.
			assert ( pMyDoc[1].m_uDocid==DOCID_MAX );

			// keep scanning and-node hits while there are hits for the last matched document
			assert ( m_uMatchedDocid==pMyDoc->m_uDocid );
			assert ( m_uMatchedDocid==m_uLastDocID );
			assert ( !m_pDoc || m_uMatchedDocid==m_pDoc->m_uDocid );
			m_pMyDoc = pMyDoc;
			if ( EmitTail(iHit) )
				m_uHitsOverFor = uFirstMatch;
			pMyDoc = m_pMyDoc;
		}
	}

	// save shortcuts
	m_pMyDoc = pMyDoc;
	m_pMyHit = pMyHit;

	assert ( iHit>=0 && iHit<MAX_HITS );
	m_dHits[iHit].m_uDocid = DOCID_MAX; // end marker
	return iHit ? m_dHits : NULL;
}

template < class FSM >
bool ExtNWay_c<FSM>::EmitTail ( int & iHit )
{
	const ExtHit_t * pHit = m_pHit;
	const ExtDoc_t * pMyDoc = m_pMyDoc;
	bool bTailFinished = false;

	while ( iHit<MAX_HITS-1 )
	{
		// and-node hits chunk end reached? get some more
		if ( pHit->m_uDocid==DOCID_MAX )
		{
			pHit = m_pHits = m_pNode->GetHitsChunk ( m_pDocs );
			if ( !pHit )
			{
				m_uMatchedDocid = 0;
				pMyDoc++;
				break;
			}
		}

		// stop and finish on the first new id
		if ( pHit->m_uDocid!=m_uMatchedDocid )
		{
			// reset hits getter; this docs chunk from above is finally over
			bTailFinished = true;
			m_uMatchedDocid = 0;
			pMyDoc++;
			break;
		}

		if ( FSM::HitFSM ( pHit, &m_dHits[iHit] ) )
			iHit++;
		pHit++;
	}
	// save shortcut
	m_pHit = pHit;
	m_pMyDoc = pMyDoc;
	return bTailFinished;
}

//////////////////////////////////////////////////////////////////////////

DWORD GetQposMask ( const CSphVector<ExtNode_i *> & dQwords )
{
	int iQposBase = dQwords[0]->m_iAtomPos;
	DWORD uQposMask = 0;
	for ( int i=1; i<dQwords.GetLength(); i++ )
	{
		int iQposDelta = dQwords[i]->m_iAtomPos - iQposBase;
		assert ( iQposDelta<(int)sizeof(uQposMask)*8 );
		uQposMask |= ( 1 << iQposDelta );
	}

	return uQposMask;
}


FSMphrase::FSMphrase ( const CSphVector<ExtNode_i *> & dQwords, const XQNode_t & , const ISphQwordSetup & tSetup )
	: m_dAtomPos ( dQwords.GetLength() )
	, m_uQposMask ( 0 )
{
	ARRAY_FOREACH ( i, dQwords )
		m_dAtomPos[i] = dQwords[i]->m_iAtomPos;

	assert ( ( m_dAtomPos.Last()-m_dAtomPos[0]+1 )>0 );
	m_dQposDelta.Resize ( m_dAtomPos.Last()-m_dAtomPos[0]+1 );
	ARRAY_FOREACH ( i, m_dQposDelta )
		m_dQposDelta[i] = -INT_MAX;
	for ( int i=1; i<(int)m_dAtomPos.GetLength(); i++ )
		m_dQposDelta [ dQwords[i-1]->m_iAtomPos - dQwords[0]->m_iAtomPos ] = dQwords[i]->m_iAtomPos - dQwords[i-1]->m_iAtomPos;

	if ( tSetup.m_bSetQposMask )
		m_uQposMask = GetQposMask ( dQwords );
}

inline bool FSMphrase::HitFSM ( const ExtHit_t * pHit, ExtHit_t * pTarget )
{
	DWORD uHitposWithField = HITMAN::GetPosWithField ( pHit->m_uHitpos );

	// adding start state for start hit
	if ( pHit->m_uQuerypos==m_dAtomPos[0] )
	{
		State_t & tState = m_dStates.Add();
		tState.m_iTagQword = 0;
		tState.m_uExpHitposWithField = uHitposWithField + m_dQposDelta[0];
	}

	// updating states
	for ( int i=m_dStates.GetLength()-1; i>=0; i-- )
	{
		if ( m_dStates[i].m_uExpHitposWithField<uHitposWithField )
		{
			m_dStates.RemoveFast(i); // failed to match
			continue;
		}

		// get next state
		if ( m_dStates[i].m_uExpHitposWithField==uHitposWithField && m_dAtomPos [ m_dStates[i].m_iTagQword+1 ]==pHit->m_uQuerypos )
		{
			m_dStates[i].m_iTagQword++; // check for next elm in query
			m_dStates[i].m_uExpHitposWithField = uHitposWithField + m_dQposDelta [ pHit->m_uQuerypos - m_dAtomPos[0] ];
		}

		// checking if state successfully matched
		if ( m_dStates[i].m_iTagQword==m_dAtomPos.GetLength()-1 )
		{
			DWORD uSpanlen = m_dAtomPos.Last() - m_dAtomPos[0];

			// emit directly into m_dHits, this is no need to disturb m_dMyHits here.
			pTarget->m_uDocid = pHit->m_uDocid;
			pTarget->m_uHitpos = uHitposWithField - uSpanlen;
			pTarget->m_uQuerypos = (WORD) m_dAtomPos[0];
			pTarget->m_uMatchlen = pTarget->m_uSpanlen = (WORD)( uSpanlen + 1 );
			pTarget->m_uWeight = m_dAtomPos.GetLength();
			pTarget->m_uQposMask = m_uQposMask;
			ResetFSM ();
			return true;
		}
	}

	return false;
}

//////////////////////////////////////////////////////////////////////////

FSMproximity::FSMproximity ( const CSphVector<ExtNode_i *> & dQwords, const XQNode_t & tNode, const ISphQwordSetup & tSetup )
	: m_iMaxDistance ( tNode.m_iOpArg )
	, m_uWordsExpected ( dQwords.GetLength() )
	, m_uExpPos ( 0 )
	, m_uQposMask ( 0 )
{
	assert ( m_iMaxDistance>0 );
	m_uMinQpos = dQwords[0]->m_iAtomPos;
	m_uQLen = dQwords.Last()->m_iAtomPos - m_uMinQpos;
	m_dProx.Resize ( m_uQLen+1 );
	m_dDeltas.Resize ( m_uQLen+1 );

	if ( tSetup.m_bSetQposMask )
		m_uQposMask = GetQposMask ( dQwords );
}

inline bool FSMproximity::HitFSM ( const ExtHit_t* pHit, ExtHit_t* pTarget )
{
	// walk through the hitlist and update context
	int iQindex = pHit->m_uQuerypos - m_uMinQpos;
	DWORD uHitposWithField = HITMAN::GetPosWithField ( pHit->m_uHitpos );

	// check if the word is new
	if ( m_dProx[iQindex]==UINT_MAX )
		m_uWords++;

	// update the context
	m_dProx[iQindex] = uHitposWithField;

	// check if the incoming hit is out of bounds, or affects min pos
	if ( uHitposWithField>=m_uExpPos // out of expected bounds
		|| iQindex==m_iMinQindex ) // or simply affects min pos
	{
		m_iMinQindex = iQindex;
		int iMinPos = uHitposWithField - m_uQLen - m_iMaxDistance;

		ARRAY_FOREACH ( i, m_dProx )
			if ( m_dProx[i]!=UINT_MAX )
			{
				if ( (int)m_dProx[i]<=iMinPos )
				{
					m_dProx[i] = UINT_MAX;
					m_uWords--;
					continue;
				}
				if ( m_dProx[i]<uHitposWithField )
				{
					m_iMinQindex = i;
					uHitposWithField = m_dProx[i];
				}
			}

		m_uExpPos = m_dProx[m_iMinQindex] + m_uQLen + m_iMaxDistance;
	}

	// all words were found within given distance?
	if ( m_uWords!=m_uWordsExpected )
		return false;

	// compute phrase weight
	//
	// FIXME! should also account for proximity factor, which is in 1 to maxdistance range:
	// m_iMaxDistance - ( pHit->m_uHitpos - m_dProx[m_iMinQindex] - m_uQLen )
	DWORD uMax = 0;
	ARRAY_FOREACH ( i, m_dProx )
		if ( m_dProx[i]!=UINT_MAX )
		{
			m_dDeltas[i] = m_dProx[i] - i;
			uMax = Max ( uMax, m_dProx[i] );
		} else
			m_dDeltas[i] = INT_MAX;

	m_dDeltas.Sort ();

	DWORD uCurWeight = 0;
	DWORD uWeight = 0;
	int iLast = -INT_MAX;
	ARRAY_FOREACH_COND ( i, m_dDeltas, m_dDeltas[i]!=INT_MAX )
	{
		if ( m_dDeltas[i]==iLast )
			uCurWeight++;
		else
		{
			uWeight += uCurWeight ? ( 1+uCurWeight ) : 0;
			uCurWeight = 0;
		}
		iLast = m_dDeltas[i];
	}

	uWeight += uCurWeight ? ( 1+uCurWeight ) : 0;
	if ( !uWeight )
		uWeight = 1;

	// emit hit
	pTarget->m_uDocid = pHit->m_uDocid;
	pTarget->m_uHitpos = Hitpos_t ( m_dProx[m_iMinQindex] ); // !COMMIT strictly speaking this is creation from LCS not value
	pTarget->m_uQuerypos = (WORD) m_uMinQpos;
	pTarget->m_uSpanlen = pTarget->m_uMatchlen = (WORD)( uMax-m_dProx[m_iMinQindex]+1 );
	pTarget->m_uWeight = uWeight;
	pTarget->m_uQposMask = m_uQposMask;

	// remove current min, and force recompue
	m_dProx[m_iMinQindex] = UINT_MAX;
	m_iMinQindex = -1;
	m_uWords--;
	m_uExpPos = 0;
	return true;
}

//////////////////////////////////////////////////////////////////////////

FSMmultinear::FSMmultinear ( const CSphVector<ExtNode_i *> & dNodes, const XQNode_t & tNode, const ISphQwordSetup & tSetup )
	: m_iNear ( tNode.m_iOpArg )
	, m_uWordsExpected ( dNodes.GetLength() )
	, m_uFirstQpos ( 65535 )
	, m_bQposMask ( tSetup.m_bSetQposMask )
{
	if ( m_uWordsExpected==2 )
		m_bTwofer = true;
	else
	{
		m_dNpos.Reserve ( m_uWordsExpected );
		m_dRing.Resize ( m_uWordsExpected );
		m_bTwofer = false;
	}
	assert ( m_iNear>0 );
}

inline bool FSMmultinear::HitFSM ( const ExtHit_t* pHit, ExtHit_t* pTarget )
{
	// walk through the hitlist and update context
	DWORD uHitposWithField = HITMAN::GetPosWithField ( pHit->m_uHitpos );
	WORD uNpos = pHit->m_uNodepos;
	WORD uQpos = pHit->m_uQuerypos;

	// skip dupe hit (may be emitted by OR node, for example)
	if ( m_uLastP==uHitposWithField )
	{
		// lets choose leftmost (in query) from all dupes. 'a NEAR/2 a' case
		if ( m_bTwofer && uNpos<m_uFirstNpos )
		{
			m_uFirstQpos = uQpos;
			m_uFirstNpos = uNpos;
			return false;
		} else if ( !m_bTwofer && uNpos<m_dRing [ RingTail() ].m_uNodepos ) // 'a NEAR/2 a NEAR/2 a' case
		{
			WORD * p = const_cast<WORD *>( m_dNpos.BinarySearch ( uNpos ) );
			if ( !p )
			{
				p = const_cast<WORD *>( m_dNpos.BinarySearch ( m_dRing [ RingTail() ].m_uNodepos ) );
				*p = uNpos;
				m_dNpos.Sort();
				m_dRing [ RingTail() ].m_uNodepos = uNpos;
				m_dRing [ RingTail() ].m_uQuerypos = uQpos;
			}
			return false;
		} else if ( m_uPrelastP && m_uLastML < pHit->m_uMatchlen ) // check if the hit is subset of another one
		{
			// roll back pre-last to check agains this new hit.
			m_uLastML = m_uPrelastML;
			m_uLastSL = m_uPrelastSL;
			m_uFirstHit = m_uLastP = m_uPrelastP;
			m_uWeight = m_uWeight - m_uLastW + m_uPrelastW;
		} else
			return false;
	}

	// probably new chain
	if ( m_uLastP==0 || ( m_uLastP + m_uLastML + m_iNear )<=uHitposWithField )
	{
		m_uFirstHit = m_uLastP = uHitposWithField;
		m_uLastML = pHit->m_uMatchlen;
		m_uLastSL = pHit->m_uSpanlen;
		m_uWeight = m_uLastW = pHit->m_uWeight;
		if ( m_bTwofer )
		{
			m_uFirstQpos = uQpos;
			m_uFirstNpos = uNpos;
		} else
		{
			m_dNpos.Resize(1);
			m_dNpos[0] = uNpos;
			Add2Ring ( pHit );
		}
		return false;
	}

	// this hit (with such querypos) already was there. Skip the hit.
	if ( m_bTwofer )
	{
		// special case for twofer: hold the overlapping
		if ( ( m_uFirstHit + m_uLastML )>uHitposWithField
			&& ( m_uFirstHit + m_uLastML )<( uHitposWithField + pHit->m_uMatchlen )
			&& m_uLastML!=pHit->m_uMatchlen )
		{
			m_uFirstHit = m_uLastP = uHitposWithField;
			m_uLastML = pHit->m_uMatchlen;
			m_uLastSL = pHit->m_uSpanlen;
			m_uWeight = m_uLastW = pHit->m_uWeight;
			m_uFirstQpos = uQpos;
			m_uFirstNpos = uNpos;
			return false;
		}
		if ( uNpos==m_uFirstNpos )
		{
			if ( m_uLastP < uHitposWithField )
			{
				m_uPrelastML = m_uLastML;
				m_uPrelastSL = m_uLastSL;
				m_uPrelastP = m_uLastP;
				m_uPrelastW = pHit->m_uWeight;

				m_uFirstHit = m_uLastP = uHitposWithField;
				m_uLastML = pHit->m_uMatchlen;
				m_uLastSL = pHit->m_uSpanlen;
				m_uWeight = m_uLastW = m_uPrelastW;
				m_uFirstQpos = uQpos;
				m_uFirstNpos = uNpos;
			}
			return false;
		}
	} else
	{
		if ( uNpos < m_dNpos[0] )
		{
			m_uFirstQpos = Min ( m_uFirstQpos, uQpos );
			m_dNpos.Insert ( 0, uNpos );
		} else if ( uNpos > m_dNpos.Last() )
		{
			m_uFirstQpos = Min ( m_uFirstQpos, uQpos );
			m_dNpos.Add ( uNpos );
		} else if ( uNpos!=m_dNpos[0] && uNpos!=m_dNpos.Last() )
		{
			int iEnd = m_dNpos.GetLength();
			int iStart = 0;
			int iMid = -1;
			while ( iEnd-iStart>1 )
			{
				iMid = ( iStart + iEnd ) / 2;
				if ( uNpos==m_dNpos[iMid] )
				{
					const ExtHit_t& dHit = m_dRing[m_iRing];
					// last addition same as the first. So, we can shift
					if ( uNpos==dHit.m_uNodepos )
					{
						m_uWeight -= dHit.m_uWeight;
						m_uFirstHit = HITMAN::GetPosWithField ( dHit.m_uHitpos );
						ShiftRing();
					// last addition same as the first. So, we can shift
					} else if ( uNpos==m_dRing [ RingTail() ].m_uNodepos )
						m_uWeight -= m_dRing [ RingTail() ].m_uWeight;
					else
						return false;
				}

				if ( uNpos<m_dNpos[iMid] )
					iEnd = iMid;
				else
					iStart = iMid;
			}
			m_dNpos.Insert ( iEnd, uNpos );
			m_uFirstQpos = Min ( m_uFirstQpos, uQpos );
		// last addition same as the first. So, we can shift
		} else if ( uNpos==m_dRing[m_iRing].m_uNodepos )
		{
			m_uWeight -= m_dRing[m_iRing].m_uWeight;
			m_uFirstHit = HITMAN::GetPosWithField ( m_dRing[m_iRing].m_uHitpos );
			ShiftRing();
		// last addition same as the tail. So, we can move the tail onto it.
		} else if ( uNpos==m_dRing [ RingTail() ].m_uNodepos )
			m_uWeight -= m_dRing [ RingTail() ].m_uWeight;
		else
			return false;
	}

	m_uWeight += pHit->m_uWeight;
	m_uLastML = pHit->m_uMatchlen;
	m_uLastSL = pHit->m_uSpanlen;
	Add2Ring ( pHit );

	// finally got the whole chain - emit it!
	// warning: we don't support overlapping in generic chains.
	if ( m_bTwofer || (int)m_uWordsExpected==m_dNpos.GetLength() )
	{
		pTarget->m_uDocid = pHit->m_uDocid;
		pTarget->m_uHitpos = Hitpos_t ( m_uFirstHit ); // !COMMIT strictly speaking this is creation from LCS not value
		pTarget->m_uMatchlen = (WORD)( uHitposWithField - m_uFirstHit + m_uLastML );
		pTarget->m_uWeight = m_uWeight;
		m_uPrelastP = 0;

		if ( m_bTwofer ) // for exactly 2 words allow overlapping - so, just shift the chain, not reset it
		{
			pTarget->m_uQuerypos = Min ( m_uFirstQpos, pHit->m_uQuerypos );
			pTarget->m_uSpanlen = 2;
			pTarget->m_uQposMask = ( 1 << ( Max ( m_uFirstQpos, pHit->m_uQuerypos ) - pTarget->m_uQuerypos ) );
			m_uFirstHit = m_uLastP = uHitposWithField;
			m_uWeight = pHit->m_uWeight;
			m_uFirstQpos = pHit->m_uQuerypos;
		} else
		{
			pTarget->m_uQuerypos = Min ( m_uFirstQpos, pHit->m_uQuerypos );
			pTarget->m_uSpanlen = (WORD) m_dNpos.GetLength();
			pTarget->m_uQposMask = 0;
			m_uLastP = 0;
			if ( m_bQposMask && pTarget->m_uSpanlen>1 )
			{
				ARRAY_FOREACH ( i, m_dNpos )
				{
					int iQposDelta = m_dNpos[i] - pTarget->m_uQuerypos;
					assert ( iQposDelta<(int)sizeof(pTarget->m_uQposMask)*8 );
					pTarget->m_uQposMask |= ( 1 << iQposDelta );
				}
			}
		}
		return true;
	}

	m_uLastP = uHitposWithField;
	return false;
}

//////////////////////////////////////////////////////////////////////////

ExtQuorum_c::ExtQuorum_c ( CSphVector<ExtNode_i*> & dQwords, const XQNode_t & tNode, const ISphQwordSetup & tSetup )
{
	assert ( tNode.GetOp()==SPH_QUERY_QUORUM );
	assert ( dQwords.GetLength()<MAX_HITS );

	m_iThresh = GetThreshold ( tNode, dQwords.GetLength() );
	m_iThresh = Max ( m_iThresh, 1 );
	m_iMyHitCount = 0;
	m_iMyLast = 0;
	m_bHasDupes = false;
	memset ( m_dQuorumHits, 0, sizeof(m_dQuorumHits) );

	assert ( dQwords.GetLength()>1 ); // use TERM instead
	assert ( dQwords.GetLength()<=256 ); // internal masks are upto 256 bits
	assert ( m_iThresh>=1 ); // 1 is also OK; it's a bit different from just OR
	assert ( m_iThresh<dQwords.GetLength() ); // use AND instead

	if ( dQwords.GetLength()>0 )
	{
		m_iAtomPos = dQwords[0]->m_iAtomPos;

		// compute duplicate keywords mask (aka dupe mask)
		// FIXME! will fail with wordforms and stuff; sorry, no wordforms vs expand vs quorum support for now!
		CSphFixedVector<QuorumDupeNodeHash_t> dHashes ( dQwords.GetLength() );
		ARRAY_FOREACH ( i, dQwords )
		{
			dHashes[i].m_uWordID = dQwords[i]->GetWordID();
			dHashes[i].m_iIndex = i;
		}
		sphSort ( dHashes.Begin(), dHashes.GetLength() );

		QuorumDupeNodeHash_t tParent = *dHashes.Begin();
		m_dInitialChildren.Add().m_pTerm = dQwords[tParent.m_iIndex];
		m_dInitialChildren.Last().m_iCount = 1;
		tParent.m_iIndex = 0;

		for ( int i=1; i<dHashes.GetLength(); i++ )
		{
			QuorumDupeNodeHash_t & tElem = dHashes[i];
			if ( tParent.m_uWordID!=tElem.m_uWordID )
			{
				tParent = tElem;
				tParent.m_iIndex = m_dInitialChildren.GetLength();
				m_dInitialChildren.Add().m_pTerm = dQwords [ tElem.m_iIndex ];
				m_dInitialChildren.Last().m_iCount = 1;
			} else
			{
				m_dInitialChildren[tParent.m_iIndex].m_iCount++;
				SafeDelete ( dQwords[tElem.m_iIndex] );
				m_bHasDupes = true;
			}
		}

		// sort back to qpos order
		m_dInitialChildren.Sort ( QuorumNodeAtomPos_fn() );
	}

	ARRAY_FOREACH ( i, m_dInitialChildren )
	{
		m_dInitialChildren[i].m_pCurDoc = NULL;
		m_dInitialChildren[i].m_pCurHit = NULL;
	}
	m_dChildren = m_dInitialChildren;
	m_uMatchedDocid = 0;

	AllocDocinfo ( tSetup );
}

ExtQuorum_c::~ExtQuorum_c ()
{
	ARRAY_FOREACH ( i, m_dInitialChildren )
		SafeDelete ( m_dInitialChildren[i].m_pTerm );
}

void ExtQuorum_c::Reset ( const ISphQwordSetup & tSetup )
{
	m_dChildren = m_dInitialChildren;
	m_uMatchedDocid = 0;
	m_iMyHitCount = 0;
	m_iMyLast = 0;

	ARRAY_FOREACH ( i, m_dChildren )
		m_dChildren[i].m_pTerm->Reset ( tSetup );
}

int ExtQuorum_c::GetQwords ( ExtQwordsHash_t & hQwords )
{
	int iMax = -1;
	ARRAY_FOREACH ( i, m_dChildren )
	{
		int iKidMax = m_dChildren[i].m_pTerm->GetQwords ( hQwords );
		iMax = Max ( iMax, iKidMax );
	}
	return iMax;
}

void ExtQuorum_c::SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
{
	ARRAY_FOREACH ( i, m_dChildren )
		m_dChildren[i].m_pTerm->SetQwordsIDF ( hQwords );
}

void ExtQuorum_c::GetTerms ( const ExtQwordsHash_t & hQwords, CSphVector<TermPos_t> & dTermDupes ) const
{
	ARRAY_FOREACH ( i, m_dChildren )
		m_dChildren[i].m_pTerm->GetTerms ( hQwords, dTermDupes );
}

uint64_t ExtQuorum_c::GetWordID() const
{
	uint64_t uHash = SPH_FNV64_SEED;
	ARRAY_FOREACH ( i, m_dChildren )
	{
		uint64_t uCur = m_dChildren[i].m_pTerm->GetWordID();
		uHash = sphFNV64 ( &uCur, sizeof(uCur), uHash );
	}

	return uHash;
}

const ExtDoc_t * ExtQuorum_c::GetDocsChunk()
{
	// warmup
	ARRAY_FOREACH ( i, m_dChildren )
	{
		TermTuple_t & tElem = m_dChildren[i];
		tElem.m_bStandStill = false; // clear this-round-match flag
		if ( tElem.m_pCurDoc && tElem.m_pCurDoc->m_uDocid!=DOCID_MAX )
			continue;

		tElem.m_pCurDoc = tElem.m_pTerm->GetDocsChunk();
		if ( tElem.m_pCurDoc )
			continue;

		m_dChildren.RemoveFast ( i );
		i--;
	}

	// main loop
	int iDoc = 0;
	CSphRowitem * pDocinfo = m_pDocinfo;
	bool bProceed2HitChunk = false;
	m_iMyHitCount = 0;
	m_iMyLast = 0;
	int iQuorumLeft = CountQuorum ( true );
	while ( iDoc<MAX_DOCS-1 && ( !m_bHasDupes || m_iMyHitCount+iQuorumLeft<MAX_HITS ) && iQuorumLeft>=m_iThresh && !bProceed2HitChunk )
	{
		// find min document ID, count occurrences
		ExtDoc_t tCand;

		tCand.m_uDocid = DOCID_MAX; // current candidate id
		tCand.m_uHitlistOffset = 0; // suppress gcc warnings
		tCand.m_pDocinfo = NULL;
		tCand.m_uDocFields = 0; // non necessary
		tCand.m_fTFIDF = 0.0f;

		int iQuorum = 0;
		ARRAY_FOREACH ( i, m_dChildren )
		{
			TermTuple_t & tElem = m_dChildren[i];
			assert ( tElem.m_pCurDoc->m_uDocid && tElem.m_pCurDoc->m_uDocid!=DOCID_MAX );
			if ( tElem.m_pCurDoc->m_uDocid < tCand.m_uDocid )
			{
				tCand = *tElem.m_pCurDoc;
				iQuorum = tElem.m_iCount;
			} else if ( tElem.m_pCurDoc->m_uDocid==tCand.m_uDocid )
			{
				tCand.m_uDocFields |= tElem.m_pCurDoc->m_uDocFields; // FIXME!!! check hits in case of dupes or field constrain
				tCand.m_fTFIDF += tElem.m_pCurDoc->m_fTFIDF;
				iQuorum += tElem.m_iCount;
			}
		}

		// FIXME!!! check that tail hits should be processed right after CollectMatchingHits
		if ( iQuorum>=m_iThresh && ( !m_bHasDupes || CollectMatchingHits ( tCand.m_uDocid, m_iThresh ) ) )
		{
			CopyExtDoc ( m_dDocs[iDoc++], tCand, &pDocinfo, m_iStride );

			// FIXME!!! move to children advancing
			ARRAY_FOREACH ( i, m_dChildren )
			{
				if ( m_dChildren[i].m_pCurDoc->m_uDocid==tCand.m_uDocid )
					m_dChildren[i].m_bStandStill = true;
			}
		}

		// advance children
		int iWasChildren = m_dChildren.GetLength();
		ARRAY_FOREACH ( i, m_dChildren )
		{
			TermTuple_t & tElem = m_dChildren[i];
			if ( tElem.m_pCurDoc->m_uDocid!=tCand.m_uDocid )
				continue;

			tElem.m_pCurDoc++;
			if ( tElem.m_pCurDoc->m_uDocid!=DOCID_MAX )
				continue;

			// should grab hits first
			if ( tElem.m_bStandStill )
			{
				bProceed2HitChunk = true;
				continue; // still should fast forward rest of children to pass current doc-id
			}

			tElem.m_pCurDoc = tElem.m_pTerm->GetDocsChunk();
			if ( tElem.m_pCurDoc )
				continue;

			m_dChildren.RemoveFast ( i );
			i--;
		}

		if ( iWasChildren!=m_dChildren.GetLength() )
			iQuorumLeft = CountQuorum ( false );
	}

	return ReturnDocsChunk ( iDoc, "quorum" );
}

const ExtHit_t * ExtQuorum_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	// dupe tail hits
	if ( m_bHasDupes && m_uMatchedDocid )
		return GetHitsChunkDupesTail();

	// quorum-buffer path
	if ( m_bHasDupes )
		return GetHitsChunkDupes ( pDocs );

	return GetHitsChunkSimple ( pDocs );
}

const ExtHit_t * ExtQuorum_c::GetHitsChunkDupesTail ()
{
	ExtDoc_t dTailDocs[2];
	dTailDocs[0].m_uDocid = m_uMatchedDocid;
	dTailDocs[1].m_uDocid = DOCID_MAX;
	int iHit = 0;
	while ( iHit<MAX_HITS-1 )
	{
		int iMinChild = -1;
		DWORD uMinPosWithField = UINT_MAX;
		ARRAY_FOREACH ( i, m_dChildren )
		{
			const ExtHit_t * pCurHit = m_dChildren[i].m_pCurHit;
			if ( pCurHit && pCurHit->m_uDocid==m_uMatchedDocid && HITMAN::GetPosWithField ( pCurHit->m_uHitpos ) < uMinPosWithField )
			{
				uMinPosWithField = HITMAN::GetPosWithField ( pCurHit->m_uHitpos );
				iMinChild = i;
			}
		}

		// no hits found at children
		if ( iMinChild<0 )
		{
			m_uMatchedDocid = 0;
			break;
		}

		m_dHits[iHit++] = *m_dChildren[iMinChild].m_pCurHit;
		m_dChildren[iMinChild].m_pCurHit++;
		if ( m_dChildren[iMinChild].m_pCurHit->m_uDocid==DOCID_MAX )
			m_dChildren[iMinChild].m_pCurHit = m_dChildren[iMinChild].m_pTerm->GetHitsChunk ( dTailDocs );
	}

	return ReturnHitsChunk ( iHit, "quorum-dupes-tail", false );
}

struct QuorumCmpHitPos_fn
{
	inline bool IsLess ( const ExtHit_t & a, const ExtHit_t & b ) const
	{
		return HITMAN::GetPosWithField ( a.m_uHitpos )<HITMAN::GetPosWithField ( b.m_uHitpos );
	}
};

const ExtHit_t * ExtQuorum_c::GetHitsChunkDupes ( const ExtDoc_t * pDocs )
{
	// quorum-buffer path
	int iHit = 0;
	while ( m_iMyLast<m_iMyHitCount && iHit<MAX_HITS-1 && pDocs->m_uDocid!=DOCID_MAX )
	{
		SphDocID_t uDocid = pDocs->m_uDocid;
		while ( m_dQuorumHits[m_iMyLast].m_uDocid<uDocid && m_iMyLast<m_iMyHitCount )
			m_iMyLast++;

		// find hits for current doc
		int iLen = 0;
		while ( m_dQuorumHits[m_iMyLast+iLen].m_uDocid==uDocid && m_iMyLast+iLen<m_iMyHitCount )
			iLen++;

		// proceed next document in case no hits found
		if ( !iLen )
		{
			pDocs++;
			continue;
		}

		// order hits by hit-position for current doc
		sphSort ( m_dQuorumHits+m_iMyLast, iLen, QuorumCmpHitPos_fn() );

		int iMyEnd = m_iMyLast + iLen;
		bool bCheckChildren = ( iMyEnd==MAX_HITS ); // should check children too in case quorum-buffer full
		while ( iHit<MAX_HITS-1 )
		{
			int iMinChild = -1;
			DWORD uMinPosWithField = ( m_iMyLast<iMyEnd ? HITMAN::GetPosWithField ( m_dQuorumHits[m_iMyLast].m_uHitpos ) : UINT_MAX );
			if ( bCheckChildren )
			{
				ARRAY_FOREACH ( i, m_dChildren )
				{
					const ExtHit_t * pCurHit = m_dChildren[i].m_pCurHit;
					if ( pCurHit && pCurHit->m_uDocid==uDocid && HITMAN::GetPosWithField ( pCurHit->m_uHitpos ) < uMinPosWithField )
					{
						uMinPosWithField = HITMAN::GetPosWithField ( pCurHit->m_uHitpos );
						iMinChild = i;
					}
				}
			}

			// no hits found at children and quorum-buffer over
			if ( iMinChild<0 && m_iMyLast==iMyEnd )
			{
				pDocs++;
				bCheckChildren = false;
				break;
			}

			if ( iMinChild<0 )
			{
				m_dHits[iHit++] = m_dQuorumHits[m_iMyLast++];
			} else
			{
				m_dHits[iHit++] = *m_dChildren[iMinChild].m_pCurHit;
				m_dChildren[iMinChild].m_pCurHit++;
				if ( m_dChildren[iMinChild].m_pCurHit->m_uDocid==DOCID_MAX )
					m_dChildren[iMinChild].m_pCurHit = m_dChildren[iMinChild].m_pTerm->GetHitsChunk ( pDocs );
			}
		}

		if ( m_iMyLast==iMyEnd && bCheckChildren ) // quorum-buffer over but children still have hits
			m_uMatchedDocid = uDocid;
	}

	return ReturnHitsChunk ( iHit, "quorum-dupes", false );
}

const ExtHit_t * ExtQuorum_c::GetHitsChunkSimple ( const ExtDoc_t * pDocs )
{
	// warmup
	ARRAY_FOREACH ( i, m_dChildren )
	{
		TermTuple_t & tElem = m_dChildren[i];
		if ( !tElem.m_pCurHit || tElem.m_pCurHit->m_uDocid==DOCID_MAX )
			tElem.m_pCurHit = tElem.m_pTerm->GetHitsChunk ( pDocs );
	}

	// main loop
	int iHit = 0;
	while ( iHit<MAX_HITS-1 )
	{
		int iMinChild = -1;
		DWORD uMinPosWithField = UINT_MAX;

		if ( m_uMatchedDocid )
		{
			// emit that id while possible
			// OPTIMIZE: full linear scan for min pos and emission, eww
			ARRAY_FOREACH ( i, m_dChildren )
			{
				const ExtHit_t * pCurHit = m_dChildren[i].m_pCurHit;
				if ( pCurHit && pCurHit->m_uDocid==m_uMatchedDocid && HITMAN::GetPosWithField ( pCurHit->m_uHitpos ) < uMinPosWithField )
				{
					uMinPosWithField = HITMAN::GetPosWithField ( pCurHit->m_uHitpos ); // !COMMIT bench/fix, is LCS right here?
					iMinChild = i;
				}
			}

			if ( iMinChild<0 )
			{
				SphDocID_t uLastDoc = m_uMatchedDocid;
				m_uMatchedDocid = 0;
				while ( pDocs->m_uDocid!=DOCID_MAX && pDocs->m_uDocid<=uLastDoc )
					pDocs++;
			}
		}

		// get min common incoming docs and hits doc-id
		if ( !m_uMatchedDocid )
		{
			bool bDocMatched = false;
			while ( !bDocMatched && pDocs->m_uDocid!=DOCID_MAX )
			{
				iMinChild = -1;
				uMinPosWithField = UINT_MAX;
				ARRAY_FOREACH ( i, m_dChildren )
				{
					TermTuple_t & tElem = m_dChildren[i];
					if ( !tElem.m_pCurHit )
						continue;

					// fast forward hits
					while ( tElem.m_pCurHit->m_uDocid<pDocs->m_uDocid && tElem.m_pCurHit->m_uDocid!=DOCID_MAX )
						tElem.m_pCurHit++;

					if ( tElem.m_pCurHit->m_uDocid==pDocs->m_uDocid )
					{
						bDocMatched = true;
						if ( HITMAN::GetPosWithField ( tElem.m_pCurHit->m_uHitpos ) < uMinPosWithField )
						{
							uMinPosWithField = HITMAN::GetPosWithField ( tElem.m_pCurHit->m_uHitpos );
							iMinChild = i;
						}
					}

					// rescan current child
					if ( tElem.m_pCurHit->m_uDocid==DOCID_MAX )
					{
						tElem.m_pCurHit = tElem.m_pTerm->GetHitsChunk ( pDocs );
						i -= ( tElem.m_pCurHit ? 1 : 0 );
					}
				}

				if ( !bDocMatched )
					pDocs++;
			}

			assert ( !bDocMatched || pDocs->m_uDocid!=DOCID_MAX );
			if ( bDocMatched )
				m_uMatchedDocid = pDocs->m_uDocid;
			else
				break;
		}

		assert ( iMinChild>=0 );
		m_dHits[iHit++] = *m_dChildren[iMinChild].m_pCurHit;
		m_dChildren[iMinChild].m_pCurHit++;
		if ( m_dChildren[iMinChild].m_pCurHit->m_uDocid==DOCID_MAX )
			m_dChildren[iMinChild].m_pCurHit = m_dChildren[iMinChild].m_pTerm->GetHitsChunk ( pDocs );
	}

	return ReturnHitsChunk ( iHit, "quorum-simple", false );
}

int ExtQuorum_c::GetThreshold ( const XQNode_t & tNode, int iQwords )
{
	return ( tNode.m_bPercentOp ? (int)floor ( 1.0f / 100.0f * tNode.m_iOpArg * iQwords + 0.5f ) : tNode.m_iOpArg );
}

bool ExtQuorum_c::CollectMatchingHits ( SphDocID_t uDocid, int iThreshold )
{
	assert ( m_dQuorumHits+m_iMyHitCount+CountQuorum ( false )<m_dQuorumHits+MAX_HITS ); // is there a space for all quorum hits?

	ExtHit_t * pHitBuf = m_dQuorumHits + m_iMyHitCount;
	int iQuorum = 0;
	int iTermHits = 0;
	ARRAY_FOREACH ( i, m_dChildren )
	{
		TermTuple_t & tElem = m_dChildren[i];

		// getting more hits
		if ( !tElem.m_pCurHit || tElem.m_pCurHit->m_uDocid==DOCID_MAX )
			tElem.m_pCurHit = tElem.m_pTerm->GetHitsChunk ( tElem.m_pCurDoc );

		// that hit stream over for now
		if ( !tElem.m_pCurHit || tElem.m_pCurHit->m_uDocid==DOCID_MAX )
		{
			iTermHits = 0;
			continue;
		}

		while ( tElem.m_pCurHit->m_uDocid!=DOCID_MAX && tElem.m_pCurHit->m_uDocid<uDocid )
			tElem.m_pCurHit++;

		// collect matched hits but only up to quorum.count per-term
		while ( tElem.m_pCurHit->m_uDocid==uDocid && iTermHits<tElem.m_iCount )
		{
			*pHitBuf++ = *tElem.m_pCurHit++;
			iQuorum++;
			iTermHits++;
		}

		// got quorum - no need to check further
		if ( iQuorum>=iThreshold )
			break;

		// there might be tail hits - rescan current child
		if ( tElem.m_pCurHit->m_uDocid==DOCID_MAX )
		{
			i--;
		} else
		{
			iTermHits = 0;
		}
	}

	// discard collected hits is case of no quorum matched
	if ( iQuorum<iThreshold )
		return false;

	// collect all hits to move docs/hits further
	ARRAY_FOREACH ( i, m_dChildren )
	{
		TermTuple_t & tElem = m_dChildren[i];

		// getting more hits
		if ( !tElem.m_pCurHit || tElem.m_pCurHit->m_uDocid==DOCID_MAX )
			tElem.m_pCurHit = tElem.m_pTerm->GetHitsChunk ( tElem.m_pCurDoc );

		// hit stream over for current term
		if ( !tElem.m_pCurHit || tElem.m_pCurHit->m_uDocid==DOCID_MAX )
			continue;

		// collect all hits
		while ( tElem.m_pCurHit->m_uDocid==uDocid && pHitBuf<m_dQuorumHits+MAX_HITS )
			*pHitBuf++ = *tElem.m_pCurHit++;

		// no more space left at quorum-buffer
		if ( pHitBuf>=m_dQuorumHits+MAX_HITS )
			break;

		// there might be tail hits - rescan current child
		if ( tElem.m_pCurHit->m_uDocid==DOCID_MAX )
			i--;
	}

	m_iMyHitCount = pHitBuf - m_dQuorumHits;
	return true;
}

//////////////////////////////////////////////////////////////////////////

ExtOrder_c::ExtOrder_c ( const CSphVector<ExtNode_i *> & dChildren, const ISphQwordSetup & tSetup )
	: m_dChildren ( dChildren )
	, m_bDone ( false )
	, m_uHitsOverFor ( 0 )
{
	int iChildren = dChildren.GetLength();
	assert ( iChildren>=2 );

	m_pDocs.Resize ( iChildren );
	m_pHits.Resize ( iChildren );
	m_pDocsChunk.Resize ( iChildren );
	m_dMyHits[0].m_uDocid = DOCID_MAX;

	if ( dChildren.GetLength()>0 )
		m_iAtomPos = dChildren[0]->m_iAtomPos;

	ARRAY_FOREACH ( i, dChildren )
	{
		assert ( m_dChildren[i] );
		m_pDocs[i] = NULL;
		m_pHits[i] = NULL;
	}

	AllocDocinfo ( tSetup );
}

void ExtOrder_c::Reset ( const ISphQwordSetup & tSetup )
{
	m_bDone = false;
	m_uHitsOverFor = 0;
	m_dMyHits[0].m_uDocid = DOCID_MAX;

	ARRAY_FOREACH ( i, m_dChildren )
	{
		assert ( m_dChildren[i] );
		m_dChildren[i]->Reset ( tSetup );
		m_pDocs[i] = NULL;
		m_pHits[i] = NULL;
	}
}

ExtOrder_c::~ExtOrder_c ()
{
	ARRAY_FOREACH ( i, m_dChildren )
		SafeDelete ( m_dChildren[i] );
}


int ExtOrder_c::GetChildIdWithNextHit ( SphDocID_t uDocid )
{
	// OPTIMIZE! implement PQ instead of full-scan
	DWORD uMinPosWithField = UINT_MAX;
	int iChild = -1;
	ARRAY_FOREACH ( i, m_dChildren )
	{
		// is this child over?
		if ( !m_pHits[i] )
			continue;

		// skip until proper hit
		while ( m_pHits[i]->m_uDocid!=DOCID_MAX && m_pHits[i]->m_uDocid<uDocid )
			m_pHits[i]++;

		// hit-chunk over? request next one, and rescan
		if ( m_pHits[i]->m_uDocid==DOCID_MAX )
		{
			// hits and docs over here
			if ( !m_pDocsChunk[i] )
				return -1;

			m_pHits[i] = m_dChildren[i]->GetHitsChunk ( m_pDocsChunk[i] );
			i--;
			continue;
		}

		// is this our man at all?
		if ( m_pHits[i]->m_uDocid==uDocid )
		{
			// is he the best we can get?
			if ( HITMAN::GetPosWithField ( m_pHits[i]->m_uHitpos ) < uMinPosWithField )
			{
				uMinPosWithField = HITMAN::GetPosWithField ( m_pHits[i]->m_uHitpos );
				iChild = i;
			}
		}
	}
	return iChild;
}


int ExtOrder_c::GetMatchingHits ( SphDocID_t uDocid, ExtHit_t * pHitbuf, int iLimit )
{
	// my trackers
	CSphVector<ExtHit_t> dAccLongest;
	CSphVector<ExtHit_t> dAccRecent;
	int iPosLongest = 0; // needed to handle cases such as "a b c" << a
	int iPosRecent = 0;
	int iField = -1;

	dAccLongest.Reserve ( m_dChildren.GetLength() );
	dAccRecent.Reserve ( m_dChildren.GetLength() );

	// while there's enough space in the buffer
	int iMyHit = 0;
	while ( iMyHit+m_dChildren.GetLength()<iLimit )
	{
		// get next hit (in hitpos ascending order)
		int iChild = GetChildIdWithNextHit ( uDocid );
		if ( iChild<0 )
			break; // OPTIMIZE? no trailing hits on this route

		const ExtHit_t * pHit = m_pHits[iChild];
		assert ( pHit->m_uDocid==uDocid );

		// most recent subseq must never be longer
		assert ( dAccRecent.GetLength()<=dAccLongest.GetLength() );

		// handle that hit!
		int iHitField = HITMAN::GetField ( pHit->m_uHitpos );
		int iHitPos = HITMAN::GetPos ( pHit->m_uHitpos );

		if ( iHitField!=iField )
		{
			// new field; reset both trackers
			dAccLongest.Resize ( 0 );
			dAccRecent.Resize ( 0 );

			// initial seeding, if needed
			if ( iChild==0 )
			{
				dAccLongest.Add ( *pHit );
				iPosLongest = iHitPos + pHit->m_uSpanlen;
				iField = iHitField;
			}

		} else if ( iChild==dAccLongest.GetLength() && iHitPos>=iPosLongest )
		{
			// it fits longest tracker
			dAccLongest.Add ( *pHit );
			iPosLongest = iHitPos + pHit->m_uSpanlen;

			// fully matched subsequence
			if ( dAccLongest.GetLength()==m_dChildren.GetLength() )
			{
				// flush longest tracker into buffer, and keep it terminated
				ARRAY_FOREACH ( i, dAccLongest )
					pHitbuf[iMyHit++] = dAccLongest[i];

				// reset both trackers
				dAccLongest.Resize ( 0 );
				dAccRecent.Resize ( 0 );
				iPosRecent = iPosLongest;
			}

		} else if ( iChild==0 )
		{
			// it restarts most-recent tracker
			dAccRecent.Resize ( 0 );
			dAccRecent.Add ( *pHit );
			iPosRecent = iHitPos + pHit->m_uSpanlen;
			if ( !dAccLongest.GetLength() )
			{
				dAccLongest.Add	( *pHit );
				iPosLongest = iHitPos + pHit->m_uSpanlen;
			}
		} else if ( iChild==dAccRecent.GetLength() && iHitPos>=iPosRecent )
		{
			// it fits most-recent tracker
			dAccRecent.Add ( *pHit );
			iPosRecent = iHitPos + pHit->m_uSpanlen;

			// maybe most-recent just became longest too?
			if ( dAccRecent.GetLength()==dAccLongest.GetLength() )
			{
				dAccLongest.SwapData ( dAccRecent );
				dAccRecent.Resize ( 0 );
				iPosLongest = iPosRecent;
			}
		}

		// advance hit stream
		m_pHits[iChild]++;
	}

	assert ( iMyHit>=0 && iMyHit<iLimit );
	pHitbuf[iMyHit].m_uDocid = DOCID_MAX;
	return iMyHit;
}


const ExtDoc_t * ExtOrder_c::GetDocsChunk()
{
	if ( m_bDone )
		return NULL;

	// warm up
	ARRAY_FOREACH ( i, m_dChildren )
	{
		if ( !m_pDocs[i] ) m_pDocs[i] = m_pDocsChunk[i] = m_dChildren[i]->GetDocsChunk();
		if ( !m_pDocs[i] )
		{
			m_bDone = true;
			return NULL;
		}
	}

	// match, while there's enough space in buffers
	CSphRowitem * pDocinfo = m_pDocinfo;
	int iDoc = 0;
	int iMyHit = 0;
	while ( iDoc<MAX_DOCS-1 && iMyHit+m_dChildren.GetLength()<MAX_HITS-1 )
	{
		// find next candidate document (that has all the words)
		SphDocID_t uDocid = m_pDocs[0]->m_uDocid;
		assert ( uDocid!=DOCID_MAX );

		for ( int i=1; i<m_dChildren.GetLength(); )
		{
			// skip docs with too small ids
			assert ( m_pDocs[i] );
			while ( m_pDocs[i]->m_uDocid < uDocid )
				m_pDocs[i]++;

			// block end marker? pull next block and keep scanning
			if ( m_pDocs[i]->m_uDocid==DOCID_MAX )
			{
				m_pDocs[i] = m_pDocsChunk[i] = m_dChildren[i]->GetDocsChunk();
				if ( !m_pDocs[i] )
				{
					m_bDone = true;
					return ReturnDocsChunk ( iDoc, "order" );
				}
				continue;
			}

			// too big id? its out next candidate
			if ( m_pDocs[i]->m_uDocid > uDocid )
			{
				uDocid = m_pDocs[i]->m_uDocid;
				i = 0;
				continue;
			}

			assert ( m_pDocs[i]->m_uDocid==uDocid );
			i++;
		}

		#ifndef NDEBUG
		assert ( uDocid!=DOCID_MAX );
		ARRAY_FOREACH ( i, m_dChildren )
		{
			assert ( m_pDocs[i] );
			assert ( m_pDocs[i]->m_uDocid==uDocid );
		}
		#endif

		// prefetch hits
		ARRAY_FOREACH ( i, m_dChildren )
		{
			if ( !m_pHits[i] )
				m_pHits[i] = m_dChildren[i]->GetHitsChunk ( m_pDocsChunk[i] );

			// every document comes with at least one hit
			// and we did not yet process current candidate's hits
			// so we MUST have hits at this point no matter what
			assert ( m_pHits[i] );
		}

		// match and save hits
		int iGotHits = GetMatchingHits ( uDocid, m_dMyHits+iMyHit, MAX_HITS-1-iMyHit );
		if ( iGotHits )
		{
			CopyExtDoc ( m_dDocs[iDoc++], *m_pDocs[0], &pDocinfo, m_iStride );
			iMyHit += iGotHits;
		}

		// advance doc stream
		m_pDocs[0]++;
		if ( m_pDocs[0]->m_uDocid==DOCID_MAX )
		{
			m_pDocs[0] = m_pDocsChunk[0] = m_dChildren[0]->GetDocsChunk();
			if ( !m_pDocs[0] )
			{
				m_bDone = true;
				break;
			}
		}
	}

	return ReturnDocsChunk ( iDoc, "order" );
}


const ExtHit_t * ExtOrder_c::GetHitsChunk ( const ExtDoc_t * pDocs )
{
	if ( pDocs->m_uDocid==m_uHitsOverFor )
		return NULL;

	// copy accumulated hits while we can
	SphDocID_t uFirstMatch = pDocs->m_uDocid;

	const ExtHit_t * pMyHits = m_dMyHits;
	int iHit = 0;

	for ( ;; )
	{
		while ( pDocs->m_uDocid!=pMyHits->m_uDocid )
		{
			while ( pDocs->m_uDocid < pMyHits->m_uDocid ) pDocs++;
			if ( pDocs->m_uDocid==DOCID_MAX ) break;

			while ( pMyHits->m_uDocid < pDocs->m_uDocid ) pMyHits++;
			if ( pMyHits->m_uDocid==DOCID_MAX ) break;
		}
		if ( pDocs->m_uDocid==DOCID_MAX || pMyHits->m_uDocid==DOCID_MAX )
			break;

		assert ( pDocs->m_uDocid==pMyHits->m_uDocid );
		while ( pDocs->m_uDocid==pMyHits->m_uDocid )
			m_dHits[iHit++] = *pMyHits++;
		assert ( iHit<MAX_HITS-1 ); // we're copying at most our internal buffer; can't go above limit
	}

	// handling trailing hits border case
	if ( iHit )
	{
		// we've been able to copy some accumulated hits...
		if ( pMyHits->m_uDocid==DOCID_MAX )
		{
			// ...all of them! setup the next run to check for trailing hits
			m_dMyHits[0].m_uDocid = DOCID_MAX;
		} else
		{
			// ...but not all of them! we ran out of docs earlier; hence, trailing hits are of no interest
			m_uHitsOverFor = uFirstMatch;
		}
	} else
	{
		// we did not copy any hits; check for trailing ones as the last resort
		if ( pDocs->m_uDocid!=DOCID_MAX )
		{
			iHit = GetMatchingHits ( pDocs->m_uDocid, m_dHits, MAX_HITS-1 );
		}
		if ( !iHit )
		{
			// actually, not *only* in this case, also in partial buffer case
			// but for simplicity, lets just run one extra GetHitsChunk() iteration
			m_uHitsOverFor = uFirstMatch;
		}
	}

	// all done
	assert ( iHit<MAX_HITS );
	m_dHits[iHit].m_uDocid = DOCID_MAX;
	return iHit ? m_dHits : NULL;
}


int ExtOrder_c::GetQwords ( ExtQwordsHash_t & hQwords )
{
	int iMax = -1;
	ARRAY_FOREACH ( i, m_dChildren )
	{
		int iKidMax = m_dChildren[i]->GetQwords ( hQwords );
		iMax = Max ( iMax, iKidMax );
	}
	return iMax;
}

void ExtOrder_c::SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
{
	ARRAY_FOREACH ( i, m_dChildren )
		m_dChildren[i]->SetQwordsIDF ( hQwords );
}

void ExtOrder_c::GetTerms ( const ExtQwordsHash_t & hQwords, CSphVector<TermPos_t> & dTermDupes ) const
{
	ARRAY_FOREACH ( i, m_dChildren )
		m_dChildren[i]->GetTerms ( hQwords, dTermDupes );
}

uint64_t ExtOrder_c::GetWordID () const
{
	uint64_t uHash = SPH_FNV64_SEED;
	ARRAY_FOREACH ( i, m_dChildren )
	{
		uint64_t uCur = m_dChildren[i]->GetWordID();
		uHash = sphFNV64 ( &uCur, sizeof(uCur), uHash );
	}

	return uHash;
}

//////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////

static void Explain ( const XQNode_t * pNode, const CSphSchema & tSchema, const CSphVector<CSphString> & dZones,
	CSphStringBuilder & tRes, int iIdent )
{
	if ( iIdent )
		tRes.Appendf ( "\n" );
	for ( int i=0; i<iIdent; i++ )
		tRes.Appendf ( "  " );
	switch ( pNode->GetOp() )
	{
		case SPH_QUERY_AND:			tRes.Appendf ( "AND(" ); break;
		case SPH_QUERY_OR:			tRes.Appendf ( "OR(" ); break;
		case SPH_QUERY_MAYBE:		tRes.Appendf ( "MAYBE(" ); break;
		case SPH_QUERY_NOT:			tRes.Appendf ( "NOT(" ); break;
		case SPH_QUERY_ANDNOT:		tRes.Appendf ( "ANDNOT(" ); break;
		case SPH_QUERY_BEFORE:		tRes.Appendf ( "BEFORE(" ); break;
		case SPH_QUERY_PHRASE:		tRes.Appendf ( "PHRASE(" ); break;
		case SPH_QUERY_PROXIMITY:	tRes.Appendf ( "PROXIMITY(distance=%d, ", pNode->m_iOpArg ); break;
		case SPH_QUERY_QUORUM:		tRes.Appendf ( "QUORUM(count=%d, ", pNode->m_iOpArg ); break;
		case SPH_QUERY_NEAR:		tRes.Appendf ( "NEAR(distance=%d", pNode->m_iOpArg ); break;
		case SPH_QUERY_SENTENCE:	tRes.Appendf ( "SENTENCE(" ); break;
		case SPH_QUERY_PARAGRAPH:	tRes.Appendf ( "PARAGRAPH(" ); break;
		default:					tRes.Appendf ( "OPERATOR-%d(", pNode->GetOp() ); break;
	}

	if ( pNode->m_dChildren.GetLength() && pNode->m_dWords.GetLength() )
		tRes.Appendf("virtually-plain, ");

	// dump spec for keyword nodes
	// FIXME? double check that spec does *not* affect non keyword nodes
	if ( !pNode->m_dSpec.IsEmpty() && pNode->m_dWords.GetLength() )
	{
		const XQLimitSpec_t & s = pNode->m_dSpec;
		if ( s.m_bFieldSpec && !s.m_dFieldMask.TestAll ( true ) )
		{
			tRes.Appendf ( "fields=(" );
			bool bNeedComma = false;
			ARRAY_FOREACH ( i, tSchema.m_dFields )
				if ( s.m_dFieldMask.Test(i) )
				{
					if ( bNeedComma )
						tRes.Appendf ( ", " );
					bNeedComma = true;
					tRes.Appendf ( "%s", tSchema.m_dFields[i].m_sName.cstr() );
				}
			tRes.Appendf ( "), " );
		}

		if ( s.m_iFieldMaxPos )
			tRes.Appendf ( "max_field_pos=%d, ", s.m_iFieldMaxPos );

		if ( s.m_dZones.GetLength() )
		{
			tRes.Appendf ( s.m_bZoneSpan ? "zonespans=(" : "zones=(" );
			bool bNeedComma = false;
			ARRAY_FOREACH ( i, s.m_dZones )
			{
				if ( bNeedComma )
					tRes.Appendf ( ", " );
				bNeedComma = true;
				tRes.Appendf ( "%s", dZones [ s.m_dZones[i] ].cstr() );
			}
			tRes.Appendf ( "), " );
		}
	}

	if ( pNode->m_dChildren.GetLength() )
	{
		ARRAY_FOREACH ( i, pNode->m_dChildren )
		{
			if ( i>0 )
				tRes.Appendf ( ", " );
			Explain ( pNode->m_dChildren[i], tSchema, dZones, tRes, iIdent+1 );
		}
	} else
	{
		ARRAY_FOREACH ( i, pNode->m_dWords )
		{
			const XQKeyword_t & w = pNode->m_dWords[i];
			if ( i>0 )
				tRes.Appendf(", ");
			tRes.Appendf ( "KEYWORD(%s, querypos=%d", w.m_sWord.cstr(), w.m_iAtomPos );
			if ( w.m_bExcluded )
				tRes.Appendf ( ", excluded" );
			if ( w.m_bExpanded )
				tRes.Appendf ( ", expanded" );
			if ( w.m_bFieldStart )
				tRes.Appendf ( ", field_start" );
			if ( w.m_bFieldEnd )
				tRes.Appendf ( ", field_end" );
			if ( w.m_bMorphed )
				tRes.Appendf ( ", morphed" );
			if ( w.m_fBoost!=1.0f ) // really comparing floats?
				tRes.Appendf ( ", boost=%f", w.m_fBoost );
			tRes.Appendf ( ")" );
		}
	}
	tRes.Appendf(")");
}


ExtRanker_c::ExtRanker_c ( const XQQuery_t & tXQ, const ISphQwordSetup & tSetup )
	: m_dZoneInfo ( 0 )
{
	assert ( tSetup.m_pCtx );

	m_iInlineRowitems = tSetup.m_iInlineRowitems;
	for ( int i=0; i<ExtNode_i::MAX_DOCS; i++ )
	{
		m_dMatches[i].Reset ( tSetup.m_iDynamicRowitems );
		m_dMyMatches[i].Reset ( tSetup.m_iDynamicRowitems );
	}
	m_tTestMatch.Reset ( tSetup.m_iDynamicRowitems );

	assert ( tXQ.m_pRoot );
	tSetup.m_pZoneChecker = this;
	m_pRoot = ExtNode_i::Create ( tXQ.m_pRoot, tSetup );

#if SPH_TREE_DUMP
	if ( m_pRoot )
		m_pRoot->DebugDump(0);
#endif

	// we generally have three (!) trees for each query
	// 1) parsed tree, a raw result of query parsing
	// 2) transformed tree, with star expansions, morphology, and other transfomations
	// 3) evaluation tree, with tiny keywords cache, and other optimizations
	// tXQ.m_pRoot, passed to ranker from the index, is the transformed tree
	// m_pRoot, internal to ranker, is the evaluation tree
	if ( tSetup.m_pCtx->m_pProfile )
	{
		tSetup.m_pCtx->m_pProfile->m_sTransformedTree.Clear();
		Explain ( tXQ.m_pRoot, tSetup.m_pIndex->GetMatchSchema(), tXQ.m_dZones,
			tSetup.m_pCtx->m_pProfile->m_sTransformedTree, 0 );
	}

	m_pDoclist = NULL;
	m_pHitlist = NULL;
	m_uPayloadMask = 0;
	m_iQwords = 0;
	m_pIndex = tSetup.m_pIndex;
	m_pCtx = tSetup.m_pCtx;
	m_pNanoBudget = tSetup.m_pStats ? tSetup.m_pStats->m_pNanoBudget : NULL;

	m_dZones = tXQ.m_dZones;
	m_dZoneStart.Resize ( m_dZones.GetLength() );
	m_dZoneEnd.Resize ( m_dZones.GetLength() );
	m_dZoneMax.Resize ( m_dZones.GetLength() );
	m_dZoneMin.Resize ( m_dZones.GetLength() );
	m_dZoneMax.Fill ( 0 );
	m_dZoneMin.Fill	( DOCID_MAX );
	m_bZSlist = tXQ.m_bNeedSZlist;
	m_dZoneInfo.Reset ( m_dZones.GetLength() );

	CSphDict * pZonesDict = NULL;
	// workaround for a particular case with following conditions
	if ( !m_pIndex->GetDictionary()->GetSettings().m_bWordDict && m_dZones.GetLength() )
		pZonesDict = m_pIndex->GetDictionary()->Clone();

	ARRAY_FOREACH ( i, m_dZones )
	{
		XQKeyword_t tDot;

		tDot.m_sWord.SetSprintf ( "%c%s", MAGIC_CODE_ZONE, m_dZones[i].cstr() );
		m_dZoneStartTerm.Add ( new ExtTerm_c ( CreateQueryWord ( tDot, tSetup, pZonesDict ), tSetup ) );
		m_dZoneStart[i] = NULL;

		tDot.m_sWord.SetSprintf ( "%c/%s", MAGIC_CODE_ZONE, m_dZones[i].cstr() );
		m_dZoneEndTerm.Add ( new ExtTerm_c ( CreateQueryWord ( tDot, tSetup, pZonesDict ), tSetup ) );
		m_dZoneEnd[i] = NULL;
	}

	SafeDelete ( pZonesDict );

	m_pQcacheEntry = NULL;
	if ( QcacheGetStatus().m_iMaxBytes>0 )
	{
		m_pQcacheEntry = new QcacheEntry_c();
		m_pQcacheEntry->m_iIndexId = m_pIndex->GetIndexId();
	}
}


ExtRanker_c::~ExtRanker_c ()
{
	SafeRelease ( m_pQcacheEntry );

	SafeDelete ( m_pRoot );
	ARRAY_FOREACH ( i, m_dZones )
	{
		SafeDelete ( m_dZoneStartTerm[i] );
		SafeDelete ( m_dZoneEndTerm[i] );
	}

	ARRAY_FOREACH ( i, m_dZoneInfo )
	{
		ARRAY_FOREACH ( iDoc, m_dZoneInfo[i] )
		{
			SafeDelete ( m_dZoneInfo[i][iDoc].m_pHits );
		}
		m_dZoneInfo[i].Reset();
	}
}

void ExtRanker_c::Reset ( const ISphQwordSetup & tSetup )
{
	if ( m_pRoot )
		m_pRoot->Reset ( tSetup );
	ARRAY_FOREACH ( i, m_dZones )
	{
		m_dZoneStartTerm[i]->Reset ( tSetup );
		m_dZoneEndTerm[i]->Reset ( tSetup );

		m_dZoneStart[i] = NULL;
		m_dZoneEnd[i] = NULL;
	}

	m_dZoneMax.Fill ( 0 );
	m_dZoneMin.Fill ( DOCID_MAX );
	ARRAY_FOREACH ( i, m_dZoneInfo )
	{
		ARRAY_FOREACH ( iDoc, m_dZoneInfo[i] )
			SafeDelete ( m_dZoneInfo[i][iDoc].m_pHits );
		m_dZoneInfo[i].Reset();
	}

	// Ranker::Reset() happens on a switch to next RT segment
	// next segment => new and shiny docids => gotta restart encoding
	if ( m_pQcacheEntry )
		m_pQcacheEntry->RankerReset();
}


void ExtRanker_c::UpdateQcache ( int iMatches )
{
	if ( m_pQcacheEntry )
		for ( int i=0; i<iMatches; i++ )
			m_pQcacheEntry->Append ( m_dMatches[i].m_uDocID, m_dMatches[i].m_iWeight );
}


void ExtRanker_c::FinalizeCache ( const ISphSchema & tSorterSchema )
{
	if ( m_pQcacheEntry )
		QcacheAdd ( m_pCtx->m_tQuery, m_pQcacheEntry, tSorterSchema );

	SafeRelease ( m_pQcacheEntry );
}


const ExtDoc_t * ExtRanker_c::GetFilteredDocs ()
{
	#if QDEBUG
	printf ( "ranker getfiltereddocs" );
	#endif

	CSphScopedProfile ( m_pCtx->m_pProfile, SPH_QSTATE_GET_DOCS );
	for ( ;; )
	{
		// get another chunk
		if ( m_pCtx->m_pProfile )
			m_pCtx->m_pProfile->Switch ( SPH_QSTATE_GET_DOCS );
		const ExtDoc_t * pCand = m_pRoot->GetDocsChunk();
		if ( !pCand )
			return NULL;

		// create matches, and filter them
		if ( m_pCtx->m_pProfile )
			m_pCtx->m_pProfile->Switch ( SPH_QSTATE_FILTER );
		int iDocs = 0;
		SphDocID_t uMaxID = 0;
		while ( pCand->m_uDocid!=DOCID_MAX )
		{
			m_tTestMatch.m_uDocID = pCand->m_uDocid;
			m_tTestMatch.m_pStatic = NULL;
			if ( pCand->m_pDocinfo )
				memcpy ( m_tTestMatch.m_pDynamic, pCand->m_pDocinfo, m_iInlineRowitems*sizeof(CSphRowitem) );

			if ( m_pIndex->EarlyReject ( m_pCtx, m_tTestMatch ) )
			{
				pCand++;
				continue;
			}

			uMaxID = pCand->m_uDocid;
			m_dMyDocs[iDocs] = *pCand;
			m_tTestMatch.m_iWeight = (int)( (pCand->m_fTFIDF+0.5f)*SPH_BM25_SCALE ); // FIXME! bench bNeedBM25
			Swap ( m_tTestMatch, m_dMyMatches[iDocs] );
			iDocs++;
			pCand++;
		}

		// clean up zone hash
		if ( !m_bZSlist )
			CleanupZones ( uMaxID );

		if ( iDocs )
		{
			if ( m_pNanoBudget )
				*m_pNanoBudget -= g_iPredictorCostMatch*iDocs;
			m_dMyDocs[iDocs].m_uDocid = DOCID_MAX;

			#if QDEBUG
			CSphStringBuilder tRes;
			tRes.Appendf ( "matched %p docs (%d) = [", this, iDocs );
			for ( int i=0; i<iDocs; i++ )
				tRes.Appendf ( i ? ", 0x%x" : "0x%x", DWORD ( m_dMyDocs[i].m_uDocid ) );
			tRes.Appendf ( "]" );
			printf ( "%s", tRes.cstr() );
			#endif

			return m_dMyDocs;
		}
	}
}

void ExtRanker_c::CleanupZones ( SphDocID_t uMaxDocid )
{
	if ( !uMaxDocid )
		return;

	ARRAY_FOREACH ( i, m_dZoneMin )
	{
		SphDocID_t uMinDocid = m_dZoneMin[i];
		if ( uMinDocid==DOCID_MAX )
			continue;

		CSphVector<ZoneInfo_t> & dZone = m_dZoneInfo[i];
		int iSpan = FindSpan ( dZone, uMaxDocid );
		if ( iSpan==-1 )
			continue;

		if ( iSpan==dZone.GetLength()-1 )
		{
			ARRAY_FOREACH ( iDoc, dZone )
				SafeDelete ( dZone[iDoc].m_pHits );
			dZone.Resize ( 0 );
			m_dZoneMin[i] = uMaxDocid;
			continue;
		}

		for ( int iDoc=0; iDoc<=iSpan; iDoc++ )
			SafeDelete ( dZone[iDoc].m_pHits );

		int iLen = dZone.GetLength() - iSpan - 1;
		memmove ( dZone.Begin(), dZone.Begin()+iSpan+1, sizeof(dZone[0]) * iLen );
		dZone.Resize ( iLen );
		m_dZoneMin[i] = dZone.Begin()->m_uDocid;
	}
}


void ExtRanker_c::SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
{
	m_iQwords = hQwords.GetLength ();
	if ( m_pRoot )
		m_pRoot->SetQwordsIDF ( hQwords );
}


static SphZoneHit_e ZoneCacheFind ( const ZoneVVector_t & dZones, int iZone, const ExtHit_t * pHit, int * pLastSpan )
{
	if ( !dZones[iZone].GetLength() )
		return SPH_ZONE_NO_DOCUMENT;

	ZoneInfo_t * pZone = sphBinarySearch ( dZones[iZone].Begin(), &dZones[iZone].Last(), bind ( &ZoneInfo_t::m_uDocid ), pHit->m_uDocid );
	if ( !pZone )
		return SPH_ZONE_NO_DOCUMENT;

	if ( pZone )
	{
		// remove end markers that might mess up ordering
		Hitpos_t uPosWithField = HITMAN::GetPosWithField ( pHit->m_uHitpos );
		int iSpan = FindSpan ( pZone->m_pHits->m_dStarts, uPosWithField );
		if ( iSpan<0 || uPosWithField>pZone->m_pHits->m_dEnds[iSpan] )
			return SPH_ZONE_NO_SPAN;
		if ( pLastSpan )
			*pLastSpan = iSpan;
		return SPH_ZONE_FOUND;
	}

	return SPH_ZONE_NO_DOCUMENT;
}


SphZoneHit_e ExtRanker_c::IsInZone ( int iZone, const ExtHit_t * pHit, int * pLastSpan )
{
	// quick route, we have current docid cached
	SphZoneHit_e eRes = ZoneCacheFind ( m_dZoneInfo, iZone, pHit, pLastSpan );
	if ( eRes!=SPH_ZONE_NO_DOCUMENT )
		return eRes;

	// is there any zone info for this document at all?
	if ( pHit->m_uDocid<=m_dZoneMax[iZone] )
		return SPH_ZONE_NO_DOCUMENT;

	// long route, read in zone info for all (!) the documents until next requested
	// that's because we might be queried out of order

	// current chunk
	const ExtDoc_t * pStart = m_dZoneStart[iZone];
	const ExtDoc_t * pEnd = m_dZoneEnd[iZone];

	// now keep caching spans until we see current id
	while ( pHit->m_uDocid > m_dZoneMax[iZone] )
	{
		// get more docs if needed
		if ( ( !pStart && m_dZoneMax[iZone]!=DOCID_MAX ) || pStart->m_uDocid==DOCID_MAX )
		{
			pStart = m_dZoneStartTerm[iZone]->GetDocsChunk();
			if ( !pStart )
			{
				m_dZoneMax[iZone] = DOCID_MAX;
				return SPH_ZONE_NO_DOCUMENT;
			}
		}

		if ( ( !pEnd && m_dZoneMax[iZone]!=DOCID_MAX ) || pEnd->m_uDocid==DOCID_MAX )
		{
			pEnd = m_dZoneEndTerm[iZone]->GetDocsChunk();
			if ( !pEnd )
			{
				m_dZoneMax[iZone] = DOCID_MAX;
				return SPH_ZONE_NO_DOCUMENT;
			}
		}

		assert ( pStart && pEnd );

		// skip zone starts past already cached stuff
		while ( pStart->m_uDocid<=m_dZoneMax[iZone] )
			pStart++;
		if ( pStart->m_uDocid==DOCID_MAX )
			continue;

		// skip zone ends until a match with start
		while ( pEnd->m_uDocid<pStart->m_uDocid )
			pEnd++;
		if ( pEnd->m_uDocid==DOCID_MAX )
			continue;

		// handle mismatching start/end ids
		// (this must never happen normally, but who knows what data we're fed)
		assert ( pStart->m_uDocid!=DOCID_MAX );
		assert ( pEnd->m_uDocid!=DOCID_MAX );
		assert ( pStart->m_uDocid<=pEnd->m_uDocid );

		if ( pStart->m_uDocid!=pEnd->m_uDocid )
		{
			while ( pStart->m_uDocid < pEnd->m_uDocid )
				pStart++;
			if ( pStart->m_uDocid==DOCID_MAX )
				continue;
		}

		// first matching uncached docid found!
		assert ( pStart->m_uDocid==pEnd->m_uDocid );
		assert ( pStart->m_uDocid > m_dZoneMax[iZone] );

		// but maybe we don't need docid this big just yet?
		if ( pStart->m_uDocid > pHit->m_uDocid )
		{
			// store current in-chunk positions
			m_dZoneStart[iZone] = pStart;
			m_dZoneEnd[iZone] = pEnd;

			// no zone info for all those precending documents (including requested one)
			m_dZoneMax[iZone] = pStart->m_uDocid-1;
			return SPH_ZONE_NO_DOCUMENT;
		}

		// cache all matching docs from current chunks below requested docid
		// (there might be more matching docs, but we are lazy and won't cache them upfront)
		ExtDoc_t dCache [ ExtNode_i::MAX_DOCS ];
		int iCache = 0;

		while ( pStart->m_uDocid<=pHit->m_uDocid )
		{
			// match
			if ( pStart->m_uDocid==pEnd->m_uDocid )
			{
				dCache[iCache++] = *pStart;
				pStart++;
				pEnd++;
				continue;
			}

			// mismatch!
			// this must not really happen, starts/ends must be in sync
			// but let's be graceful anyway, and just skip to next match
			if ( pStart->m_uDocid==DOCID_MAX || pEnd->m_uDocid==DOCID_MAX )
				break;

			while ( pStart->m_uDocid < pEnd->m_uDocid )
				pStart++;
			if ( pStart->m_uDocid==DOCID_MAX )
				break;

			while ( pEnd->m_uDocid < pStart->m_uDocid )
				pEnd++;
			if ( pEnd->m_uDocid==DOCID_MAX )
				break;
		}

		// should have found at least one id to cache
		assert ( iCache );
		assert ( iCache < ExtNode_i::MAX_DOCS );
		dCache[iCache].m_uDocid = DOCID_MAX;

		// do caching
		const ExtHit_t * pStartHits = m_dZoneStartTerm[iZone]->GetHitsChunk ( dCache );
		const ExtHit_t * pEndHits = m_dZoneEndTerm[iZone]->GetHitsChunk ( dCache );
		int iReserveStart = m_dZoneStartTerm[iZone]->GetHitsCount() / Max ( m_dZoneStartTerm[iZone]->GetDocsCount(), 1 );
		int iReserveEnd = m_dZoneEndTerm[iZone]->GetHitsCount() / Max ( m_dZoneEndTerm[iZone]->GetDocsCount(), 1 );
		int iReserve = Max ( iReserveStart, iReserveEnd );

		// loop documents one by one
		while ( pStartHits && pEndHits )
		{
			// load all hits for current document
			SphDocID_t uCur = pStartHits->m_uDocid;

			// FIXME!!! replace by iterate then add elements to vector instead of searching each time
			ZoneHits_t * pZone = NULL;
			CSphVector<ZoneInfo_t> & dZones = m_dZoneInfo[iZone];
			if ( dZones.GetLength() )
			{
				ZoneInfo_t * pInfo = sphBinarySearch ( dZones.Begin(), &dZones.Last(), bind ( &ZoneInfo_t::m_uDocid ), uCur );
				if ( pInfo )
					pZone = pInfo->m_pHits;
			}
			if ( !pZone )
			{
				if ( dZones.GetLength() && dZones.Last().m_uDocid>uCur )
				{
					int iInsertPos = FindSpan ( dZones, uCur );
					assert ( iInsertPos>=0 );
					dZones.Insert ( iInsertPos, ZoneInfo_t() );
					dZones[iInsertPos].m_uDocid = uCur;
					pZone = dZones[iInsertPos].m_pHits = new ZoneHits_t();
				} else
				{
					ZoneInfo_t & tElem = dZones.Add ();
					tElem.m_uDocid = uCur;
					pZone = tElem.m_pHits = new ZoneHits_t();
				}
				pZone->m_dStarts.Reserve ( iReserve );
				pZone->m_dEnds.Reserve ( iReserve );
			}

			assert ( pEndHits->m_uDocid==uCur );

			// load all the pairs of start and end hits for it
			// do it by with the FSM:
			//
			// state 'begin':
			// - start marker -> set state 'inspan', startspan=pos++
			// - end marker -> pos++
			// - end of doc -> set state 'finish'
			//
			// state 'inspan':
			// - start marker -> startspan = pos++
			// - end marker -> set state 'outspan', endspan=pos++
			// - end of doc -> set state 'finish'
			//
			// state 'outspan':
			// - start marker -> set state 'inspan', commit span, startspan=pos++
			// - end marker -> endspan = pos++
			// - end of doc -> set state 'finish', commit span
			//
			// state 'finish':
			// - we are done.

			int bEofDoc = 0;

			// state 'begin' is here.
			while ( !bEofDoc && pEndHits->m_uHitpos < pStartHits->m_uHitpos )
			{
				++pEndHits;
				bEofDoc |= (pEndHits->m_uDocid!=uCur)?2:0;
			}

			if ( !bEofDoc )
			{
				// state 'inspan' (true) or 'outspan' (false)
				bool bInSpan = true;
				Hitpos_t iSpanBegin = pStartHits->m_uHitpos;
				Hitpos_t iSpanEnd = pEndHits->m_uHitpos;
				while ( bEofDoc!=3 ) /// action end-of-doc
				{
					// action inspan/start-marker
					if ( bInSpan )
					{
						++pStartHits;
						bEofDoc |= (pStartHits->m_uDocid!=uCur)?1:0;
					} else
						// action outspan/end-marker
					{
						++pEndHits;
						bEofDoc |= (pEndHits->m_uDocid!=uCur)?2:0;
					}

					if ( pStartHits->m_uHitpos<pEndHits->m_uHitpos && !( bEofDoc & 1 ) )
					{
						// actions for outspan/start-marker state
						// <b>...<b>..<b>..</b> will ignore all the <b> inside.
						if ( !bInSpan )
						{
							bInSpan = true;
							pZone->m_dStarts.Add ( iSpanBegin );
							pZone->m_dEnds.Add ( iSpanEnd );
							iSpanBegin = pStartHits->m_uHitpos;
						}
					} else if ( !( bEofDoc & 2 ) )
					{
						// actions for inspan/end-marker state
						// so, <b>...</b>..</b>..</b> will ignore all the </b> inside.
						bInSpan = false;
						iSpanEnd = pEndHits->m_uHitpos;
					}
				}
				// action 'commit' for outspan/end-of-doc
				if ( iSpanBegin < iSpanEnd )
				{
					pZone->m_dStarts.Add ( iSpanBegin );
					pZone->m_dEnds.Add ( iSpanEnd );
				}

				if ( pStartHits->m_uDocid==DOCID_MAX )
					pStartHits = m_dZoneStartTerm[iZone]->GetHitsChunk ( dCache );
				if ( pEndHits->m_uDocid==DOCID_MAX )
					pEndHits = m_dZoneEndTerm[iZone]->GetHitsChunk ( dCache );
			}

			// data sanity checks
			assert ( pZone->m_dStarts.GetLength()==pZone->m_dEnds.GetLength() );

			// update cache status
			m_dZoneMax[iZone] = uCur;
			m_dZoneMin[iZone] = Min ( m_dZoneMin[iZone], uCur );
		}
	}

	// store current in-chunk positions
	m_dZoneStart[iZone] = pStart;
	m_dZoneEnd[iZone] = pEnd;

	// cached a bunch of spans, try our check again
	return ZoneCacheFind ( m_dZoneInfo, iZone, pHit, pLastSpan );
}

//////////////////////////////////////////////////////////////////////////

template < bool USE_BM25 >
int ExtRanker_WeightSum_c<USE_BM25>::GetMatches ()
{
	if ( !m_pRoot )
		return 0;

	if ( m_pCtx->m_pProfile )
		m_pCtx->m_pProfile->Switch ( SPH_QSTATE_RANK );

	const ExtDoc_t * pDoc = m_pDoclist;
	int iMatches = 0;

	while ( iMatches<ExtNode_i::MAX_DOCS )
	{
		if ( !pDoc || pDoc->m_uDocid==DOCID_MAX ) pDoc = GetFilteredDocs ();
		if ( !pDoc ) break;

		DWORD uRank = 0;
		DWORD uMask = pDoc->m_uDocFields;
		if ( !uMask )
		{
			// possible if we have more than 32 fields
			// honestly loading all hits etc is cumbersome, so let's just fake it
			uRank = 1;
		} else
		{
			// just sum weights over the lowest 32 fields
			int iWeights = Min ( m_iWeights, 32 );
			for ( int i=0; i<iWeights; i++ )
				if ( pDoc->m_uDocFields & (1<<i) )
					uRank += m_pWeights[i];
		}

		Swap ( m_dMatches[iMatches], m_dMyMatches[pDoc-m_dMyDocs] ); // OPTIMIZE? can avoid this swap and simply return m_dMyMatches (though in lesser chunks)
		m_dMatches[iMatches].m_iWeight = USE_BM25
			? ( m_dMatches[iMatches].m_iWeight + uRank*SPH_BM25_SCALE )
			: uRank;

		iMatches++;
		pDoc++;
	}

	UpdateQcache ( iMatches );

	m_pDoclist = pDoc;
	return iMatches;
}

//////////////////////////////////////////////////////////////////////////

int ExtRanker_None_c::GetMatches ()
{
	if ( !m_pRoot )
		return 0;

	if ( m_pCtx->m_pProfile )
		m_pCtx->m_pProfile->Switch ( SPH_QSTATE_RANK );

	const ExtDoc_t * pDoc = m_pDoclist;
	int iMatches = 0;

	while ( iMatches<ExtNode_i::MAX_DOCS )
	{
		if ( !pDoc || pDoc->m_uDocid==DOCID_MAX ) pDoc = GetFilteredDocs ();
		if ( !pDoc ) break;

		Swap ( m_dMatches[iMatches], m_dMyMatches[pDoc-m_dMyDocs] ); // OPTIMIZE? can avoid this swap and simply return m_dMyMatches (though in lesser chunks)
		m_dMatches[iMatches].m_iWeight = 1;
		iMatches++;
		pDoc++;
	}

	UpdateQcache ( iMatches );

	m_pDoclist = pDoc;
	return iMatches;
}

//////////////////////////////////////////////////////////////////////////

template < typename STATE >
ExtRanker_T<STATE>::ExtRanker_T ( const XQQuery_t & tXQ, const ISphQwordSetup & tSetup )
	: ExtRanker_c ( tXQ, tSetup )
{
	// FIXME!!! move out the disable of m_bZSlist in case no zonespan nodes
	if ( m_bZSlist )
		m_dZonespans.Reserve ( ExtNode_i::MAX_DOCS * m_dZones.GetLength() );

	m_pHitBase = NULL;
}


static inline const ExtHit_t * RankerGetHits ( CSphQueryProfile * pProfile, ExtNode_i * pRoot, const ExtDoc_t * pDocs )
{
	if ( !pProfile )
		return pRoot->GetHitsChunk ( pDocs );

	pProfile->Switch ( SPH_QSTATE_GET_HITS );
	const ExtHit_t * pHlist = pRoot->GetHitsChunk ( pDocs );
	pProfile->Switch ( SPH_QSTATE_RANK );
	return pHlist;
}


template < typename STATE >
int ExtRanker_T<STATE>::GetMatches ()
{
	if ( !m_pRoot )
		return 0;

	if ( m_pCtx->m_pProfile )
		m_pCtx->m_pProfile->Switch ( SPH_QSTATE_RANK );

	CSphQueryProfile * pProfile = m_pCtx->m_pProfile;
	int iMatches = 0;
	const ExtHit_t * pHlist = m_pHitlist;
	const ExtHit_t * pHitBase = m_pHitBase;
	const ExtDoc_t * pDocs = m_pDoclist;
	m_dZonespans.Resize(1);
	int	iLastZoneData = 0;

	CSphVector<int> dSpans;
	if ( m_bZSlist )
	{
		dSpans.Resize ( m_dZones.GetLength() );
		dSpans.Fill ( -1 );
	}

	// warmup if necessary
	if ( !pHlist )
	{
		if ( !pDocs )
			pDocs = GetFilteredDocs ();
		if ( !pDocs )
		{
			UpdateQcache(0);
			return 0;
		}
		pHlist = RankerGetHits ( pProfile, m_pRoot, pDocs );
		if ( !pHlist )
		{
			UpdateQcache(0);
			return 0;
		}
	}

	if ( !pHitBase )
		pHitBase = pHlist;

	// main matching loop
	const ExtDoc_t * pDoc = pDocs;
	for ( SphDocID_t uCurDocid=0; iMatches<ExtNode_i::MAX_DOCS; )
	{
		// keep ranking
		while ( pHlist->m_uDocid==uCurDocid )
		{
			m_tState.Update ( pHlist );
			if ( m_bZSlist )
			{
				ARRAY_FOREACH ( i, m_dZones )
				{
					int iSpan;
					if ( IsInZone ( i, pHlist, &iSpan )!=SPH_ZONE_FOUND )
						continue;

					if ( iSpan!=dSpans[i] )
					{
						m_dZonespans.Add ( i );
						m_dZonespans.Add ( iSpan );
						dSpans[i] = iSpan;
					}
				}
			}
			++pHlist;
		}

		// if hits block is over, get next block, but do *not* flush current doc
		if ( pHlist->m_uDocid==DOCID_MAX )
		{
			assert ( pDocs );
			pHlist = RankerGetHits ( pProfile, m_pRoot, pDocs );
			if ( pHlist )
				continue;
		}

		// otherwise (new match or no next hits block), flush current doc
		if ( uCurDocid )
		{
			assert ( uCurDocid==pDoc->m_uDocid );
			Swap ( m_dMatches[iMatches], m_dMyMatches[pDoc-m_dMyDocs] );
			m_dMatches[iMatches].m_iWeight = m_tState.Finalize ( m_dMatches[iMatches] );
			if ( m_bZSlist )
			{
				m_dZonespans[iLastZoneData] = m_dZonespans.GetLength() - iLastZoneData - 1;
				m_dMatches[iMatches].m_iTag = iLastZoneData;

				iLastZoneData = m_dZonespans.GetLength();
				m_dZonespans.Add(0);

				dSpans.Fill ( -1 );
			}
			iMatches++;
		}

		// boundary checks
		if ( !pHlist )
		{
			if ( m_bZSlist && uCurDocid )
				CleanupZones ( uCurDocid );

			// there are no more hits for current docs block; do we have a next one?
			assert ( pDocs );
			pDoc = pDocs = GetFilteredDocs ();

			// we don't, so bail out
			if ( !pDocs )
				break;

			// we do, get some hits with proper profile
			pHlist = RankerGetHits ( pProfile, m_pRoot, pDocs );
			assert ( pHlist ); // fresh docs block, must have hits
		}

		// skip until next good doc/hit pair
		assert ( pDoc->m_uDocid<=pHlist->m_uDocid );
		while ( pDoc->m_uDocid<pHlist->m_uDocid ) pDoc++;
		assert ( pDoc->m_uDocid==pHlist->m_uDocid );

		uCurDocid = pHlist->m_uDocid;
	}

	m_pDoclist = pDocs;
	m_pHitlist = pHlist;
	if ( !m_pHitBase )
		m_pHitBase = pHitBase;

	UpdateQcache ( iMatches );

	return iMatches;
}

//////////////////////////////////////////////////////////////////////////


FactorPool_c::FactorPool_c ()
	: m_iElementSize	( 0 )
	, m_dPool ( 0 )
	, m_dHash ( 0 )
{
}


void FactorPool_c::Prealloc ( int iElementSize, int nElements )
{
	m_iElementSize = iElementSize;

	m_dPool.Reset ( nElements*GetIntElementSize() );
	m_dHash.Reset ( nElements );
	m_dFree.Reset ( nElements );

	memset ( m_dHash.Begin(), 0, sizeof(m_dHash[0])*m_dHash.GetLength() );
}


BYTE * FactorPool_c::Alloc ()
{
	int iIndex = m_dFree.Get();
	assert ( iIndex>=0 && iIndex*GetIntElementSize()<m_dPool.GetLength() );
	return m_dPool.Begin() + iIndex * GetIntElementSize();
}


void FactorPool_c::Free ( BYTE * pPtr )
{
	if ( !pPtr )
		return;

	assert ( (pPtr-m_dPool.Begin() ) % GetIntElementSize()==0);
	assert ( pPtr>=m_dPool.Begin() && pPtr<&( m_dPool.Last() ) );

	int iIndex = ( pPtr-m_dPool.Begin() )/GetIntElementSize();
	m_dFree.Free ( iIndex );
}


int FactorPool_c::GetIntElementSize () const
{
	return m_iElementSize+sizeof(SphFactorHashEntry_t);
}


int	FactorPool_c::GetElementSize() const
{
	return m_iElementSize;
}


void FactorPool_c::AddToHash ( SphDocID_t uId, BYTE * pPacked )
{
	SphFactorHashEntry_t * pNew = (SphFactorHashEntry_t *)(pPacked+m_iElementSize);
	memset ( pNew, 0, sizeof(SphFactorHashEntry_t) );

	DWORD uKey = HashFunc ( uId );
	if ( m_dHash[uKey] )
	{
		SphFactorHashEntry_t * pStart = m_dHash[uKey];
		pNew->m_pPrev = NULL;
		pNew->m_pNext = pStart;
		pStart->m_pPrev = pNew;
	}

	pNew->m_pData = pPacked;
	pNew->m_iId = uId;
	m_dHash[uKey] = pNew;
}


SphFactorHashEntry_t * FactorPool_c::Find ( SphDocID_t uId ) const
{
	DWORD uKey = HashFunc ( uId );
	if ( m_dHash[uKey] )
	{
		SphFactorHashEntry_t * pEntry = m_dHash[uKey];
		while ( pEntry )
		{
			if ( pEntry->m_iId==uId )
				return pEntry;

			pEntry = pEntry->m_pNext;
		}
	}

	return NULL;
}


void FactorPool_c::AddRef ( SphDocID_t uId )
{
	if ( !uId )
		return;

	SphFactorHashEntry_t * pEntry = Find ( uId );
	if ( pEntry )
		pEntry->m_iRefCount++;
}


void FactorPool_c::Release ( SphDocID_t uId )
{
	if ( !uId )
		return;

	SphFactorHashEntry_t * pEntry = Find ( uId );
	if ( pEntry )
	{
		pEntry->m_iRefCount--;
		bool bHead = !pEntry->m_pPrev;
		SphFactorHashEntry_t * pNext = pEntry->m_pNext;
		if ( FlushEntry ( pEntry ) && bHead )
			m_dHash [ HashFunc ( uId ) ] = pNext;
	}
}


bool FactorPool_c::FlushEntry ( SphFactorHashEntry_t * pEntry )
{
	assert ( pEntry );
	assert ( pEntry->m_iRefCount>=0 );
	if ( pEntry->m_iRefCount )
		return false;

	if ( pEntry->m_pPrev )
		pEntry->m_pPrev->m_pNext = pEntry->m_pNext;

	if ( pEntry->m_pNext )
		pEntry->m_pNext->m_pPrev = pEntry->m_pPrev;

	Free ( pEntry->m_pData );

	return true;
}


void FactorPool_c::Flush()
{
	ARRAY_FOREACH ( i, m_dHash )
	{
		SphFactorHashEntry_t * pEntry = m_dHash[i];
		while ( pEntry )
		{
			SphFactorHashEntry_t * pNext = pEntry->m_pNext;
			bool bHead = !pEntry->m_pPrev;
			if ( FlushEntry(pEntry) && bHead )
				m_dHash[i] = pNext;

			pEntry = pNext;
		}
	}
}


inline DWORD FactorPool_c::HashFunc ( SphDocID_t uId ) const
{
	return (DWORD)( uId % m_dHash.GetLength() );
}


bool FactorPool_c::IsInitialized() const
{
	return !!m_iElementSize;
}


SphFactorHash_t * FactorPool_c::GetHashPtr ()
{
	return &m_dHash;
}


/////////////////////////////////////

/// ctor
template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
RankerState_Expr_fn <NEED_PACKEDFACTORS, HANDLE_DUPES>::RankerState_Expr_fn ()
	: m_pWeights ( NULL )
	, m_sExpr ( NULL )
	, m_pExpr ( NULL )
	, m_iPoolMatchCapacity ( 0 )
	, m_iMaxLCS ( 0 )
	, m_iQueryWordCount ( 0 )
	, m_iAtcHitStart ( 0 )
	, m_iAtcHitCount ( 0 )
	, m_uAtcField ( 0 )
	, m_bAtcHeadProcessed ( false )
	, m_bHaveAtc ( false )
	, m_bWantAtc ( false )
{}


/// dtor
template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
RankerState_Expr_fn <NEED_PACKEDFACTORS, HANDLE_DUPES>::~RankerState_Expr_fn ()
{
	SafeRelease ( m_pExpr );
}


/// initialize ranker state
template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
bool RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::Init ( int iFields, const int * pWeights, ExtRanker_c * pRanker, CSphString & sError,
																	DWORD uFactorFlags )
{
	m_iFields = iFields;
	m_pWeights = pWeights;
	m_uDocBM25 = 0;
	m_tMatchedFields.Init ( iFields );
	m_tExactHit.Init ( iFields );
	m_tExactOrder.Init ( iFields );
	m_iCurrentField = 0;
	m_iMaxQpos = pRanker->m_iMaxQpos; // already copied in SetQwords, but anyway
	m_iWindowSize = 1;
	m_iHaveMinWindow = 0;
	m_dMinWindowHits.Reserve ( Max ( m_iMaxQpos, 32 ) );
	memset ( m_dLCCS, 0 , sizeof(m_dLCCS) );
	memset ( m_dWLCCS, 0, sizeof(m_dWLCCS) );
	m_iQueryPosLCCS = 0;
	m_iHitPosLCCS = 0;
	m_iLenLCCS = 0;
	m_fWeightLCCS = 0.0f;
	m_dAtcTerms.Resize ( m_iMaxQpos + 1 );
	m_dAtcProcessedTerms.Init ( m_iMaxQpos + 1 );
	m_bAtcHeadProcessed = false;
	ResetDocFactors();

	// compute query level constants
	// max_lcs, aka m_iMaxLCS (for matchany ranker emulation) gets computed here
	// query_word_count, aka m_iQueryWordCount is set elsewhere (in SetQwordsIDF())
	m_iMaxLCS = 0;
	for ( int i=0; i<iFields; i++ )
		m_iMaxLCS += pWeights[i] * pRanker->m_iQwords;

	for ( int i=0; i<m_pSchema->GetAttrsCount(); i++ )
	{
		if ( m_pSchema->GetAttr(i).m_eAttrType!=ESphAttr::SPH_ATTR_TOKENCOUNT )
			continue;
		m_tFieldLensLoc = m_pSchema->GetAttr(i).m_tLocator;
		break;
	}

	m_fAvgDocLen = 0.0f;
	m_pFieldLens = pRanker->GetIndex()->GetFieldLens();
	if ( m_pFieldLens )
		for ( int i=0; i<iFields; i++ )
			m_fAvgDocLen += m_pFieldLens[i];
	else
		m_fAvgDocLen = 1.0f;
	m_iTotalDocuments = pRanker->GetIndex()->GetStats().m_iTotalDocuments;
	m_fAvgDocLen /= m_iTotalDocuments;

	m_fParamK1 = 1.2f;
	m_fParamB = 0.75f;

	// not in SetQwords, because we only get iFields here
	m_dFieldTF.Resize ( m_iFields*(m_iMaxQpos+1) );
	m_dFieldTF.Fill ( 0 );

	// parse expression
	bool bUsesWeight;
	ExprRankerHook_T<NEED_PACKEDFACTORS, HANDLE_DUPES> tHook ( this );
	m_pExpr = sphExprParse ( m_sExpr, *m_pSchema, &m_eExprType, &bUsesWeight, sError, NULL, SPH_COLLATION_DEFAULT, &tHook ); // FIXME!!! profile UDF here too
	if ( !m_pExpr )
		return false;
	if ( m_eExprType!=ESphAttr::SPH_ATTR_INTEGER && m_eExprType!=ESphAttr::SPH_ATTR_FLOAT )
	{
		sError = "ranking expression must evaluate to integer or float";
		return false;
	}
	if ( bUsesWeight )
	{
		sError = "ranking expression must not refer to WEIGHT()";
		return false;
	}
	if ( tHook.m_sCheckError )
	{
		sError = tHook.m_sCheckError;
		return false;
	}

	int iUniq = m_iMaxQpos;
	if_const ( HANDLE_DUPES )
	{
		iUniq = 0;
		ARRAY_FOREACH ( i, m_dTermDupes )
			iUniq += ( IsTermSkipped(i) ? 0 : 1 );
	}

	m_iHaveMinWindow = iUniq;

	// we either have an ATC factor in the expression or packedfactors() without no_atc=1
	if ( m_bWantAtc || ( uFactorFlags & SPH_FACTOR_CALC_ATC ) )
		m_bHaveAtc = ( iUniq>1 );

	// all seems ok
	return true;
}


/// process next hit, update factors
template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
void RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::Update ( const ExtHit_t * pHlist )
{
	const DWORD uField = HITMAN::GetField ( pHlist->m_uHitpos );
	const int iPos = HITMAN::GetPos ( pHlist->m_uHitpos );
	const DWORD uPosWithField = HITMAN::GetPosWithField ( pHlist->m_uHitpos );

	if_const ( !HANDLE_DUPES )
	{
		// update LCS
		int iDelta = uPosWithField - pHlist->m_uQuerypos;
		if ( iDelta==m_iExpDelta )
		{
			if ( (int)uPosWithField>m_iLastHitPos )
				m_uCurLCS = (BYTE)( m_uCurLCS + pHlist->m_uWeight );
			if ( HITMAN::IsEnd ( pHlist->m_uHitpos ) && (int)pHlist->m_uQuerypos==m_iMaxQpos && iPos==m_iMaxQpos )
				m_tExactHit.BitSet ( uField );
		} else
		{
			if ( (int)uPosWithField>m_iLastHitPos )
				m_uCurLCS = BYTE(pHlist->m_uWeight);
			if ( iPos==1 && HITMAN::IsEnd ( pHlist->m_uHitpos ) && m_iMaxQpos==1 )
				m_tExactHit.BitSet ( uField );
		}

		if ( m_uCurLCS>m_uLCS[uField] )
		{
			m_uLCS[uField] = m_uCurLCS;
			// for the first hit in current field just use current position as min_best_span_pos
			// else adjust for current lcs
			if ( !m_iMinBestSpanPos [ uField ] )
				m_iMinBestSpanPos [ uField ] = iPos;
			else
				m_iMinBestSpanPos [ uField ] = iPos - m_uCurLCS + 1;
		}
		m_iExpDelta = iDelta + pHlist->m_uSpanlen - 1;
		m_iLastHitPos = uPosWithField;
	} else
	{
		// reset accumulated data from previous field
		if ( (DWORD)HITMAN::GetField ( m_uCurPos )!=uField )
		{
			m_uCurPos = 0;
			m_uLcsTailPos = 0;
			m_uCurQposMask = 0;
			m_uCurLCS = 0;
		}

		if ( (DWORD)uPosWithField!=m_uCurPos )
		{
			// next new and shiny hitpos in line
			// FIXME!? what do we do with longer spans? keep looking? reset?
			if ( m_uCurLCS<2 )
			{
				m_uLcsTailPos = m_uCurPos;
				m_uLcsTailQposMask = m_uCurQposMask;
				m_uCurLCS = 1;
			}
			m_uCurQposMask = 0;
			m_uCurPos = uPosWithField;
			if ( m_uLCS [ uField ]<pHlist->m_uWeight )
			{
				m_uLCS [ uField ] = BYTE ( pHlist->m_uWeight );
				m_iMinBestSpanPos [ uField ] = iPos;
				m_uLastSpanStart = iPos;
			}
		}

		// add that qpos to current qpos mask (for the current hitpos)
		m_uCurQposMask |= ( 1UL << pHlist->m_uQuerypos );

		// and check if that results in a better lcs match now
		int iDelta = ( m_uCurPos-m_uLcsTailPos );
		if ( iDelta && iDelta<32 && ( m_uCurQposMask >> iDelta ) & m_uLcsTailQposMask )
		{
			// cool, it matched!
			m_uLcsTailQposMask = ( 1UL << pHlist->m_uQuerypos ); // our lcs span now ends with a specific qpos
			m_uLcsTailPos = m_uCurPos; // and in a specific position
			m_uCurLCS = BYTE ( m_uCurLCS+pHlist->m_uWeight ); // and it's longer
			m_uCurQposMask = 0; // and we should avoid matching subsequent hits on the same hitpos

			// update per-field vector
			if ( m_uCurLCS>m_uLCS[uField] )
			{
				m_uLCS[uField] = m_uCurLCS;
				m_iMinBestSpanPos[uField] = m_uLastSpanStart;
			}
		}

		if ( iDelta==m_iExpDelta )
		{
			if ( HITMAN::IsEnd ( pHlist->m_uHitpos ) && (int)pHlist->m_uQuerypos==m_iMaxQpos && iPos==m_iMaxQpos )
				m_tExactHit.BitSet ( uField );
		} else
		{
			if ( iPos==1 && HITMAN::IsEnd ( pHlist->m_uHitpos ) && m_iMaxQpos==1 )
				m_tExactHit.BitSet ( uField );
		}
		m_iExpDelta = iDelta + pHlist->m_uSpanlen - 1;
	}

	bool bLetsKeepup = false;
	// update LCCS
	if ( m_iQueryPosLCCS==pHlist->m_uQuerypos && m_iHitPosLCCS==iPos )
	{
		m_iLenLCCS++;
		m_fWeightLCCS += m_dIDF [ pHlist->m_uQuerypos ];
	} else
	{
		if_const ( HANDLE_DUPES && m_iHitPosLCCS && iPos<=m_iHitPosLCCS && m_tHasMultiQpos.BitGet ( pHlist->m_uQuerypos ) )
		{
			bLetsKeepup = true;
		} else
		{
			m_iLenLCCS = 1;
			m_fWeightLCCS = m_dIDF[pHlist->m_uQuerypos];
		}
	}
	if ( !bLetsKeepup )
	{
		WORD iNextQPos = m_dNextQueryPos[pHlist->m_uQuerypos];
		m_iQueryPosLCCS = iNextQPos;
		m_iHitPosLCCS = iPos + pHlist->m_uSpanlen + iNextQPos - pHlist->m_uQuerypos - 1;
	}
	if ( m_dLCCS[uField]<=m_iLenLCCS ) // FIXME!!! check weight too or keep length and weight separate
	{
		m_dLCCS[uField] = m_iLenLCCS;
		m_dWLCCS[uField] = m_fWeightLCCS;
	}

	// update ATC
	if ( m_bHaveAtc )
	{
		if ( m_uAtcField!=uField || m_iAtcHitCount==XRANK_ATC_BUFFER_LEN )
		{
			UpdateATC ( m_uAtcField!=uField );
			if ( m_uAtcField!=uField )
			{
				m_uAtcField = uField;
			}
			if ( m_iAtcHitCount==XRANK_ATC_BUFFER_LEN ) // advance ring buffer
			{
				m_iAtcHitStart = ( m_iAtcHitStart + XRANK_ATC_WINDOW_LEN ) % XRANK_ATC_BUFFER_LEN;
				m_iAtcHitCount -= XRANK_ATC_WINDOW_LEN;
			}
		}
		assert ( m_iAtcHitStart<XRANK_ATC_BUFFER_LEN && m_iAtcHitCount<XRANK_ATC_BUFFER_LEN );
		int iRing = ( m_iAtcHitStart + m_iAtcHitCount ) % XRANK_ATC_BUFFER_LEN;
		AtcHit_t & tAtcHit = m_dAtcHits [ iRing ];
		tAtcHit.m_iHitpos = iPos;
		tAtcHit.m_uQuerypos = pHlist->m_uQuerypos;
		m_iAtcHitCount++;
	}

	// update other stuff
	m_tMatchedFields.BitSet ( uField );

	// keywords can be duplicated in the query, so we need this extra check
	WORD uQpos = pHlist->m_uQuerypos;
	bool bUniq = m_tKeywords.BitGet ( pHlist->m_uQuerypos );
	if_const ( HANDLE_DUPES && bUniq )
	{
		uQpos = m_dTermDupes [ uQpos ];
		bUniq = ( m_dTermsHit[uQpos]!=pHlist->m_uHitpos && m_dTermsHit[0]!=pHlist->m_uHitpos );
		m_dTermsHit[uQpos] = pHlist->m_uHitpos;
		m_dTermsHit[0] = pHlist->m_uHitpos;
	}
	if ( bUniq )
	{
		UpdateFreq ( uQpos, uField );
	}
	// handle hit with multiple terms
	if ( pHlist->m_uSpanlen>1 )
	{
		WORD uQposSpanned = pHlist->m_uQuerypos+1;
		DWORD uQposMask = ( pHlist->m_uQposMask>>1 );
		while ( uQposMask!=0 )
		{
			WORD uQposFixed = uQposSpanned;
			if ( ( uQposMask & 1 )==1 )
			{
				bool bUniqSpanned = true;
				if_const ( HANDLE_DUPES )
				{
					uQposFixed = m_dTermDupes[uQposFixed];
					bUniqSpanned = ( m_dTermsHit[uQposFixed]!=pHlist->m_uHitpos );
					m_dTermsHit[uQposFixed] = pHlist->m_uHitpos;
				}
				if ( bUniqSpanned )
					UpdateFreq ( uQposFixed, uField );
			}
			uQposSpanned++;
			uQposMask = ( uQposMask>>1 );
		}
	}

	if ( !m_iMinHitPos[uField] )
		m_iMinHitPos[uField] = iPos;

	// update hit window, max_window_hits factor
	if ( m_iWindowSize>1 )
	{
		if ( m_dWindow.GetLength() )
		{
			// sorted_remove_if ( _1 + winsize <= hitpos ) )
			int i = 0;
			while ( i<m_dWindow.GetLength() && ( m_dWindow[i] + m_iWindowSize )<=pHlist->m_uHitpos )
				i++;
			for ( int j=0; j<m_dWindow.GetLength()-i; j++ )
				m_dWindow[j] = m_dWindow[j+i];
			m_dWindow.Resize ( m_dWindow.GetLength()-i );
		}
		m_dWindow.Add ( pHlist->m_uHitpos );
		m_iMaxWindowHits[uField] = Max ( m_iMaxWindowHits[uField],(int) m_dWindow.GetLength() );
	} else
		m_iMaxWindowHits[uField] = 1;

	// update exact_order factor
	if ( (int)uField!=m_iLastField )
	{
		m_iLastQuerypos = 0;
		m_iExactOrderWords = 0;
		m_iLastField = (int)uField;
	}
	if ( pHlist->m_uQuerypos==m_iLastQuerypos+1 )
	{
		if ( ++m_iExactOrderWords==m_iQueryWordCount )
			m_tExactOrder.BitSet ( uField );
		m_iLastQuerypos++;
	}

	// update min_gaps factor
	if ( bUniq && m_iHaveMinWindow>1 )
	{
		uQpos = pHlist->m_uQuerypos;
		if_const ( HANDLE_DUPES )
			uQpos = m_dTermDupes[uQpos];

		switch ( m_iHaveMinWindow )
		{
		// 2 keywords, special path
		case 2:
			if ( m_dMinWindowHits.GetLength() && HITMAN::GetField ( m_dMinWindowHits[0].m_uHitpos )!=(int)uField )
			{
				m_iMinWindowWords = 0;
				m_dMinWindowHits.Resize ( 0 );
			}

			if ( !m_dMinWindowHits.GetLength() )
			{
				m_dMinWindowHits.Add() = *pHlist; // {} => {A}
				m_dMinWindowHits.Last().m_uQuerypos = uQpos;
				break;
			}

			assert ( m_dMinWindowHits.GetLength()==1 );
			if ( uQpos==m_dMinWindowHits[0].m_uQuerypos )
				m_dMinWindowHits[0].m_uHitpos = pHlist->m_uHitpos;
			else
			{
				UpdateGap ( uField, 2, HITMAN::GetPos ( pHlist->m_uHitpos ) - HITMAN::GetPos ( m_dMinWindowHits[0].m_uHitpos ) - 1 );
				m_dMinWindowHits[0] = *pHlist;
				m_dMinWindowHits[0].m_uQuerypos = uQpos;
			}
			break;

		// 3 keywords, special path
		case 3:
			if ( m_dMinWindowHits.GetLength() && HITMAN::GetField ( m_dMinWindowHits.Last().m_uHitpos )!=(int)uField )
			{
				m_iMinWindowWords = 0;
				m_dMinWindowHits.Resize ( 0 );
			}

			// how many unique words are already there in the current candidate?
			switch ( m_dMinWindowHits.GetLength() )
			{
				case 0:
					m_dMinWindowHits.Add() = *pHlist; // {} => {A}
					m_dMinWindowHits.Last().m_uQuerypos = uQpos;
					break;

				case 1:
					if ( m_dMinWindowHits[0].m_uQuerypos==uQpos )
						m_dMinWindowHits[0] = *pHlist; // {A} + A2 => {A2}
					else
					{
						UpdateGap ( uField, 2, HITMAN::GetPos ( pHlist->m_uHitpos ) - HITMAN::GetPos ( m_dMinWindowHits[0].m_uHitpos ) - 1 );
						m_dMinWindowHits.Add() = *pHlist; // {A} + B => {A,B}
						m_dMinWindowHits.Last().m_uQuerypos = uQpos;
					}
					break;

				case 2:
					if ( m_dMinWindowHits[0].m_uQuerypos==uQpos )
					{
						UpdateGap ( uField, 2, HITMAN::GetPos ( pHlist->m_uHitpos ) - HITMAN::GetPos ( m_dMinWindowHits[1].m_uHitpos ) - 1 );
						m_dMinWindowHits[0] = m_dMinWindowHits[1]; // {A,B} + A2 => {B,A2}
						m_dMinWindowHits[1] = *pHlist;
						m_dMinWindowHits[1].m_uQuerypos = uQpos;
					} else if ( m_dMinWindowHits[1].m_uQuerypos==uQpos )
					{
						m_dMinWindowHits[1] = *pHlist; // {A,B} + B2 => {A,B2}
						m_dMinWindowHits[1].m_uQuerypos = uQpos;
					} else
					{
						// new {A,B,C} window!
						// handle, and then immediately reduce it to {B,C}
						UpdateGap ( uField, 3, HITMAN::GetPos ( pHlist->m_uHitpos ) - HITMAN::GetPos ( m_dMinWindowHits[0].m_uHitpos ) - 2 );
						m_dMinWindowHits[0] = m_dMinWindowHits[1];
						m_dMinWindowHits[1] = *pHlist;
						m_dMinWindowHits[1].m_uQuerypos = uQpos;
					}
					break;

				default:
					assert ( 0 && "min_gaps current window size not in 0..2 range; must not happen" );
			}
			break;

		// slow generic update
		default:
			UpdateMinGaps ( pHlist );
			break;
		}
	}
}


template < bool PF, bool HANDLE_DUPES >
void RankerState_Expr_fn<PF, HANDLE_DUPES>::UpdateFreq ( WORD uQpos, DWORD uField )
{
	float fIDF = m_dIDF [ uQpos ];
	DWORD uHitPosMask = 1<<uQpos;

	if ( !( m_uWordCount[uField] & uHitPosMask ) )
		m_dSumIDF[uField] += fIDF;

	if ( fIDF < m_dMinIDF[uField] )
		m_dMinIDF[uField] = fIDF;

	if ( fIDF > m_dMaxIDF[uField] )
		m_dMaxIDF[uField] = fIDF;

	m_uHitCount[uField]++;
	m_uWordCount[uField] |= uHitPosMask;
	m_uDocWordCount |= uHitPosMask;
	m_dTFIDF[uField] += fIDF;

	// avoid duplicate check for BM25A, BM25F though
	// (that sort of automatically accounts for qtf factor)
	m_dTF [ uQpos ]++;
	m_dFieldTF [ uField*(1+m_iMaxQpos) + uQpos ]++;
}


template < bool PF, bool HANDLE_DUPES >
void RankerState_Expr_fn<PF, HANDLE_DUPES>::UpdateMinGaps ( const ExtHit_t * pHlist )
{
	// update the minimum MW, aka matching window, for min_gaps and ymw factors
	// we keep a window with all the positions of all the matched words
	// we keep it left-minimal at all times, so that leftmost keyword only occurs once
	// thus, when a previously unseen keyword is added, the window is guaranteed to be minimal

	WORD uQpos = pHlist->m_uQuerypos;
	if_const ( HANDLE_DUPES )
		uQpos = m_dTermDupes[uQpos];

	// handle field switch
	const int iField = HITMAN::GetField ( pHlist->m_uHitpos );
	if ( m_dMinWindowHits.GetLength() && HITMAN::GetField ( m_dMinWindowHits.Last().m_uHitpos )!=iField )
	{
		m_dMinWindowHits.Resize ( 0 );
		m_dMinWindowCounts.Fill ( 0 );
		m_iMinWindowWords = 0;
	}

	// assert we are left-minimal
	assert ( m_dMinWindowHits.GetLength()==0 || m_dMinWindowCounts [ m_dMinWindowHits[0].m_uQuerypos ]==1 );

	// another occurrence of the trailing word?
	// just update hitpos, effectively dumping the current occurrence
	if ( m_dMinWindowHits.GetLength() && m_dMinWindowHits.Last().m_uQuerypos==uQpos )
	{
		m_dMinWindowHits.Last().m_uHitpos = pHlist->m_uHitpos;
		return;
	}

	// add that word
	LeanHit_t & t = m_dMinWindowHits.Add();
	t.m_uQuerypos = uQpos;
	t.m_uHitpos = pHlist->m_uHitpos;

	int iWord = uQpos;
	m_dMinWindowCounts[iWord]++;

	// new, previously unseen keyword? just update the window size
	if ( m_dMinWindowCounts[iWord]==1 )
	{
		m_iMinGaps[iField] = HITMAN::GetPos ( pHlist->m_uHitpos ) - HITMAN::GetPos ( m_dMinWindowHits[0].m_uHitpos ) - m_iMinWindowWords;
		m_iMinWindowWords++;
		return;
	}

	// check if we can shrink the left boundary
	if ( iWord!=m_dMinWindowHits[0].m_uQuerypos )
		return;

	// yes, we can!
	// keep removing the leftmost keyword until it's unique (in the window) again
	assert ( m_dMinWindowCounts [ m_dMinWindowHits[0].m_uQuerypos ]==2 );
	int iShrink = 0;
	while ( m_dMinWindowCounts [ m_dMinWindowHits [ iShrink ].m_uQuerypos ]!=1 )
	{
		m_dMinWindowCounts [ m_dMinWindowHits [ iShrink ].m_uQuerypos ]--;
		iShrink++;
	}

	int iNewLen = m_dMinWindowHits.GetLength() - iShrink;
	memmove ( m_dMinWindowHits.Begin(), &m_dMinWindowHits[iShrink], iNewLen*sizeof(LeanHit_t) );
	m_dMinWindowHits.Resize ( iNewLen );

	int iNewGaps = HITMAN::GetPos ( pHlist->m_uHitpos ) - HITMAN::GetPos ( m_dMinWindowHits[0].m_uHitpos ) - m_iMinWindowWords + 1;
	m_iMinGaps[iField] = Min ( m_iMinGaps[iField], iNewGaps );
}


template<bool A1, bool A2>
int RankerState_Expr_fn<A1,A2>::GetMaxPackedLength()
{
	return sizeof(DWORD)*( 8 + m_tExactHit.GetSize() + m_tExactOrder.GetSize() + m_iFields*15 + m_iMaxQpos*4 + m_dFieldTF.GetLength() );
}


template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
BYTE * RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::PackFactors()
{
	DWORD * pPackStart = (DWORD *)m_tFactorPool.Alloc();
	DWORD * pPack = pPackStart;
	assert ( pPackStart );

	// leave space for size
	pPack++;
	assert ( m_tMatchedFields.GetSize()==m_tExactHit.GetSize() && m_tExactHit.GetSize()==m_tExactOrder.GetSize() );

	// document level factors
	*pPack++ = m_uDocBM25;
	*pPack++ = sphF2DW ( m_fDocBM25A );
	*pPack++ = *m_tMatchedFields.Begin();
	*pPack++ = m_uDocWordCount;

	// field level factors
	*pPack++ = (DWORD)m_iFields;
	// v.6 set these depends on number of fields
	for ( int i=0; i<m_tExactHit.GetSize(); i++ )
		*pPack++ = *( m_tExactHit.Begin() + i );
	for ( int i=0; i<m_tExactOrder.GetSize(); i++ )
		*pPack++ = *( m_tExactOrder.Begin() + i );

	for ( int i=0; i<m_iFields; i++ )
	{
		DWORD uHit = m_uHitCount[i];
		*pPack++ = uHit;
		if ( uHit )
		{
			*pPack++ = (DWORD)i;
			*pPack++ = m_uLCS[i];
			*pPack++ = m_uWordCount[i];
			*pPack++ = sphF2DW ( m_dTFIDF[i] );
			*pPack++ = sphF2DW ( m_dMinIDF[i] );
			*pPack++ = sphF2DW ( m_dMaxIDF[i] );
			*pPack++ = sphF2DW ( m_dSumIDF[i] );
			*pPack++ = (DWORD)m_iMinHitPos[i];
			*pPack++ = (DWORD)m_iMinBestSpanPos[i];
			// had exact_hit here before v.4
			*pPack++ = (DWORD)m_iMaxWindowHits[i];
			*pPack++ = (DWORD)m_iMinGaps[i]; // added in v.3
			*pPack++ = sphF2DW ( m_dAtc[i] );			// added in v.4
			*pPack++ = m_dLCCS[i];					// added in v.5
			*pPack++ = sphF2DW ( m_dWLCCS[i] );	// added in v.5
		}
	}

	// word level factors
	*pPack++ = (DWORD)m_iMaxQpos;
	for ( int i=1; i<=m_iMaxQpos; i++ )
	{
		DWORD uKeywordMask = !IsTermSkipped(i); // !COMMIT !m_tExcluded.BitGet(i);
		*pPack++ = uKeywordMask;
		if ( uKeywordMask )
		{
			*pPack++ = (DWORD)i;
			*pPack++ = (DWORD)m_dTF[i];
			*pPack++ = *(DWORD*)&m_dIDF[i];
		}
	}

	// m_dFieldTF = iWord + iField * ( 1 + iWordsCount )
	// FIXME! pack these sparse factors ( however these should fit into fixed-size FactorPool block )
	*pPack++ = m_dFieldTF.GetLength();
	memcpy ( pPack, m_dFieldTF.Begin(), m_dFieldTF.GetLength()*sizeof(m_dFieldTF[0]) );
	pPack += m_dFieldTF.GetLength();

	*pPackStart = (pPack-pPackStart)*sizeof(DWORD);
	assert ( (pPack-pPackStart)*sizeof(DWORD)<=(DWORD)m_tFactorPool.GetElementSize() );
	return (BYTE*)pPackStart;
}


template <bool NEED_PACKEDFACTORS, bool HANDLE_DUPES>
bool RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::ExtraDataImpl ( ExtraData_e eType, void ** ppResult )
{
	if_const ( eType==EXTRA_SET_MVAPOOL || eType==EXTRA_SET_STRINGPOOL || NEED_PACKEDFACTORS )
	{
		switch ( eType )
		{
			case EXTRA_SET_MVAPOOL:
				m_pExpr->Command ( SPH_EXPR_SET_MVA_POOL, ppResult );
				return true;
			case EXTRA_SET_STRINGPOOL:
				m_pExpr->Command ( SPH_EXPR_SET_STRING_POOL, ppResult );
				return true;
			case EXTRA_SET_POOL_CAPACITY:
				m_iPoolMatchCapacity = *(int*)ppResult;
				m_iPoolMatchCapacity += ExtNode_i::MAX_DOCS;
				return true;
			case EXTRA_SET_MATCHPUSHED:
				m_tFactorPool.AddRef ( *(SphDocID_t*)ppResult );
				return true;
			case EXTRA_SET_MATCHPOPPED:
				{
					const CSphTightVector<SphDocID_t> & dReleased = *(CSphTightVector<SphDocID_t>*)ppResult;
					ARRAY_FOREACH ( i, dReleased )
						m_tFactorPool.Release ( dReleased[i] );
				}
				return true;
			case EXTRA_GET_DATA_PACKEDFACTORS:
				*ppResult = m_tFactorPool.GetHashPtr();
				return true;
			case EXTRA_GET_DATA_RANKER_STATE:
				{
					SphExtraDataRankerState_t * pState = (SphExtraDataRankerState_t *)ppResult;
					pState->m_iFields = m_iFields;
					pState->m_pSchema = m_pSchema;
					pState->m_pFieldLens = m_pFieldLens;
					pState->m_iTotalDocuments = m_iTotalDocuments;
					pState->m_tFieldLensLoc = m_tFieldLensLoc;
					pState->m_iMaxQpos = m_iMaxQpos;
				}
				return true;
			case EXTRA_GET_POOL_SIZE:
				if_const ( NEED_PACKEDFACTORS )
				{
					*(int64_t*)ppResult = (int64_t)GetMaxPackedLength() * m_iPoolMatchCapacity;
					return true;
				} else
					return false;
			default:
				return false;
		}
	} else
		return false;
}


/// finish document processing, compute weight from factors
template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
DWORD RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::Finalize ( const CSphMatch & tMatch )
{
#ifndef NDEBUG
	// sanity check
	for ( int i=0; i<m_iFields; ++i )
	{
		assert ( m_iMinHitPos[i]<=m_iMinBestSpanPos[i] );
		if ( m_uLCS[i]==1 )
			assert ( m_iMinHitPos[i]==m_iMinBestSpanPos[i] );
	}
#endif // NDEBUG

	// finishing touches
	FinalizeDocFactors ( tMatch );
	UpdateATC ( true );

	if_const ( NEED_PACKEDFACTORS )
	{
		// pack factors
		if ( !m_tFactorPool.IsInitialized() )
			m_tFactorPool.Prealloc ( GetMaxPackedLength(), m_iPoolMatchCapacity );
		m_tFactorPool.AddToHash ( tMatch.m_uDocID, PackFactors() );
	}

	// compute expression
	DWORD uRes = ( m_eExprType==ESphAttr::SPH_ATTR_INTEGER )
		? m_pExpr->IntEval ( tMatch )
		: (DWORD)m_pExpr->Eval ( tMatch );

	if_const ( HANDLE_DUPES )
	{
		m_uCurPos = 0;
		m_uLcsTailPos = 0;
		m_uLcsTailQposMask = 0;
		m_uCurQposMask = 0;
	}

	// cleanup
	ResetDocFactors();
	memset ( m_dLCCS, 0 , sizeof(m_dLCCS) );
	memset ( m_dWLCCS, 0, sizeof(m_dWLCCS) );
	m_iQueryPosLCCS = 0;
	m_iHitPosLCCS = 0;
	m_iLenLCCS = 0;
	m_fWeightLCCS = 0.0f;

	// done
	return uRes;
}


template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
bool RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::IsTermSkipped ( int iTerm )
{
	assert ( iTerm>=0 && iTerm<m_iMaxQpos+1 );
	if_const ( HANDLE_DUPES )
		return !m_tKeywords.BitGet ( iTerm ) || m_dTermDupes[iTerm]!=iTerm;
	else
		return !m_tKeywords.BitGet ( iTerm );
}


template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
float RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::TermTC ( int iTerm, bool bLeft )
{
	// border case short-cut
	if ( ( bLeft && iTerm==m_iAtcHitStart ) || ( !bLeft && iTerm==m_iAtcHitStart+m_iAtcHitCount-1 ) )
		return 0.0f;

	int iRing = iTerm % XRANK_ATC_BUFFER_LEN;
	int iHitpos = m_dAtcHits[iRing].m_iHitpos;
	WORD uQuerypos = m_dAtcHits[iRing].m_uQuerypos;

	m_dAtcProcessedTerms.Clear();

	float fTC = 0.0f;

	// loop bounds for down \ up climbing
	int iStart, iEnd, iStep;
	if ( bLeft )
	{
		iStart = iTerm - 1;
		iEnd = Max ( iStart - XRANK_ATC_WINDOW_LEN, m_iAtcHitStart-1 );
		iStep = -1;
	} else
	{
		iStart = iTerm + 1;
		iEnd = Min ( iStart + XRANK_ATC_WINDOW_LEN, m_iAtcHitStart + m_iAtcHitCount );
		iStep = 1;
	}

	int iFound = 0;
	for ( int i=iStart; i!=iEnd && iFound!=m_iMaxQpos; i+=iStep )
	{
		iRing = i % XRANK_ATC_BUFFER_LEN;
		const AtcHit_t & tCur = m_dAtcHits[iRing];
		bool bGotDup = ( uQuerypos==tCur.m_uQuerypos );

		if ( m_dAtcProcessedTerms.BitGet ( tCur.m_uQuerypos ) || iHitpos==tCur.m_iHitpos )
			continue;

		float fWeightedDist = (float)pow ( float ( abs ( iHitpos - tCur.m_iHitpos ) ), XRANK_ATC_EXP );
		float fTermTC = ( m_dIDF[tCur.m_uQuerypos] / fWeightedDist );
		if ( bGotDup )
			fTermTC *= XRANK_ATC_DUP_DIV;
		fTC += fTermTC;

		m_dAtcProcessedTerms.BitSet ( tCur.m_uQuerypos );
		iFound++;
	}

	return fTC;
}


template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
void RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES>::UpdateATC ( bool bFlushField )
{
	if ( !m_iAtcHitCount )
		return;

	int iWindowStart = m_iAtcHitStart + XRANK_ATC_WINDOW_LEN;
	int iWindowEnd = Min ( iWindowStart + XRANK_ATC_WINDOW_LEN, m_iAtcHitStart+m_iAtcHitCount );
	// border cases (hits: +below ATC window collected since start of buffer; +at the end of buffer and less then ATC window)
	if ( !m_bAtcHeadProcessed )
		iWindowStart = m_iAtcHitStart;
	if ( bFlushField )
		iWindowEnd = m_iAtcHitStart+m_iAtcHitCount;

	assert ( iWindowStart<iWindowEnd && iWindowStart>=m_iAtcHitStart && iWindowEnd<=m_iAtcHitStart+m_iAtcHitCount );
	// per term ATC
	// sigma(t' E query) ( idf(t') \ left_deltapos(t, t')^z + idf (t') \ right_deltapos(t,t')^z ) * ( t==t' ? 0.25 : 1 )
	for ( int iWinPos=iWindowStart; iWinPos<iWindowEnd; iWinPos++ )
	{
		float fTC = TermTC ( iWinPos, true ) + TermTC ( iWinPos, false );

		int iRing = iWinPos % XRANK_ATC_BUFFER_LEN;
		m_dAtcTerms [ m_dAtcHits[iRing].m_uQuerypos ] += fTC;
	}

	m_bAtcHeadProcessed = true;
	if ( bFlushField )
	{
		float fWeightedSum = 0.0f;
		ARRAY_FOREACH ( i, m_dAtcTerms )
		{
			fWeightedSum += m_dAtcTerms[i] * m_dIDF[i];
			m_dAtcTerms[i] = 0.0f;
		}

		m_dAtc[m_uAtcField] = (float)log ( 1.0f + fWeightedSum );
		m_iAtcHitStart = 0;
		m_iAtcHitCount = 0;
		m_bAtcHeadProcessed = false;
	}
}


/// expression ranker
template < bool NEED_PACKEDFACTORS, bool HANDLE_DUPES >
class ExtRanker_Expr_T : public ExtRanker_T< RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES> >
{
public:
	ExtRanker_Expr_T ( const XQQuery_t & tXQ, const ISphQwordSetup & tSetup, const char * sExpr, const CSphSchema & tSchema )
		: ExtRanker_T< RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES> > ( tXQ, tSetup )
	{
		// tricky bit, we stash the pointer to expr here, but it will be parsed
		// somewhat later during InitState() call, when IDFs etc are computed
		this->m_tState.m_sExpr = sExpr;
		this->m_tState.m_pSchema = &tSchema;
	}

	void SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
	{
		ExtRanker_T< RankerState_Expr_fn<NEED_PACKEDFACTORS, HANDLE_DUPES> >::SetQwordsIDF ( hQwords );
		this->m_tState.m_iMaxQpos = this->m_iMaxQpos;
		this->m_tState.SetQwords ( hQwords );
	}

	virtual int GetMatches ()
	{
		if_const ( NEED_PACKEDFACTORS )
			this->m_tState.FlushMatches ();

		return ExtRanker_T<RankerState_Expr_fn <NEED_PACKEDFACTORS, HANDLE_DUPES> >::GetMatches ();
	}

	virtual void SetTermDupes ( const ExtQwordsHash_t & hQwords, int iMaxQpos )
	{
		this->m_tState.SetTermDupes ( hQwords, iMaxQpos, this->m_pRoot );
	}
};

//////////////////////////////////////////////////////////////////////////
// EXPRESSION FACTORS EXPORT RANKER
//////////////////////////////////////////////////////////////////////////

/// ranker state that computes BM25 as weight, but also all known factors for export purposes
struct RankerState_Export_fn : public RankerState_Expr_fn<>
{
public:
	CSphOrderedHash < CSphString, SphDocID_t, IdentityHash_fn, 256 > m_hFactors;

public:
	DWORD Finalize ( const CSphMatch & tMatch )
	{
		// finalize factor computations
		FinalizeDocFactors ( tMatch );

		// build document level factors
		// FIXME? should we build query level factors too? max_lcs, query_word_count, etc
		const int MAX_STR_LEN = 1024;
		CSphVector<char> dVal;
		dVal.Resize ( MAX_STR_LEN );
		snprintf ( dVal.Begin(), dVal.GetLength(), "bm25=%d, bm25a=%f, field_mask=%d, doc_word_count=%d",
			m_uDocBM25, m_fDocBM25A, *m_tMatchedFields.Begin(), m_uDocWordCount );

		char sTmp[MAX_STR_LEN];

		// build field level factors
		for ( int i=0; i<m_iFields; i++ )
			if ( m_uHitCount[i] )
		{
			snprintf ( sTmp, MAX_STR_LEN, ", field%d="
				"(lcs=%d, hit_count=%d, word_count=%d, "
				"tf_idf=%f, min_idf=%f, max_idf=%f, sum_idf=%f, "
				"min_hit_pos=%d, min_best_span_pos=%d, exact_hit=%d, max_window_hits=%d)",
				i,
				m_uLCS[i], m_uHitCount[i], m_uWordCount[i],
				m_dTFIDF[i], m_dMinIDF[i], m_dMaxIDF[i], m_dSumIDF[i],
				m_iMinHitPos[i], m_iMinBestSpanPos[i], m_tExactHit.BitGet ( i ), m_iMaxWindowHits[i] );

			int iValLen = strlen ( dVal.Begin() );
			int iTotalLen = iValLen+strlen(sTmp);
			if ( dVal.GetLength() < iTotalLen+1 )
				dVal.Resize ( iTotalLen+1 );

			strcpy ( &(dVal[iValLen]), sTmp ); //NOLINT
		}

		// build word level factors
		for ( int i=1; i<=m_iMaxQpos; i++ )
			if ( !IsTermSkipped ( i ) )
		{
			snprintf ( sTmp, MAX_STR_LEN, ", word%d=(tf=%d, idf=%f)", i, m_dTF[i], m_dIDF[i] );
			int iValLen = strlen ( dVal.Begin() );
			int iTotalLen = iValLen+strlen(sTmp);
			if ( dVal.GetLength() < iTotalLen+1 )
				dVal.Resize ( iTotalLen+1 );

			strcpy ( &(dVal[iValLen]), sTmp ); //NOLINT
		}

		// export factors
		m_hFactors.Add ( dVal.Begin(), tMatch.m_uDocID );

		// compute sorting expression now
		DWORD uRes = ( m_eExprType==ESphAttr::SPH_ATTR_INTEGER )
			? m_pExpr->IntEval ( tMatch )
			: (DWORD)m_pExpr->Eval ( tMatch );

		// cleanup and return!
		ResetDocFactors();
		return uRes;
	}

	virtual bool ExtraDataImpl ( ExtraData_e eType, void ** ppResult )
	{
		if ( eType==EXTRA_GET_DATA_RANKFACTORS )
			*ppResult = &m_hFactors;
		return true;
	}
};

/// export ranker that emits BM25 as the weight, but computes and export all the factors
/// useful for research purposes, eg. exporting the data for machine learning
class ExtRanker_Export_c : public ExtRanker_T<RankerState_Export_fn>
{
public:
	ExtRanker_Export_c ( const XQQuery_t & tXQ, const ISphQwordSetup & tSetup, const char * sExpr, const CSphSchema & tSchema )
		: ExtRanker_T<RankerState_Export_fn> ( tXQ, tSetup )
	{
		m_tState.m_sExpr = sExpr;
		m_tState.m_pSchema = &tSchema;
	}

	void SetQwordsIDF ( const ExtQwordsHash_t & hQwords )
	{
		ExtRanker_T<RankerState_Export_fn>::SetQwordsIDF ( hQwords );
		m_tState.m_iMaxQpos = m_iMaxQpos;
		m_tState.SetQwords ( hQwords );
	}
};

//////////////////////////////////////////////////////////////////////////
// RANKER FACTORY
//////////////////////////////////////////////////////////////////////////

static void CheckQueryWord ( const char * szWord, CSphQueryResult * pResult, const CSphIndexSettings & tSettings )
{
	if ( ( !tSettings.m_iMinPrefixLen && !tSettings.m_iMinInfixLen ) || !szWord )
		return;

	int iLen = strlen ( szWord );
	bool bHeadStar = szWord[0]=='*';
	bool bTailStar = szWord[iLen-1]=='*';
	int iLenWOStars = iLen - ( bHeadStar ? 1 : 0 ) - ( bTailStar ? 1 : 0 );
	if ( bHeadStar || bTailStar )
	{
		if ( tSettings.m_iMinInfixLen > 0 && iLenWOStars < tSettings.m_iMinInfixLen )
			pResult->m_sWarning.SetSprintf ( "Query word length is less than min infix length. word: '%s' ", szWord );
		else
			if ( tSettings.m_iMinPrefixLen > 0 && iLenWOStars < tSettings.m_iMinPrefixLen )
				pResult->m_sWarning.SetSprintf ( "Query word length is less than min prefix length. word: '%s' ", szWord );
	}
}


static void CheckExtendedQuery ( const XQNode_t * pNode, CSphQueryResult * pResult, const CSphIndexSettings & tSettings )
{
	ARRAY_FOREACH ( i, pNode->m_dWords )
		CheckQueryWord ( pNode->m_dWords[i].m_sWord.cstr(), pResult, tSettings );

	ARRAY_FOREACH ( i, pNode->m_dChildren )
		CheckExtendedQuery ( pNode->m_dChildren[i], pResult, tSettings );
}


struct ExtQwordOrderbyQueryPos_t
{
	bool IsLess ( const ExtQword_t * pA, const ExtQword_t * pB ) const
	{
		return pA->m_iQueryPos<pB->m_iQueryPos;
	}
};


static bool HasQwordDupes ( XQNode_t * pNode, SmallStringHash_T<int> & hQwords )
{
	ARRAY_FOREACH ( i, pNode->m_dChildren )
		if ( HasQwordDupes ( pNode->m_dChildren[i], hQwords ) )
			return true;
	ARRAY_FOREACH ( i, pNode->m_dWords )
		if ( !hQwords.Add ( 1, pNode->m_dWords[i].m_sWord ) )
			return true;
	return false;
}


static bool HasQwordDupes ( XQNode_t * pNode )
{
	SmallStringHash_T<int> hQwords;
	return HasQwordDupes ( pNode, hQwords );
}


ISphRanker * sphCreateRanker ( const XQQuery_t & tXQ, const CSphQuery * pQuery, CSphQueryResult * pResult,
	const ISphQwordSetup & tTermSetup, const CSphQueryContext & tCtx, const ISphSchema & tSorterSchema )
{
	// shortcut
	const CSphIndex * pIndex = tTermSetup.m_pIndex;

	// check the keywords
	CheckExtendedQuery ( tXQ.m_pRoot, pResult, pIndex->GetSettings() );

	// fill payload mask
	DWORD uPayloadMask = 0;
	ARRAY_FOREACH ( i, pIndex->GetMatchSchema().m_dFields )
		uPayloadMask |= pIndex->GetMatchSchema().m_dFields[i].m_bPayload << i;

	bool bGotDupes = HasQwordDupes ( tXQ.m_pRoot );

	// can we serve this from cache?
	QcacheEntry_c * pCached = QcacheFind ( pIndex->GetIndexId(), *pQuery, tSorterSchema );
	if ( pCached )
		return QcacheRanker ( pCached, tTermSetup );
	SafeRelease ( pCached );

	// setup eval-tree
	ExtRanker_c * pRanker = NULL;
	switch ( pQuery->m_eRanker )
	{
		case SPH_RANK_PROXIMITY_BM25:
			if ( uPayloadMask )
				pRanker = new ExtRanker_T < RankerState_ProximityPayload_fn<true> > ( tXQ, tTermSetup );
			else if ( tXQ.m_bSingleWord )
				pRanker = new ExtRanker_WeightSum_c<WITH_BM25> ( tXQ, tTermSetup );
			else if ( bGotDupes )
				pRanker = new ExtRanker_T < RankerState_Proximity_fn<true,true> > ( tXQ, tTermSetup );
			else
				pRanker = new ExtRanker_T < RankerState_Proximity_fn<true,false> > ( tXQ, tTermSetup );
			break;
		case SPH_RANK_BM25:				pRanker = new ExtRanker_WeightSum_c<WITH_BM25> ( tXQ, tTermSetup ); break;
		case SPH_RANK_NONE:				pRanker = new ExtRanker_None_c ( tXQ, tTermSetup ); break;
		case SPH_RANK_WORDCOUNT:		pRanker = new ExtRanker_T < RankerState_Wordcount_fn > ( tXQ, tTermSetup ); break;
		case SPH_RANK_PROXIMITY:
			if ( tXQ.m_bSingleWord )
				pRanker = new ExtRanker_WeightSum_c<> ( tXQ, tTermSetup );
			else if ( bGotDupes )
				pRanker = new ExtRanker_T < RankerState_Proximity_fn<false,true> > ( tXQ, tTermSetup );
			else
				pRanker = new ExtRanker_T < RankerState_Proximity_fn<false,false> > ( tXQ, tTermSetup );
			break;
		case SPH_RANK_MATCHANY:			pRanker = new ExtRanker_T < RankerState_MatchAny_fn > ( tXQ, tTermSetup ); break;
		case SPH_RANK_FIELDMASK:		pRanker = new ExtRanker_T < RankerState_Fieldmask_fn > ( tXQ, tTermSetup ); break;
		case SPH_RANK_SPH04:			pRanker = new ExtRanker_T < RankerState_ProximityBM25Exact_fn > ( tXQ, tTermSetup ); break;
		case SPH_RANK_EXPR:
			{
				// we need that mask in case these factors usage:
				// min_idf,max_idf,sum_idf,hit_count,word_count,doc_word_count,tf_idf,tf,field_tf
				// however ranker expression got parsed later at Init stage
				// FIXME!!! move QposMask initialization past Init
				tTermSetup.m_bSetQposMask = true;
				bool bNeedFactors = !!(tCtx.m_uPackedFactorFlags & SPH_FACTOR_ENABLE);
				if ( bNeedFactors && bGotDupes )
					pRanker = new ExtRanker_Expr_T <true, true> ( tXQ, tTermSetup, pQuery->m_sRankerExpr.cstr(), pIndex->GetMatchSchema() );
				else if ( bNeedFactors && !bGotDupes )
					pRanker = new ExtRanker_Expr_T <true, false> ( tXQ, tTermSetup, pQuery->m_sRankerExpr.cstr(), pIndex->GetMatchSchema() );
				else if ( !bNeedFactors && bGotDupes )
					pRanker = new ExtRanker_Expr_T <false, true> ( tXQ, tTermSetup, pQuery->m_sRankerExpr.cstr(), pIndex->GetMatchSchema() );
				else if ( !bNeedFactors && !bGotDupes )
					pRanker = new ExtRanker_Expr_T <false, false> ( tXQ, tTermSetup, pQuery->m_sRankerExpr.cstr(), pIndex->GetMatchSchema() );
			}
			break;

		case SPH_RANK_EXPORT:
			// TODO: replace Export ranker to Expression ranker to remove duplicated code
			tTermSetup.m_bSetQposMask = true;
			pRanker = new ExtRanker_Export_c ( tXQ, tTermSetup, pQuery->m_sRankerExpr.cstr(), pIndex->GetMatchSchema() );
			break;

		default:
			pResult->m_sWarning.SetSprintf ( "unknown ranking mode %d; using default", (int)pQuery->m_eRanker );
			if ( bGotDupes )
				pRanker = new ExtRanker_T < RankerState_Proximity_fn<true,true> > ( tXQ, tTermSetup );
			else
				pRanker = new ExtRanker_T < RankerState_Proximity_fn<true,false> > ( tXQ, tTermSetup );
			break;

		case SPH_RANK_PLUGIN:
			{
				const PluginRanker_c * p = (const PluginRanker_c *) sphPluginGet ( PLUGIN_RANKER, pQuery->m_sUDRanker.cstr() );
				// might be a case for query to distributed index
				if ( p )
				{
					pRanker = new ExtRanker_T < RankerState_Plugin_fn > ( tXQ, tTermSetup );
					pRanker->ExtraData ( EXTRA_SET_RANKER_PLUGIN, (void**)p );
					pRanker->ExtraData ( EXTRA_SET_RANKER_PLUGIN_OPTS, (void**)pQuery->m_sUDRankerOpts.cstr() );
				} else
				{
					// create default ranker in case of missed plugin
					pResult->m_sWarning.SetSprintf ( "unknown ranker plugin '%s'; using default", pQuery->m_sUDRanker.cstr() );
					if ( bGotDupes )
						pRanker = new ExtRanker_T < RankerState_Proximity_fn<true,true> > ( tXQ, tTermSetup );
					else
						pRanker = new ExtRanker_T < RankerState_Proximity_fn<true,false> > ( tXQ, tTermSetup );
				}
			}
			break;
	}
	assert ( pRanker );
	pRanker->m_uPayloadMask = uPayloadMask;

	// setup IDFs
	ExtQwordsHash_t hQwords;
	int iMaxQpos = pRanker->GetQwords ( hQwords );

	const int iQwords = hQwords.GetLength ();
	int64_t iTotalDocuments = pIndex->GetStats().m_iTotalDocuments;
	if ( tCtx.m_pLocalDocs )
		iTotalDocuments = tCtx.m_iTotalDocs;

	CSphVector<const ExtQword_t *> dWords;
	dWords.Reserve ( hQwords.GetLength() );

	hQwords.IterateStart ();
	while ( hQwords.IterateNext() )
	{
		ExtQword_t & tWord = hQwords.IterateGet ();

		int64_t iTermDocs = tWord.m_iDocs;
		// shared docs count
		if ( tCtx.m_pLocalDocs )
		{
			int64_t * pDocs = (*tCtx.m_pLocalDocs)( tWord.m_sWord );
			if ( pDocs )
				iTermDocs = *pDocs;
		}

		// build IDF
		float fIDF = 0.0f;
		if ( pQuery->m_bGlobalIDF )
			fIDF = pIndex->GetGlobalIDF ( tWord.m_sWord, iTermDocs, pQuery->m_bPlainIDF );
		else if ( iTermDocs )
		{
			// (word_docs > total_docs) case *is* occasionally possible
			// because of dupes, or delayed purging in RT, etc
			// FIXME? we don't expect over 4G docs per just 1 local index
			const int64_t iTotalClamped = Max ( iTotalDocuments, iTermDocs );

			if ( !pQuery->m_bPlainIDF )
			{
				// bm25 variant, idf = log((N-n+1)/n), as per Robertson et al
				//
				// idf \in [-log(N), log(N)]
				// weight \in [-NumWords*log(N), NumWords*log(N)]
				// we want weight \in [0, 1] range
				// we prescale idfs and get weight \in [-0.5, 0.5] range
				// then add 0.5 as our final step
				//
				// for the record, idf = log((N-n+0.5)/(n+0.5)) in the original paper
				// but our variant is a bit easier to compute, and has a better (symmetric) range
				float fLogTotal = logf ( float ( 1+iTotalClamped ) );
				fIDF = logf ( float ( iTotalClamped-iTermDocs+1 ) / float ( iTermDocs ) )
					/ ( 2*fLogTotal );
			} else
			{
				// plain variant, idf=log(N/n), as per Sparck-Jones
				//
				// idf \in [0, log(N)]
				// weight \in [0, NumWords*log(N)]
				// we prescale idfs and get weight in [0, 0.5] range
				// then add 0.5 as our final step
				float fLogTotal = logf ( float ( 1+iTotalClamped ) );
				fIDF = logf ( float ( iTotalClamped ) / float ( iTermDocs ) )
					/ ( 2*fLogTotal );
			}
		}

		// optionally normalize IDFs so that sum(TF*IDF) fits into [0, 1]
		if ( pQuery->m_bNormalizedTFIDF )
			fIDF /= iQwords;

		tWord.m_fIDF = fIDF * tWord.m_fBoost;
		dWords.Add ( &tWord );
	}

	dWords.Sort ( ExtQwordOrderbyQueryPos_t() );
	ARRAY_FOREACH ( i, dWords )
	{
		const ExtQword_t * pWord = dWords[i];
		if ( !pWord->m_bExpanded )
			pResult->AddStat ( pWord->m_sDictWord, pWord->m_iDocs, pWord->m_iHits );
	}

	pRanker->m_iMaxQpos = iMaxQpos;
	pRanker->SetQwordsIDF ( hQwords );
	if ( bGotDupes )
		pRanker->SetTermDupes ( hQwords, iMaxQpos );
	if ( !pRanker->InitState ( tCtx, pResult->m_sError ) )
		SafeDelete ( pRanker );
	return pRanker;
}

//////////////////////////////////////////////////////////////////////////
/// HIT MARKER
//////////////////////////////////////////////////////////////////////////

void CSphHitMarker::Mark ( CSphVector<SphHitMark_t> & dMarked )
{
	if ( !m_pRoot )
		return;

	const ExtHit_t * pHits = NULL;
	const ExtDoc_t * pDocs = NULL;

	pDocs = m_pRoot->GetDocsChunk();
	if ( !pDocs )
		return;

	for ( ;; )
	{
		pHits = m_pRoot->GetHitsChunk ( pDocs );
		if ( !pHits )
			break;

		for ( ; pHits->m_uDocid!=DOCID_MAX; pHits++ )
		{
			SphHitMark_t tMark;
			tMark.m_uPosition = HITMAN::GetPos ( pHits->m_uHitpos );
			tMark.m_uSpan = pHits->m_uMatchlen;

			dMarked.Add ( tMark );
		}
	}
}


CSphHitMarker::~CSphHitMarker ()
{
	SafeDelete ( m_pRoot );
}


CSphHitMarker * CSphHitMarker::Create ( const XQNode_t * pRoot, const ISphQwordSetup & tSetup )
{
	ExtNode_i * pNode = NULL;
	if ( pRoot )
		pNode = ExtNode_i::Create ( pRoot, tSetup );

	if ( !pRoot || pNode )
	{
		CSphHitMarker * pMarker = new CSphHitMarker;
		pMarker->m_pRoot = pNode;
		return pMarker;
	}
	return NULL;
}

}


//
// $Id$
//
