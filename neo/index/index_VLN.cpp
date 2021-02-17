#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/int/queue.h"
#include "neo/int/query_filter.h"
#include "neo/index/index_VLN.h"
#include "neo/index/enums.h"
#include "neo/dict/dict_exact.h"
#include "neo/dict/dict_header.h"
#include "neo/dict/dict_reader.h"
#include "neo/dict/dict_star.h"
#include "neo/core/version.h"
#include "neo/core/build_header.h"
#include "neo/core/arena.h"
#include "neo/core/hit_builder.h"
#include "neo/core/ranker.h"
#include "neo/core/build_header.h"
#include "neo/core/skip_list.h"
#include "neo/core/die.h"
#include "neo/core/merger.h"
#include "neo/tokenizer/tokenizer_settings.h"
#include "neo/query/filter_settings.h"
#include "neo/query/node_cache.h"
#include "neo/query/attr_update.h"
#include "neo/query/disk_misc.h"
#include "neo/query/get_keyword_settings.h"
#include "neo/query/sort_docinfo.h"
#include "neo/query/match_sorter.h"
#include "neo/tools/docinfo_transformer.h"
#include "neo/platform/random.h"
#include "neo/io/io_stats.h"
#include "neo/io/binlog.h"
#include "neo/int/throttle_state.h"



#include "neo/sphinxint.h"
#include "neo/sphinxexpr.h"
#include "neo/sphinx/xjson.h"
#include "neo/sphinx/xqcache.h"
#include "neo/sphinx/xrlp.h"
#include "neo/sphinx/xquery.h"
#include "neo/sphinx/xfilter.h"
#include "neo/sphinx/xsearch.h"


namespace NEO {

volatile int CSphIndex_VLN::m_iIndexTagSeq = 0;

CSphString CSphIndex_VLN::GetIndexFileName ( const char * sExt ) const
{
	CSphString sRes;
	sRes.SetSprintf ( "%s.%s", m_sFilename.cstr(), sExt );
	return sRes;
}



bool CSphIndex_VLN::WriteHeader ( const BuildHeader_t & tBuildHeader, CSphWriter & fdInfo ) const
{
	// version
	fdInfo.PutDword ( INDEX_MAGIC_HEADER );
	fdInfo.PutDword ( INDEX_FORMAT_VERSION );

	// bits
	fdInfo.PutDword ( USE_64BIT );

	// docinfo
	fdInfo.PutDword ( m_tSettings.m_eDocinfo );

	// schema
	WriteSchema ( fdInfo, m_tSchema );

	// min doc
	fdInfo.PutOffset ( tBuildHeader.m_uMinDocid ); // was dword in v.1
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
		fdInfo.PutBytes ( tBuildHeader.m_pMinRow, m_tSchema.GetRowSize()*sizeof(CSphRowitem) );

	// wordlist checkpoints
	fdInfo.PutOffset ( tBuildHeader.m_iDictCheckpointsOffset );
	fdInfo.PutDword ( tBuildHeader.m_iDictCheckpoints );
	fdInfo.PutByte ( tBuildHeader.m_iInfixCodepointBytes );
	fdInfo.PutDword ( (DWORD)tBuildHeader.m_iInfixBlocksOffset );
	fdInfo.PutDword ( tBuildHeader.m_iInfixBlocksWordsSize );

	// index stats
	fdInfo.PutDword ( (DWORD)tBuildHeader.m_iTotalDocuments ); // FIXME? we don't expect over 4G docs per just 1 local index
	fdInfo.PutOffset ( tBuildHeader.m_iTotalBytes );
	fdInfo.PutDword ( tBuildHeader.m_iTotalDups );

	// index settings
	SaveIndexSettings ( fdInfo, m_tSettings );

	// tokenizer info
	assert ( m_pTokenizer );
	SaveTokenizerSettings ( fdInfo, m_pTokenizer, m_tSettings.m_iEmbeddedLimit );

	// dictionary info
	assert ( m_pDict );
	SaveDictionarySettings ( fdInfo, m_pDict, false, m_tSettings.m_iEmbeddedLimit );

	fdInfo.PutDword ( tBuildHeader.m_uKillListSize );
	fdInfo.PutOffset ( tBuildHeader.m_iMinMaxIndex );

	// field filter info
	SaveFieldFilterSettings ( fdInfo, m_pFieldFilter );

	// average field lengths
	if ( m_tSettings.m_bIndexFieldLens )
		ARRAY_FOREACH ( i, m_tSchema.m_dFields )
			fdInfo.PutOffset ( m_dFieldLens[i] );

	return true;
}


bool CSphIndex_VLN::BuildDone ( const BuildHeader_t & tBuildHeader, CSphString & sError ) const
{
	CSphWriter fdInfo;
	fdInfo.SetThrottle ( tBuildHeader.m_pThrottle );
	fdInfo.OpenFile ( GetIndexFileName ( tBuildHeader.m_sHeaderExtension ), sError );
	if ( fdInfo.IsError() )
		return false;

	if ( !WriteHeader ( tBuildHeader, fdInfo ) )
		return false;

	// close header
	fdInfo.CloseFile ();
	return !fdInfo.IsError();
}


CSphIndex_VLN::CSphIndex_VLN ( const char* sIndexName, const char * sFilename )
	: CSphIndex ( sIndexName, sFilename )
	, m_iLockFD ( -1 )
	, m_iTotalDups ( 0 )
	, m_dMinRow ( 0 )
	, m_dFieldLens ( SPH_MAX_FIELDS )
{
	m_sFilename = sFilename;

	m_iDocinfo = 0;
	m_iDocinfoIndex = 0;
	m_pDocinfoIndex = NULL;

	m_bMlock = false;
	m_bOndiskAllAttr = false;
	m_bOndiskPoolAttr = false;
	m_bArenaProhibit = false;
	m_uVersion = INDEX_FORMAT_VERSION;
	m_bPassedRead = false;
	m_bPassedAlloc = false;
	m_bIsEmpty = true;
	m_bHaveSkips = false;
	m_bDebugCheck = false;
	m_uAttrsStatus = 0;

	m_iMinMaxIndex = 0;
	m_iIndexTag = -1;
	m_uMinDocid = 0;

	ARRAY_FOREACH ( i, m_dFieldLens )
		m_dFieldLens[i] = 0;
}


CSphIndex_VLN::~CSphIndex_VLN ()
{
	if ( m_iIndexTag>=0 && g_pMvaArena )
		g_tMvaArena.TaggedFreeTag ( m_iIndexTag );

	Unlock();
}



int CSphIndex_VLN::UpdateAttributes ( const CSphAttrUpdate & tUpd, int iIndex, CSphString & sError, CSphString & sWarning )
{
	// check if we can
	if ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN )
	{
		sError.SetSprintf ( "docinfo=extern required for updates" );
		return -1;
	}

	assert ( tUpd.m_dDocids.GetLength()==tUpd.m_dRows.GetLength() );
	assert ( tUpd.m_dDocids.GetLength()==tUpd.m_dRowOffset.GetLength() );
	DWORD uRows = tUpd.m_dDocids.GetLength();

	// check if we have to
	if ( !m_iDocinfo || !uRows )
		return 0;

	// remap update schema to index schema
	int iUpdLen = tUpd.m_dAttrs.GetLength();
	CSphVector<CSphAttrLocator> dLocators ( iUpdLen );
	CSphBitvec dFloats ( iUpdLen );
	CSphBitvec dBigints ( iUpdLen );
	CSphBitvec dDoubles ( iUpdLen );
	CSphBitvec dJsonFields ( iUpdLen );
	CSphBitvec dBigint2Float ( iUpdLen );
	CSphBitvec dFloat2Bigint ( iUpdLen );
	CSphVector < CSphRefcountedPtr<ISphExpr> > dExpr ( iUpdLen );
	memset ( dLocators.Begin(), 0, dLocators.GetSizeBytes() );

	uint64_t uDst64 = 0;
	ARRAY_FOREACH ( i, tUpd.m_dAttrs )
	{
		int iIdx = m_tSchema.GetAttrIndex ( tUpd.m_dAttrs[i] );

		if ( iIdx<0 )
		{
			CSphString sJsonCol, sJsonKey;
			if ( sphJsonNameSplit ( tUpd.m_dAttrs[i], &sJsonCol, &sJsonKey ) )
			{
				iIdx = m_tSchema.GetAttrIndex ( sJsonCol.cstr() );
				if ( iIdx>=0 )
					dExpr[i] = sphExprParse ( tUpd.m_dAttrs[i], m_tSchema, NULL, NULL, sError, NULL );
			}
		}

		if ( iIdx>=0 )
		{
			// forbid updates on non-int columns
			const CSphColumnInfo & tCol = m_tSchema.GetAttr(iIdx);
			if ( !( tCol.m_eAttrType==ESphAttr::SPH_ATTR_BOOL || tCol.m_eAttrType==ESphAttr::SPH_ATTR_INTEGER || tCol.m_eAttrType==ESphAttr::SPH_ATTR_TIMESTAMP
				|| tCol.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET
				|| tCol.m_eAttrType==ESphAttr::SPH_ATTR_BIGINT || tCol.m_eAttrType==ESphAttr::SPH_ATTR_FLOAT || tCol.m_eAttrType==ESphAttr::SPH_ATTR_JSON ))
			{
				sError.SetSprintf ( "attribute '%s' can not be updated "
					"(must be boolean, integer, bigint, float, timestamp, MVA or JSON)",
					tUpd.m_dAttrs[i] );
				return -1;
			}

			// forbid updates on MVA columns if there's no arena
			if ( ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET ) && !g_pMvaArena )
			{
				sError.SetSprintf ( "MVA attribute '%s' can not be updated (MVA arena not initialized)", tCol.m_sName.cstr() );
				return -1;
			}
			if ( ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET ) && m_bArenaProhibit )
			{
				sError.SetSprintf ( "MVA attribute '%s' can not be updated (already so many MVA " INT64_FMT ", should be less %d)",
					tCol.m_sName.cstr(), m_tMva.GetNumEntries(), INT_MAX );
				return -1;
			}

			bool bSrcMva = ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET );
			bool bDstMva = ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_UINT32SET || tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_INT64SET );
			if ( bSrcMva!=bDstMva )
			{
				sError.SetSprintf ( "attribute '%s' MVA flag mismatch", tUpd.m_dAttrs[i] );
				return -1;
			}

			if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET && tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_INT64SET )
			{
				sError.SetSprintf ( "attribute '%s' MVA bits (dst=%d, src=%d) mismatch", tUpd.m_dAttrs[i],
					tCol.m_eAttrType, tUpd.m_dTypes[i] );
				return -1;
			}

			if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
				uDst64 |= ( U64C(1)<<i );

			if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_FLOAT )
			{
				if ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_BIGINT )
					dBigint2Float.BitSet(i);

				dFloats.BitSet(i);
			} else if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_JSON )
				dJsonFields.BitSet(i);
			else if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_BIGINT )
			{
				if ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_FLOAT )
					dFloat2Bigint.BitSet(i);
			}

			dLocators[i] = ( tCol.m_tLocator );
		} else if ( tUpd.m_bIgnoreNonexistent )
		{
			continue;
		} else
		{
			sError.SetSprintf ( "attribute '%s' not found", tUpd.m_dAttrs[i] );
			return -1;
		}

		// this is a hack
		// Query parser tries to detect an attribute type. And this is wrong because, we should
		// take attribute type from schema. Probably we'll rewrite updates in future but
		// for now this fix just works.
		// Fixes cases like UPDATE float_attr=1 WHERE id=1;
		assert ( iIdx>=0 );
		if ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_INTEGER && m_tSchema.GetAttr(iIdx).m_eAttrType==ESphAttr::SPH_ATTR_FLOAT )
		{
			const_cast<CSphAttrUpdate &>(tUpd).m_dTypes[i] = ESphAttr::SPH_ATTR_FLOAT;
			const_cast<CSphAttrUpdate &>(tUpd).m_dPool[i] = sphF2DW ( (float)tUpd.m_dPool[i] );
		}

		if ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_BIGINT )
			dBigints.BitSet(i);
		else if ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_FLOAT )
			dDoubles.BitSet(i);
	}

	// FIXME! FIXME! FIXME! overwriting just-freed blocks might hurt concurrent searchers;
	// should implement a simplistic MVCC-style delayed-free to avoid that

	// do the update
	const int iFirst = ( iIndex<0 ) ? 0 : iIndex;
	const int iLast = ( iIndex<0 ) ? uRows : iIndex+1;

	// first pass, if needed
	if ( tUpd.m_bStrict )
	{
		for ( int iUpd=iFirst; iUpd<iLast; iUpd++ )
		{
			const DWORD * pEntry = ( tUpd.m_dRows[iUpd] ? tUpd.m_dRows[iUpd] : FindDocinfo ( tUpd.m_dDocids[iUpd] ) );
			if ( !pEntry )
				continue; // no such id

			// raw row might be from RT (another RAM segment or disk chunk)
			const DWORD * pRows = m_tAttr.GetWritePtr();
			const DWORD * pRowsEnd = pRows + m_tAttr.GetNumEntries();
			bool bValidRow = ( pRows<=pEntry && pEntry<pRowsEnd );
			if ( !bValidRow )
				continue;

			pEntry = DOCINFO2ATTRS(pEntry);
			int iPos = tUpd.m_dRowOffset[iUpd];
			ARRAY_FOREACH ( iCol, tUpd.m_dAttrs )
				if ( dJsonFields.BitGet ( iCol ) )
				{
					ESphJsonType eType = dDoubles.BitGet ( iCol )
						? JSON_DOUBLE
						: ( dBigints.BitGet ( iCol ) ? JSON_INT64 : JSON_INT32 );

					SphAttr_t uValue = dDoubles.BitGet ( iCol )
						? sphD2QW ( (double)sphDW2F ( tUpd.m_dPool[iPos] ) )
						: dBigints.BitGet ( iCol ) ? MVA_UPSIZE ( &tUpd.m_dPool[iPos] ) : tUpd.m_dPool[iPos];

					if ( !sphJsonInplaceUpdate ( eType, uValue, dExpr[iCol].Ptr(), m_tString.GetWritePtr(), pEntry, false ) )
					{
						sError.SetSprintf ( "attribute '%s' can not be updated (not found or incompatible types)", tUpd.m_dAttrs[iCol] );
						return -1;
					}

					iPos += dBigints.BitGet ( iCol ) ? 2 : 1;
				}
		}
	}

	// row update must leave it in cosistent state; so let's preallocate all the needed MVA
	// storage upfront to avoid suddenly having to rollback if allocation fails later
	int iNumMVA = 0;
	ARRAY_FOREACH ( i, tUpd.m_dAttrs )
		if ( tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_UINT32SET || tUpd.m_dTypes[i]==ESphAttr::SPH_ATTR_INT64SET )
			iNumMVA++;

	// OPTIMIZE! execute the code below conditionally
	CSphVector<DWORD*> dRowPtrs;
	CSphVector<int> dMvaPtrs;

	dRowPtrs.Resize ( uRows );
	dMvaPtrs.Resize ( uRows*iNumMVA );
	dMvaPtrs.Fill ( -1 );

	// preallocate
	bool bFailed = false;
	for ( int iUpd=iFirst; iUpd<iLast && !bFailed; iUpd++ )
	{
		dRowPtrs[iUpd] = NULL;
		DWORD * pEntry = const_cast < DWORD * > ( tUpd.m_dRows[iUpd] ? tUpd.m_dRows[iUpd] : FindDocinfo ( tUpd.m_dDocids[iUpd] ) );
		if ( !pEntry )
			continue; // no such id

		// raw row might be from RT (another RAM segment or disk chunk) or another index from same update query
		const DWORD * pRows = m_tAttr.GetWritePtr();
		const DWORD * pRowsEnd = pRows + m_tAttr.GetNumEntries();
		bool bValidRow = ( pRows<=pEntry && pEntry<pRowsEnd );
		if ( !bValidRow )
			continue;

		dRowPtrs[iUpd] = pEntry;

		int iPoolPos = tUpd.m_dRowOffset[iUpd];
		int iMvaPtr = iUpd*iNumMVA;
		ARRAY_FOREACH_COND ( iCol, tUpd.m_dAttrs, !bFailed )
		{
			bool bSrcMva32 = ( tUpd.m_dTypes[iCol]==ESphAttr::SPH_ATTR_UINT32SET );
			bool bSrcMva64 = ( tUpd.m_dTypes[iCol]==ESphAttr::SPH_ATTR_INT64SET );
			if (!( bSrcMva32 || bSrcMva64 )) // FIXME! optimize using a prebuilt dword mask?
			{
				iPoolPos++;
				if ( dBigints.BitGet ( iCol ) )
					iPoolPos++;
				continue;
			}

			// get the requested new count
			int iNewCount = (int)tUpd.m_dPool[iPoolPos++];
			iPoolPos += iNewCount;

			// try to alloc
			int iAlloc = -1;
			if ( iNewCount )
			{
				bool bDst64 = ( uDst64 & ( U64C(1) << iCol ) )!=0;
				assert ( (iNewCount%2)==0 );
				int iLen = ( bDst64 ? iNewCount : iNewCount/2 );
				iAlloc = g_tMvaArena.TaggedAlloc ( m_iIndexTag, (1+iLen)*sizeof(DWORD)+sizeof(SphDocID_t) );
				if ( iAlloc<0 )
					bFailed = true;
			}

			// whatever the outcome, move the pointer
			dMvaPtrs[iMvaPtr++] = iAlloc;
		}
	}

	// if there were any allocation failures, rollback everything
	if ( bFailed )
	{
		ARRAY_FOREACH ( i, dMvaPtrs )
			if ( dMvaPtrs[i]>=0 )
				g_tMvaArena.TaggedFreeIndex ( m_iIndexTag, dMvaPtrs[i] );

		sError.SetSprintf ( "out of pool memory on MVA update" );
		return -1;
	}

	// preallocation went OK; do the actual update
	int iRowStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	int iUpdated = 0;
	DWORD uUpdateMask = 0;
	int iJsonWarnings = 0;

	for ( int iUpd=iFirst; iUpd<iLast; iUpd++ )
	{
		bool bUpdated = false;

		DWORD * pEntry = dRowPtrs[iUpd];
		if ( !pEntry )
			continue; // no such id

		int64_t iBlock = int64_t ( pEntry-m_tAttr.GetWritePtr() ) / ( iRowStride*DOCINFO_INDEX_FREQ );
		DWORD * pBlockRanges = m_pDocinfoIndex + ( iBlock * iRowStride * 2 );
		DWORD * pIndexRanges = m_pDocinfoIndex + ( m_iDocinfoIndex * iRowStride * 2 );
		assert ( iBlock>=0 && iBlock<m_iDocinfoIndex );

		pEntry = DOCINFO2ATTRS(pEntry);

		int iPos = tUpd.m_dRowOffset[iUpd];
		int iMvaPtr = iUpd*iNumMVA;
		ARRAY_FOREACH ( iCol, tUpd.m_dAttrs )
		{
			bool bSrcMva32 = ( tUpd.m_dTypes[iCol]==ESphAttr::SPH_ATTR_UINT32SET );
			bool bSrcMva64 = ( tUpd.m_dTypes[iCol]==ESphAttr::SPH_ATTR_INT64SET );
			bool bSrcJson = dJsonFields.BitGet ( iCol );
			if (!( bSrcMva32 || bSrcMva64 || bSrcJson )) // FIXME! optimize using a prebuilt dword mask?
			{
				// plain update
				SphAttr_t uValue = dBigints.BitGet ( iCol ) ? MVA_UPSIZE ( &tUpd.m_dPool[iPos] ) : tUpd.m_dPool[iPos];

				if ( dBigint2Float.BitGet(iCol) ) // handle bigint(-1) -> float attr updates
					uValue = sphF2DW ( float((int64_t)uValue) );
				else if ( dFloat2Bigint.BitGet(iCol) ) // handle float(1.0) -> bigint attr updates
					uValue = (int64_t)sphDW2F((DWORD)uValue);

				sphSetRowAttr ( pEntry, dLocators[iCol], uValue );

				// update block and index ranges
				for ( int i=0; i<2; i++ )
				{
					DWORD * pBlock = i ? pBlockRanges : pIndexRanges;
					SphAttr_t uMin = sphGetRowAttr ( DOCINFO2ATTRS ( pBlock ), dLocators[iCol] );
					SphAttr_t uMax = sphGetRowAttr ( DOCINFO2ATTRS ( pBlock+iRowStride ) , dLocators[iCol] );
					if ( dFloats.BitGet ( iCol ) ) // update float's indexes assumes float comparision
					{
						float fValue = sphDW2F ( (DWORD) uValue );
						float fMin = sphDW2F ( (DWORD) uMin );
						float fMax = sphDW2F ( (DWORD) uMax );
						if ( fValue<fMin )
							sphSetRowAttr ( DOCINFO2ATTRS ( pBlock ), dLocators[iCol], sphF2DW ( fValue ) );
						if ( fValue>fMax )
							sphSetRowAttr ( DOCINFO2ATTRS ( pBlock+iRowStride ), dLocators[iCol], sphF2DW ( fValue ) );
					} else // update usual integers
					{
						if ( uValue<uMin )
							sphSetRowAttr ( DOCINFO2ATTRS ( pBlock ), dLocators[iCol], uValue );
						if ( uValue>uMax )
							sphSetRowAttr ( DOCINFO2ATTRS ( pBlock+iRowStride ), dLocators[iCol], uValue );
					}
				}

				bUpdated = true;
				uUpdateMask |= ATTRS_UPDATED;

				// next
				iPos += dBigints.BitGet ( iCol ) ? 2 : 1;
				continue;
			}

			if ( bSrcJson )
			{
				ESphJsonType eType = dDoubles.BitGet ( iCol )
					? JSON_DOUBLE
					: ( dBigints.BitGet ( iCol ) ? JSON_INT64 : JSON_INT32 );

				SphAttr_t uValue = dDoubles.BitGet ( iCol )
					? sphD2QW ( (double)sphDW2F ( tUpd.m_dPool[iPos] ) )
					: dBigints.BitGet ( iCol ) ? MVA_UPSIZE ( &tUpd.m_dPool[iPos] ) : tUpd.m_dPool[iPos];

				if ( sphJsonInplaceUpdate ( eType, uValue, dExpr[iCol].Ptr(), m_tString.GetWritePtr(), pEntry, true ) )
				{
					bUpdated = true;
					uUpdateMask |= ATTRS_STRINGS_UPDATED;

				} else
					iJsonWarnings++;

				iPos += dBigints.BitGet ( iCol ) ? 2 : 1;
				continue;
			}

			// MVA update
			DWORD uOldIndex = MVA_DOWNSIZE ( sphGetRowAttr ( pEntry, dLocators[iCol] ) );

			// get new count, store new data if needed
			DWORD uNew = tUpd.m_dPool[iPos++];
			const DWORD * pSrc = tUpd.m_dPool.Begin() + iPos;
			iPos += uNew;

			int64_t iNewMin = LLONG_MAX, iNewMax = LLONG_MIN;
			int iNewIndex = dMvaPtrs[iMvaPtr++];
			if ( uNew )
			{
				assert ( iNewIndex>=0 );
				SphDocID_t* pDocid = (SphDocID_t *)(g_pMvaArena + iNewIndex);
				*pDocid++ = ( tUpd.m_dRows[iUpd] ? DOCINFO2ID ( tUpd.m_dRows[iUpd] ) : tUpd.m_dDocids[iUpd] );
				iNewIndex = (DWORD *)pDocid - g_pMvaArena;

				assert ( iNewIndex>=0 );
				DWORD * pDst = g_pMvaArena + iNewIndex;

				bool bDst64 = ( uDst64 & ( U64C(1) << iCol ) )!=0;
				assert ( ( uNew%2 )==0 );
				int iLen = ( bDst64 ? uNew : uNew/2 );
				// setup new value (flagged index) to store within row
				uNew = DWORD(iNewIndex) | MVA_ARENA_FLAG;

				// MVA values counter first
				*pDst++ = iLen;
				if ( bDst64 )
				{
					while ( iLen )
					{
						int64_t uValue = MVA_UPSIZE ( pSrc );
						iNewMin = Min ( iNewMin, uValue );
						iNewMax = Max ( iNewMax, uValue );
						*pDst++ = *pSrc++;
						*pDst++ = *pSrc++;
						iLen -= 2;
					}
				} else
				{
					while ( iLen-- )
					{
						DWORD uValue = *pSrc;
						pSrc += 2;
						*pDst++ = uValue;
						iNewMin = Min ( iNewMin, uValue );
						iNewMax = Max ( iNewMax, uValue );
					}
				}
			}

			// store new value
			sphSetRowAttr ( pEntry, dLocators[iCol], uNew );

			// update block and index ranges
			if ( uNew )
				for ( int i=0; i<2; i++ )
			{
				DWORD * pBlock = i ? pBlockRanges : pIndexRanges;
				int64_t iMin = sphGetRowAttr ( DOCINFO2ATTRS ( pBlock ), dLocators[iCol] );
				int64_t iMax = sphGetRowAttr ( DOCINFO2ATTRS ( pBlock+iRowStride ), dLocators[iCol] );
				if ( iNewMin<iMin || iNewMax>iMax )
				{
					sphSetRowAttr ( DOCINFO2ATTRS ( pBlock ), dLocators[iCol], Min ( iMin, iNewMin ) );
					sphSetRowAttr ( DOCINFO2ATTRS ( pBlock+iRowStride ), dLocators[iCol], Max ( iMax, iNewMax ) );
				}
			}

			// free old storage if needed
			if ( uOldIndex & MVA_ARENA_FLAG )
			{
				uOldIndex = ((DWORD*)((SphDocID_t*)(g_pMvaArena + (uOldIndex & MVA_OFFSET_MASK))-1))-g_pMvaArena;
				g_tMvaArena.TaggedFreeIndex ( m_iIndexTag, uOldIndex );
			}

			bUpdated = true;
			uUpdateMask |= ATTRS_MVA_UPDATED;
		}

		if ( bUpdated )
			iUpdated++;
	}

	if ( iJsonWarnings>0 )
	{
		sWarning.SetSprintf ( "%d attribute(s) can not be updated (not found or incompatible types)", iJsonWarnings );
		if ( iUpdated==0 )
		{
			sError = sWarning;
			return -1;
		}
	}

	if ( uUpdateMask && m_bBinlog && g_pBinlog )
		g_pBinlog->BinlogUpdateAttributes ( &m_iTID, m_sIndexName.cstr(), tUpd );

	m_uAttrsStatus |= uUpdateMask; // FIXME! add lock/atomic?

	return iUpdated;
}

bool CSphIndex_VLN::LoadPersistentMVA ( CSphString & sError )
{
	// prepare the file to load
	CSphAutoreader fdReader;
	if ( !fdReader.Open ( GetIndexFileName("mvp"), m_sLastError ) )
	{
		// no mvp means no saved attributes.
		m_sLastError = "";
		return true;
	}

	// check if we can
	if ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN )
	{
		sError.SetSprintf ( "docinfo=extern required for updates" );
		return false;
	}
	if ( m_bArenaProhibit )
	{
		sError.SetSprintf ( "MVA update disabled (already so many MVA " INT64_FMT ", should be less %d)", m_tMva.GetNumEntries(), INT_MAX );
		return false;
	}

	DWORD uDocs = fdReader.GetDword();

	// if we have docs to update
	if ( !uDocs )
		return false;

	CSphVector<SphDocID_t> dAffected ( uDocs );
	fdReader.GetBytes ( &dAffected[0], uDocs*sizeof(SphDocID_t) );

	// collect the indexes of MVA schema attributes
	CSphVector<CSphAttrLocator> dMvaLocators;
	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
		if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
			dMvaLocators.Add ( tAttr.m_tLocator );
	}
#ifndef NDEBUG
	int iMva64 = dMvaLocators.GetLength();
#endif
	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
		if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
			dMvaLocators.Add ( tAttr.m_tLocator );
	}
	assert ( dMvaLocators.GetLength()!=0 );

	if ( g_tMvaArena.GetError() ) // have to reset affected MVA in case of ( persistent MVA + no MVA arena )
	{
		ARRAY_FOREACH ( iDoc, dAffected )
		{
			DWORD * pDocinfo = const_cast<DWORD*> ( FindDocinfo ( dAffected[iDoc] ) );
			assert ( pDocinfo );
			DWORD * pAttrs = DOCINFO2ATTRS ( pDocinfo );
			ARRAY_FOREACH ( iMva, dMvaLocators )
			{
				// reset MVA from arena
				if ( MVA_DOWNSIZE ( sphGetRowAttr ( pAttrs, dMvaLocators[iMva] ) ) & MVA_ARENA_FLAG )
					sphSetRowAttr ( pAttrs, dMvaLocators[iMva], 0 );
			}
		}

		sphWarning ( "index '%s' forced to reset persistent MVAs ( %s )", m_sIndexName.cstr(), g_tMvaArena.GetError() );
		fdReader.Close();
		return true;
	}

	CSphVector<DWORD*> dRowPtrs ( uDocs );
	CSphVector<int> dAllocs;
	dAllocs.Reserve ( uDocs );

	// prealloc values (and also preload)
	bool bFailed = false;
	ARRAY_FOREACH ( i, dAffected )
	{
		DWORD* pDocinfo = const_cast<DWORD*> ( FindDocinfo ( dAffected[i] ) );
		assert ( pDocinfo );
		pDocinfo = DOCINFO2ATTRS ( pDocinfo );
		ARRAY_FOREACH_COND ( j, dMvaLocators, !bFailed )
		{
			// if this MVA was updated
			if ( MVA_DOWNSIZE ( sphGetRowAttr ( pDocinfo, dMvaLocators[j] ) ) & MVA_ARENA_FLAG )
			{
				DWORD uCount = fdReader.GetDword();
				if ( uCount )
				{
					assert ( j<iMva64 || ( uCount%2 )==0 );
					int iAlloc = g_tMvaArena.TaggedAlloc ( m_iIndexTag, (1+uCount)*sizeof(DWORD)+sizeof(SphDocID_t) );
					if ( iAlloc<0 )
						bFailed = true;
					else
					{
						SphDocID_t *pDocid = (SphDocID_t*)(g_pMvaArena + iAlloc);
						*pDocid++ = dAffected[i];
						DWORD * pData = (DWORD*)pDocid;
						*pData++ = uCount;
						fdReader.GetBytes ( pData, uCount*sizeof(DWORD) );
						dAllocs.Add ( iAlloc );
					}
				}
			}
		}
		if ( bFailed )
			break;
		dRowPtrs[i] = pDocinfo;
	}
	fdReader.Close();

	if ( bFailed )
	{
		ARRAY_FOREACH ( i, dAllocs )
			g_tMvaArena.TaggedFreeIndex ( m_iIndexTag, dAllocs[i] );

		sError.SetSprintf ( "out of pool memory on loading persistent MVA values" );
		return false;
	}

	// prealloc && load ok, fix the attributes now
	int iAllocIndex = 0;
	ARRAY_FOREACH ( i, dAffected )
	{
		DWORD* pDocinfo = dRowPtrs[i];
		assert ( pDocinfo );
		ARRAY_FOREACH_COND ( j, dMvaLocators, !bFailed )
			// if this MVA was updated
			if ( MVA_DOWNSIZE ( sphGetRowAttr ( pDocinfo, dMvaLocators[j] ) ) & MVA_ARENA_FLAG )
				sphSetRowAttr ( pDocinfo, dMvaLocators[j],
					((DWORD*)(((SphDocID_t*)(g_pMvaArena + dAllocs[iAllocIndex++]))+1) - g_pMvaArena) | MVA_ARENA_FLAG );
	}
	return true;
}

//////////////////////////////////////////////////////////////////////////

bool CSphIndex_VLN::PrecomputeMinMax()
{
	if ( !m_iDocinfo )
		return true;

	m_tProgress.m_ePhase = CSphIndexProgress::PHASE_PRECOMPUTE;
	m_tProgress.m_iDone = 0;

	m_iMinMaxIndex = 0;
	int iStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	m_iDocinfoIndex = ( m_iDocinfo+DOCINFO_INDEX_FREQ-1 ) / DOCINFO_INDEX_FREQ;

	if ( !m_tMinMaxLegacy.Alloc ( ( ( m_iDocinfoIndex+1 ) * 2 * iStride ), m_sLastError ) )
		return false;

	m_pDocinfoIndex = m_tMinMaxLegacy.GetWritePtr();
	const DWORD * pEnd = m_tMinMaxLegacy.GetWritePtr() + m_tMinMaxLegacy.GetNumEntries();

	AttrIndexBuilder_c tBuilder ( m_tSchema );
	tBuilder.Prepare ( m_pDocinfoIndex, pEnd );

	for ( int64_t iIndexEntry=0; iIndexEntry<m_iDocinfo; iIndexEntry++ )
	{
		if ( !tBuilder.Collect ( m_tAttr.GetWritePtr() + iIndexEntry * iStride, m_tMva.GetWritePtr(), m_tMva.GetNumEntries(), m_sLastError, true ) )
				return false;

		// show progress
		int64_t iDone = (iIndexEntry+1)*1000/m_iDocinfoIndex;
		if ( iDone!=m_tProgress.m_iDone )
		{
			m_tProgress.m_iDone = (int)iDone;
			m_tProgress.Show ( m_tProgress.m_iDone==1000 );
		}
	}

	tBuilder.FinishCollect();
	return true;
}

/////////////////////////////////////////////

// safely rename an index file
bool CSphIndex_VLN::JuggleFile ( const char* szExt, CSphString & sError, bool bNeedOrigin ) const
{
	CSphString sExt = GetIndexFileName ( szExt );
	CSphString sExtNew, sExtOld;
	sExtNew.SetSprintf ( "%s.tmpnew", sExt.cstr() );
	sExtOld.SetSprintf ( "%s.tmpold", sExt.cstr() );

	if ( ::rename ( sExt.cstr(), sExtOld.cstr() ) )
	{
		if ( bNeedOrigin )
		{
			sError.SetSprintf ( "rename '%s' to '%s' failed: %s", sExt.cstr(), sExtOld.cstr(), strerror(errno) );
			return false;
		}
	}

	if ( ::rename ( sExtNew.cstr(), sExt.cstr() ) )
	{
		if ( bNeedOrigin && !::rename ( sExtOld.cstr(), sExt.cstr() ) )
		{
			// rollback failed too!
			sError.SetSprintf ( "rollback rename to '%s' failed: %s; INDEX UNUSABLE; FIX FILE NAMES MANUALLY", sExt.cstr(), strerror(errno) );
		} else
		{
			// rollback went ok
			sError.SetSprintf ( "rename '%s' to '%s' failed: %s", sExtNew.cstr(), sExt.cstr(), strerror(errno) );
		}
		return false;
	}

	// all done
	::unlink ( sExtOld.cstr() );
	return true;
}

bool CSphIndex_VLN::SaveAttributes ( CSphString & sError ) const
{
	if ( !m_uAttrsStatus || !m_iDocinfo )
		return true;

	DWORD uAttrStatus = m_uAttrsStatus;

	sphLogDebugvv ( "index '%s' attrs (%d) saving...", m_sIndexName.cstr(), uAttrStatus );

	assert ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && m_iDocinfo && m_tAttr.GetWritePtr() );

	for ( ; uAttrStatus & ATTRS_MVA_UPDATED ; )
	{
		// collect the indexes of MVA schema attributes
		CSphVector<CSphAttrLocator> dMvaLocators;
		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
			if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
				dMvaLocators.Add ( tAttr.m_tLocator );
		}
#ifndef NDEBUG
		int iMva64 = dMvaLocators.GetLength();
#endif
		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
			if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
				dMvaLocators.Add ( tAttr.m_tLocator );
		}

		// collect the list of all docids with changed MVA attributes
		CSphVector<SphDocID_t> dAffected;
		{
			tDocCollector dCollect ( dAffected );
			g_tMvaArena.ExamineTag ( &dCollect, m_iIndexTag );
		}
		dAffected.Uniq();

		if ( !dAffected.GetLength() )
			break;

		// prepare the file to save into;
		CSphWriter fdFlushMVA;
		fdFlushMVA.OpenFile ( GetIndexFileName("mvp.tmpnew"), sError );
		if ( fdFlushMVA.IsError() )
			return false;

		// save the vector of affected docids
		DWORD uPos = dAffected.GetLength();
		fdFlushMVA.PutDword ( uPos );
		fdFlushMVA.PutBytes ( &dAffected[0], uPos*sizeof(SphDocID_t) );

		// save the updated MVA vectors
		ARRAY_FOREACH ( i, dAffected )
		{
			DWORD* pDocinfo = const_cast<DWORD*> ( FindDocinfo ( dAffected[i] ) );
			assert ( pDocinfo );

			pDocinfo = DOCINFO2ATTRS ( pDocinfo );
			ARRAY_FOREACH ( j, dMvaLocators )
			{
				DWORD uOldIndex = MVA_DOWNSIZE ( sphGetRowAttr ( pDocinfo, dMvaLocators[j] ) );
				// if this MVA was updated
				if ( uOldIndex & MVA_ARENA_FLAG )
				{
					DWORD * pMva = g_pMvaArena + ( uOldIndex & MVA_OFFSET_MASK );
					DWORD uCount = *pMva;
					assert ( j<iMva64 || ( uCount%2 )==0 );
					fdFlushMVA.PutDword ( uCount );
					fdFlushMVA.PutBytes ( pMva+1, uCount*sizeof(DWORD) );
				}
			}
		}
		fdFlushMVA.CloseFile();
		if ( !JuggleFile ( "mvp", sError, false ) )
			return false;
		break;
	}

	assert ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && m_iDocinfo && m_tAttr.GetWritePtr() );

	// save current state
	CSphAutofile fdTmpnew ( GetIndexFileName("spa.tmpnew"), SPH_O_NEW, sError );
	if ( fdTmpnew.GetFD()<0 )
		return false;

	int uStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	int64_t iSize = m_iDocinfo*sizeof(DWORD)*uStride;
	if ( m_uVersion>=20 )
		iSize += (m_iDocinfoIndex+1)*uStride*sizeof(CSphRowitem)*2;

	if ( !sphWriteThrottled ( fdTmpnew.GetFD(), m_tAttr.GetWritePtr(), iSize, "docinfo", sError, &g_tThrottle ) )
		return false;

	fdTmpnew.Close ();

	if ( !JuggleFile ( "spa", sError ) )
		return false;

	if ( m_bBinlog && g_pBinlog )
		g_pBinlog->NotifyIndexFlush ( m_sIndexName.cstr(), m_iTID, false );

	// save .sps file (inplace update only, no remapping/resizing)
	if ( uAttrStatus & ATTRS_STRINGS_UPDATED )
	{
		CSphWriter tStrWriter;
		if ( !tStrWriter.OpenFile ( GetIndexFileName("sps.tmpnew"), sError ) )
			return false;
		tStrWriter.PutBytes ( m_tString.GetWritePtr(), m_tString.GetLengthBytes() );
		tStrWriter.CloseFile();
		if ( !JuggleFile ( "sps", sError ) )
			return false;
	}

	if ( m_uAttrsStatus==uAttrStatus )
		const_cast<DWORD &>( m_uAttrsStatus ) = 0;

	sphLogDebugvv ( "index '%s' attrs (%d) saved", m_sIndexName.cstr(), m_uAttrsStatus );

	return true;
}

DWORD CSphIndex_VLN::GetAttributeStatus () const
{
	return m_uAttrsStatus;
}



template <typename T>
BYTE PrereadMapping(const char* sIndexName, const char* sFor, bool bMlock, bool bOnDisk, CSphBufferTrait<T>& tBuf)
{
	if (bOnDisk || tBuf.IsEmpty())
		return 0xff;

	const BYTE* pCur = (BYTE*)tBuf.GetWritePtr();
	const BYTE* pEnd = (BYTE*)tBuf.GetWritePtr() + tBuf.GetLengthBytes();
	const int iHalfPage = 2048;

	BYTE uHash = 0xff;
	for (; pCur < pEnd; pCur += iHalfPage)
		uHash ^= *pCur;
	uHash ^= *(pEnd - 1);

	// we want to prevent PrereadMapping() from being aggressively optimized away
	// volatile return values *should* normally achieve that
	volatile BYTE uRes = uHash;

	CSphString sWarning;
	if (bMlock && !tBuf.MemLock(sWarning))
		sphWarning("index '%s': %s for %s", sIndexName, sWarning.cstr(), sFor);

	return uRes;
}


static const CSphRowitem* CopyRow(const CSphRowitem* pDocinfo, DWORD* pTmpDocinfo, const CSphColumnInfo* pNewAttr, int iOldStride)
{
	SphDocID_t uDocId = DOCINFO2ID(pDocinfo);
	const DWORD* pAttrs = DOCINFO2ATTRS(pDocinfo);
	memcpy(DOCINFO2ATTRS(pTmpDocinfo), pAttrs, (iOldStride - DOCINFO_IDSIZE) * sizeof(DWORD));
	sphSetRowAttr(DOCINFO2ATTRS(pTmpDocinfo), pNewAttr->m_tLocator, 0);
	DOCINFOSETID(pTmpDocinfo, uDocId);
	return pDocinfo + iOldStride;
}


static const CSphRowitem* CopyRowAttrByAttr(const CSphRowitem* pDocinfo, DWORD* pTmpDocinfo, const CSphSchema& tOldSchema, const CSphSchema& tNewSchema, int iAttrToRemove, const CSphVector<int>& dAttrMap, int iOldStride)
{
	DOCINFOSETID(pTmpDocinfo, DOCINFO2ID(pDocinfo));

	for (int iAttr = 0; iAttr < tOldSchema.GetAttrsCount(); iAttr++)
		if (iAttr != iAttrToRemove)
		{
			SphAttr_t tValue = sphGetRowAttr(DOCINFO2ATTRS(pDocinfo), tOldSchema.GetAttr(iAttr).m_tLocator);
			sphSetRowAttr(DOCINFO2ATTRS(pTmpDocinfo), tNewSchema.GetAttr(dAttrMap[iAttr]).m_tLocator, tValue);
		}

	return pDocinfo + iOldStride;
}


static void CreateAttrMap(CSphVector<int>& dAttrMap, const CSphSchema& tOldSchema, const CSphSchema& tNewSchema, int iAttrToRemove)
{
	dAttrMap.Resize(tOldSchema.GetAttrsCount());
	for (int iAttr = 0; iAttr < tOldSchema.GetAttrsCount(); iAttr++)
		if (iAttr != iAttrToRemove)
		{
			dAttrMap[iAttr] = tNewSchema.GetAttrIndex(tOldSchema.GetAttr(iAttr).m_sName.cstr());
			assert(dAttrMap[iAttr] >= 0);
		}
		else
			dAttrMap[iAttr] = -1;
}



bool CSphIndex_VLN::AddRemoveAttribute ( bool bAddAttr, const CSphString & sAttrName, ESphAttr eAttrType, CSphString & sError )
{
	CSphSchema tNewSchema = m_tSchema;

	if ( bAddAttr )
	{
		CSphColumnInfo tInfo ( sAttrName.cstr(), eAttrType );
		tNewSchema.AddAttr ( tInfo, false );
	} else
		tNewSchema.RemoveAttr ( sAttrName.cstr(), false );

	CSphFixedVector<CSphRowitem> dMinRow ( tNewSchema.GetRowSize() );
	int iOldStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	int iNewStride = DOCINFO_IDSIZE + tNewSchema.GetRowSize();

	int64_t iNewMinMaxIndex = m_iDocinfo * iNewStride;

	BuildHeader_t tBuildHeader ( m_tStats );
	tBuildHeader.m_sHeaderExtension = "sph.tmpnew";
	tBuildHeader.m_pThrottle = &g_tThrottle;
	tBuildHeader.m_pMinRow = dMinRow.Begin();
	tBuildHeader.m_uMinDocid = m_uMinDocid;
	tBuildHeader.m_uKillListSize = (int)m_tKillList.GetNumEntries();
	tBuildHeader.m_iMinMaxIndex = iNewMinMaxIndex;

	*(DictHeader_t*)&tBuildHeader = *(DictHeader_t*)&m_tWordlist;

	CSphSchema tOldSchema = m_tSchema;
	m_tSchema = tNewSchema;

	// save the header
	bool bBuildRes = BuildDone ( tBuildHeader, sError );
	m_tSchema = tOldSchema;
	if ( !bBuildRes )
		return false;

	// generate a new .SPA file
	CSphWriter tSPAWriter;
	tSPAWriter.SetBufferSize ( 524288 );
	CSphString sSPAfile = GetIndexFileName ( "spa.tmpnew" );
	if ( !tSPAWriter.OpenFile ( sSPAfile, sError ) )
		return false;

	const CSphRowitem * pDocinfo = m_tAttr.GetWritePtr();
	if ( !pDocinfo )
	{
		sError = "index must have at least one attribute";
		return false;
	}

	CSphFixedVector<DWORD> dTmpDocinfos ( iNewStride );

	if ( bAddAttr )
	{
		const CSphColumnInfo * pNewAttr = tNewSchema.GetAttr ( sAttrName.cstr() );
		assert ( pNewAttr );

		for ( int i = 0; i < m_iDocinfo + (m_iDocinfoIndex+1)*2 && !tSPAWriter.IsError(); i++ )
		{
			pDocinfo = CopyRow ( pDocinfo, dTmpDocinfos.Begin(), pNewAttr, iOldStride );
			tSPAWriter.PutBytes ( dTmpDocinfos.Begin(), iNewStride*sizeof(DWORD) );
		}
	} else
	{
		int iAttrToRemove = tOldSchema.GetAttrIndex ( sAttrName.cstr() );
		assert ( iAttrToRemove>=0 );

		CSphVector<int> dAttrMap;
		CreateAttrMap ( dAttrMap, tOldSchema, tNewSchema, iAttrToRemove );

		for ( int i = 0; i < m_iDocinfo + (m_iDocinfoIndex+1)*2 && !tSPAWriter.IsError(); i++ )
		{
			pDocinfo = CopyRowAttrByAttr ( pDocinfo, dTmpDocinfos.Begin(), tOldSchema, tNewSchema, iAttrToRemove, dAttrMap, iOldStride );
			tSPAWriter.PutBytes ( dTmpDocinfos.Begin(), iNewStride*sizeof(DWORD) );
		}
	}

	if ( tSPAWriter.IsError() )
	{
		sError.SetSprintf ( "error writing to %s", sSPAfile.cstr() );
		return false;
	}

	tSPAWriter.CloseFile();

	if ( !JuggleFile ( "spa", sError ) )
		return false;

	if ( !JuggleFile ( "sph", sError ) )
		return false;

	m_tAttr.Reset();

	if ( !m_tAttr.Setup ( GetIndexFileName("spa").cstr(), sError, true ) )
		return false;

	m_tSchema = tNewSchema;
	m_iMinMaxIndex = iNewMinMaxIndex;
	m_pDocinfoIndex = m_tAttr.GetWritePtr() + m_iMinMaxIndex;
	m_iDocinfoIndex = ( ( m_tAttr.GetNumEntries() - m_iMinMaxIndex ) / iNewStride / 2 ) - 1;

	PrereadMapping ( m_sIndexName.cstr(), "attributes", m_bMlock, m_bOndiskAllAttr, m_tAttr );
	return true;
}

//////////////////////////////////


struct CmpHit_fn
{
	inline bool IsLess(const CSphWordHit& a, const CSphWordHit& b) const
	{
		return (a.m_uWordID < b.m_uWordID) ||
			(a.m_uWordID == b.m_uWordID && a.m_uDocID < b.m_uDocID) ||
			(a.m_uWordID == b.m_uWordID && a.m_uDocID == b.m_uDocID && HITMAN::GetPosWithField(a.m_uWordPos) < HITMAN::GetPosWithField(b.m_uWordPos));
	}
};

struct MvaEntry_t
{
	SphDocID_t	m_uDocID;
	int			m_iAttr;
	int64_t		m_iValue;

	inline bool operator < ( const MvaEntry_t & rhs ) const
	{
		if ( m_uDocID!=rhs.m_uDocID ) return m_uDocID<rhs.m_uDocID;
		if ( m_iAttr!=rhs.m_iAttr ) return m_iAttr<rhs.m_iAttr;
		return m_iValue<rhs.m_iValue;
	}
};


struct MvaEntryTag_t : public MvaEntry_t
{
	int			m_iTag;
};


struct MvaEntryCmp_fn
{
	static inline bool IsLess ( const MvaEntry_t & a, const MvaEntry_t & b )
	{
		return a<b;
	}
};


bool CSphIndex_VLN::BuildMVA ( const CSphVector<CSphSource*> & dSources, CSphFixedVector<CSphWordHit> & dHits,
		int iArenaSize, int iFieldFD, int nFieldMVAs, int iFieldMVAInPool, CSphIndex_VLN * pPrevIndex, const CSphBitvec * pPrevMva )
{
	// initialize writer (data file must always exist)
	CSphWriter wrMva;
	if ( !wrMva.OpenFile ( GetIndexFileName("spm"), m_sLastError ) )
		return false;

	// calcs and checks
	bool bOnlyFieldMVAs = true;
	CSphVector<int> dMvaIndexes;
	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
		if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
		{
			dMvaIndexes.Add ( i );
			if ( tAttr.m_eSrc!=SPH_ATTRSRC_FIELD )
				bOnlyFieldMVAs = false;
		}
	}
	int iMva64 = dMvaIndexes.GetLength();
	// mva32 first
	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
		if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
		{
			dMvaIndexes.Add ( i );
			if ( tAttr.m_eSrc!=SPH_ATTRSRC_FIELD )
				bOnlyFieldMVAs = false;
		}
	}

	if ( dMvaIndexes.GetLength()<=0 )
		return true;

	// reuse hits pool
	MvaEntry_t * pMvaPool = (MvaEntry_t*) dHits.Begin();
	MvaEntry_t * pMvaMax = pMvaPool + ( iArenaSize/sizeof(MvaEntry_t) );
	MvaEntry_t * pMva = pMvaPool;

	// create temp file
	CSphAutofile fdTmpMva ( GetIndexFileName("tmp3"), SPH_O_NEW, m_sLastError, true );
	if ( fdTmpMva.GetFD()<0 )
		return false;

	//////////////////////////////
	// collect and partially sort
	//////////////////////////////

	CSphVector<int> dBlockLens;
	dBlockLens.Reserve ( 1024 );

	m_tProgress.m_ePhase = CSphIndexProgress::PHASE_COLLECT_MVA;

	if ( !bOnlyFieldMVAs )
	{
		ARRAY_FOREACH ( iSource, dSources )
		{
			CSphSource * pSource = dSources[iSource];
			if ( !pSource->Connect ( m_sLastError ) )
				return false;

			ARRAY_FOREACH ( i, dMvaIndexes )
			{
				int iAttr = dMvaIndexes[i];
				const CSphColumnInfo & tAttr = m_tSchema.GetAttr(iAttr);

				if ( tAttr.m_eSrc==SPH_ATTRSRC_FIELD )
					continue;

				if ( !pSource->IterateMultivaluedStart ( iAttr, m_sLastError ) )
					return false;

				while ( pSource->IterateMultivaluedNext () )
				{
					// keep all mva from old index or
					// keep only enumerated mva
					if ( pPrevIndex && pPrevIndex->FindDocinfo ( pSource->m_tDocInfo.m_uDocID ) && ( !pPrevMva || ( pPrevMva && pPrevMva->BitGet ( iAttr ) ) ) )
						continue;

					pMva->m_uDocID = pSource->m_tDocInfo.m_uDocID;
					pMva->m_iAttr = i;
					if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
					{
						pMva->m_iValue = pSource->m_dMva[0];
					} else
					{
						pMva->m_iValue = MVA_UPSIZE ( pSource->m_dMva.Begin() );
					}

					if ( ++pMva>=pMvaMax )
					{
						sphSort ( pMvaPool, pMva-pMvaPool );
						if ( !sphWriteThrottled ( fdTmpMva.GetFD(), pMvaPool, (pMva-pMvaPool)*sizeof(MvaEntry_t), "temp_mva", m_sLastError, &g_tThrottle ) )
							return false;

						dBlockLens.Add ( pMva-pMvaPool );
						m_tProgress.m_iAttrs += pMva-pMvaPool;
						pMva = pMvaPool;

						m_tProgress.Show ( false );
					}
				}
			}

			pSource->Disconnect ();
		}

		if ( pMva>pMvaPool )
		{
			sphSort ( pMvaPool, pMva-pMvaPool );
			if ( !sphWriteThrottled ( fdTmpMva.GetFD(), pMvaPool, (pMva-pMvaPool)*sizeof(MvaEntry_t), "temp_mva", m_sLastError, &g_tThrottle ) )
				return false;

			dBlockLens.Add ( pMva-pMvaPool );
			m_tProgress.m_iAttrs += pMva-pMvaPool;
		}
	}

	m_tProgress.Show ( true );

	///////////////////////////
	// free memory for sorting
	///////////////////////////

	dHits.Reset ( 0 );

	//////////////
	// fully sort
	//////////////

	m_tProgress.m_ePhase = CSphIndexProgress::PHASE_SORT_MVA;
	m_tProgress.m_iAttrs = m_tProgress.m_iAttrs + nFieldMVAs;
	m_tProgress.m_iAttrsTotal = m_tProgress.m_iAttrs;
	m_tProgress.Show ( false );

	int	nLastBlockFieldMVAs = iFieldMVAInPool ? ( nFieldMVAs % iFieldMVAInPool ) : 0;
	int nFieldBlocks = iFieldMVAInPool ? ( nFieldMVAs / iFieldMVAInPool + ( nLastBlockFieldMVAs ? 1 : 0 ) ) : 0;

	// initialize readers
	CSphVector<CSphBin*> dBins;
	dBins.Reserve ( dBlockLens.GetLength() + nFieldBlocks );

	int iBinSize = CSphBin::CalcBinSize ( iArenaSize, dBlockLens.GetLength() + nFieldBlocks, "sort_mva" );
	SphOffset_t iSharedOffset = -1;

	ARRAY_FOREACH ( i, dBlockLens )
	{
		dBins.Add ( new CSphBin() );
		dBins[i]->m_iFileLeft = dBlockLens[i]*sizeof(MvaEntry_t);
		dBins[i]->m_iFilePos = ( i==0 ) ? 0 : dBins[i-1]->m_iFilePos + dBins[i-1]->m_iFileLeft;
		dBins[i]->Init ( fdTmpMva.GetFD(), &iSharedOffset, iBinSize );
	}

	SphOffset_t iSharedFieldOffset = -1;
	SphOffset_t uStart = 0;
	for ( int i = 0; i < nFieldBlocks; i++ )
	{
		dBins.Add ( new CSphBin() );
		int iBin = dBins.GetLength () - 1;

		dBins[iBin]->m_iFileLeft = sizeof(MvaEntry_t)*( i==nFieldBlocks-1
			? ( nLastBlockFieldMVAs ? nLastBlockFieldMVAs : iFieldMVAInPool )
			: iFieldMVAInPool );
		dBins[iBin]->m_iFilePos = uStart;
		dBins[iBin]->Init ( iFieldFD, &iSharedFieldOffset, iBinSize );

		uStart += dBins [iBin]->m_iFileLeft;
	}

	// do the sort
	CSphQueue < MvaEntryTag_t, MvaEntryCmp_fn > qMva ( Max ( 1,(int) dBins.GetLength() ) );
	ARRAY_FOREACH ( i, dBins )
	{
		MvaEntryTag_t tEntry;
		if ( dBins[i]->ReadBytes ( (MvaEntry_t*) &tEntry, sizeof(MvaEntry_t) )!=BIN_READ_OK )
		{
			m_sLastError.SetSprintf ( "sort_mva: warmup failed (io error?)" );
			return false;
		}

		tEntry.m_iTag = i;
		qMva.Push ( tEntry );
	}

	// spm-file := info-list [ 0+ ]
	// info-list := docid, values-list [ index.schema.mva-count ]
	// values-list := values-count, value [ values-count ]
	// note that mva32 come first then mva64
	SphDocID_t uCurID = 0;
	CSphVector < CSphVector<int64_t> > dCurInfo;
	dCurInfo.Resize ( dMvaIndexes.GetLength() );

	for ( ;; )
	{
		// flush previous per-document info-list
		if ( !qMva.GetLength() || qMva.Root().m_uDocID!=uCurID )
		{
			if ( uCurID )
			{
				wrMva.PutDocid ( uCurID );
				ARRAY_FOREACH ( i, dCurInfo )
				{
					int iLen = dCurInfo[i].GetLength();
					if ( i>=iMva64 )
					{
						wrMva.PutDword ( iLen*2 );
						wrMva.PutBytes ( dCurInfo[i].Begin(), sizeof(int64_t)*iLen );
					} else
					{
						wrMva.PutDword ( iLen );
						ARRAY_FOREACH ( iVal, dCurInfo[i] )
						{
							wrMva.PutDword ( (DWORD)dCurInfo[i][iVal] );
						}
					}
				}
			}

			if ( !qMva.GetLength() )
				break;

			uCurID = qMva.Root().m_uDocID;
			ARRAY_FOREACH ( i, dCurInfo )
				dCurInfo[i].Resize ( 0 );
		}

		// accumulate this entry
#if PARANOID
		assert ( dCurInfo [ qMva.Root().m_iAttr ].GetLength()==0
			|| dCurInfo [ qMva.Root().m_iAttr ].Last()<=qMva.Root().m_iValue );
#endif
		dCurInfo [ qMva.Root().m_iAttr ].AddUnique ( qMva.Root().m_iValue );

		// get next entry
		int iBin = qMva.Root().m_iTag;
		qMva.Pop ();

		MvaEntryTag_t tEntry;
		ESphBinRead iRes = dBins[iBin]->ReadBytes ( (MvaEntry_t*)&tEntry, sizeof(MvaEntry_t) );
		tEntry.m_iTag = iBin;

		if ( iRes==BIN_READ_OK )
			qMva.Push ( tEntry );

		if ( iRes==BIN_READ_ERROR )
		{
			m_sLastError.SetSprintf ( "sort_mva: read error" );
			return false;
		}
	}

	// clean up readers
	ARRAY_FOREACH ( i, dBins )
		SafeDelete ( dBins[i] );

	wrMva.CloseFile ();
	if ( wrMva.IsError() )
		return false;

	m_tProgress.Show ( true );

	return true;
}


struct FieldMVARedirect_t
{
	CSphAttrLocator		m_tLocator;
	int					m_iAttr;
	int					m_iMVAAttr;
	bool				m_bMva64;
};


bool CSphIndex_VLN::RelocateBlock ( int iFile, BYTE * pBuffer, int iRelocationSize,
	SphOffset_t * pFileSize, CSphBin * pMinBin, SphOffset_t * pSharedOffset )
{
	assert ( pBuffer && pFileSize && pMinBin && pSharedOffset );

	SphOffset_t iBlockStart = pMinBin->m_iFilePos;
	SphOffset_t iBlockLeft = pMinBin->m_iFileLeft;

	ESphBinRead eRes = pMinBin->Precache ();
	switch ( eRes )
	{
	case BIN_PRECACHE_OK:
		return true;
	case BIN_READ_ERROR:
		m_sLastError = "block relocation: preread error";
		return false;
	default:
		break;
	}

	int nTransfers = (int)( ( iBlockLeft+iRelocationSize-1) / iRelocationSize );

	SphOffset_t uTotalRead = 0;
	SphOffset_t uNewBlockStart = *pFileSize;

	for ( int i = 0; i < nTransfers; i++ )
	{
		sphSeek ( iFile, iBlockStart + uTotalRead, SEEK_SET );

		int iToRead = i==nTransfers-1 ? (int)( iBlockLeft % iRelocationSize ) : iRelocationSize;
		size_t iRead = sphReadThrottled ( iFile, pBuffer, iToRead, &g_tThrottle );
		if ( iRead!=size_t(iToRead) )
		{
			m_sLastError.SetSprintf ( "block relocation: read error (%d of %d bytes read): %s", (int)iRead, iToRead, strerror(errno) );
			return false;
		}

		sphSeek ( iFile, *pFileSize, SEEK_SET );
		uTotalRead += iToRead;

		if ( !sphWriteThrottled ( iFile, pBuffer, iToRead, "block relocation", m_sLastError, &g_tThrottle ) )
			return false;

		*pFileSize += iToRead;
	}

	assert ( uTotalRead==iBlockLeft );

	// update block pointers
	pMinBin->m_iFilePos = uNewBlockStart;
	*pSharedOffset = *pFileSize;

	return true;
}


bool CSphIndex_VLN::LoadHitlessWords ( CSphVector<SphWordID_t> & dHitlessWords )
{
	assert ( dHitlessWords.GetLength()==0 );

	if ( m_tSettings.m_sHitlessFiles.IsEmpty() )
		return true;

	const char * szStart = m_tSettings.m_sHitlessFiles.cstr();

	while ( *szStart )
	{
		while ( *szStart && ( sphIsSpace ( *szStart ) || *szStart==',' ) )
			++szStart;

		if ( !*szStart )
			break;

		const char * szWordStart = szStart;

		while ( *szStart && !sphIsSpace ( *szStart ) && *szStart!=',' )
			++szStart;

		if ( szStart - szWordStart > 0 )
		{
			CSphString sFilename;
			sFilename.SetBinary ( szWordStart, szStart-szWordStart );

			CSphAutofile tFile ( sFilename.cstr(), SPH_O_READ, m_sLastError );
			if ( tFile.GetFD()==-1 )
				return false;

			CSphVector<BYTE> dBuffer ( (int)tFile.GetSize() );
			if ( !tFile.Read ( &dBuffer[0], dBuffer.GetLength(), m_sLastError ) )
				return false;

			// FIXME!!! dict=keywords + hitless_words=some
			m_pTokenizer->SetBuffer ( &dBuffer[0], dBuffer.GetLength() );
			while ( BYTE * sToken = m_pTokenizer->GetToken() )
				dHitlessWords.Add ( m_pDict->GetWordID ( sToken ) );
		}
	}

	dHitlessWords.Uniq();
	return true;
}



class DeleteOnFail : public ISphNoncopyable
{
public:
	DeleteOnFail() : m_bShitHappened ( true )
	{}
	~DeleteOnFail()
	{
		if ( m_bShitHappened )
		{
			ARRAY_FOREACH ( i, m_dWriters )
				m_dWriters[i]->UnlinkFile();

			ARRAY_FOREACH ( i, m_dAutofiles )
				m_dAutofiles[i]->SetTemporary();
		}
	}
	void AddWriter ( CSphWriter * pWr )
	{
		if ( pWr )
			m_dWriters.Add ( pWr );
	}
	void AddAutofile ( CSphAutofile * pAf )
	{
		if ( pAf )
			m_dAutofiles.Add ( pAf );
	}
	void AllIsDone()
	{
		m_bShitHappened = false;
	}
private:
	bool	m_bShitHappened;
	CSphVector<CSphWriter*> m_dWriters;
	CSphVector<CSphAutofile*> m_dAutofiles;
};

static void CopyRow ( const CSphRowitem * pSrc, const ISphSchema & tSchema, const CSphVector<int> & dAttrs, CSphRowitem * pDst )
{
	assert ( pSrc && pDst );
	ARRAY_FOREACH ( i, dAttrs )
	{
		const CSphAttrLocator & tLoc = tSchema.GetAttr ( dAttrs[i] ).m_tLocator;
		SphAttr_t uVal = sphGetRowAttr ( pSrc, tLoc );
		sphSetRowAttr ( pDst, tLoc, uVal );
	}
}

/////////////////////////////////////////////////////////////////////////////

/// hit priority queue entry
struct CSphHitQueueEntry : public CSphAggregateHit
{
	int m_iBin;
};


/// hit priority queue
struct CSphHitQueue
{
public:
	CSphHitQueueEntry* m_pData;
	int						m_iSize;
	int						m_iUsed;

public:
	/// create queue
	explicit CSphHitQueue(int iSize)
	{
		assert(iSize > 0);
		m_iSize = iSize;
		m_iUsed = 0;
		m_pData = new CSphHitQueueEntry[iSize];
	}

	/// destroy queue
	~CSphHitQueue()
	{
		SafeDeleteArray(m_pData);
	}

	/// add entry to the queue
	void Push(CSphAggregateHit& tHit, int iBin)
	{
		// check for overflow and do add
		assert(m_iUsed < m_iSize);
		m_pData[m_iUsed].m_uDocID = tHit.m_uDocID;
		m_pData[m_iUsed].m_uWordID = tHit.m_uWordID;
		m_pData[m_iUsed].m_sKeyword = tHit.m_sKeyword; // bin must hold the actual data for the queue
		m_pData[m_iUsed].m_iWordPos = tHit.m_iWordPos;
		m_pData[m_iUsed].m_dFieldMask = tHit.m_dFieldMask;
		m_pData[m_iUsed].m_iBin = iBin;

		int iEntry = m_iUsed++;

		// sift up if needed
		while (iEntry)
		{
			int iParent = (iEntry - 1) >> 1;
			if (SPH_CMPAGGRHIT_LESS(m_pData[iEntry], m_pData[iParent]))
			{
				// entry is less than parent, should float to the top
				Swap(m_pData[iEntry], m_pData[iParent]);
				iEntry = iParent;
			}
			else
			{
				break;
			}
		}
	}

	/// remove root (ie. top priority) entry
	void Pop()
	{
		assert(m_iUsed);
		if (!(--m_iUsed)) // empty queue? just return
			return;

		// make the last entry my new root
		m_pData[0] = m_pData[m_iUsed];

		// sift down if needed
		int iEntry = 0;
		for (;; )
		{
			// select child
			int iChild = (iEntry << 1) + 1;
			if (iChild >= m_iUsed)
				break;

			// select smallest child
			if (iChild + 1 < m_iUsed)
				if (SPH_CMPAGGRHIT_LESS(m_pData[iChild + 1], m_pData[iChild]))
					iChild++;

			// if smallest child is less than entry, do float it to the top
			if (SPH_CMPAGGRHIT_LESS(m_pData[iChild], m_pData[iEntry]))
			{
				Swap(m_pData[iChild], m_pData[iEntry]);
				iEntry = iChild;
				continue;
			}

			break;
		}
	}
};


struct CmpQueuedDocinfo_fn
{
	static DWORD* m_pStorage;
	static int		m_iStride;

	static inline bool IsLess(const int a, const int b)
	{
		return DOCINFO2ID(m_pStorage + a * m_iStride) < DOCINFO2ID(m_pStorage + b * m_iStride);
	}
};
DWORD* CmpQueuedDocinfo_fn::m_pStorage = NULL;
int			CmpQueuedDocinfo_fn::m_iStride = 1;

/////////////////////////////////////////////////////////////////////////////


int CSphIndex_VLN::Build ( const CSphVector<CSphSource*> & dSources, int iMemoryLimit, int iWriteBuffer )
{
	assert ( dSources.GetLength() );

	CSphVector<SphWordID_t> dHitlessWords;

	if ( !LoadHitlessWords ( dHitlessWords ) )
		return 0;

	int iHitBuilderBufferSize = ( iWriteBuffer>0 )
		? Max ( iWriteBuffer, MIN_WRITE_BUFFER )
		: DEFAULT_WRITE_BUFFER;

	// vars shared between phases
	CSphVector<CSphBin*> dBins;
	SphOffset_t iSharedOffset = -1;

	m_pDict->HitblockBegin();

	// setup sources
	ARRAY_FOREACH ( iSource, dSources )
	{
		CSphSource * pSource = dSources[iSource];
		assert ( pSource );

		pSource->SetDict ( m_pDict );
		pSource->Setup ( m_tSettings );
	}

	// connect 1st source and fetch its schema
	if ( !dSources[0]->Connect ( m_sLastError )
		|| !dSources[0]->IterateStart ( m_sLastError )
		|| !dSources[0]->UpdateSchema ( &m_tSchema, m_sLastError ) )
	{
		return 0;
	}

	if ( m_tSchema.m_dFields.GetLength()==0 )
	{
		m_sLastError.SetSprintf ( "No fields in schema - will not index" );
		return 0;
	}

	// check docinfo
	if ( m_tSchema.GetAttrsCount()==0 && m_tSettings.m_eDocinfo!=SPH_DOCINFO_NONE )
	{
		sphWarning ( "Attribute count is 0: switching to none docinfo" );
		m_tSettings.m_eDocinfo = SPH_DOCINFO_NONE;
	}

	if ( dSources[0]->HasJoinedFields() && m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
	{
		m_sLastError.SetSprintf ( "got joined fields, but docinfo is 'inline' (fix your config file)" );
		return 0;
	}

	if ( m_tSchema.GetAttrsCount()>0 && m_tSettings.m_eDocinfo==SPH_DOCINFO_NONE )
	{
		m_sLastError.SetSprintf ( "got attributes, but docinfo is 'none' (fix your config file)" );
		return 0;
	}

	bool bHaveFieldMVAs = false;
	int iFieldLens = m_tSchema.GetAttrId_FirstFieldLen();
	CSphVector<int> dMvaIndexes;
	CSphVector<CSphAttrLocator> dMvaLocators;

	// strings storage
	CSphVector<int> dStringAttrs;

	// chunks to partically sort string attributes
	CSphVector<DWORD> dStringChunks;
	SphOffset_t uStringChunk = 0;

	// Sphinx-BSON storage
	CSphVector<BYTE> dBson;
	dBson.Reserve ( 1024 );

	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tCol = m_tSchema.GetAttr(i);
		switch ( tCol.m_eAttrType )
		{
			case ESphAttr::SPH_ATTR_UINT32SET:
				if ( tCol.m_eSrc==SPH_ATTRSRC_FIELD )
					bHaveFieldMVAs = true;
				dMvaIndexes.Add ( i );
				dMvaLocators.Add ( tCol.m_tLocator );
				break;
			case ESphAttr::SPH_ATTR_STRING:
			case ESphAttr::SPH_ATTR_JSON:
				dStringAttrs.Add ( i );
				break;
			default:
				break;
		}
	}

	// no field lengths for docinfo=inline
	assert ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN || iFieldLens==-1 );

	// this loop must NOT be merged with the previous one;
	// mva64 must intentionally be after all the mva32
	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tCol = m_tSchema.GetAttr(i);
		if ( tCol.m_eAttrType!=ESphAttr::SPH_ATTR_INT64SET )
			continue;
		if ( tCol.m_eSrc==SPH_ATTRSRC_FIELD )
			bHaveFieldMVAs = true;
		dMvaIndexes.Add ( i );
		dMvaLocators.Add ( tCol.m_tLocator );
	}

	bool bGotMVA = ( dMvaIndexes.GetLength()!=0 );
	if ( bGotMVA && m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN )
	{
		m_sLastError.SetSprintf ( "multi-valued attributes require docinfo=extern (fix your config file)" );
		return 0;
	}

	if ( dStringAttrs.GetLength() && m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN )
	{
		m_sLastError.SetSprintf ( "string attributes require docinfo=extern (fix your config file)" );
		return 0;
	}

	if ( !m_pTokenizer->SetFilterSchema ( m_tSchema, m_sLastError ) )
		return 0;

	CSphHitBuilder tHitBuilder ( m_tSettings, dHitlessWords, false, iHitBuilderBufferSize, m_pDict, &m_sLastError );

	////////////////////////////////////////////////
	// collect and partially sort hits and docinfos
	////////////////////////////////////////////////

	// killlist storage
	CSphVector <SphDocID_t> dKillList;

	// adjust memory requirements
	int iOldLimit = iMemoryLimit;

	// book memory to store at least 64K attribute rows
	const int iDocinfoStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	int iDocinfoMax = Max ( iMemoryLimit/16/iDocinfoStride/sizeof(DWORD), 65536ul );
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_NONE )
		iDocinfoMax = 1;

	// book at least 32 KB for field MVAs, if needed
	int iFieldMVAPoolSize = Max ( 32768, iMemoryLimit/16 );
	if ( bHaveFieldMVAs==0 )
		iFieldMVAPoolSize = 0;

	// book at least 2 MB for keywords dict, if needed
	int iDictSize = 0;
	if ( m_pDict->GetSettings().m_bWordDict )
		iDictSize = Max ( MIN_KEYWORDS_DICT, iMemoryLimit/8 );

	// do we have enough left for hits?
	int iHitsMax = 1048576;

	iMemoryLimit -= iDocinfoMax*iDocinfoStride*sizeof(DWORD) + iFieldMVAPoolSize + iDictSize;
	if ( iMemoryLimit < iHitsMax*(int)sizeof(CSphWordHit) )
	{
		iMemoryLimit = iOldLimit + iHitsMax*sizeof(CSphWordHit) - iMemoryLimit;
		sphWarn ( "collect_hits: mem_limit=%d kb too low, increasing to %d kb",
			iOldLimit/1024, iMemoryLimit/1024 );
	} else
	{
		iHitsMax = iMemoryLimit / sizeof(CSphWordHit);
	}

	// allocate raw hits block
	CSphFixedVector<CSphWordHit> dHits ( iHitsMax + MAX_SOURCE_HITS );
	CSphWordHit * pHits = dHits.Begin();
	CSphWordHit * pHitsMax = dHits.Begin() + iHitsMax;

	// after finishing with hits this pool will be used to sort strings
	int iPoolSize = dHits.GetSizeBytes();

	// allocate docinfos buffer
	CSphFixedVector<DWORD> dDocinfos ( iDocinfoMax*iDocinfoStride );
	DWORD * pDocinfo = dDocinfos.Begin();
	const DWORD * pDocinfoMax = dDocinfos.Begin() + iDocinfoMax*iDocinfoStride;
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_NONE )
	{
		pDocinfo = NULL;
		pDocinfoMax = NULL;
	}

	CSphVector < MvaEntry_t > dFieldMVAs;
	dFieldMVAs.Reserve ( 16384 );

	CSphVector < SphOffset_t > dFieldMVABlocks;
	dFieldMVABlocks.Reserve ( 4096 );

	CSphVector < FieldMVARedirect_t > dFieldMvaIndexes;

	if ( bHaveFieldMVAs )
		dFieldMvaIndexes.Reserve ( 8 );

	int iMaxPoolFieldMVAs = iFieldMVAPoolSize / sizeof ( MvaEntry_t );
	int nFieldMVAs = 0;

	CSphScopedPtr<CSphIndex_VLN> pPrevIndex(NULL);
	CSphVector<int>	dPrevAttrsPlain;
	CSphBitvec dPrevAttrsMva;
	CSphBitvec dPrevAttrsString;
	bool bKeepSelectedAttrMva = false;
	bool bKeepSelectedAttrString = false;
	if ( !m_sKeepAttrs.IsEmpty() )
	{
		CSphString sWarning;
		pPrevIndex = (CSphIndex_VLN *)sphCreateIndexPhrase ( "keep-attrs", m_sKeepAttrs.cstr() );
		pPrevIndex->SetMemorySettings ( false, true, true );
		if ( !pPrevIndex->Prealloc ( false ) )
		{
			CSphString sError;
			if ( !sWarning.IsEmpty() )
				sError.SetSprintf ( "warning: '%s',", sWarning.cstr() );
			if ( !pPrevIndex->GetLastError().IsEmpty() )
				sError.SetSprintf ( "%serror: '%s'", sError.scstr(), pPrevIndex->GetLastError().cstr() );
			sphWarn ( "unable to load 'keep-attrs' index (%s); ignoring --keep-attrs", sError.cstr() );

			pPrevIndex.Reset();
		} else
		{
			// check schemas
			CSphString sError;
			if ( !m_tSchema.CompareTo ( pPrevIndex->m_tSchema, sError, false ) )
			{
				sphWarn ( "schemas are different (%s); ignoring --keep-attrs", sError.cstr() );
				pPrevIndex.Reset();
			}
		}

		if ( pPrevIndex.Ptr() && m_dKeepAttrs.GetLength() )
		{
			dPrevAttrsMva.Init ( m_tSchema.GetAttrsCount() );
			dPrevAttrsString.Init ( m_tSchema.GetAttrsCount() );

			ARRAY_FOREACH ( i, m_dKeepAttrs )
			{
				int iCol = m_tSchema.GetAttrIndex ( m_dKeepAttrs[i].cstr() );
				if ( iCol==-1 )
				{
					sphWarn ( "no attribute found '%s'; ignoring --keep-attrs", m_dKeepAttrs[i].cstr() );
					pPrevIndex.Reset();
					break;
				}

				const CSphColumnInfo & tCol = m_tSchema.GetAttr ( iCol );
				if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
				{
					dPrevAttrsMva.BitSet ( iCol );
					bKeepSelectedAttrMva = true;
				} else if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_STRING || tCol.m_eAttrType==ESphAttr::SPH_ATTR_JSON )
				{
					dPrevAttrsString.BitSet ( iCol );
					bKeepSelectedAttrString = true;
				} else
				{
					dPrevAttrsPlain.Add ( iCol );
				}
			}
		}

		if ( pPrevIndex.Ptr() )
			pPrevIndex->Preread();
	}

	// create temp files
	CSphAutofile fdLock ( GetIndexFileName("tmp0"), SPH_O_NEW, m_sLastError, true );
	CSphAutofile fdHits ( GetIndexFileName ( m_bInplaceSettings ? "spp" : "tmp1" ), SPH_O_NEW, m_sLastError, !m_bInplaceSettings );
	CSphAutofile fdDocinfos ( GetIndexFileName ( m_bInplaceSettings ? "spa" : "tmp2" ), SPH_O_NEW, m_sLastError, !m_bInplaceSettings );
	CSphAutofile fdTmpFieldMVAs ( GetIndexFileName("tmp7"), SPH_O_NEW, m_sLastError, true );
	CSphWriter tStrWriter;
	CSphWriter tStrFinalWriter;

	if ( !tStrWriter.OpenFile ( GetIndexFileName("tmps"), m_sLastError ) )
		return 0;
	tStrWriter.PutByte ( 0 ); // dummy byte, to reserve magic zero offset

	if ( !tStrFinalWriter.OpenFile ( GetIndexFileName("sps"), m_sLastError ) )
		return 0;
	tStrFinalWriter.PutByte ( 0 ); // dummy byte, to reserve magic zero offset

	DeleteOnFail dFileWatchdog;

	if ( m_bInplaceSettings )
	{
		dFileWatchdog.AddAutofile ( &fdHits );
		dFileWatchdog.AddAutofile ( &fdDocinfos );
	}

	dFileWatchdog.AddWriter ( &tStrWriter );
	dFileWatchdog.AddWriter ( &tStrFinalWriter );

	if ( fdLock.GetFD()<0 || fdHits.GetFD()<0 || fdDocinfos.GetFD()<0 || fdTmpFieldMVAs.GetFD ()<0 )
		return 0;

	SphOffset_t iHitsGap = 0;
	SphOffset_t iDocinfosGap = 0;

	if ( m_bInplaceSettings )
	{
		const int HIT_SIZE_AVG = 4;
		const float HIT_BLOCK_FACTOR = 1.0f;
		const float DOCINFO_BLOCK_FACTOR = 1.0f;

		if ( m_iHitGap )
			iHitsGap = (SphOffset_t) m_iHitGap;
		else
			iHitsGap = (SphOffset_t)( iHitsMax*HIT_BLOCK_FACTOR*HIT_SIZE_AVG );

		iHitsGap = Max ( iHitsGap, 1 );
		sphSeek ( fdHits.GetFD (), iHitsGap, SEEK_SET );

		if ( m_iDocinfoGap )
			iDocinfosGap = (SphOffset_t) m_iDocinfoGap;
		else
			iDocinfosGap = (SphOffset_t)( iDocinfoMax*DOCINFO_BLOCK_FACTOR*iDocinfoStride*sizeof(DWORD) );

		iDocinfosGap = Max ( iDocinfosGap, 1 );
		sphSeek ( fdDocinfos.GetFD (), iDocinfosGap, SEEK_SET );
	}

	if ( !sphLockEx ( fdLock.GetFD(), false ) )
	{
		m_sLastError.SetSprintf ( "failed to lock '%s': another indexer running?", fdLock.GetFilename() );
		return 0;
	}

	// setup accumulating docinfo IDs range
	m_dMinRow.Reset ( m_tSchema.GetRowSize() );
	m_uMinDocid = DOCID_MAX;
	ARRAY_FOREACH ( i, m_dMinRow )
		m_dMinRow[i] = ROWITEM_MAX;

	m_tStats.Reset ();
	m_tProgress.m_ePhase = CSphIndexProgress::PHASE_COLLECT;
	m_tProgress.m_iAttrs = 0;

	CSphVector<int> dHitBlocks;
	dHitBlocks.Reserve ( 1024 );

	int iDocinfoBlocks = 0;

	ARRAY_FOREACH ( iSource, dSources )
	{
		// connect and check schema, if it's not the first one
		CSphSource * pSource = dSources[iSource];

		if ( iSource )
		{
			if ( !pSource->Connect ( m_sLastError )
				|| !pSource->IterateStart ( m_sLastError )
				|| !pSource->UpdateSchema ( &m_tSchema, m_sLastError ) )
			{
				return 0;
			}

			if ( pSource->HasJoinedFields() && m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
			{
				m_sLastError.SetSprintf ( "got joined fields, but docinfo is 'inline' (fix your config file)" );
				return 0;
			}
		}

		dFieldMvaIndexes.Resize ( 0 );

		ARRAY_FOREACH ( i, dMvaIndexes )
		{
			int iAttr = dMvaIndexes[i];
			const CSphColumnInfo & tCol = m_tSchema.GetAttr ( iAttr );
			if ( tCol.m_eSrc==SPH_ATTRSRC_FIELD )
			{
				FieldMVARedirect_t & tRedirect = dFieldMvaIndexes.Add();
				tRedirect.m_tLocator = tCol.m_tLocator;
				tRedirect.m_iAttr = iAttr;
				tRedirect.m_iMVAAttr = i;
				tRedirect.m_bMva64 = ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET );
			}
		}

		// joined filter
		bool bGotJoined = ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_INLINE ) && pSource->HasJoinedFields();

		// fetch documents
		for ( ;; )
		{
			// get next doc, and handle errors
			bool bGotDoc = pSource->IterateDocument ( m_sLastError );
			if ( !bGotDoc )
				return 0;

			// ensure docid is sane
			if ( pSource->m_tDocInfo.m_uDocID==DOCID_MAX )
			{
				m_sLastError.SetSprintf ( "docid==DOCID_MAX (source broken?)" );
				return 0;
			}

			// check for eof
			if ( !pSource->m_tDocInfo.m_uDocID )
				break;

			// show progress bar
			if ( ( pSource->GetStats().m_iTotalDocuments % 1000 )==0 )
			{
				m_tProgress.m_iDocuments = m_tStats.m_iTotalDocuments + pSource->GetStats().m_iTotalDocuments;
				m_tProgress.m_iBytes = m_tStats.m_iTotalBytes + pSource->GetStats().m_iTotalBytes;
				m_tProgress.Show ( false );
			}

			// update crashdump
			g_iIndexerCurrentDocID = pSource->m_tDocInfo.m_uDocID;
			g_iIndexerCurrentHits = pHits-dHits.Begin();

			const DWORD * pPrevDocinfo = NULL;
			if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && pPrevIndex.Ptr() )
				pPrevDocinfo = pPrevIndex->FindDocinfo ( pSource->m_tDocInfo.m_uDocID );

			if ( dMvaIndexes.GetLength() && pPrevDocinfo && pPrevIndex->m_tMva.GetWritePtr() )
			{
				// fetch old mva values
				ARRAY_FOREACH ( i, dMvaIndexes )
				{
					int iAttr = dMvaIndexes[i];
					if ( bKeepSelectedAttrMva && !dPrevAttrsMva.BitGet ( iAttr ) )
						continue;

					const CSphColumnInfo & tCol = m_tSchema.GetAttr ( iAttr );
					SphAttr_t uOff = sphGetRowAttr ( DOCINFO2ATTRS ( pPrevDocinfo ), tCol.m_tLocator );
					if ( !uOff )
						continue;

					const DWORD * pMVA = pPrevIndex->m_tMva.GetWritePtr()+uOff;
					int nMVAs = *pMVA++;
					for ( int iMVA = 0; iMVA < nMVAs; iMVA++ )
					{
						MvaEntry_t & tMva = dFieldMVAs.Add();
						tMva.m_uDocID = pSource->m_tDocInfo.m_uDocID;
						tMva.m_iAttr = i;
						if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
						{
							tMva.m_iValue = MVA_UPSIZE(pMVA);
							pMVA++;
						} else
							tMva.m_iValue = *pMVA;

						pMVA++;

						int iLength = dFieldMVAs.GetLength ();
						if ( iLength==iMaxPoolFieldMVAs )
						{
							dFieldMVAs.Sort();
							if ( !sphWriteThrottled ( fdTmpFieldMVAs.GetFD (), &dFieldMVAs[0],
								iLength*sizeof(MvaEntry_t), "temp_field_mva", m_sLastError, &g_tThrottle ) )
								return 0;

							dFieldMVAs.Resize ( 0 );

							nFieldMVAs += iMaxPoolFieldMVAs;
						}
					}
				}
			}

			if ( bHaveFieldMVAs )
			{
				// store field MVAs
				ARRAY_FOREACH ( i, dFieldMvaIndexes )
				{
					int iAttr = dFieldMvaIndexes[i].m_iAttr;
					int iMVA = dFieldMvaIndexes[i].m_iMVAAttr;
					bool bMva64 = dFieldMvaIndexes[i].m_bMva64;
					int iStep = ( bMva64 ? 2 : 1 );

					if ( pPrevDocinfo && ( !bKeepSelectedAttrMva || ( bKeepSelectedAttrMva && dPrevAttrsMva.BitGet ( iAttr ) ) ) )
						continue;

					// store per-document MVAs
					SphRange_t tFieldMva = pSource->IterateFieldMVAStart ( iAttr );
					m_tProgress.m_iAttrs += ( tFieldMva.m_iLength / iStep );

					assert ( ( tFieldMva.m_iStart + tFieldMva.m_iLength )<=pSource->m_dMva.GetLength() );
					for ( int j=tFieldMva.m_iStart; j<( tFieldMva.m_iStart+tFieldMva.m_iLength); j+=iStep )
					{
						MvaEntry_t & tMva = dFieldMVAs.Add();
						tMva.m_uDocID = pSource->m_tDocInfo.m_uDocID;
						tMva.m_iAttr = iMVA;
						if ( bMva64 )
						{
							tMva.m_iValue = MVA_UPSIZE ( pSource->m_dMva.Begin() + j );
						} else
						{
							tMva.m_iValue = pSource->m_dMva[j];
						}

						int iLength = dFieldMVAs.GetLength ();
						if ( iLength==iMaxPoolFieldMVAs )
						{
							dFieldMVAs.Sort();
							if ( !sphWriteThrottled ( fdTmpFieldMVAs.GetFD (), &dFieldMVAs[0],
								iLength*sizeof(MvaEntry_t), "temp_field_mva", m_sLastError, &g_tThrottle ) )
									return 0;

							dFieldMVAs.Resize ( 0 );

							nFieldMVAs += iMaxPoolFieldMVAs;
						}
					}
				}
			}

			// store strings and JSON blobs
			{
				ARRAY_FOREACH ( i, dStringAttrs )
				{
					// FIXME! optimize locators etc?
					// FIXME! support binary strings w/embedded zeroes?
					// get data, calc length
					int iStrAttr = dStringAttrs[i];
					const CSphColumnInfo & tCol = m_tSchema.GetAttr ( iStrAttr );
					bool bKeepPrevAttr = ( pPrevDocinfo && ( !bKeepSelectedAttrString || ( bKeepSelectedAttrString && dPrevAttrsString.BitGet ( iStrAttr ) ) ) );
					const char * sData = NULL;
					int iLen = 0;

					if ( !bKeepPrevAttr )
					{
						sData = pSource->m_dStrAttrs[iStrAttr].cstr();
						iLen = pSource->m_dStrAttrs[iStrAttr].Length();
					} else
					{
						SphAttr_t uPrevOff = sphGetRowAttr ( DOCINFO2ATTRS ( pPrevDocinfo ), tCol.m_tLocator );
						BYTE * pBase = pPrevIndex->m_tString.GetWritePtr();
						if ( uPrevOff && pBase )
							iLen = sphUnpackStr ( pBase+uPrevOff, (const BYTE **)&sData );
					}

					// no data
					if ( !iLen )
					{
						pSource->m_tDocInfo.SetAttr ( tCol.m_tLocator, 0 );
						continue;
					}

					// handle JSON
					if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_JSON && !bKeepPrevAttr ) // FIXME? optimize?
					{
						// WARNING, tricky bit
						// flex lexer needs last two (!) bytes to be zeroes
						// asciiz string supplies one, and we fill out the extra one
						// and that works, because CSphString always allocates a small extra gap
						char * pData = const_cast<char*>(sData);
						pData[iLen+1] = '\0';

						dBson.Resize ( 0 );
						if ( !sphJsonParse ( dBson, pData, g_bJsonAutoconvNumbers, g_bJsonKeynamesToLowercase, m_sLastError ) )
						{
							m_sLastError.SetSprintf ( "document " DOCID_FMT ", attribute %s: JSON error: %s",
								pSource->m_tDocInfo.m_uDocID, tCol.m_sName.cstr(),
								m_sLastError.cstr() );

							// bail?
							if ( g_bJsonStrict )
								return 0;

							// warn and ignore
							sphWarning ( "%s", m_sLastError.cstr() );
							m_sLastError = "";
							pSource->m_tDocInfo.SetAttr ( tCol.m_tLocator, 0 );
							continue;
						}
						if ( !dBson.GetLength() )
						{
							// empty SphinxBSON, need not save any data
							pSource->m_tDocInfo.SetAttr ( tCol.m_tLocator, 0 );
							continue;
						}

						// let's go save the newly built SphinxBSON blob
						sData = (const char*)dBson.Begin();
						iLen = dBson.GetLength();
					}

					// calc offset, do sanity checks
					SphOffset_t uOff = tStrWriter.GetPos();
					if ( uint64_t(uOff)>>32 )
					{
						m_sLastError.SetSprintf ( "too many string attributes (current index format allows up to 4 GB)" );
						return 0;
					}
					pSource->m_tDocInfo.SetAttr ( tCol.m_tLocator, DWORD(uOff) );

					// pack length, emit it, emit data
					BYTE dPackedLen[4];
					int iLenLen = sphPackStrlen ( dPackedLen, iLen );
					tStrWriter.PutBytes ( &dPackedLen, iLenLen );
					tStrWriter.PutBytes ( sData, iLen );

					// check if current pos is the good one for sorting
					if ( uOff+iLenLen+iLen-uStringChunk > iPoolSize )
					{
						dStringChunks.Add ( DWORD ( uOff-uStringChunk ) );
						uStringChunk = uOff;
					}
				}
			}

			// docinfo=inline might be flushed while collecting hits
			if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
			{
				// store next entry
				DOCINFOSETID ( pDocinfo, pSource->m_tDocInfo.m_uDocID );
				memcpy ( DOCINFO2ATTRS ( pDocinfo ), pSource->m_tDocInfo.m_pDynamic, sizeof(CSphRowitem)*m_tSchema.GetRowSize() );
				pDocinfo += iDocinfoStride;

				// update min docinfo
				assert ( pSource->m_tDocInfo.m_uDocID );
				m_uMinDocid = Min ( m_uMinDocid, pSource->m_tDocInfo.m_uDocID );
				ARRAY_FOREACH ( i, m_dMinRow )
					m_dMinRow[i] = Min ( m_dMinRow[i], pSource->m_tDocInfo.m_pDynamic[i] );
			}

			// store hits
			while ( const ISphHits * pDocHits = pSource->IterateHits ( m_sLastWarning ) )
			{
				int iDocHits = pDocHits->Length();
#if PARANOID
				for ( int i=0; i<iDocHits; i++ )
				{
					assert ( pDocHits->m_dData[i].m_uDocID==pSource->m_tDocInfo.m_uDocID );
					assert ( pDocHits->m_dData[i].m_uWordID );
					assert ( pDocHits->m_dData[i].m_iWordPos );
				}
#endif

				assert ( ( pHits+iDocHits )<=( pHitsMax+MAX_SOURCE_HITS ) );

				memcpy ( pHits, pDocHits->First(), iDocHits*sizeof(CSphWordHit) );
				pHits += iDocHits;

				// check if we need to flush
				if ( pHits<pHitsMax
					&& !( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE && pDocinfo>=pDocinfoMax )
					&& !( iDictSize && m_pDict->HitblockGetMemUse() > iDictSize ) )
				{
					continue;
				}

				// update crashdump
				g_iIndexerPoolStartDocID = pSource->m_tDocInfo.m_uDocID;
				g_iIndexerPoolStartHit = pHits-dHits.Begin();

				// sort hits
				int iHits = pHits - dHits.Begin();
				{
					sphSort ( dHits.Begin(), iHits, CmpHit_fn() );
					m_pDict->HitblockPatch ( dHits.Begin(), iHits );
				}
				pHits = dHits.Begin();

				if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
				{
					// we're inlining, so let's flush both hits and docs
					int iDocs = ( pDocinfo - dDocinfos.Begin() ) / iDocinfoStride;
					pDocinfo = dDocinfos.Begin();

					sphSortDocinfos ( dDocinfos.Begin(), iDocs, iDocinfoStride );

					dHitBlocks.Add ( tHitBuilder.cidxWriteRawVLB ( fdHits.GetFD(), dHits.Begin(), iHits,
						dDocinfos.Begin(), iDocs, iDocinfoStride ) );

					// we are inlining, so if there are more hits in this document,
					// we'll need to know it's info next flush
					if ( iDocHits )
					{
						DOCINFOSETID ( pDocinfo, pSource->m_tDocInfo.m_uDocID );
						memcpy ( DOCINFO2ATTRS ( pDocinfo ), pSource->m_tDocInfo.m_pDynamic, sizeof(CSphRowitem)*m_tSchema.GetRowSize() );
						pDocinfo += iDocinfoStride;
					}
				} 
				else
				{
					// we're not inlining, so only flush hits, docs are flushed independently
					dHitBlocks.Add ( tHitBuilder.cidxWriteRawVLB ( fdHits.GetFD(), dHits.Begin(), iHits,
						NULL, 0, 0 ) );
				}
				m_pDict->HitblockReset ();

				if ( dHitBlocks.Last()<0 )
					return 0;

				// progress bar
				m_tProgress.m_iHitsTotal += iHits;
				m_tProgress.m_iDocuments = m_tStats.m_iTotalDocuments + pSource->GetStats().m_iTotalDocuments;
				m_tProgress.m_iBytes = m_tStats.m_iTotalBytes + pSource->GetStats().m_iTotalBytes;
				m_tProgress.Show ( false );
			}

			// update min docinfo
			assert ( pSource->m_tDocInfo.m_uDocID );
			m_uMinDocid = Min ( m_uMinDocid, pSource->m_tDocInfo.m_uDocID );
			if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
			{
				ARRAY_FOREACH ( i, m_dMinRow )
					m_dMinRow[i] = Min ( m_dMinRow[i], pSource->m_tDocInfo.m_pDynamic[i] );
			}

			// update total field lengths
			if ( iFieldLens>=0 )
			{
				ARRAY_FOREACH ( i, m_tSchema.m_dFields )
					m_dFieldLens[i] += pSource->m_tDocInfo.GetAttr ( m_tSchema.GetAttr ( i+iFieldLens ).m_tLocator );
			}

			// store docinfo
			// with the advent of ESphAttr::SPH_ATTR_TOKENCOUNT, now MUST be done AFTER iterating the hits
			// because field lengths are computed during that iterating
			if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN )
			{
				// store next entry
				DOCINFOSETID ( pDocinfo, pSource->m_tDocInfo.m_uDocID );

				CSphRowitem * pAttr = DOCINFO2ATTRS ( pDocinfo );
				if ( !pPrevDocinfo )
				{
					memcpy ( pAttr, pSource->m_tDocInfo.m_pDynamic, sizeof(CSphRowitem)*m_tSchema.GetRowSize() );
				} else
				{
					if ( !m_dKeepAttrs.GetLength() )
					{
						// copy whole row from old index
						memcpy ( pAttr, DOCINFO2ATTRS ( pPrevDocinfo ), sizeof(CSphRowitem)*m_tSchema.GetRowSize() );

						// copy some strings attributes
						// 2nd stage - copy offsets from source, data already copied at string indexing
						if ( dStringAttrs.GetLength() )
							CopyRow ( pSource->m_tDocInfo.m_pDynamic, m_tSchema, dStringAttrs, pAttr );

					} else
					{
						// copy new attributes, however keep some of them from old index
						memcpy ( pAttr, pSource->m_tDocInfo.m_pDynamic, sizeof(CSphRowitem)*m_tSchema.GetRowSize() );

						// copy some plain attributes
						if ( dPrevAttrsPlain.GetLength() )
							CopyRow ( DOCINFO2ATTRS ( pPrevDocinfo ), m_tSchema, dPrevAttrsPlain, pAttr );

						// copy some strings attributes
						// 2nd stage - copy offsets from source, data already copied at string indexing
						if ( dStringAttrs.GetLength() )
							CopyRow ( pSource->m_tDocInfo.m_pDynamic, m_tSchema, dStringAttrs, pAttr );
					}
				}

				pDocinfo += iDocinfoStride;

				// if not inlining, flush buffer if it's full
				// (if inlining, it will flushed later, along with the hits)
				if ( pDocinfo>=pDocinfoMax )
				{
					assert ( pDocinfo==pDocinfoMax );
					int iLen = iDocinfoMax*iDocinfoStride*sizeof(DWORD);

					sphSortDocinfos ( dDocinfos.Begin(), iDocinfoMax, iDocinfoStride );
					if ( !sphWriteThrottled ( fdDocinfos.GetFD(), dDocinfos.Begin(), iLen, "raw_docinfos", m_sLastError, &g_tThrottle ) )
						return 0;

					pDocinfo = dDocinfos.Begin();
					iDocinfoBlocks++;
				}
			}

			// go on, loop next document
		}

		// FIXME! uncontrolled memory usage; add checks and/or diskbased sort in the future?
		if ( pSource->IterateKillListStart ( m_sLastError ) )
		{
			SphDocID_t uDocId;
			while ( pSource->IterateKillListNext ( uDocId ) )
				dKillList.Add ( uDocId );
		}

		// fetch joined fields
		if ( bGotJoined )
		{
			// flush tail of regular hits
			int iHits = pHits - dHits.Begin();
			if ( iDictSize && m_pDict->HitblockGetMemUse() && iHits )
			{
				sphSort ( dHits.Begin(), iHits, CmpHit_fn() );
				m_pDict->HitblockPatch ( dHits.Begin(), iHits );
				pHits = dHits.Begin();
				m_tProgress.m_iHitsTotal += iHits;
				dHitBlocks.Add ( tHitBuilder.cidxWriteRawVLB ( fdHits.GetFD(), dHits.Begin(), iHits, NULL, 0, 0 ) );
				if ( dHitBlocks.Last()<0 )
					return 0;
				m_pDict->HitblockReset ();
			}

			for ( ;; )
			{
				// get next doc, and handle errors
				ISphHits * pJoinedHits = pSource->IterateJoinedHits ( m_sLastError );
				if ( !pJoinedHits )
					return 0;

				// ensure docid is sane
				if ( pSource->m_tDocInfo.m_uDocID==DOCID_MAX )
				{
					m_sLastError.SetSprintf ( "joined_docid==DOCID_MAX (source broken?)" );
					return 0;
				}

				// check for eof
				if ( !pSource->m_tDocInfo.m_uDocID )
					break;

				int iJoinedHits = pJoinedHits->Length();
				memcpy ( pHits, pJoinedHits->First(), iJoinedHits*sizeof(CSphWordHit) );
				pHits += iJoinedHits;

				// check if we need to flush
				if ( pHits<pHitsMax && !( iDictSize && m_pDict->HitblockGetMemUse() > iDictSize ) )
					continue;

				// store hits
				int iHits = pHits - dHits.Begin();
				sphSort ( dHits.Begin(), iHits, CmpHit_fn() );
				m_pDict->HitblockPatch ( dHits.Begin(), iHits );

				pHits = dHits.Begin();
				m_tProgress.m_iHitsTotal += iHits;

				dHitBlocks.Add ( tHitBuilder.cidxWriteRawVLB ( fdHits.GetFD(), dHits.Begin(), iHits, NULL, 0, 0 ) );
				if ( dHitBlocks.Last()<0 )
					return 0;
				m_pDict->HitblockReset ();
			}
		}

		// this source is over, disconnect and update stats
		pSource->Disconnect ();

		m_tStats.m_iTotalDocuments += pSource->GetStats().m_iTotalDocuments;
		m_tStats.m_iTotalBytes += pSource->GetStats().m_iTotalBytes;
	}

	if ( m_tStats.m_iTotalDocuments>=INT_MAX )
	{
		m_sLastError.SetSprintf ( "index over %d documents not supported (got documents count=" INT64_FMT ")", INT_MAX, m_tStats.m_iTotalDocuments );
		return 0;
	}

	// flush last docinfo block
	int iDocinfoLastBlockSize = 0;
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && pDocinfo>dDocinfos.Begin() )
	{
		iDocinfoLastBlockSize = ( pDocinfo - dDocinfos.Begin() ) / iDocinfoStride;
		assert ( pDocinfo==( dDocinfos.Begin() + iDocinfoLastBlockSize*iDocinfoStride ) );

		int iLen = iDocinfoLastBlockSize*iDocinfoStride*sizeof(DWORD);
		sphSortDocinfos ( dDocinfos.Begin(), iDocinfoLastBlockSize, iDocinfoStride );
		if ( !sphWriteThrottled ( fdDocinfos.GetFD(), dDocinfos.Begin(), iLen, "raw_docinfos", m_sLastError, &g_tThrottle ) )
			return 0;

		iDocinfoBlocks++;
	}

	// flush last hit block
	if ( pHits>dHits.Begin() )
	{
		int iHits = pHits - dHits.Begin();
		{
			sphSort ( dHits.Begin(), iHits, CmpHit_fn() );
			m_pDict->HitblockPatch ( dHits.Begin(), iHits );
		}
		m_tProgress.m_iHitsTotal += iHits;

		if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
		{
			int iDocs = ( pDocinfo - dDocinfos.Begin() ) / iDocinfoStride;
			sphSortDocinfos ( dDocinfos.Begin(), iDocs, iDocinfoStride );
			dHitBlocks.Add ( tHitBuilder.cidxWriteRawVLB ( fdHits.GetFD(), dHits.Begin(), iHits,
				dDocinfos.Begin(), iDocs, iDocinfoStride ) );
		} else
		{
			dHitBlocks.Add ( tHitBuilder.cidxWriteRawVLB ( fdHits.GetFD(), dHits.Begin(), iHits, NULL, 0, 0 ) );
		}
		m_pDict->HitblockReset ();

		if ( dHitBlocks.Last()<0 )
			return 0;
	}

	// flush last field MVA block
	if ( bHaveFieldMVAs && dFieldMVAs.GetLength () )
	{
		int iLength = dFieldMVAs.GetLength ();
		nFieldMVAs += iLength;

		dFieldMVAs.Sort();
		if ( !sphWriteThrottled ( fdTmpFieldMVAs.GetFD (), &dFieldMVAs[0],
			iLength*sizeof(MvaEntry_t), "temp_field_mva", m_sLastError, &g_tThrottle ) )
				return 0;

		dFieldMVAs.Reset ();
	}

	m_tProgress.m_iDocuments = m_tStats.m_iTotalDocuments;
	m_tProgress.m_iBytes = m_tStats.m_iTotalBytes;
	m_tProgress.Show ( true );

	///////////////////////////////////////
	// collect and sort multi-valued attrs
	///////////////////////////////////////
	const CSphBitvec * pPrevAttrsMva = NULL;
	if ( bKeepSelectedAttrMva )
		pPrevAttrsMva = &dPrevAttrsMva;

	if ( !BuildMVA ( dSources, dHits, iHitsMax*sizeof(CSphWordHit), fdTmpFieldMVAs.GetFD (), nFieldMVAs, iMaxPoolFieldMVAs, pPrevIndex.Ptr(), pPrevAttrsMva ) )
		return 0;

	// reset persistent mva update pool
	::unlink ( GetIndexFileName("mvp").cstr() );

	// reset hits pool
	dHits.Reset ( 0 );

	CSphString sFieldMVAFile = fdTmpFieldMVAs.GetFilename ();
	fdTmpFieldMVAs.Close ();
	::unlink ( sFieldMVAFile.cstr () );

	/////////////////
	// sort docinfos
	/////////////////

	// initialize MVA reader
	CSphAutoreader rdMva;
	if ( !rdMva.Open ( GetIndexFileName("spm"), m_sLastError ) )
		return 0;

	SphDocID_t uMvaID = rdMva.GetDocid();

	// initialize writer
	int iDocinfoFD = -1;
	SphOffset_t iDocinfoWritePos = 0;
	CSphScopedPtr<CSphAutofile> pfdDocinfoFinal ( NULL );

	if ( m_bInplaceSettings )
		iDocinfoFD = fdDocinfos.GetFD ();
	else
	{
		pfdDocinfoFinal = new CSphAutofile ( GetIndexFileName("spa"), SPH_O_NEW, m_sLastError );
		iDocinfoFD = pfdDocinfoFinal->GetFD();
		if ( iDocinfoFD < 0 )
			return 0;
	}

	int iDupes = 0;
	int iMinBlock = -1;

	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && iDocinfoBlocks )
	{
		// initialize readers
		assert ( dBins.GetLength()==0 );
		dBins.Reserve ( iDocinfoBlocks );

		float fReadFactor = 1.0f;
		float fRelocFactor = 0.0f;
		if ( m_bInplaceSettings )
		{
			assert ( m_fRelocFactor > 0.005f && m_fRelocFactor < 0.95f );
			fRelocFactor = m_fRelocFactor;
			fReadFactor -= fRelocFactor;
		}

		int iBinSize = CSphBin::CalcBinSize ( int ( iMemoryLimit * fReadFactor ), iDocinfoBlocks, "sort_docinfos" );
		int iRelocationSize = m_bInplaceSettings ? int ( iMemoryLimit * fRelocFactor ) : 0;
		CSphFixedVector<BYTE> dRelocationBuffer ( iRelocationSize );
		iSharedOffset = -1;

		for ( int i=0; i<iDocinfoBlocks; i++ )
		{
			dBins.Add ( new CSphBin() );
			dBins[i]->m_iFileLeft = ( ( i==iDocinfoBlocks-1 ) ? iDocinfoLastBlockSize : iDocinfoMax )*iDocinfoStride*sizeof(DWORD);
			dBins[i]->m_iFilePos = ( i==0 ) ? iDocinfosGap : dBins[i-1]->m_iFilePos + dBins[i-1]->m_iFileLeft;
			dBins[i]->Init ( fdDocinfos.GetFD(), &iSharedOffset, iBinSize );
		}

		SphOffset_t iDocinfoFileSize = 0;
		if ( iDocinfoBlocks )
			iDocinfoFileSize = dBins [ iDocinfoBlocks-1 ]->m_iFilePos + dBins [ iDocinfoBlocks-1 ]->m_iFileLeft;

		// docinfo queue
		CSphFixedVector<DWORD> dDocinfoQueue ( iDocinfoBlocks*iDocinfoStride );
		CSphQueue < int, CmpQueuedDocinfo_fn > tDocinfo ( iDocinfoBlocks );

		CmpQueuedDocinfo_fn::m_pStorage = dDocinfoQueue.Begin();
		CmpQueuedDocinfo_fn::m_iStride = iDocinfoStride;

		pDocinfo = dDocinfoQueue.Begin();
		for ( int i=0; i<iDocinfoBlocks; i++ )
		{
			if ( dBins[i]->ReadBytes ( pDocinfo, iDocinfoStride*sizeof(DWORD) )!=BIN_READ_OK )
			{
				m_sLastError.SetSprintf ( "sort_docinfos: warmup failed (io error?)" );
				return 0;
			}
			pDocinfo += iDocinfoStride;
			tDocinfo.Push ( i );
		}

		// while the queue has data for us
		pDocinfo = dDocinfos.Begin();
		SphDocID_t uLastId = 0;
		m_iMinMaxIndex = 0;

		// prepare the collector for min/max of attributes
		AttrIndexBuilder_c tMinMax ( m_tSchema );
		int64_t iMinMaxSize = tMinMax.GetExpectedSize ( m_tStats.m_iTotalDocuments );
		if ( iMinMaxSize>INT_MAX || m_tStats.m_iTotalDocuments>INT_MAX )
		{
			m_sLastError.SetSprintf ( "attribute files (.spa) over 128 GB are not supported (min-max approximate=" INT64_FMT ", documents count=" INT64_FMT ")",
				iMinMaxSize, m_tStats.m_iTotalDocuments );
			return 0;
		}
		CSphFixedVector<DWORD> dMinMaxBuffer ( (int)iMinMaxSize );
		memset ( dMinMaxBuffer.Begin(), 0, (int)iMinMaxSize*sizeof(DWORD) );

		// { fixed row + dummy value ( zero offset elimination ) + mva data for that row } fixed row - for MinMaxBuilder
		CSphVector < DWORD > dMvaPool;
		tMinMax.Prepare ( dMinMaxBuffer.Begin(), dMinMaxBuffer.Begin() + dMinMaxBuffer.GetLength() ); // FIXME!!! for over INT_MAX blocks
		uint64_t uLastMvaOff = 0;

		// the last (or, lucky, the only, string chunk)
		dStringChunks.Add ( DWORD ( tStrWriter.GetPos()-uStringChunk ) );

		tStrWriter.CloseFile();
		if ( !dStringAttrs.GetLength() )
			::unlink ( GetIndexFileName("tmps").cstr() );

		SphDocID_t uLastDupe = 0;
		while ( tDocinfo.GetLength() )
		{
			// obtain bin index and next entry
			int iBin = tDocinfo.Root();
			DWORD * pEntry = dDocinfoQueue.Begin() + iBin*iDocinfoStride;

			assert ( DOCINFO2ID ( pEntry )>=uLastId && "descending documents" );

			// skip duplicates
			if ( DOCINFO2ID ( pEntry )==uLastId )
			{
				// dupe, report it
				if ( m_tSettings.m_bVerbose && uLastDupe!=uLastId )
					sphWarn ( "duplicated document id=" DOCID_FMT, uLastId );

				uLastDupe = uLastId;
				iDupes++;

			} else
			{
				// new unique document, handle it
				m_iMinMaxIndex += iDocinfoStride;

				CSphRowitem * pCollectibleRow = pEntry;
				// update MVA
				if ( bGotMVA )
				{
					// go to next id
					while ( uMvaID<DOCINFO2ID(pEntry) )
					{
						ARRAY_FOREACH ( i, dMvaIndexes )
						{
							int iCount = rdMva.GetDword();
							rdMva.SkipBytes ( iCount*sizeof(DWORD) );
						}

						uMvaID = rdMva.GetDocid();
						if ( !uMvaID )
							uMvaID = DOCID_MAX;
					}

					assert ( uMvaID>=DOCINFO2ID(pEntry) );
					if ( uMvaID==DOCINFO2ID(pEntry) )
					{
						// fixed row + dummy value ( zero offset elemination )
						dMvaPool.Resize ( iDocinfoStride+1 );
						memcpy ( dMvaPool.Begin(), pEntry, iDocinfoStride * sizeof(DWORD) );

						CSphRowitem * pAttr = DOCINFO2ATTRS ( pEntry );
						ARRAY_FOREACH ( i, dMvaIndexes )
						{
							uLastMvaOff = rdMva.GetPos()/sizeof(DWORD);
							int iPoolOff = dMvaPool.GetLength();
							if ( uLastMvaOff>UINT_MAX )
								sphDie ( "MVA counter overflows " UINT64_FMT " at document " DOCID_FMT ", total MVA entries " UINT64_FMT " ( try to index less documents )", uLastMvaOff, uMvaID, rdMva.GetFilesize() );

							sphSetRowAttr ( pAttr, dMvaLocators[i], uLastMvaOff );
							// there is the cloned row at the beginning of MVA pool, lets skip it
							sphSetRowAttr ( dMvaPool.Begin()+DOCINFO_IDSIZE, dMvaLocators[i], iPoolOff - iDocinfoStride );

							DWORD iMvaCount = rdMva.GetDword();
							dMvaPool.Resize ( iPoolOff+iMvaCount+1 );
							dMvaPool[iPoolOff] = iMvaCount;
							rdMva.GetBytes ( dMvaPool.Begin()+iPoolOff+1, sizeof(DWORD)*iMvaCount );
						}
						pCollectibleRow = dMvaPool.Begin();

						uMvaID = rdMva.GetDocid();
						if ( !uMvaID )
							uMvaID = DOCID_MAX;
					}
				}

				if ( !tMinMax.Collect ( pCollectibleRow, dMvaPool.Begin()+iDocinfoStride, dMvaPool.GetLength()-iDocinfoStride, m_sLastError, false ) )
					return 0;
				dMvaPool.Resize ( iDocinfoStride );

				// emit it
				memcpy ( pDocinfo, pEntry, iDocinfoStride*sizeof(DWORD) );
				pDocinfo += iDocinfoStride;
				uLastId = DOCINFO2ID(pEntry);

				if ( pDocinfo>=pDocinfoMax )
				{
					int iLen = iDocinfoMax*iDocinfoStride*sizeof(DWORD);

					if ( m_bInplaceSettings )
					{
						if ( iMinBlock==-1 || dBins[iMinBlock]->IsEOF () )
						{
							iMinBlock = -1;
							ARRAY_FOREACH ( i, dBins )
								if ( !dBins[i]->IsEOF () && ( iMinBlock==-1 || dBins [i]->m_iFilePos<dBins[iMinBlock]->m_iFilePos ) )
									iMinBlock = i;
						}

						if ( iMinBlock!=-1 && ( iDocinfoWritePos + iLen ) > dBins[iMinBlock]->m_iFilePos )
						{
							if ( !RelocateBlock ( iDocinfoFD, dRelocationBuffer.Begin(), iRelocationSize, &iDocinfoFileSize, dBins[iMinBlock], &iSharedOffset ) )
								return 0;

							iMinBlock = (iMinBlock+1) % dBins.GetLength ();
						}

						sphSeek ( iDocinfoFD, iDocinfoWritePos, SEEK_SET );
						iSharedOffset = iDocinfoWritePos;
					}

					if ( !sphWriteThrottled ( iDocinfoFD, dDocinfos.Begin(), iLen, "sort_docinfo", m_sLastError,&g_tThrottle ) )
						return 0;

					iDocinfoWritePos += iLen;
					pDocinfo = dDocinfos.Begin();
				}
			}

			// pop its index, update it, push its index again
			tDocinfo.Pop ();
			ESphBinRead eRes = dBins[iBin]->ReadBytes ( pEntry, iDocinfoStride*sizeof(DWORD) );
			if ( eRes==BIN_READ_ERROR )
			{
				m_sLastError.SetSprintf ( "sort_docinfo: failed to read entry" );
				return 0;
			}
			if ( eRes==BIN_READ_OK )
				tDocinfo.Push ( iBin );
		}

		if ( pDocinfo>dDocinfos.Begin() )
		{
			assert ( 0==( pDocinfo-dDocinfos.Begin() ) % iDocinfoStride );
			int iLen = ( pDocinfo - dDocinfos.Begin() )*sizeof(DWORD);

			if ( m_bInplaceSettings )
				sphSeek ( iDocinfoFD, iDocinfoWritePos, SEEK_SET );

			if ( !sphWriteThrottled ( iDocinfoFD, dDocinfos.Begin(), iLen, "sort_docinfo", m_sLastError,&g_tThrottle ) )
				return 0;

			if ( m_bInplaceSettings )
				if ( !sphTruncate ( iDocinfoFD ) )
					sphWarn ( "failed to truncate %s", fdDocinfos.GetFilename() );

			iDocinfoWritePos += iLen;
		}
		tMinMax.FinishCollect();
		int64_t iMinMaxRealSize = tMinMax.GetActualSize() * sizeof(DWORD);
		if ( !sphWriteThrottled ( iDocinfoFD, dMinMaxBuffer.Begin(), iMinMaxRealSize, "minmax_docinfo", m_sLastError,&g_tThrottle ) )
				return 0;

		// clean up readers
		ARRAY_FOREACH ( i, dBins )
			SafeDelete ( dBins[i] );

		dBins.Reset ();

		if ( uLastMvaOff>INT_MAX )
			sphWarning ( "MVA update disabled (collected MVA " INT64_FMT ", should be less %d)", uLastMvaOff, INT_MAX );
	}

	dDocinfos.Reset ( 0 );
	pDocinfo = NULL;

	// iDocinfoWritePos now contains the true size of pure attributes (without block indexes) in bytes
	int iStringStride = dStringAttrs.GetLength();
	SphOffset_t iNumDocs = iDocinfoWritePos/sizeof(DWORD)/iDocinfoStride;
	CSphTightVector<DWORD> dStrOffsets;

	if ( iStringStride )
	{
		// read only non-zero string locators
		{
			CSphReader tAttrReader;
			tAttrReader.SetFile ( iDocinfoFD, GetIndexFileName ( "spa" ).cstr() );
			CSphFixedVector<DWORD> dDocinfo ( iDocinfoStride );
			pDocinfo = dDocinfo.Begin();
			for ( SphOffset_t i=0; i<iNumDocs; ++i )
			{
				tAttrReader.GetBytes ( pDocinfo, iDocinfoStride*sizeof(DWORD) );
				CSphRowitem * pAttrs = DOCINFO2ATTRS ( pDocinfo );
				ARRAY_FOREACH ( j, dStringAttrs )
				{
					const CSphAttrLocator & tLoc = m_tSchema.GetAttr ( dStringAttrs[j] ).m_tLocator;
					DWORD uData = (DWORD)sphGetRowAttr ( pAttrs, tLoc );
					if ( uData )
						dStrOffsets.Add ( uData );
				}
			}
		} // the spa reader eliminates out of this scope
		DWORD iNumStrings = dStrOffsets.GetLength();

		// reopen strings for reading
		CSphAutofile tRawStringsFile;
		CSphReader tStrReader;
		if ( tRawStringsFile.Open ( GetIndexFileName("tmps"), SPH_O_READ, m_sLastError, true )<0 )
			return 0;
		tStrReader.SetFile ( tRawStringsFile );

		// now just load string chunks and resort them...
		CSphFixedVector<BYTE> dStringPool ( iPoolSize );
		BYTE* pStringsBegin = dStringPool.Begin();

		// if we have more than 1 string chunks, we need several passes and bitmask to distinquish them
		if ( dStringChunks.GetLength()>1 )
		{
			dStrOffsets.Resize ( iNumStrings+( iNumStrings>>5 )+1 );
			DWORD* pDocinfoBitmap = &dStrOffsets [ iNumStrings ];
			for ( DWORD i=0; i<1+( iNumStrings>>5 ); ++i )
				pDocinfoBitmap[i] = 0;
			SphOffset_t iMinStrings = 0;

			ARRAY_FOREACH ( i, dStringChunks )
			{
				// read the current chunk
				SphOffset_t iMaxStrings = iMinStrings + dStringChunks[i];
				tStrReader.GetBytes ( pStringsBegin, dStringChunks[i] );

				// walk throw the attributes and put the strings in the new order
				DWORD uMaskOff = 0;
				DWORD uMask = 1;
				for ( DWORD k=0; k<iNumStrings; ++k )
				{
					if ( uMask==0x80000000 )
					{
						uMask = 1;
						++uMaskOff;
					} else
						uMask <<= 1;
					DWORD& uCurStr = dStrOffsets[k];
					// already processed, or hit out of the the current chunk?
					if ( pDocinfoBitmap[uMaskOff]&uMask || !uCurStr || uCurStr<iMinStrings || uCurStr>=iMaxStrings )
						continue;

					const BYTE * pStr = NULL;
					int iLen = sphUnpackStr ( pStringsBegin + uCurStr - iMinStrings, &pStr );
					if ( !iLen )
						uCurStr = 0;
					else
					{
						uCurStr = (DWORD)tStrFinalWriter.GetPos();
						BYTE dPackedLen[4];
						int iLenLen = sphPackStrlen ( dPackedLen, iLen );
						tStrFinalWriter.PutBytes ( &dPackedLen, iLenLen );
						tStrFinalWriter.PutBytes ( pStr, iLen );
					}
					pDocinfoBitmap[uMaskOff]|=uMask;
				}
				iMinStrings = iMaxStrings;
			}
		} else if ( dStringChunks.GetLength()==1 ) // only one chunk. Plain and simple!
		{
			DWORD iStringChunk = dStringChunks[0];
			tStrReader.GetBytes ( pStringsBegin, iStringChunk );

			// walk throw the attributes and put the strings in the new order
			for ( DWORD k=0; k<iNumStrings; ++k )
			{
				DWORD& uOffset = dStrOffsets[k];
				// already processed, or hit out of the the current chunk?
				if ( uOffset<1 || uOffset>=iStringChunk )
					continue;

				const BYTE * pStr = NULL;
				int iLen = sphUnpackStr ( pStringsBegin + uOffset, &pStr );
				if ( !iLen )
					uOffset = 0;
				else
				{
					uOffset = (DWORD)tStrFinalWriter.GetPos();
					BYTE dPackedLen[4];
					int iLenLen = sphPackStrlen ( dPackedLen, iLen );
					tStrFinalWriter.PutBytes ( &dPackedLen, iLenLen );
					tStrFinalWriter.PutBytes ( pStr, iLen );
				}
			}
		}
		dStringPool.Reset(0);
		// now save back patched string locators
		{
			DWORD iDocPoolSize = iPoolSize/iDocinfoStride/sizeof(DWORD);
			CSphFixedVector<DWORD> dDocinfoPool ( iDocPoolSize*iDocinfoStride );
			pDocinfo = dDocinfoPool.Begin();
			DWORD iToRead = Min ( iDocPoolSize, DWORD(iNumDocs) );
			SphOffset_t iPos = 0;
			DWORD iStr = 0;
			while ( iToRead )
			{
				sphSeek ( iDocinfoFD, iPos, SEEK_SET );
				STATS::sphRead ( iDocinfoFD, pDocinfo, iToRead*iDocinfoStride*sizeof(DWORD));
				for ( DWORD i=0; i<iToRead; ++i )
				{
					CSphRowitem * pAttrs = DOCINFO2ATTRS ( pDocinfo+i*iDocinfoStride );
					ARRAY_FOREACH ( j, dStringAttrs )
					{
						const CSphAttrLocator& tLocator = m_tSchema.GetAttr ( dStringAttrs[j] ).m_tLocator;
						if ( sphGetRowAttr ( pAttrs, tLocator ) )
							sphSetRowAttr ( pAttrs, tLocator, dStrOffsets[iStr++] );
					}
				}
				sphSeek ( iDocinfoFD, iPos, SEEK_SET );
				sphWrite ( iDocinfoFD, pDocinfo, iToRead*iDocinfoStride*sizeof(DWORD));
				iPos+=iToRead*iDocinfoStride*sizeof(DWORD);
				iNumDocs-=iToRead;
				iToRead = Min ( iDocPoolSize, DWORD(iNumDocs) );
			}
		} // all temporary buffers eliminates out of this scope
	}

	// it might be zero-length, but it must exist
	if ( m_bInplaceSettings )
		fdDocinfos.Close ();
	else
	{
		assert ( pfdDocinfoFinal.Ptr () );
		pfdDocinfoFinal->Close ();
	}

	// dump killlist
	CSphAutofile tKillList ( GetIndexFileName("spk"), SPH_O_NEW, m_sLastError );
	if ( tKillList.GetFD()<0 )
		return 0;

	DWORD uKillistSize = 0;
	if ( dKillList.GetLength () )
	{
		dKillList.Uniq ();
		uKillistSize = dKillList.GetLength ();

		if ( !sphWriteThrottled ( tKillList.GetFD(), dKillList.Begin(),
			sizeof(SphDocID_t)*uKillistSize, "kill list", m_sLastError,&g_tThrottle ) )
				return 0;
	}

	dKillList.Reset();
	tKillList.Close ();

	///////////////////////////////////
	// sort and write compressed index
	///////////////////////////////////

	// initialize readers
	assert ( dBins.GetLength()==0 );
	dBins.Reserve ( dHitBlocks.GetLength() );

	iSharedOffset = -1;

	float fReadFactor = 1.0f;
	int iRelocationSize = 0;
	iWriteBuffer = iHitBuilderBufferSize;

	if ( m_bInplaceSettings )
	{
		assert ( m_fRelocFactor > 0.005f && m_fRelocFactor < 0.95f );
		assert ( m_fWriteFactor > 0.005f && m_fWriteFactor < 0.95f );
		assert ( m_fWriteFactor+m_fRelocFactor < 1.0f );

		fReadFactor -= m_fRelocFactor + m_fWriteFactor;

		iRelocationSize = int ( iMemoryLimit * m_fRelocFactor );
		iWriteBuffer = int ( iMemoryLimit * m_fWriteFactor );
	}

	int iBinSize = CSphBin::CalcBinSize ( int ( iMemoryLimit * fReadFactor ),
		dHitBlocks.GetLength() + m_pDict->GetSettings().m_bWordDict, "sort_hits" );

	CSphFixedVector <BYTE> dRelocationBuffer ( iRelocationSize );
	iSharedOffset = -1;

	ARRAY_FOREACH ( i, dHitBlocks )
	{
		dBins.Add ( new CSphBin ( m_tSettings.m_eHitless, m_pDict->GetSettings().m_bWordDict ) );
		dBins[i]->m_iFileLeft = dHitBlocks[i];
		dBins[i]->m_iFilePos = ( i==0 ) ? iHitsGap : dBins[i-1]->m_iFilePos + dBins[i-1]->m_iFileLeft;
		dBins[i]->Init ( fdHits.GetFD(), &iSharedOffset, iBinSize );
	}

	// if there were no hits, create zero-length index files
	int iRawBlocks = dBins.GetLength();

	//////////////////////////////
	// create new index files set
	//////////////////////////////

	tHitBuilder.CreateIndexFiles ( GetIndexFileName("spd").cstr(), GetIndexFileName("spp").cstr(),
		GetIndexFileName("spe").cstr(), m_bInplaceSettings, iWriteBuffer, fdHits, &iSharedOffset );

	// dict files
	CSphAutofile fdTmpDict ( GetIndexFileName("tmp8"), SPH_O_NEW, m_sLastError, true );
	CSphAutofile fdDict ( GetIndexFileName("spi"), SPH_O_NEW, m_sLastError, false );
	if ( fdTmpDict.GetFD()<0 || fdDict.GetFD()<0 )
		return 0;
	m_pDict->DictBegin ( fdTmpDict, fdDict, iBinSize,&g_tThrottle );

	// adjust min IDs, and fill header
	assert ( m_uMinDocid>0 );
	m_uMinDocid--;
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
		ARRAY_FOREACH ( i, m_dMinRow )
			m_dMinRow[i]--;

	tHitBuilder.SetMin ( m_dMinRow.Begin(), m_dMinRow.GetLength() );

	//////////////
	// final sort
	//////////////

	if ( iRawBlocks )
	{
		int iLastBin = dBins.GetLength () - 1;
		SphOffset_t iHitFileSize = dBins[iLastBin]->m_iFilePos + dBins [iLastBin]->m_iFileLeft;

		CSphHitQueue tQueue ( iRawBlocks );
		CSphAggregateHit tHit;

		// initialize hitlist encoder state
		tHitBuilder.HitReset();

		// initial fill
		int iRowitems = ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE ) ? m_tSchema.GetRowSize() : 0;
		CSphFixedVector<CSphRowitem> dInlineAttrs ( iRawBlocks*iRowitems );

		CSphFixedVector<BYTE> dActive ( iRawBlocks );
		for ( int i=0; i<iRawBlocks; i++ )
		{
			if ( !dBins[i]->ReadHit ( &tHit, iRowitems, dInlineAttrs.Begin() + i * iRowitems ) )
			{
				m_sLastError.SetSprintf ( "sort_hits: warmup failed (io error?)" );
				return 0;
			}
			dActive[i] = ( tHit.m_uWordID!=0 );
			if ( dActive[i] )
				tQueue.Push ( tHit, i );
		}

		// init progress meter
		m_tProgress.m_ePhase = CSphIndexProgress::PHASE_SORT;
		m_tProgress.m_iHits = 0;

		// while the queue has data for us
		// FIXME! analyze binsRead return code
		int iHitsSorted = 0;
		iMinBlock = -1;
		while ( tQueue.m_iUsed )
		{
			int iBin = tQueue.m_pData->m_iBin;

			// pack and emit queue root
			tQueue.m_pData->m_uDocID -= m_uMinDocid;

			if ( m_bInplaceSettings )
			{
				if ( iMinBlock==-1 || dBins[iMinBlock]->IsEOF () || !dActive[iMinBlock] )
				{
					iMinBlock = -1;
					ARRAY_FOREACH ( i, dBins )
						if ( !dBins[i]->IsEOF () && dActive[i] && ( iMinBlock==-1 || dBins[i]->m_iFilePos < dBins[iMinBlock]->m_iFilePos ) )
							iMinBlock = i;
				}

				int iToWriteMax = 3*sizeof(DWORD);
				if ( iMinBlock!=-1 && ( tHitBuilder.GetHitfilePos() + iToWriteMax ) > dBins[iMinBlock]->m_iFilePos )
				{
					if ( !RelocateBlock ( fdHits.GetFD (), dRelocationBuffer.Begin(), iRelocationSize, &iHitFileSize, dBins[iMinBlock], &iSharedOffset ) )
						return 0;

					iMinBlock = (iMinBlock+1) % dBins.GetLength ();
				}
			}

			tHitBuilder.cidxHit ( tQueue.m_pData, iRowitems ? dInlineAttrs.Begin() + iBin * iRowitems : NULL );
			if ( tHitBuilder.IsError() )
				return 0;

			// pop queue root and push next hit from popped bin
			tQueue.Pop ();
			if ( dActive[iBin] )
			{
				dBins[iBin]->ReadHit ( &tHit, iRowitems, dInlineAttrs.Begin() + iBin * iRowitems );
				dActive[iBin] = ( tHit.m_uWordID!=0 );
				if ( dActive[iBin] )
					tQueue.Push ( tHit, iBin );
			}

			// progress
			if ( ++iHitsSorted==1000000 )
			{
				m_tProgress.m_iHits += iHitsSorted;
				m_tProgress.Show ( false );
				iHitsSorted = 0;
			}
		}

		m_tProgress.m_iHits = m_tProgress.m_iHitsTotal; // sum might be less than total because of dupes!
		m_tProgress.Show ( true );

		ARRAY_FOREACH ( i, dBins )
			SafeDelete ( dBins[i] );
		dBins.Reset ();

		CSphAggregateHit tFlush;
		tFlush.m_uDocID = 0;
		tFlush.m_uWordID = 0;
		tFlush.m_sKeyword = NULL;
		tFlush.m_iWordPos = EMPTY_HIT;
		tFlush.m_dFieldMask.UnsetAll();
		tHitBuilder.cidxHit ( &tFlush, NULL );

		if ( m_bInplaceSettings )
		{
			tHitBuilder.CloseHitlist();
			if ( !sphTruncate ( fdHits.GetFD () ) )
				sphWarn ( "failed to truncate %s", fdHits.GetFilename() );
		}
	}

	if ( iDupes )
		sphWarn ( "%d duplicate document id pairs found", iDupes );

	BuildHeader_t tBuildHeader ( m_tStats );
	if ( !tHitBuilder.cidxDone ( iMemoryLimit, m_tSettings.m_iMinInfixLen, m_pTokenizer->GetMaxCodepointLength(), &tBuildHeader ) )
		return 0;

	tBuildHeader.m_sHeaderExtension = "sph";
	tBuildHeader.m_pMinRow = m_dMinRow.Begin();
	tBuildHeader.m_uMinDocid = m_uMinDocid;
	tBuildHeader.m_pThrottle =&g_tThrottle;
	tBuildHeader.m_uKillListSize = uKillistSize;
	tBuildHeader.m_iMinMaxIndex = m_iMinMaxIndex;
	tBuildHeader.m_iTotalDups = iDupes;

	// we're done
	if ( !BuildDone ( tBuildHeader, m_sLastError ) )
		return 0;

	// when the party's over..
	ARRAY_FOREACH ( i, dSources )
		dSources[i]->PostIndex ();

	dFileWatchdog.AllIsDone();
	return 1;
} // NOLINT function length



static void CopyRowString(const BYTE* pBase, const CSphVector<CSphAttrLocator>& dString, CSphRowitem* pRow, CSphWriter& wrTo)
{
	if (!dString.GetLength())
		return;

	CSphRowitem* pAttr = DOCINFO2ATTRS(pRow);
	ARRAY_FOREACH(i, dString)
	{
		SphAttr_t uOff = sphGetRowAttr(pAttr, dString[i]);
		// magic offset? do nothing
		if (!uOff)
			continue;

		const BYTE* pStr = NULL;
		int iLen = sphUnpackStr(pBase + uOff, &pStr);

		// no data? do nothing
		if (!iLen)
			continue;

		// copy bytes
		uOff = (SphAttr_t)wrTo.GetPos();
		assert(uOff < UINT_MAX);
		sphSetRowAttr(pAttr, dString[i], uOff);

		BYTE dPackedLen[4];
		int iLenLen = sphPackStrlen(dPackedLen, iLen);
		wrTo.PutBytes(&dPackedLen, iLenLen);
		wrTo.PutBytes(pStr, iLen);
	}
}

static void CopyRowMVA(const DWORD* pBase, const CSphVector<CSphAttrLocator>& dMva,
	SphDocID_t uDocid, CSphRowitem* pRow, CSphWriter& wrTo)
{
	if (!dMva.GetLength())
		return;

	CSphRowitem* pAttr = DOCINFO2ATTRS(pRow);
	bool bDocidWriten = false;
	ARRAY_FOREACH(i, dMva)
	{
		SphAttr_t uOff = sphGetRowAttr(pAttr, dMva[i]);
		if (!uOff)
			continue;

		assert(pBase);
		if (!bDocidWriten)
		{
			assert(DOCINFO2ID(pBase + uOff - DOCINFO_IDSIZE) == uDocid); // there is DocID prior to 1st MVA
			wrTo.PutDocid(uDocid);
			bDocidWriten = true;
		}

		assert(wrTo.GetPos() / sizeof(DWORD) <= UINT_MAX);
		SphAttr_t uNewOff = (DWORD)wrTo.GetPos() / sizeof(DWORD);
		sphSetRowAttr(pAttr, dMva[i], uNewOff);

		DWORD iValues = pBase[uOff];
		wrTo.PutBytes(pBase + uOff, (iValues + 1) * sizeof(DWORD));
	}
}





static ISphFilter* CreateMergeFilters(const CSphVector<CSphFilterSettings>& dSettings,
	const CSphSchema& tSchema, const DWORD* pMvaPool, const BYTE* pStrings, bool bArenaProhibit)
{
	CSphString sError, sWarning;
	ISphFilter* pResult = NULL;
	ARRAY_FOREACH(i, dSettings)
	{
		ISphFilter* pFilter = sphCreateFilter(dSettings[i], tSchema, pMvaPool, pStrings, sError, sWarning, SPH_COLLATION_DEFAULT, bArenaProhibit);
		if (pFilter)
			pResult = sphJoinFilters(pResult, pFilter);
	}
	return pResult;
}

static bool CheckDocsCount(int64_t iDocs, CSphString& sError)
{
	if (iDocs < INT_MAX)
		return true;

	sError.SetSprintf("index over %d documents not supported (got " INT64_FMT " documents)", INT_MAX, iDocs);
	return false;
}


//mergers


template < typename QWORDDST, typename QWORDSRC >
bool CSphIndex_VLN::MergeWords ( const CSphIndex_VLN * pDstIndex, const CSphIndex_VLN * pSrcIndex,
								const ISphFilter * pFilter, const CSphVector<SphDocID_t> & dKillList, SphDocID_t uMinID,
								CSphHitBuilder * pHitBuilder, CSphString & sError, CSphSourceStats & tStat,
								CSphIndexProgress & tProgress, ThrottleState_t * pThrottle, volatile bool * pGlobalStop, volatile bool * pLocalStop )
{
	CSphAutofile tDummy;
	pHitBuilder->CreateIndexFiles ( pDstIndex->GetIndexFileName("tmp.spd").cstr(),
		pDstIndex->GetIndexFileName("tmp.spp").cstr(),
		pDstIndex->GetIndexFileName("tmp.spe").cstr(),
		false, 0, tDummy, NULL );

	CSphDictReader tDstReader;
	CSphDictReader tSrcReader;

	bool bWordDict = pHitBuilder->IsWordDict();

	if ( !tDstReader.Setup ( pDstIndex->GetIndexFileName("spi"), pDstIndex->m_tWordlist.m_iWordsEnd,
		pDstIndex->m_tSettings.m_eHitless, sError, bWordDict, pThrottle, pDstIndex->m_tWordlist.m_bHaveSkips ) )
			return false;
	if ( !tSrcReader.Setup ( pSrcIndex->GetIndexFileName("spi"), pSrcIndex->m_tWordlist.m_iWordsEnd,
		pSrcIndex->m_tSettings.m_eHitless, sError, bWordDict, pThrottle, pSrcIndex->m_tWordlist.m_bHaveSkips ) )
			return false;

	const SphDocID_t uDstMinID = pDstIndex->m_uMinDocid;
	const SphDocID_t uSrcMinID = pSrcIndex->m_uMinDocid;

	/// prepare for indexing
	pHitBuilder->HitblockBegin();
	pHitBuilder->HitReset();
	pHitBuilder->SetMin ( pDstIndex->m_dMinRow.Begin(), pDstIndex->m_dMinRow.GetLength() );

	/// setup qwords

	QWORDDST tDstQword ( false, false );
	QWORDSRC tSrcQword ( false, false );

	CSphAutofile tSrcDocs, tSrcHits;
	tSrcDocs.Open ( pSrcIndex->GetIndexFileName("spd"), SPH_O_READ, sError );
	tSrcHits.Open ( pSrcIndex->GetIndexFileName("spp"), SPH_O_READ, sError );

	CSphAutofile tDstDocs, tDstHits;
	tDstDocs.Open ( pDstIndex->GetIndexFileName("spd"), SPH_O_READ, sError );
	tDstHits.Open ( pDstIndex->GetIndexFileName("spp"), SPH_O_READ, sError );

	if ( !sError.IsEmpty() || *pGlobalStop || *pLocalStop )
		return false;

	int iDstInlineSize = pDstIndex->m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE ? pDstIndex->m_tSchema.GetRowSize() : 0;
	int iSrcInlineSize = pSrcIndex->m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE ? pSrcIndex->m_tSchema.GetRowSize() : 0;

	CSphMerger tMerger ( pHitBuilder, Max ( iDstInlineSize, iSrcInlineSize ), uMinID );

	CSphMerger::ConfigureQword<QWORDDST> ( tDstQword, tDstHits, tDstDocs,
		pDstIndex->m_tSchema.GetDynamicSize(), iDstInlineSize,
		pDstIndex->m_dMinRow.Begin(), pThrottle );
	CSphMerger::ConfigureQword<QWORDSRC> ( tSrcQword, tSrcHits, tSrcDocs,
		pSrcIndex->m_tSchema.GetDynamicSize(), iSrcInlineSize,
		pSrcIndex->m_dMinRow.Begin(), pThrottle );

	/// merge

	bool bDstWord = tDstReader.Read();
	bool bSrcWord = tSrcReader.Read();

	tProgress.m_ePhase = CSphIndexProgress::PHASE_MERGE;
	tProgress.Show ( false );

	int iWords = 0;
	int iHitlistsDiscarded = 0;
	for ( ; bDstWord || bSrcWord; iWords++ )
	{
		if ( iWords==1000 )
		{
			tProgress.m_iWords += 1000;
			tProgress.Show ( false );
			iWords = 0;
		}

		if ( *pGlobalStop || *pLocalStop )
			return false;

		const int iCmp = tDstReader.CmpWord ( tSrcReader );

		if ( !bSrcWord || ( bDstWord && iCmp<0 ) )
		{
			// transfer documents and hits from destination
			CSphMerger::PrepareQword<QWORDDST> ( tDstQword, tDstReader, uDstMinID, bWordDict );
			tMerger.TransferData<QWORDDST> ( tDstQword, tDstReader.m_uWordID, tDstReader.GetWord(), pDstIndex, pFilter, dKillList, pGlobalStop, pLocalStop );
			bDstWord = tDstReader.Read();

		} else if ( !bDstWord || ( bSrcWord && iCmp>0 ) )
		{
			// transfer documents and hits from source
			CSphMerger::PrepareQword<QWORDSRC> ( tSrcQword, tSrcReader, uSrcMinID, bWordDict );
			tMerger.TransferData<QWORDSRC> ( tSrcQword, tSrcReader.m_uWordID, tSrcReader.GetWord(), pSrcIndex, NULL, CSphVector<SphDocID_t>(), pGlobalStop, pLocalStop );
			bSrcWord = tSrcReader.Read();

		} else // merge documents and hits inside the word
		{
			assert ( iCmp==0 );

			bool bHitless = !tDstReader.m_bHasHitlist;
			if ( tDstReader.m_bHasHitlist!=tSrcReader.m_bHasHitlist )
			{
				iHitlistsDiscarded++;
				bHitless = true;
			}

			CSphMerger::PrepareQword<QWORDDST> ( tDstQword, tDstReader, uDstMinID, bWordDict );
			CSphMerger::PrepareQword<QWORDSRC> ( tSrcQword, tSrcReader, uSrcMinID, bWordDict );

			CSphAggregateHit tHit;
			tHit.m_uWordID = tDstReader.m_uWordID; // !COMMIT m_sKeyword anyone?
			tHit.m_sKeyword = tDstReader.GetWord();
			tHit.m_dFieldMask.UnsetAll();

			bool bDstDocs = tMerger.NextDocument ( tDstQword, pDstIndex, pFilter, dKillList );
			bool bSrcDocs = true;

			tSrcQword.GetNextDoc ( tMerger.AcquireInline() );
			tSrcQword.SeekHitlist ( tSrcQword.m_iHitlistPos );

			while ( bDstDocs || bSrcDocs )
			{
				if ( *pGlobalStop || *pLocalStop )
					return false;

				if ( !bSrcDocs || ( bDstDocs && tDstQword.m_tDoc.m_uDocID < tSrcQword.m_tDoc.m_uDocID ) )
				{
					// transfer hits from destination
					if ( bHitless )
					{
						while ( tDstQword.m_bHasHitlist && tDstQword.GetNextHit()!=EMPTY_HIT );

						tHit.m_uDocID = tDstQword.m_tDoc.m_uDocID - uMinID;
						tHit.m_dFieldMask = tDstQword.m_dQwordFields;
						tHit.SetAggrCount ( tDstQword.m_uMatchHits );
						pHitBuilder->cidxHit ( &tHit, tMerger.GetInline() );
					} else
						tMerger.TransferHits ( tDstQword, tHit );
					bDstDocs = tMerger.NextDocument ( tDstQword, pDstIndex, pFilter, dKillList );

				} else if ( !bDstDocs || ( bSrcDocs && tDstQword.m_tDoc.m_uDocID > tSrcQword.m_tDoc.m_uDocID ) )
				{
					// transfer hits from source
					if ( bHitless )
					{
						while ( tSrcQword.m_bHasHitlist && tSrcQword.GetNextHit()!=EMPTY_HIT );

						tHit.m_uDocID = tSrcQword.m_tDoc.m_uDocID - uMinID;
						tHit.m_dFieldMask = tSrcQword.m_dQwordFields;
						tHit.SetAggrCount ( tSrcQword.m_uMatchHits );
						pHitBuilder->cidxHit ( &tHit, tMerger.GetInline() );
					} else
						tMerger.TransferHits ( tSrcQword, tHit );
					bSrcDocs = tMerger.NextDocument ( tSrcQword, pSrcIndex, NULL, CSphVector<SphDocID_t>() );

				} else
				{
					// merge hits inside the document
					assert ( bDstDocs );
					assert ( bSrcDocs );
					assert ( tDstQword.m_tDoc.m_uDocID==tSrcQword.m_tDoc.m_uDocID );

					tHit.m_uDocID = tDstQword.m_tDoc.m_uDocID - uMinID;

					if ( bHitless )
					{
						while ( tDstQword.m_bHasHitlist && tDstQword.GetNextHit()!=EMPTY_HIT );
						while ( tSrcQword.m_bHasHitlist && tSrcQword.GetNextHit()!=EMPTY_HIT );

						for ( int i=0; i<FieldMask_t::SIZE; i++ )
							tHit.m_dFieldMask[i] = tDstQword.m_dQwordFields[i] | tSrcQword.m_dQwordFields[i];
						tHit.SetAggrCount ( tDstQword.m_uMatchHits + tSrcQword.m_uMatchHits );
						pHitBuilder->cidxHit ( &tHit, tMerger.GetInline() );

					} else
					{
						Hitpos_t uDstHit = tDstQword.GetNextHit();
						Hitpos_t uSrcHit = tSrcQword.GetNextHit();

						while ( uDstHit!=EMPTY_HIT || uSrcHit!=EMPTY_HIT )
						{
							if ( uSrcHit==EMPTY_HIT || ( uDstHit!=EMPTY_HIT && uDstHit<uSrcHit ) )
							{
								tHit.m_iWordPos = uDstHit;
								pHitBuilder->cidxHit ( &tHit, tMerger.GetInline() );
								uDstHit = tDstQword.GetNextHit();

							} else if ( uDstHit==EMPTY_HIT || ( uSrcHit!=EMPTY_HIT && uSrcHit<uDstHit ) )
							{
								tHit.m_iWordPos = uSrcHit;
								pHitBuilder->cidxHit ( &tHit, tMerger.GetInline() );
								uSrcHit = tSrcQword.GetNextHit();

							} else
							{
								assert ( uDstHit==uSrcHit );

								tHit.m_iWordPos = uDstHit;
								pHitBuilder->cidxHit ( &tHit, tMerger.GetInline() );

								uDstHit = tDstQword.GetNextHit();
								uSrcHit = tSrcQword.GetNextHit();
							}
						}
					}

					// next document
					bDstDocs = tMerger.NextDocument ( tDstQword, pDstIndex, pFilter, dKillList );
					bSrcDocs = tMerger.NextDocument ( tSrcQword, pSrcIndex, NULL, CSphVector<SphDocID_t>() );
				}
			}
			// next word
			bDstWord = tDstReader.Read();
			bSrcWord = tSrcReader.Read();
		}
	}

	tStat.m_iTotalDocuments += pSrcIndex->m_tStats.m_iTotalDocuments;
	tStat.m_iTotalBytes += pSrcIndex->m_tStats.m_iTotalBytes;

	tProgress.m_iWords += iWords;
	tProgress.Show ( false );

	if ( iHitlistsDiscarded )
		sphWarning ( "discarded hitlists for %u words", iHitlistsDiscarded );

	return true;
}


bool CSphIndex_VLN::Merge ( CSphIndex * pSource, const CSphVector<CSphFilterSettings> & dFilters, bool bMergeKillLists )
{
	SetMemorySettings ( false, true, true );
	if ( !Prealloc ( false ) )
		return false;
	Preread ();
	pSource->SetMemorySettings ( false, true, true );
	if ( !pSource->Prealloc ( false ) )
	{
		m_sLastError.SetSprintf ( "source index preload failed: %s", pSource->GetLastError().cstr() );
		return false;
	}
	pSource->Preread();

	// create filters
	CSphScopedPtr<ISphFilter> pFilter ( CreateMergeFilters ( dFilters, m_tSchema, m_tMva.GetWritePtr(), m_tString.GetWritePtr(), m_bArenaProhibit ) );
	CSphVector<SphDocID_t> dKillList ( pSource->GetKillListSize()+2 );
	for ( int i=0; i<dKillList.GetLength()-2; ++i )
		dKillList [ i+1 ] = pSource->GetKillList()[i];
	dKillList[0] = 0;
	dKillList.Last() = DOCID_MAX;

	bool bGlobalStop = false;
	bool bLocalStop = false;
	return CSphIndex_VLN::DoMerge ( this, (const CSphIndex_VLN *)pSource, bMergeKillLists, pFilter.Ptr(),
									dKillList, m_sLastError, m_tProgress,&g_tThrottle, &bGlobalStop, &bLocalStop );
}

bool CSphIndex_VLN::DoMerge ( const CSphIndex_VLN * pDstIndex, const CSphIndex_VLN * pSrcIndex,
							bool bMergeKillLists, ISphFilter * pFilter, const CSphVector<SphDocID_t> & dKillList
							, CSphString & sError, CSphIndexProgress & tProgress, ThrottleState_t * pThrottle,
							volatile bool * pGlobalStop, volatile bool * pLocalStop )
{
	assert ( pDstIndex && pSrcIndex );

	const CSphSchema & tDstSchema = pDstIndex->m_tSchema;
	const CSphSchema & tSrcSchema = pSrcIndex->m_tSchema;
	if ( !tDstSchema.CompareTo ( tSrcSchema, sError ) )
		return false;

	if ( pDstIndex->m_tSettings.m_eHitless!=pSrcIndex->m_tSettings.m_eHitless )
	{
		sError = "hitless settings must be the same on merged indices";
		return false;
	}

	// FIXME!
	if ( pDstIndex->m_tSettings.m_eDocinfo!=pSrcIndex->m_tSettings.m_eDocinfo && !( pDstIndex->m_bIsEmpty || pSrcIndex->m_bIsEmpty ) )
	{
		sError.SetSprintf ( "docinfo storage on non-empty indexes must be the same (dst docinfo %d, empty %d, src docinfo %d, empty %d",
			pDstIndex->m_tSettings.m_eDocinfo, pDstIndex->m_bIsEmpty, pSrcIndex->m_tSettings.m_eDocinfo, pSrcIndex->m_bIsEmpty );
		return false;
	}

	if ( pDstIndex->m_pDict->GetSettings().m_bWordDict!=pSrcIndex->m_pDict->GetSettings().m_bWordDict )
	{
		sError.SetSprintf ( "dictionary types must be the same (dst dict=%s, src dict=%s )",
			pDstIndex->m_pDict->GetSettings().m_bWordDict ? "keywords" : "crc",
			pSrcIndex->m_pDict->GetSettings().m_bWordDict ? "keywords" : "crc" );
		return false;
	}

	BuildHeader_t tBuildHeader ( pDstIndex->m_tStats );

	/////////////////////////////////////////
	// merging attributes (.spa, .spm, .sps)
	/////////////////////////////////////////

	CSphWriter tSPMWriter, tSPSWriter;
	tSPMWriter.SetThrottle ( pThrottle );
	tSPSWriter.SetThrottle ( pThrottle );
	if ( !tSPMWriter.OpenFile ( pDstIndex->GetIndexFileName("tmp.spm"), sError )
		|| !tSPSWriter.OpenFile ( pDstIndex->GetIndexFileName("tmp.sps"), sError ) )
	{
		return false;
	}
	tSPSWriter.PutByte ( 0 ); // dummy byte, to reserve magic zero offset

	/// merging
	CSphVector<CSphAttrLocator> dMvaLocators;
	CSphVector<CSphAttrLocator> dStringLocators;
	for ( int i=0; i<tDstSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tInfo = tDstSchema.GetAttr(i);
		if ( tInfo.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
			dMvaLocators.Add ( tInfo.m_tLocator );
		if ( tInfo.m_eAttrType==ESphAttr::SPH_ATTR_STRING || tInfo.m_eAttrType==ESphAttr::SPH_ATTR_JSON )
			dStringLocators.Add ( tInfo.m_tLocator );
	}
	for ( int i=0; i<tDstSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tInfo = tDstSchema.GetAttr(i);
		if ( tInfo.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
			dMvaLocators.Add ( tInfo.m_tLocator );
	}

	CSphVector<SphDocID_t> dPhantomKiller;

	int64_t iTotalDocuments = 0;
	bool bNeedInfinum = true;
	// minimal docid-1 for merging
	SphDocID_t uMergeInfinum = 0;

	if ( pDstIndex->m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && pSrcIndex->m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN )
	{
		int iStride = DOCINFO_IDSIZE + pDstIndex->m_tSchema.GetRowSize();
		CSphFixedVector<CSphRowitem> dRow ( iStride );

		CSphWriter wrRows;
		wrRows.SetThrottle ( pThrottle );
		if ( !wrRows.OpenFile ( pDstIndex->GetIndexFileName("tmp.spa"), sError ) )
			return false;

		int64_t iExpectedDocs = pDstIndex->m_tStats.m_iTotalDocuments + pSrcIndex->GetStats().m_iTotalDocuments;
		AttrIndexBuilder_c tMinMax ( pDstIndex->m_tSchema );
		int64_t iMinMaxSize = tMinMax.GetExpectedSize ( iExpectedDocs );
		if ( iMinMaxSize>INT_MAX || iExpectedDocs>INT_MAX )
		{
			if ( iMinMaxSize>INT_MAX )
				sError.SetSprintf ( "attribute files over 128 GB are not supported (projected_minmax_size=" INT64_FMT ")", iMinMaxSize );
			else if ( iExpectedDocs>INT_MAX )
				sError.SetSprintf ( "indexes over 2B docs are not supported (projected_docs=" INT64_FMT ")", iExpectedDocs );
			return false;
		}
		CSphFixedVector<DWORD> dMinMaxBuffer ( (int)iMinMaxSize );
		tMinMax.Prepare ( dMinMaxBuffer.Begin(), dMinMaxBuffer.Begin() + dMinMaxBuffer.GetLength() ); // FIXME!!! for over INT_MAX blocks

		const DWORD * pSrcRow = pSrcIndex->m_tAttr.GetWritePtr(); // they *can* be null if the respective index is empty
		const DWORD * pDstRow = pDstIndex->m_tAttr.GetWritePtr();

		int64_t iSrcCount = 0;
		int64_t iDstCount = 0;

		int iKillListIdx = 0;

		CSphMatch tMatch;
		while ( iSrcCount < pSrcIndex->m_iDocinfo || iDstCount < pDstIndex->m_iDocinfo )
		{
			if ( *pGlobalStop || *pLocalStop )
				return false;

			SphDocID_t iDstDocID, iSrcDocID;

			if ( iDstCount < pDstIndex->m_iDocinfo )
			{
				iDstDocID = DOCINFO2ID ( pDstRow );

				// kill list filter goes first
				while ( dKillList [ iKillListIdx ]<iDstDocID )
					iKillListIdx++;
				if ( dKillList [ iKillListIdx ]==iDstDocID )
				{
					pDstRow += iStride;
					iDstCount++;
					continue;
				}

				if ( pFilter )
				{
					tMatch.m_uDocID = iDstDocID;
					tMatch.m_pStatic = DOCINFO2ATTRS ( pDstRow );
					tMatch.m_pDynamic = NULL;
					if ( !pFilter->Eval ( tMatch ) )
					{
						pDstRow += iStride;
						iDstCount++;
						continue;
					}
				}
			} else
				iDstDocID = 0;

			if ( iSrcCount < pSrcIndex->m_iDocinfo )
				iSrcDocID = DOCINFO2ID ( pSrcRow );
			else
				iSrcDocID = 0;

			if ( ( iDstDocID && iDstDocID < iSrcDocID ) || ( iDstDocID && !iSrcDocID ) )
			{
				Verify ( tMinMax.Collect ( pDstRow, pDstIndex->m_tMva.GetWritePtr(), pDstIndex->m_tMva.GetNumEntries(), sError, true ) );

				if ( dMvaLocators.GetLength() || dStringLocators.GetLength() )
				{
					memcpy ( dRow.Begin(), pDstRow, iStride * sizeof ( CSphRowitem ) );
					CopyRowMVA ( pDstIndex->m_tMva.GetWritePtr(), dMvaLocators, iDstDocID, dRow.Begin(), tSPMWriter );
					CopyRowString ( pDstIndex->m_tString.GetWritePtr(), dStringLocators, dRow.Begin(), tSPSWriter );
					wrRows.PutBytes ( dRow.Begin(), sizeof(DWORD)*iStride );
				} else
				{
					wrRows.PutBytes ( pDstRow, sizeof(DWORD)*iStride );
				}

				tBuildHeader.m_iMinMaxIndex += iStride;
				pDstRow += iStride;
				iDstCount++;
				iTotalDocuments++;
				if ( bNeedInfinum )
				{
					bNeedInfinum = false;
					uMergeInfinum = iDstDocID - 1;
				}

			} else if ( iSrcDocID )
			{
				Verify ( tMinMax.Collect ( pSrcRow, pSrcIndex->m_tMva.GetWritePtr(), pSrcIndex->m_tMva.GetNumEntries(), sError, true ) );

				if ( dMvaLocators.GetLength() || dStringLocators.GetLength() )
				{
					memcpy ( dRow.Begin(), pSrcRow, iStride * sizeof ( CSphRowitem ) );
					CopyRowMVA ( pSrcIndex->m_tMva.GetWritePtr(), dMvaLocators, iSrcDocID, dRow.Begin(), tSPMWriter );
					CopyRowString ( pSrcIndex->m_tString.GetWritePtr(), dStringLocators, dRow.Begin(), tSPSWriter );
					wrRows.PutBytes ( dRow.Begin(), sizeof(DWORD)*iStride );
				} else
				{
					wrRows.PutBytes ( pSrcRow, sizeof(DWORD)*iStride );
				}

				tBuildHeader.m_iMinMaxIndex += iStride;
				pSrcRow += iStride;
				iSrcCount++;
				iTotalDocuments++;
				if ( bNeedInfinum )
				{
					bNeedInfinum = false;
					uMergeInfinum = iSrcDocID - 1;
				}

				if ( iDstDocID==iSrcDocID )
				{
					dPhantomKiller.Add ( iSrcDocID );
					pDstRow += iStride;
					iDstCount++;
				}
			}
		}

		if ( iTotalDocuments )
		{
			tMinMax.FinishCollect();
			iMinMaxSize = tMinMax.GetActualSize() * sizeof(DWORD);
			wrRows.PutBytes ( dMinMaxBuffer.Begin(), iMinMaxSize );
		}
		wrRows.CloseFile();
		if ( wrRows.IsError() )
			return false;

	} else if ( pDstIndex->m_bIsEmpty || pSrcIndex->m_bIsEmpty )
	{
		// one of the indexes has no documents; copy the .spa file from the other one
		CSphString sSrc = !pDstIndex->m_bIsEmpty ? pDstIndex->GetIndexFileName("spa") : pSrcIndex->GetIndexFileName("spa");
		CSphString sDst = pDstIndex->GetIndexFileName("tmp.spa");

		if ( !CopyFile ( sSrc.cstr(), sDst.cstr(), sError, pThrottle, pGlobalStop, pLocalStop ) )
			return false;

	} else
	{
		// storage is not extern; create dummy .spa file
		CSphAutofile fdSpa ( pDstIndex->GetIndexFileName("tmp.spa"), SPH_O_NEW, sError );
		fdSpa.Close();
	}

	if ( !CheckDocsCount ( iTotalDocuments, sError ) )
		return false;

	if ( tSPSWriter.GetPos()>SphOffset_t( U64C(1)<<32 ) )
	{
		sError.SetSprintf ( "resulting .sps file is over 4 GB" );
		return false;
	}

	if ( tSPMWriter.GetPos()>SphOffset_t( U64C(4)<<32 ) )
	{
		sError.SetSprintf ( "resulting .spm file is over 16 GB" );
		return false;
	}

	int iOldLen = dPhantomKiller.GetLength();
	int iKillLen = dKillList.GetLength();
	dPhantomKiller.Resize ( iOldLen+iKillLen );
	memcpy ( dPhantomKiller.Begin()+iOldLen, dKillList.Begin(), sizeof(SphDocID_t)*iKillLen );
	dPhantomKiller.Uniq();

	CSphAutofile tTmpDict ( pDstIndex->GetIndexFileName("tmp8.spi"), SPH_O_NEW, sError, true );
	CSphAutofile tDict ( pDstIndex->GetIndexFileName("tmp.spi"), SPH_O_NEW, sError );

	if ( !sError.IsEmpty() || tTmpDict.GetFD()<0 || tDict.GetFD()<0 || *pGlobalStop || *pLocalStop )
		return false;

	CSphScopedPtr<CSphDict> pDict ( pDstIndex->m_pDict->Clone() );

	int iHitBufferSize = 8 * 1024 * 1024;
	CSphVector<SphWordID_t> dDummy;
	CSphHitBuilder tHitBuilder ( pDstIndex->m_tSettings, dDummy, true, iHitBufferSize, pDict.Ptr(), &sError );
	tHitBuilder.SetThrottle ( pThrottle );

	CSphFixedVector<CSphRowitem> dMinRow ( pDstIndex->m_dMinRow.GetLength() );
	memcpy ( dMinRow.Begin(), pDstIndex->m_dMinRow.Begin(), sizeof(CSphRowitem)*dMinRow.GetLength() );
	// correct infinum might be already set during spa merging.
	SphDocID_t uMinDocid = ( !uMergeInfinum ) ? Min ( pDstIndex->m_uMinDocid, pSrcIndex->m_uMinDocid ) : uMergeInfinum;
	tBuildHeader.m_uMinDocid = uMinDocid;
	tBuildHeader.m_pMinRow = dMinRow.Begin();

	// FIXME? is this magic dict block constant any good?..
	pDict->DictBegin ( tTmpDict, tDict, iHitBufferSize, pThrottle );

	// merge dictionaries, doclists and hitlists
	if ( pDict->GetSettings().m_bWordDict )
	{
		WITH_QWORD ( pDstIndex, false, QwordDst,
			WITH_QWORD ( pSrcIndex, false, QwordSrc,
		{
			if ( !CSphIndex_VLN::MergeWords < QwordDst, QwordSrc > ( pDstIndex, pSrcIndex, pFilter, dPhantomKiller,
																	uMinDocid, &tHitBuilder, sError, tBuildHeader,
																	tProgress, pThrottle, pGlobalStop, pLocalStop ) )
				return false;
		} ) );
	} else
	{
		WITH_QWORD ( pDstIndex, true, QwordDst,
			WITH_QWORD ( pSrcIndex, true, QwordSrc,
		{
			if ( !CSphIndex_VLN::MergeWords < QwordDst, QwordSrc > ( pDstIndex, pSrcIndex, pFilter, dPhantomKiller
																	, uMinDocid, &tHitBuilder, sError, tBuildHeader,
																	tProgress,	pThrottle, pGlobalStop, pLocalStop ) )
				return false;
		} ) );
	}

	if ( iTotalDocuments )
		tBuildHeader.m_iTotalDocuments = iTotalDocuments;

	// merge kill-lists
	CSphAutofile tKillList ( pDstIndex->GetIndexFileName("tmp.spk"), SPH_O_NEW, sError );
	if ( tKillList.GetFD () < 0 )
		return false;

	if ( bMergeKillLists )
	{
		// merge spk
		CSphVector<SphDocID_t> dKillList;
		dKillList.Reserve ( pDstIndex->GetKillListSize()+pSrcIndex->GetKillListSize() );
		for ( int i=0; i<pSrcIndex->GetKillListSize(); i++ ) dKillList.Add ( pSrcIndex->GetKillList()[i] );
		for ( int i=0; i<pDstIndex->GetKillListSize(); i++ ) dKillList.Add ( pDstIndex->GetKillList()[i] );
		dKillList.Uniq ();

		tBuildHeader.m_uKillListSize = dKillList.GetLength ();

		if ( *pGlobalStop || *pLocalStop )
			return false;

		if ( dKillList.GetLength() )
		{
			if ( !sphWriteThrottled ( tKillList.GetFD(), &dKillList[0], dKillList.GetLength()*sizeof(SphDocID_t), "kill_list", sError, pThrottle ) )
				return false;
		}
	}

	tKillList.Close ();

	if ( *pGlobalStop || *pLocalStop )
		return false;

	// finalize
	CSphAggregateHit tFlush;
	tFlush.m_uDocID = 0;
	tFlush.m_uWordID = 0;
	tFlush.m_sKeyword = (BYTE*)""; // tricky: assertion in cidxHit calls strcmp on this in case of empty index!
	tFlush.m_iWordPos = EMPTY_HIT;
	tFlush.m_dFieldMask.UnsetAll();
	tHitBuilder.cidxHit ( &tFlush, NULL );

	if ( !tHitBuilder.cidxDone ( iHitBufferSize, pDstIndex->m_tSettings.m_iMinInfixLen,
								pDstIndex->m_pTokenizer->GetMaxCodepointLength(), &tBuildHeader ) )
		return false;

	tBuildHeader.m_sHeaderExtension = "tmp.sph";
	tBuildHeader.m_pThrottle = pThrottle;

	pDstIndex->BuildDone ( tBuildHeader, sError ); // FIXME? is this magic dict block constant any good?..

	// we're done
	tProgress.Show ( true );

	return true;
}


bool sphMerge ( const CSphIndex * pDst, const CSphIndex * pSrc, const CSphVector<SphDocID_t> & dKillList,
				CSphString & sError, CSphIndexProgress & tProgress, ThrottleState_t * pThrottle,
				volatile bool * pGlobalStop, volatile bool * pLocalStop )
{
	const CSphIndex_VLN * pDstIndex = (const CSphIndex_VLN *)pDst;
	const CSphIndex_VLN * pSrcIndex = (const CSphIndex_VLN *)pSrc;

	return CSphIndex_VLN::DoMerge ( pDstIndex, pSrcIndex, false, NULL, dKillList, sError, tProgress, pThrottle, pGlobalStop, pLocalStop );
}



inline bool sphGroupMatch ( SphAttr_t iGroup, const SphAttr_t * pGroups, int iGroups )
{
	if ( !pGroups ) return true;
	const SphAttr_t * pA = pGroups;
	const SphAttr_t * pB = pGroups+iGroups-1;
	if ( iGroup==*pA || iGroup==*pB ) return true;
	if ( iGroup<(*pA) || iGroup>(*pB) ) return false;

	while ( pB-pA>1 )
	{
		const SphAttr_t * pM = pA + ((pB-pA)/2);
		if ( iGroup==(*pM) )
			return true;
		if ( iGroup<(*pM) )
			pB = pM;
		else
			pA = pM;
	}
	return false;
}


bool CSphIndex_VLN::EarlyReject ( CSphQueryContext * pCtx, CSphMatch & tMatch ) const
{
	// might be needed even when we do not have a filter
	if ( pCtx->m_bLookupFilter )
	{
		const CSphRowitem * pRow = FindDocinfo ( tMatch.m_uDocID );
		if ( !pRow && m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN )
		{
			pCtx->m_iBadRows++;
			return true;
		}
		CopyDocinfo ( pCtx, tMatch, pRow );
	}
	pCtx->CalcFilter ( tMatch ); // FIXME!!! leak of filtered STRING_PTR

	return pCtx->m_pFilter ? !pCtx->m_pFilter->Eval ( tMatch ) : false;
}

SphDocID_t * CSphIndex_VLN::GetKillList () const
{
	return m_tKillList.GetWritePtr();
}

int CSphIndex_VLN::GetKillListSize () const
{
	return (int)m_tKillList.GetNumEntries();
}

bool CSphIndex_VLN::BuildDocList ( SphAttr_t ** ppDocList, int64_t * pCount, CSphString * pError ) const
{
	assert ( ppDocList && pCount && pError );
	*ppDocList = NULL;
	*pCount = 0;
	if ( !m_iDocinfo )
		return true;

	// new[] might fail on 32bit here
	int64_t iSizeMax = (size_t)m_iDocinfo;
	if ( iSizeMax!=m_iDocinfo )
	{
		pError->SetSprintf ( "doc-list build size_t overflow (docs count=" INT64_FMT ", size max=" INT64_FMT ")", m_iDocinfo, iSizeMax );
		return false;
	}

	int iStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	SphAttr_t * pDst = new SphAttr_t [(size_t)m_iDocinfo];
	*ppDocList = pDst;
	*pCount = m_iDocinfo;

	const CSphRowitem * pRow = m_tAttr.GetWritePtr();
	const CSphRowitem * pEnd = m_tAttr.GetWritePtr() + m_iDocinfo*iStride;
	while ( pRow<pEnd )
	{
		*pDst++ = DOCINFO2ID ( pRow );
		pRow += iStride;
	}

	return true;
}

bool CSphIndex_VLN::ReplaceKillList ( const SphDocID_t * pKillist, int iCount )
{
	// dump killlist
	CSphAutofile tKillList ( GetIndexFileName("spk.tmpnew"), SPH_O_NEW, m_sLastError );
	if ( tKillList.GetFD()<0 )
		return false;

	if ( !sphWriteThrottled ( tKillList.GetFD(), pKillist, sizeof(SphDocID_t)*iCount, "kill list", m_sLastError,&g_tThrottle ) )
		return false;

	tKillList.Close ();

	BuildHeader_t tBuildHeader ( m_tStats );
	(DictHeader_t &)tBuildHeader = (DictHeader_t)m_tWordlist;
	tBuildHeader.m_sHeaderExtension = "sph";
	tBuildHeader.m_pThrottle =&g_tThrottle;
	tBuildHeader.m_uMinDocid = m_uMinDocid;
	tBuildHeader.m_uKillListSize = iCount;
	tBuildHeader.m_iMinMaxIndex = m_iMinMaxIndex;

	if ( !BuildDone ( tBuildHeader, m_sLastError ) )
		return false;

	if ( !JuggleFile ( "spk", m_sLastError ) )
		return false;

	m_tKillList.Reset();
	if ( !m_tKillList.Setup ( GetIndexFileName("spk").cstr(), m_sLastError, true ) )
		return false;

	PrereadMapping ( m_sIndexName.cstr(), "kill-list", m_bMlock, m_bOndiskAllAttr, m_tKillList );
	return true;
}


bool CSphIndex_VLN::HasDocid ( SphDocID_t uDocid ) const
{
	return FindDocinfo ( uDocid )!=NULL;
}


const DWORD * CSphIndex_VLN::FindDocinfo ( SphDocID_t uDocID ) const
{
	if ( m_iDocinfo<=0 )
		return NULL;

	assert ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN );
	assert ( !m_tAttr.IsEmpty() );
	assert ( m_tSchema.GetAttrsCount() );

	int iStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
	int64_t iStart = 0;
	int64_t iEnd = m_iDocinfo-1;

#define LOC_ROW(_index) &m_tAttr [ _index*iStride ]
#define LOC_ID(_index) DOCINFO2ID(LOC_ROW(_index))

	// docinfo-hash got filled at read
	if ( m_bPassedRead && m_tDocinfoHash.GetLengthBytes() )
	{
		SphDocID_t uFirst = LOC_ID(0);
		SphDocID_t uLast = LOC_ID(iEnd);
		if ( uDocID<uFirst || uDocID>uLast )
			return NULL;

		int64_t iHash = ( ( uDocID - uFirst ) >> m_tDocinfoHash[0] );
		if ( iHash > ( 1 << DOCINFO_HASH_BITS ) ) // possible in case of broken data, for instance
			return NULL;

		iStart = m_tDocinfoHash [ iHash+1 ];
		iEnd = m_tDocinfoHash [ iHash+2 ] - 1;
	}

	if ( uDocID==LOC_ID(iStart) )
		return LOC_ROW(iStart);

	if ( uDocID==LOC_ID(iEnd) )
		return LOC_ROW(iEnd);

	while ( iEnd-iStart>1 )
	{
		// check if nothing found
		if ( uDocID<LOC_ID(iStart) || uDocID>LOC_ID(iEnd) )
			return NULL;
		assert ( uDocID > LOC_ID(iStart) );
		assert ( uDocID < LOC_ID(iEnd) );

		int64_t iMid = iStart + (iEnd-iStart)/2;
		if ( uDocID==LOC_ID(iMid) )
			return LOC_ROW(iMid);
		else if ( uDocID<LOC_ID(iMid) )
			iEnd = iMid;
		else
			iStart = iMid;
	}

#undef LOC_ID
#undef LOC_ROW

	return NULL;
}

void CSphIndex_VLN::CopyDocinfo ( const CSphQueryContext * pCtx, CSphMatch & tMatch, const DWORD * pFound ) const
{
	if ( !pFound )
		return;

	// setup static pointer
	assert ( DOCINFO2ID(pFound)==tMatch.m_uDocID );
	tMatch.m_pStatic = DOCINFO2ATTRS(pFound);

	// patch if necessary
	if ( pCtx->m_pOverrides )
		ARRAY_FOREACH ( i, (*pCtx->m_pOverrides) )
		{
			const CSphAttrOverride & tOverride = (*pCtx->m_pOverrides)[i]; // shortcut
			const CSphAttrOverride::IdValuePair_t * pEntry = tOverride.m_dValues.BinarySearch (
				bind ( &CSphAttrOverride::IdValuePair_t::m_uDocID ), tMatch.m_uDocID );
			tMatch.SetAttr ( pCtx->m_dOverrideOut[i], pEntry
							? pEntry->m_uValue
							: sphGetRowAttr ( tMatch.m_pStatic, pCtx->m_dOverrideIn[i] ) );
		}
}


void CSphIndex_VLN::MatchExtended ( CSphQueryContext * pCtx, const CSphQuery * pQuery, int iSorters, ISphMatchSorter ** ppSorters,
									ISphRanker * pRanker, int iTag, int iIndexWeight ) const
{
	CSphQueryProfile * pProfile = pCtx->m_pProfile;

	int iCutoff = pQuery->m_iCutoff;
	if ( iCutoff<=0 )
		iCutoff = -1;

	// do searching
	CSphMatch * pMatch = pRanker->GetMatchesBuffer();
	for ( ;; )
	{
		// ranker does profile switches internally in GetMatches()
		int iMatches = pRanker->GetMatches();
		if ( iMatches<=0 )
			break;

		if ( pProfile )
			pProfile->Switch ( SPH_QSTATE_SORT );
		for ( int i=0; i<iMatches; i++ )
		{
			if ( pCtx->m_bLookupSort )
			{
				const CSphRowitem * pRow = FindDocinfo ( pMatch[i].m_uDocID );
				if ( !pRow && m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN )
				{
					pCtx->m_iBadRows++;
					continue;
				}
				CopyDocinfo ( pCtx, pMatch[i], pRow );
			}

			pMatch[i].m_iWeight *= iIndexWeight;
			pCtx->CalcSort ( pMatch[i] );

			if ( pCtx->m_pWeightFilter && !pCtx->m_pWeightFilter->Eval ( pMatch[i] ) )
			{
				pCtx->FreeStrSort ( pMatch[i] );
				continue;
			}

			pMatch[i].m_iTag = iTag;

			bool bRand = false;
			bool bNewMatch = false;
			for ( int iSorter=0; iSorter<iSorters; iSorter++ )
			{
				// all non-random sorters are in the beginning,
				// so we can avoid the simple 'first-element' assertion
				if ( !bRand && ppSorters[iSorter]->m_bRandomize )
				{
					bRand = true;
					pMatch[i].m_iWeight = ( sphRand() & 0xffff ) * iIndexWeight;

					if ( pCtx->m_pWeightFilter && !pCtx->m_pWeightFilter->Eval ( pMatch[i] ) )
						break;
				}
				bNewMatch |= ppSorters[iSorter]->Push ( pMatch[i] );

				if ( pCtx->m_uPackedFactorFlags & SPH_FACTOR_ENABLE )
				{
					pRanker->ExtraData ( EXTRA_SET_MATCHPUSHED, (void**)&(ppSorters[iSorter]->m_iJustPushed) );
					pRanker->ExtraData ( EXTRA_SET_MATCHPOPPED, (void**)&(ppSorters[iSorter]->m_dJustPopped) );
				}
			}
			pCtx->FreeStrSort ( pMatch[i] );

			if ( bNewMatch )
				if ( --iCutoff==0 )
					break;
		}

		if ( iCutoff==0 )
			break;
	}

	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_UNKNOWN );
}


bool CSphIndex_VLN::MultiScan ( const CSphQuery * pQuery, CSphQueryResult * pResult,
	int iSorters, ISphMatchSorter ** ppSorters, const CSphMultiQueryArgs & tArgs ) const
{
	assert ( pQuery->m_sQuery.IsEmpty() );
	assert ( tArgs.m_iTag>=0 );

	// check if index is ready
	if ( !m_bPassedAlloc )
	{
		pResult->m_sError = "index not preread";
		return false;
	}

	// check if index supports scans
	if ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN || !m_tSchema.GetAttrsCount() )
	{
		pResult->m_sError = "fullscan requires extern docinfo";
		return false;
	}

	// we count documents only (before filters)
	if ( pQuery->m_iMaxPredictedMsec )
		pResult->m_bHasPrediction = true;

	if ( tArgs.m_uPackedFactorFlags & SPH_FACTOR_ENABLE )
		pResult->m_sWarning.SetSprintf ( "packedfactors() will not work with a fullscan; you need to specify a query" );

	// check if index has data
	if ( m_bIsEmpty || m_iDocinfo<=0 || m_tAttr.IsEmpty() )
		return true;

	// start counting
	int64_t tmQueryStart = sphMicroTimer();

	ScopedThreadPriority_c tPrio ( pQuery->m_bLowPriority );

	// select the sorter with max schema
	// uses GetAttrsCount to get working facets (was GetRowSize)
	int iMaxSchemaSize = -1;
	int iMaxSchemaIndex = -1;
	for ( int i=0; i<iSorters; i++ )
		if ( ppSorters[i]->GetSchema().GetAttrsCount() > iMaxSchemaSize )
		{
			iMaxSchemaSize = ppSorters[i]->GetSchema().GetAttrsCount();
			iMaxSchemaIndex = i;
		}

	// setup calculations and result schema
	CSphQueryContext tCtx ( *pQuery );
	if ( !tCtx.SetupCalc ( pResult, ppSorters[iMaxSchemaIndex]->GetSchema(), m_tSchema, m_tMva.GetWritePtr(), m_bArenaProhibit ) )
		return false;

	// set string pool for string on_sort expression fix up
	tCtx.SetStringPool ( m_tString.GetWritePtr() );

	// setup filters
	if ( !tCtx.CreateFilters ( true, &pQuery->m_dFilters, ppSorters[iMaxSchemaIndex]->GetSchema(),
		m_tMva.GetWritePtr(), m_tString.GetWritePtr(), pResult->m_sError, pResult->m_sWarning, pQuery->m_eCollation, m_bArenaProhibit, tArgs.m_dKillList ) )
			return false;

	// check if we can early reject the whole index
	if ( tCtx.m_pFilter && m_iDocinfoIndex )
	{
		DWORD uStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
		DWORD * pMinEntry = const_cast<DWORD*> ( &m_pDocinfoIndex [ m_iDocinfoIndex*uStride*2 ] );
		DWORD * pMaxEntry = pMinEntry + uStride;

		if ( !tCtx.m_pFilter->EvalBlock ( pMinEntry, pMaxEntry ) )
		{
			pResult->m_iQueryTime += (int)( ( sphMicroTimer()-tmQueryStart )/1000 );
			return true;
		}
	}

	// setup lookup
	tCtx.m_bLookupFilter = false;
	tCtx.m_bLookupSort = true;

	// setup sorters vs. MVA
	for ( int i=0; i<iSorters; i++ )
	{
		(ppSorters[i])->SetMVAPool ( m_tMva.GetWritePtr(), m_bArenaProhibit );
		(ppSorters[i])->SetStringPool ( m_tString.GetWritePtr() );
	}

	// setup overrides
	if ( !tCtx.SetupOverrides ( pQuery, pResult, m_tSchema, ppSorters[iMaxSchemaIndex]->GetSchema() ) )
		return false;

	// prepare to work them rows
	bool bRandomize = ppSorters[0]->m_bRandomize;

	CSphMatch tMatch;
	tMatch.Reset ( ppSorters[iMaxSchemaIndex]->GetSchema().GetDynamicSize() );
	tMatch.m_iWeight = tArgs.m_iIndexWeight;
	tMatch.m_iTag = tCtx.m_dCalcFinal.GetLength() ? -1 : tArgs.m_iTag;

	if ( pResult->m_pProfile )
		pResult->m_pProfile->Switch ( SPH_QSTATE_FULLSCAN );

	// optimize direct lookups by id
	// run full scan with block and row filtering for everything else
	if ( pQuery->m_dFilters.GetLength()==1
		&& pQuery->m_dFilters[0].m_eType==SPH_FILTER_VALUES
		&& pQuery->m_dFilters[0].m_bExclude==false
		&& pQuery->m_dFilters[0].m_sAttrName=="@id"
		&& tArgs.m_dKillList.GetLength()==0 )
	{
		// run id lookups
		for ( int i=0; i<pQuery->m_dFilters[0].GetNumValues(); i++ )
		{
			pResult->m_tStats.m_iFetchedDocs++;
			SphDocID_t uDocid = (SphDocID_t) pQuery->m_dFilters[0].GetValue(i);
			const DWORD * pRow = FindDocinfo ( uDocid );

			if ( !pRow )
				continue;

			assert ( uDocid==DOCINFO2ID(pRow) );
			tMatch.m_uDocID = uDocid;
			CopyDocinfo ( &tCtx, tMatch, pRow );

			if ( bRandomize )
				tMatch.m_iWeight = ( sphRand() & 0xffff ) * tArgs.m_iIndexWeight;

			// submit match to sorters
			tCtx.CalcSort ( tMatch );

			for ( int iSorter=0; iSorter<iSorters; iSorter++ )
				ppSorters[iSorter]->Push ( tMatch );

			// stringptr expressions should be duplicated (or taken over) at this point
			tCtx.FreeStrSort ( tMatch );
		}
	} else
	{
		bool bReverse = pQuery->m_bReverseScan; // shortcut
		int iCutoff = ( pQuery->m_iCutoff<=0 ) ? -1 : pQuery->m_iCutoff;

		DWORD uStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
		int64_t iStart = bReverse ? m_iDocinfoIndex-1 : 0;
		int64_t iEnd = bReverse ? -1 : m_iDocinfoIndex;
		int64_t iStep = bReverse ? -1 : 1;
		for ( int64_t iIndexEntry=iStart; iIndexEntry!=iEnd; iIndexEntry+=iStep )
		{
			// block-level filtering
			const DWORD * pMin = &m_pDocinfoIndex[ iIndexEntry*uStride*2 ];
			const DWORD * pMax = pMin + uStride;
			if ( tCtx.m_pFilter && !tCtx.m_pFilter->EvalBlock ( pMin, pMax ) )
				continue;

			// row-level filtering
			const DWORD * pBlockStart = m_tAttr.GetWritePtr() + ( iIndexEntry*uStride*DOCINFO_INDEX_FREQ );
			const DWORD * pBlockEnd = m_tAttr.GetWritePtr() + ( Min ( ( iIndexEntry+1 )*DOCINFO_INDEX_FREQ, m_iDocinfo )*uStride );
			if ( bReverse )
			{
				pBlockStart = m_tAttr.GetWritePtr() + ( ( Min ( ( iIndexEntry+1 )*DOCINFO_INDEX_FREQ, m_iDocinfo ) - 1 ) * uStride );
				pBlockEnd = m_tAttr.GetWritePtr() + uStride*( iIndexEntry*DOCINFO_INDEX_FREQ-1 );
			}
			int iDocinfoStep = bReverse ? -(int)uStride : (int)uStride;

			if ( !tCtx.m_pOverrides && tCtx.m_pFilter && !pQuery->m_iCutoff && !tCtx.m_dCalcFilter.GetLength() && !tCtx.m_dCalcSort.GetLength() )
			{
				// kinda fastpath
				for ( const DWORD * pDocinfo=pBlockStart; pDocinfo!=pBlockEnd; pDocinfo+=iDocinfoStep )
				{
					pResult->m_tStats.m_iFetchedDocs++;
					tMatch.m_uDocID = DOCINFO2ID ( pDocinfo );
					tMatch.m_pStatic = DOCINFO2ATTRS ( pDocinfo );

					if ( tCtx.m_pFilter->Eval ( tMatch ) )
					{
						if ( bRandomize )
							tMatch.m_iWeight = ( sphRand() & 0xffff ) * tArgs.m_iIndexWeight;
						for ( int iSorter=0; iSorter<iSorters; iSorter++ )
							ppSorters[iSorter]->Push ( tMatch );
					}
					// stringptr expressions should be duplicated (or taken over) at this point
					tCtx.FreeStrFilter ( tMatch );
				}
			} else
			{
				// generic path
				for ( const DWORD * pDocinfo=pBlockStart; pDocinfo!=pBlockEnd; pDocinfo+=iDocinfoStep )
				{
					pResult->m_tStats.m_iFetchedDocs++;
					tMatch.m_uDocID = DOCINFO2ID ( pDocinfo );
					CopyDocinfo ( &tCtx, tMatch, pDocinfo );

					// early filter only (no late filters in full-scan because of no @weight)
					tCtx.CalcFilter ( tMatch );
					if ( tCtx.m_pFilter && !tCtx.m_pFilter->Eval ( tMatch ) )
					{
						tCtx.FreeStrFilter ( tMatch );
						continue;
					}

					if ( bRandomize )
						tMatch.m_iWeight = ( sphRand() & 0xffff ) * tArgs.m_iIndexWeight;

					// submit match to sorters
					tCtx.CalcSort ( tMatch );

					bool bNewMatch = false;
					for ( int iSorter=0; iSorter<iSorters; iSorter++ )
						bNewMatch |= ppSorters[iSorter]->Push ( tMatch );

					// stringptr expressions should be duplicated (or taken over) at this point
					tCtx.FreeStrFilter ( tMatch );
					tCtx.FreeStrSort ( tMatch );

					// handle cutoff
					if ( bNewMatch && --iCutoff==0 )
					{
						iIndexEntry = iEnd - iStep; // outer break
						break;
					}
				}
			}
		}
	}

	if ( pResult->m_pProfile )
		pResult->m_pProfile->Switch ( SPH_QSTATE_FINALIZE );

	// do final expression calculations
	if ( tCtx.m_dCalcFinal.GetLength() )
	{
		SphFinalMatchCalc_t tFinal ( tArgs.m_iTag, NULL, tCtx );
		for ( int iSorter=0; iSorter<iSorters; iSorter++ )
		{
			ISphMatchSorter * pTop = ppSorters[iSorter];
			pTop->Finalize ( tFinal, false );
		}
		tCtx.m_iBadRows += tFinal.m_iBadRows;
	}

	// done
	pResult->m_pMva = m_tMva.GetWritePtr();
	pResult->m_pStrings = m_tString.GetWritePtr();
	pResult->m_bArenaProhibit = m_bArenaProhibit;
	pResult->m_iQueryTime += (int)( ( sphMicroTimer()-tmQueryStart )/1000 );
	pResult->m_iBadRows += tCtx.m_iBadRows;

	return true;
}




bool CSphIndex_VLN::Lock ()
{
	CSphString sName = GetIndexFileName("spl");
	sphLogDebug ( "Locking the index via file %s", sName.cstr() );

	if ( m_iLockFD<0 )
	{
		m_iLockFD = ::open ( sName.cstr(), SPH_O_NEW, 0644 );
		if ( m_iLockFD<0 )
		{
			m_sLastError.SetSprintf ( "failed to open %s: %s", sName.cstr(), strerror(errno) );
			sphLogDebug ( "failed to open %s: %s", sName.cstr(), strerror(errno) );
			return false;
		}
	}

	if ( !sphLockEx ( m_iLockFD, false ) )
	{
		m_sLastError.SetSprintf ( "failed to lock %s: %s", sName.cstr(), strerror(errno) );
		::close ( m_iLockFD );
		m_iLockFD = -1;
		return false;
	}
	sphLogDebug ( "lock %s success", sName.cstr() );
	return true;
}


void CSphIndex_VLN::Unlock()
{
	CSphString sName = GetIndexFileName("spl");
	sphLogDebug ( "Unlocking the index (lock %s)", sName.cstr() );
	if ( m_iLockFD>=0 )
	{
		sphLogDebug ( "File ID ok, closing lock FD %d, unlinking %s", m_iLockFD, sName.cstr() );
		sphLockUn ( m_iLockFD );
		::close ( m_iLockFD );
		::unlink ( sName.cstr() );
		m_iLockFD = -1;
	}
}

void CSphIndex_VLN::Dealloc ()
{
	if ( !m_bPassedAlloc )
		return;

	m_tDoclistFile.Close ();
	m_tHitlistFile.Close ();

	m_tAttr.Reset ();
	m_tMva.Reset ();
	m_tString.Reset ();
	m_tKillList.Reset ();
	m_tSkiplists.Reset ();
	m_tWordlist.Reset ();
	m_tDocinfoHash.Reset ();
	m_tMinMaxLegacy.Reset();

	m_iDocinfo = 0;
	m_iMinMaxIndex = 0;
	m_tSettings.m_eDocinfo = SPH_DOCINFO_NONE;

	SafeDelete ( m_pFieldFilter );
	SafeDelete ( m_pQueryTokenizer );
	SafeDelete ( m_pTokenizer );
	SafeDelete ( m_pDict );

	if ( m_iIndexTag>=0 && g_pMvaArena )
		g_tMvaArena.TaggedFreeTag ( m_iIndexTag );
	m_iIndexTag = -1;

	m_bPassedRead = false;
	m_bPassedAlloc = false;
	m_uAttrsStatus = false;

	QcacheDeleteIndex ( m_iIndexId );
	m_iIndexId = m_tIdGenerator.Inc();
}




bool CSphIndex_VLN::LoadHeader ( const char * sHeaderName, bool bStripPath, CSphEmbeddedFiles & tEmbeddedFiles, CSphString & sWarning )
{
	const int MAX_HEADER_SIZE = 32768;
	CSphFixedVector<BYTE> dCacheInfo ( MAX_HEADER_SIZE );

	CSphAutoreader rdInfo ( dCacheInfo.Begin(), MAX_HEADER_SIZE ); // to avoid mallocs
	if ( !rdInfo.Open ( sHeaderName, m_sLastError ) )
		return false;

	// magic header
	const char* sFmt = CheckFmtMagic ( rdInfo.GetDword () );
	if ( sFmt )
	{
		m_sLastError.SetSprintf ( sFmt, sHeaderName );
		return false;
	}

	// version
	m_uVersion = rdInfo.GetDword();
	if ( m_uVersion==0 || m_uVersion>INDEX_FORMAT_VERSION )
	{
		m_sLastError.SetSprintf ( "%s is v.%d, binary is v.%d", sHeaderName, m_uVersion, INDEX_FORMAT_VERSION );
		return false;
	}

	// bits
	bool bUse64 = false;
	if ( m_uVersion>=2 )
		bUse64 = ( rdInfo.GetDword ()!=0 );

	if ( bUse64!=USE_64BIT )
	{
		m_sLastError.SetSprintf ( "'%s' is id%d, and this binary is id%d",
			GetIndexFileName("sph").cstr(),
			bUse64 ? 64 : 32, USE_64BIT ? 64 : 32 );
		return false;
	}

	// skiplists
	m_bHaveSkips = ( m_uVersion>=31 );

	// docinfo
	m_tSettings.m_eDocinfo = (ESphDocinfo) rdInfo.GetDword();

	// schema
	// 4th arg means that inline attributes need be dynamic in searching time too
	ReadSchema ( rdInfo, m_tSchema, m_uVersion, m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE );

	// check schema for dupes
	for ( int iAttr=1; iAttr<m_tSchema.GetAttrsCount(); iAttr++ )
	{
		const CSphColumnInfo & tCol = m_tSchema.GetAttr(iAttr);
		for ( int i=0; i<iAttr; i++ )
			if ( m_tSchema.GetAttr(i).m_sName==tCol.m_sName )
				sWarning.SetSprintf ( "duplicate attribute name: %s", tCol.m_sName.cstr() );
	}

	// in case of *fork rotation we reuse min match from 1st rotated index ( it could be less than my size and inline ( m_pDynamic ) )
	// min doc
	m_dMinRow.Reset ( m_tSchema.GetRowSize() );
	if ( m_uVersion>=2 )
		m_uMinDocid = (SphDocID_t) rdInfo.GetOffset (); // v2+; losing high bits when !USE_64 is intentional, check is performed on bUse64 above
	else
		m_uMinDocid = rdInfo.GetDword(); // v1
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
		rdInfo.GetBytes ( m_dMinRow.Begin(), sizeof(CSphRowitem)*m_tSchema.GetRowSize() );

	// dictionary header (wordlist checkpoints, infix blocks, etc)
	m_tWordlist.m_iDictCheckpointsOffset = rdInfo.GetOffset();
	m_tWordlist.m_iDictCheckpoints = rdInfo.GetDword();
	if ( m_uVersion>=27 )
	{
		m_tWordlist.m_iInfixCodepointBytes = rdInfo.GetByte();
		m_tWordlist.m_iInfixBlocksOffset = rdInfo.GetDword();
	}
	if ( m_uVersion>=34 )
		m_tWordlist.m_iInfixBlocksWordsSize = rdInfo.GetDword();

	m_tWordlist.m_dCheckpoints.Reset ( m_tWordlist.m_iDictCheckpoints );

	// index stats
	m_tStats.m_iTotalDocuments = rdInfo.GetDword ();
	m_tStats.m_iTotalBytes = rdInfo.GetOffset ();
	if ( m_uVersion>=40 )
		m_iTotalDups = rdInfo.GetDword();

	LoadIndexSettings ( m_tSettings, rdInfo, m_uVersion );
	if ( m_uVersion<9 )
		m_bStripperInited = false;

	CSphTokenizerSettings tTokSettings;
	if ( m_uVersion>=9 )
	{
		// tokenizer stuff
		if ( !LoadTokenizerSettings ( rdInfo, tTokSettings, tEmbeddedFiles, m_uVersion, m_sLastError ) )
			return false;

		if ( bStripPath )
			StripPath ( tTokSettings.m_sSynonymsFile );

		ISphTokenizer * pTokenizer = ISphTokenizer::Create ( tTokSettings, &tEmbeddedFiles, m_sLastError );
		if ( !pTokenizer )
			return false;

		// dictionary stuff
		CSphDictSettings tDictSettings;
		LoadDictionarySettings ( rdInfo, tDictSettings, tEmbeddedFiles, m_uVersion, sWarning );

		if ( bStripPath )
		{
			StripPath ( tDictSettings.m_sStopwords );
			ARRAY_FOREACH ( i, tDictSettings.m_dWordforms )
				StripPath ( tDictSettings.m_dWordforms[i] );
		}

		CSphDict * pDict = tDictSettings.m_bWordDict
			? sphCreateDictionaryKeywords ( tDictSettings, &tEmbeddedFiles, pTokenizer, m_sIndexName.cstr(), m_sLastError )
			: sphCreateDictionaryCRC ( tDictSettings, &tEmbeddedFiles, pTokenizer, m_sIndexName.cstr(), m_sLastError );

		if ( !pDict )
			return false;

		if ( tDictSettings.m_sMorphFingerprint!=pDict->GetMorphDataFingerprint() )
			sWarning.SetSprintf ( "different lemmatizer dictionaries (index='%s', current='%s')",
				tDictSettings.m_sMorphFingerprint.cstr(),
				pDict->GetMorphDataFingerprint().cstr() );

		SetDictionary ( pDict );

		pTokenizer = ISphTokenizer::CreateMultiformFilter ( pTokenizer, pDict->GetMultiWordforms () );
		SetTokenizer ( pTokenizer );
		SetupQueryTokenizer();

		// initialize AOT if needed
		m_tSettings.m_uAotFilterMask = sphParseMorphAot ( tDictSettings.m_sMorphology.cstr() );
	}

	if ( m_uVersion>=10 )
		rdInfo.GetDword ();

	if ( m_uVersion>=33 )
		m_iMinMaxIndex = rdInfo.GetOffset ();
	else if ( m_uVersion>=20 )
		m_iMinMaxIndex = rdInfo.GetDword ();

	if ( m_uVersion>=28 )
	{
		ISphFieldFilter * pFieldFilter = NULL;
		CSphFieldFilterSettings tFieldFilterSettings;
		LoadFieldFilterSettings ( rdInfo, tFieldFilterSettings );
		if ( tFieldFilterSettings.m_dRegexps.GetLength() )
			pFieldFilter = sphCreateRegexpFilter ( tFieldFilterSettings, m_sLastError );

		if ( !sphSpawnRLPFilter ( pFieldFilter, m_tSettings, tTokSettings, sHeaderName, m_sLastError ) )
		{
			SafeDelete ( pFieldFilter );
			return false;
		}

		SetFieldFilter ( pFieldFilter );
	}


	if ( m_uVersion>=35 && m_tSettings.m_bIndexFieldLens )
		ARRAY_FOREACH ( i, m_tSchema.m_dFields )
			m_dFieldLens[i] = rdInfo.GetOffset(); // FIXME? ideally 64bit even when off is 32bit..

	// post-load stuff.. for now, bigrams
	CSphIndexSettings & s = m_tSettings;
	if ( s.m_eBigramIndex!=SPH_BIGRAM_NONE && s.m_eBigramIndex!=SPH_BIGRAM_ALL )
	{
		BYTE * pTok;
		m_pTokenizer->SetBuffer ( (BYTE*)s.m_sBigramWords.cstr(), s.m_sBigramWords.Length() );
		while ( ( pTok = m_pTokenizer->GetToken() )!=NULL )
			s.m_dBigramWords.Add() = (const char*)pTok;
		s.m_dBigramWords.Sort();
	}


	if ( rdInfo.GetErrorFlag() )
		m_sLastError.SetSprintf ( "%s: failed to parse header (unexpected eof)", sHeaderName );
	return !rdInfo.GetErrorFlag();
}


void CSphIndex_VLN::DebugDumpHeader ( FILE * fp, const char * sHeaderName, bool bConfig )
{
	CSphEmbeddedFiles tEmbeddedFiles;
	CSphString sWarning;
	if ( !LoadHeader ( sHeaderName, false, tEmbeddedFiles, sWarning ) )
	{
		fprintf ( fp, "FATAL: failed to load header: %s.\n", m_sLastError.cstr() );
		return;
	}

	if ( !sWarning.IsEmpty () )
		fprintf ( fp, "WARNING: %s\n", sWarning.cstr () );

	///////////////////////////////////////////////
	// print header in index config section format
	///////////////////////////////////////////////

	if ( bConfig )
	{
		fprintf ( fp, "\nsource $dump\n{\n" );

		fprintf ( fp, "\tsql_query = SELECT id \\\n" );
		ARRAY_FOREACH ( i, m_tSchema.m_dFields )
			fprintf ( fp, "\t, %s \\\n", m_tSchema.m_dFields[i].m_sName.cstr() );
		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
			fprintf ( fp, "\t, %s \\\n", tAttr.m_sName.cstr() );
		}
		fprintf ( fp, "\tFROM documents\n" );

		if ( m_tSchema.GetAttrsCount() )
			fprintf ( fp, "\n" );

		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
			if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
				fprintf ( fp, "\tsql_attr_multi = uint %s from field\n", tAttr.m_sName.cstr() );
			else if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
				fprintf ( fp, "\tsql_attr_multi = bigint %s from field\n", tAttr.m_sName.cstr() );
			else if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INTEGER && tAttr.m_tLocator.IsBitfield() )
				fprintf ( fp, "\tsql_attr_uint = %s:%d\n", tAttr.m_sName.cstr(), tAttr.m_tLocator.m_iBitCount );
			else if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_TOKENCOUNT )
			{; // intendedly skip, as these are autogenerated by index_field_lengths=1
			} else
				fprintf ( fp, "\t%s = %s\n", sphTypeDirective ( tAttr.m_eAttrType ), tAttr.m_sName.cstr() );
		}

		fprintf ( fp, "}\n\nindex $dump\n{\n\tsource = $dump\n\tpath = $dump\n" );

		if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
			fprintf ( fp, "\tdocinfo = inline\n" );
		if ( m_tSettings.m_iMinPrefixLen )
			fprintf ( fp, "\tmin_prefix_len = %d\n", m_tSettings.m_iMinPrefixLen );
		if ( m_tSettings.m_iMinInfixLen )
			fprintf ( fp, "\tmin_prefix_len = %d\n", m_tSettings.m_iMinInfixLen );
		if ( m_tSettings.m_iMaxSubstringLen )
			fprintf ( fp, "\tmax_substring_len = %d\n", m_tSettings.m_iMaxSubstringLen );
		if ( m_tSettings.m_bIndexExactWords )
			fprintf ( fp, "\tindex_exact_words = %d\n", m_tSettings.m_bIndexExactWords ? 1 : 0 );
		if ( m_tSettings.m_bHtmlStrip )
			fprintf ( fp, "\thtml_strip = 1\n" );
		if ( !m_tSettings.m_sHtmlIndexAttrs.IsEmpty() )
			fprintf ( fp, "\thtml_index_attrs = %s\n", m_tSettings.m_sHtmlIndexAttrs.cstr () );
		if ( !m_tSettings.m_sHtmlRemoveElements.IsEmpty() )
			fprintf ( fp, "\thtml_remove_elements = %s\n", m_tSettings.m_sHtmlRemoveElements.cstr () );
		if ( !m_tSettings.m_sZones.IsEmpty() )
			fprintf ( fp, "\tindex_zones = %s\n", m_tSettings.m_sZones.cstr() );
		if ( m_tSettings.m_bIndexFieldLens )
			fprintf ( fp, "\tindex_field_lengths = 1\n" );
		if ( m_tSettings.m_bIndexSP )
			fprintf ( fp, "\tindex_sp = 1\n" );
		if ( m_tSettings.m_iBoundaryStep!=0 )
			fprintf ( fp, "\tphrase_boundary_step = %d\n", m_tSettings.m_iBoundaryStep );
		if ( m_tSettings.m_iStopwordStep!=1 )
			fprintf ( fp, "\tstopword_step = %d\n", m_tSettings.m_iStopwordStep );
		if ( m_tSettings.m_iOvershortStep!=1 )
			fprintf ( fp, "\tovershort_step = %d\n", m_tSettings.m_iOvershortStep );
		if ( m_tSettings.m_eBigramIndex!=SPH_BIGRAM_NONE )
			fprintf ( fp, "\tbigram_index = %s\n", sphBigramName ( m_tSettings.m_eBigramIndex ) );
		if ( !m_tSettings.m_sBigramWords.IsEmpty() )
			fprintf ( fp, "\tbigram_freq_words = %s\n", m_tSettings.m_sBigramWords.cstr() );
		if ( !m_tSettings.m_sRLPContext.IsEmpty() )
			fprintf ( fp, "\trlp_context = %s\n", m_tSettings.m_sRLPContext.cstr() );
		if ( !m_tSettings.m_sIndexTokenFilter.IsEmpty() )
			fprintf ( fp, "\tindex_token_filter = %s\n", m_tSettings.m_sIndexTokenFilter.cstr() );


		CSphFieldFilterSettings tFieldFilter;
		GetFieldFilterSettings ( tFieldFilter );
		ARRAY_FOREACH ( i, tFieldFilter.m_dRegexps )
			fprintf ( fp, "\tregexp_filter = %s\n", tFieldFilter.m_dRegexps[i].cstr() );

		if ( m_pTokenizer )
		{
			const CSphTokenizerSettings & tSettings = m_pTokenizer->GetSettings ();
			fprintf ( fp, "\tcharset_type = %s\n", ( tSettings.m_iType==TOKENIZER_UTF8 || tSettings.m_iType==TOKENIZER_NGRAM )
					? "utf-8"
					: "unknown tokenizer (deprecated sbcs?)" );
			if ( !tSettings.m_sCaseFolding.IsEmpty() )
				fprintf ( fp, "\tcharset_table = %s\n", tSettings.m_sCaseFolding.cstr () );
			if ( tSettings.m_iMinWordLen>1 )
				fprintf ( fp, "\tmin_word_len = %d\n", tSettings.m_iMinWordLen );
			if ( tSettings.m_iNgramLen && !tSettings.m_sNgramChars.IsEmpty() )
				fprintf ( fp, "\tngram_len = %d\nngram_chars = %s\n",
					tSettings.m_iNgramLen, tSettings.m_sNgramChars.cstr () );
			if ( !tSettings.m_sSynonymsFile.IsEmpty() )
				fprintf ( fp, "\texceptions = %s\n", tSettings.m_sSynonymsFile.cstr () );
			if ( !tSettings.m_sBoundary.IsEmpty() )
				fprintf ( fp, "\tphrase_boundary = %s\n", tSettings.m_sBoundary.cstr () );
			if ( !tSettings.m_sIgnoreChars.IsEmpty() )
				fprintf ( fp, "\tignore_chars = %s\n", tSettings.m_sIgnoreChars.cstr () );
			if ( !tSettings.m_sBlendChars.IsEmpty() )
				fprintf ( fp, "\tblend_chars = %s\n", tSettings.m_sBlendChars.cstr () );
			if ( !tSettings.m_sBlendMode.IsEmpty() )
				fprintf ( fp, "\tblend_mode = %s\n", tSettings.m_sBlendMode.cstr () );
		}

		if ( m_pDict )
		{
			const CSphDictSettings & tSettings = m_pDict->GetSettings ();
			if ( tSettings.m_bWordDict )
				fprintf ( fp, "\tdict = keywords\n" );
			if ( !tSettings.m_sMorphology.IsEmpty() )
				fprintf ( fp, "\tmorphology = %s\n", tSettings.m_sMorphology.cstr () );
			if ( !tSettings.m_sStopwords.IsEmpty() )
				fprintf ( fp, "\tstopwords = %s\n", tSettings.m_sStopwords.cstr () );
			if ( tSettings.m_dWordforms.GetLength() )
			{
				fprintf ( fp, "\twordforms =" );
				ARRAY_FOREACH ( i, tSettings.m_dWordforms )
					fprintf ( fp, " %s", tSettings.m_dWordforms[i].cstr () );
				fprintf ( fp, "\n" );
			}
			if ( tSettings.m_iMinStemmingLen>1 )
				fprintf ( fp, "\tmin_stemming_len = %d\n", tSettings.m_iMinStemmingLen );
			if ( tSettings.m_bStopwordsUnstemmed )
				fprintf ( fp, "\tstopwords_unstemmed = 1\n" );
		}

		fprintf ( fp, "}\n" );
		return;
	}

	///////////////////////////////////////////////
	// print header and stats in "readable" format
	///////////////////////////////////////////////

	fprintf ( fp, "version: %d\n",			m_uVersion );
	fprintf ( fp, "idbits: %d\n",			USE_64BIT ? 64 : 32 );
	fprintf ( fp, "docinfo: " );
	switch ( m_tSettings.m_eDocinfo )
	{
		case SPH_DOCINFO_NONE:		fprintf ( fp, "none\n" ); break;
		case SPH_DOCINFO_INLINE:	fprintf ( fp, "inline\n" ); break;
		case SPH_DOCINFO_EXTERN:	fprintf ( fp, "extern\n" ); break;
		default:					fprintf ( fp, "unknown (value=%d)\n", m_tSettings.m_eDocinfo ); break;
	}

	fprintf ( fp, "fields: %d\n",(int) m_tSchema.m_dFields.GetLength() );
	ARRAY_FOREACH ( i, m_tSchema.m_dFields )
		fprintf ( fp, "  field %d: %s\n", (int)i, m_tSchema.m_dFields[i].m_sName.cstr() );

	fprintf ( fp, "attrs: %d\n", m_tSchema.GetAttrsCount() );
	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
		fprintf ( fp, "  attr %d: %s, %s", i, tAttr.m_sName.cstr(), sphTypeName ( tAttr.m_eAttrType ) );
		if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INTEGER && tAttr.m_tLocator.m_iBitCount!=32 )
			fprintf ( fp, ", bits %d", tAttr.m_tLocator.m_iBitCount );
		fprintf ( fp, ", bitoff %d\n", tAttr.m_tLocator.m_iBitOffset );
	}

	// skipped min doc, wordlist checkpoints
	fprintf ( fp, "total-documents: " INT64_FMT "\n", m_tStats.m_iTotalDocuments );
	fprintf ( fp, "total-bytes: " INT64_FMT "\n", int64_t(m_tStats.m_iTotalBytes) );
	fprintf ( fp, "total-duplicates: %d\n", m_iTotalDups );

	fprintf ( fp, "min-prefix-len: %d\n", m_tSettings.m_iMinPrefixLen );
	fprintf ( fp, "min-infix-len: %d\n", m_tSettings.m_iMinInfixLen );
	fprintf ( fp, "max-substring-len: %d\n", m_tSettings.m_iMaxSubstringLen );
	fprintf ( fp, "exact-words: %d\n", m_tSettings.m_bIndexExactWords ? 1 : 0 );
	fprintf ( fp, "html-strip: %d\n", m_tSettings.m_bHtmlStrip ? 1 : 0 );
	fprintf ( fp, "html-index-attrs: %s\n", m_tSettings.m_sHtmlIndexAttrs.cstr () );
	fprintf ( fp, "html-remove-elements: %s\n", m_tSettings.m_sHtmlRemoveElements.cstr () );
	fprintf ( fp, "index-zones: %s\n", m_tSettings.m_sZones.cstr() );
	fprintf ( fp, "index-field-lengths: %d\n", m_tSettings.m_bIndexFieldLens ? 1 : 0 );
	fprintf ( fp, "index-sp: %d\n", m_tSettings.m_bIndexSP ? 1 : 0 );
	fprintf ( fp, "phrase-boundary-step: %d\n", m_tSettings.m_iBoundaryStep );
	fprintf ( fp, "stopword-step: %d\n", m_tSettings.m_iStopwordStep );
	fprintf ( fp, "overshort-step: %d\n", m_tSettings.m_iOvershortStep );
	fprintf ( fp, "bigram-index: %s\n", sphBigramName ( m_tSettings.m_eBigramIndex ) );
	fprintf ( fp, "bigram-freq-words: %s\n", m_tSettings.m_sBigramWords.cstr() );
	fprintf ( fp, "rlp-context: %s\n", m_tSettings.m_sRLPContext.cstr() );
	fprintf ( fp, "index-token-filter: %s\n", m_tSettings.m_sIndexTokenFilter.cstr() );
	CSphFieldFilterSettings tFieldFilter;
	GetFieldFilterSettings ( tFieldFilter );
	ARRAY_FOREACH ( i, tFieldFilter.m_dRegexps )
		fprintf ( fp, "regexp-filter: %s\n", tFieldFilter.m_dRegexps[i].cstr() );

	if ( m_pTokenizer )
	{
		const CSphTokenizerSettings & tSettings = m_pTokenizer->GetSettings ();
		fprintf ( fp, "tokenizer-type: %d\n", tSettings.m_iType );
		fprintf ( fp, "tokenizer-case-folding: %s\n", tSettings.m_sCaseFolding.cstr () );
		fprintf ( fp, "tokenizer-min-word-len: %d\n", tSettings.m_iMinWordLen );
		fprintf ( fp, "tokenizer-ngram-chars: %s\n", tSettings.m_sNgramChars.cstr () );
		fprintf ( fp, "tokenizer-ngram-len: %d\n", tSettings.m_iNgramLen );
		fprintf ( fp, "tokenizer-exceptions: %s\n", tSettings.m_sSynonymsFile.cstr () );
		fprintf ( fp, "tokenizer-phrase-boundary: %s\n", tSettings.m_sBoundary.cstr () );
		fprintf ( fp, "tokenizer-ignore-chars: %s\n", tSettings.m_sIgnoreChars.cstr () );
		fprintf ( fp, "tokenizer-blend-chars: %s\n", tSettings.m_sBlendChars.cstr () );
		fprintf ( fp, "tokenizer-blend-mode: %s\n", tSettings.m_sBlendMode.cstr () );
		fprintf ( fp, "tokenizer-blend-mode: %s\n", tSettings.m_sBlendMode.cstr () );

		fprintf ( fp, "dictionary-embedded-exceptions: %d\n", tEmbeddedFiles.m_bEmbeddedSynonyms ? 1 : 0 );
		if ( tEmbeddedFiles.m_bEmbeddedSynonyms )
		{
			ARRAY_FOREACH ( i, tEmbeddedFiles.m_dSynonyms )
				fprintf ( fp, "\tdictionary-embedded-exception [%d]: %s\n", (int)i, tEmbeddedFiles.m_dSynonyms[i].cstr () );
		}
	}

	if ( m_pDict )
	{
		const CSphDictSettings & tSettings = m_pDict->GetSettings ();
		fprintf ( fp, "dict: %s\n", tSettings.m_bWordDict ? "keywords" : "crc" );
		fprintf ( fp, "dictionary-morphology: %s\n", tSettings.m_sMorphology.cstr () );

		fprintf ( fp, "dictionary-stopwords-file: %s\n", tSettings.m_sStopwords.cstr () );
		fprintf ( fp, "dictionary-embedded-stopwords: %d\n", tEmbeddedFiles.m_bEmbeddedStopwords ? 1 : 0 );
		if ( tEmbeddedFiles.m_bEmbeddedStopwords )
		{
			ARRAY_FOREACH ( i, tEmbeddedFiles.m_dStopwords )
				fprintf ( fp, "\tdictionary-embedded-stopword [%d]: " DOCID_FMT "\n", (int)i, tEmbeddedFiles.m_dStopwords[i] );
		}

		ARRAY_FOREACH ( i, tSettings.m_dWordforms )
			fprintf ( fp, "dictionary-wordform-file [%d]: %s\n", (int)i, tSettings.m_dWordforms[i].cstr () );

		fprintf ( fp, "dictionary-embedded-wordforms: %d\n", tEmbeddedFiles.m_bEmbeddedWordforms ? 1 : 0 );
		if ( tEmbeddedFiles.m_bEmbeddedWordforms )
		{
			ARRAY_FOREACH ( i, tEmbeddedFiles.m_dWordforms )
				fprintf ( fp, "\tdictionary-embedded-wordform [%d]: %s\n", (int)i, tEmbeddedFiles.m_dWordforms[i].cstr () );
		}

		fprintf ( fp, "min-stemming-len: %d\n", tSettings.m_iMinStemmingLen );
		fprintf ( fp, "stopwords-unstemmed: %d\n", tSettings.m_bStopwordsUnstemmed ? 1 : 0 );
	}

	fprintf ( fp, "killlist-size: %u\n", 0 );
	fprintf ( fp, "min-max-index: " INT64_FMT "\n", m_iMinMaxIndex );
}


void CSphIndex_VLN::DebugDumpDocids ( FILE * fp )
{
	if ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN )
	{
		fprintf ( fp, "FATAL: docids dump only supported for docinfo=extern\n" );
		return;
	}

	const int iRowStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();

	const int64_t iNumMinMaxRow = ( m_uVersion>=20 ) ? ( (m_iDocinfoIndex+1)*iRowStride*2 ) : 0;
	const int64_t iNumRows = (m_tAttr.GetNumEntries()-iNumMinMaxRow) / iRowStride;

	const int64_t iDocinfoSize = iRowStride*m_iDocinfo*sizeof(DWORD);
	const int64_t iMinmaxSize = iNumMinMaxRow*sizeof(CSphRowitem);

	fprintf ( fp, "docinfo-bytes: docinfo=" INT64_FMT ", min-max=" INT64_FMT ", total=" UINT64_FMT "\n"
		, iDocinfoSize, iMinmaxSize, (uint64_t)m_tAttr.GetLengthBytes() );
	fprintf ( fp, "docinfo-stride: %d\n", (int)(iRowStride*sizeof(DWORD)) );
	fprintf ( fp, "docinfo-rows: " INT64_FMT "\n", iNumRows );

	if ( !m_tAttr.GetNumEntries() )
		return;

	DWORD * pDocinfo = m_tAttr.GetWritePtr();
	for ( int64_t iRow=0; iRow<iNumRows; iRow++, pDocinfo+=iRowStride )
		printf ( INT64_FMT". id=" DOCID_FMT "\n", iRow+1, DOCINFO2ID ( pDocinfo ) );
	printf ( "--- min-max=" INT64_FMT " ---\n", iNumMinMaxRow );
	for ( int64_t iRow=0; iRow<(m_iDocinfoIndex+1)*2; iRow++, pDocinfo+=iRowStride )
		printf ( "id=" DOCID_FMT "\n", DOCINFO2ID ( pDocinfo ) );
}


void CSphIndex_VLN::DebugDumpHitlist ( FILE * fp, const char * sKeyword, bool bID )
{
	WITH_QWORD ( this, false, Qword, DumpHitlist<Qword> ( fp, sKeyword, bID ) );
}


template < class Qword >
void CSphIndex_VLN::DumpHitlist ( FILE * fp, const char * sKeyword, bool bID )
{
	// get keyword id
	SphWordID_t uWordID = 0;
	BYTE * sTok = NULL;
	if ( !bID )
	{
		CSphString sBuf ( sKeyword );

		m_pTokenizer->SetBuffer ( (BYTE*)sBuf.cstr(), strlen ( sBuf.cstr() ) );
		sTok = m_pTokenizer->GetToken();

		if ( !sTok )
			sphDie ( "keyword=%s, no token (too short?)", sKeyword );

		uWordID = m_pDict->GetWordID ( sTok );
		if ( !uWordID )
			sphDie ( "keyword=%s, tok=%s, no wordid (stopped?)", sKeyword, sTok );

		fprintf ( fp, "keyword=%s, tok=%s, wordid=" UINT64_FMT "\n", sKeyword, sTok, uint64_t(uWordID) );

	} else
	{
		uWordID = (SphWordID_t) strtoull ( sKeyword, NULL, 10 );
		if ( !uWordID )
			sphDie ( "failed to convert keyword=%s to id (must be integer)", sKeyword );

		fprintf ( fp, "wordid=" UINT64_FMT "\n", uint64_t(uWordID) );
	}

	// open files
	CSphAutofile tDoclist, tHitlist;
	if ( tDoclist.Open ( GetIndexFileName("spd"), SPH_O_READ, m_sLastError ) < 0 )
		sphDie ( "failed to open doclist: %s", m_sLastError.cstr() );

	if ( tHitlist.Open ( GetIndexFileName ( m_uVersion>=3 ? "spp" : "spd" ), SPH_O_READ, m_sLastError ) < 0 )
		sphDie ( "failed to open hitlist: %s", m_sLastError.cstr() );

	// aim
	DiskIndexQwordSetup_c tTermSetup ( tDoclist, tHitlist, m_tSkiplists.GetWritePtr(), NULL );
	tTermSetup.m_pDict = m_pDict;
	tTermSetup.m_pIndex = this;
	tTermSetup.m_eDocinfo = m_tSettings.m_eDocinfo;
	tTermSetup.m_uMinDocid = m_uMinDocid;
	tTermSetup.m_pMinRow = m_dMinRow.Begin();
	tTermSetup.m_bSetupReaders = true;

	Qword tKeyword ( false, false );
	tKeyword.m_tDoc.m_uDocID = m_uMinDocid;
	tKeyword.m_uWordID = uWordID;
	tKeyword.m_sWord = sKeyword;
	tKeyword.m_sDictWord = (const char *)sTok;
	if ( !tTermSetup.QwordSetup ( &tKeyword ) )
		sphDie ( "failed to setup keyword" );

	int iSize = m_tSchema.GetRowSize();
	CSphVector<CSphRowitem> dAttrs ( iSize );

	// press play on tape
	for ( ;; )
	{
		tKeyword.GetNextDoc ( iSize ? &dAttrs[0] : NULL );
		if ( !tKeyword.m_tDoc.m_uDocID )
			break;
		tKeyword.SeekHitlist ( tKeyword.m_iHitlistPos );

		int iHits = 0;
		if ( tKeyword.m_bHasHitlist )
			for ( Hitpos_t uHit = tKeyword.GetNextHit(); uHit!=EMPTY_HIT; uHit = tKeyword.GetNextHit() )
			{
				fprintf ( fp, "doc=" DOCID_FMT ", hit=0x%08x\n", tKeyword.m_tDoc.m_uDocID, uHit ); // FIXME?
				iHits++;
			}

		if ( !iHits )
		{
			uint64_t uOff = tKeyword.m_iHitlistPos;
			fprintf ( fp, "doc=" DOCID_FMT ", NO HITS, inline=%d, off=" UINT64_FMT "\n",
				tKeyword.m_tDoc.m_uDocID, (int)(uOff>>63), (uOff<<1)>>1 );
		}
	}
}


void CSphIndex_VLN::DebugDumpDict ( FILE * fp )
{
	if ( !m_pDict->GetSettings().m_bWordDict )
	{
		fprintf ( fp, "sorry, DebugDumpDict() only supports dict=keywords for now\n" );
		return;
	}

	fprintf ( fp, "keyword,docs,hits,offset\n" );
	m_tWordlist.DebugPopulateCheckpoints();
	ARRAY_FOREACH ( i, m_tWordlist.m_dCheckpoints )
	{
		KeywordsBlockReader_c tCtx ( m_tWordlist.AcquireDict ( &m_tWordlist.m_dCheckpoints[i] ), m_bHaveSkips );
		while ( tCtx.UnpackWord() )
			fprintf ( fp, "%s,%d,%d," INT64_FMT "\n", tCtx.GetWord(), tCtx.m_iDocs, tCtx.m_iHits, int64_t(tCtx.m_iDoclistOffset) );
	}
}

//////////////////////////////////////////////////////////////////////////

bool CSphIndex_VLN::Prealloc ( bool bStripPath )
{
	MEMORY ( MEM_INDEX_DISK );

	// reset
	Dealloc ();

	CSphEmbeddedFiles tEmbeddedFiles;

	// preload schema
	if ( !LoadHeader ( GetIndexFileName("sph").cstr(), bStripPath, tEmbeddedFiles, m_sLastWarning ) )
		return false;

	tEmbeddedFiles.Reset();

	// verify that data files are readable
	if ( !sphIsReadable ( GetIndexFileName("spd").cstr(), &m_sLastError ) )
		return false;

	if ( m_uVersion>=3 && !sphIsReadable ( GetIndexFileName("spp").cstr(), &m_sLastError ) )
		return false;

	if ( m_bHaveSkips && !sphIsReadable ( GetIndexFileName("spe").cstr(), &m_sLastError ) )
		return false;

	// preopen
	if ( m_bKeepFilesOpen )
	{
		if ( m_tDoclistFile.Open ( GetIndexFileName("spd"), SPH_O_READ, m_sLastError ) < 0 )
			return false;

		if ( m_tHitlistFile.Open ( GetIndexFileName ( m_uVersion>=3 ? "spp" : "spd" ), SPH_O_READ, m_sLastError ) < 0 )
			return false;
	}

	/////////////////////
	// prealloc wordlist
	/////////////////////

	if ( m_uVersion>=3 && !sphIsReadable ( GetIndexFileName("spi").cstr(), &m_sLastError ) )
		return false;

	// might be no dictionary at this point for old index format
	bool bWordDict = m_pDict && m_pDict->GetSettings().m_bWordDict;

	// only checkpoint and wordlist infixes are actually read here; dictionary itself is just mapped
	if ( !m_tWordlist.Preread ( GetIndexFileName("spi").cstr() , m_uVersion, bWordDict, m_sLastError ) )
		return false;

	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN )
	{
		CSphAutofile tDocinfo ( GetIndexFileName("spa"), SPH_O_READ, m_sLastError );
		if ( tDocinfo.GetFD()<0 )
			return false;

		m_bIsEmpty = ( tDocinfo.GetSize ( 0, false, m_sLastError )==0 );
	} else
		m_bIsEmpty = ( m_tWordlist.m_tBuf.GetLengthBytes()<=1 );

	if ( ( m_tWordlist.m_tBuf.GetLengthBytes()<=1 )!=( m_tWordlist.m_dCheckpoints.GetLength()==0 ) )
		sphWarning ( "wordlist size mismatch (size=" INT64_FMT ", checkpoints=%d)", m_tWordlist.m_tBuf.GetLengthBytes(), m_tWordlist.m_dCheckpoints.GetLength() );

	// make sure checkpoints are loadable
	// pre-11 indices use different offset type (this is fixed up later during the loading)
	assert ( m_tWordlist.m_iDictCheckpointsOffset>0 );

	/////////////////////
	// prealloc docinfos
	/////////////////////

	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && !m_bIsEmpty )
	{
		/////////////
		// attr data
		/////////////

		int iStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();

		if ( !m_tAttr.Setup ( GetIndexFileName("spa").cstr(), m_sLastError, true ) )
			return false;

		int64_t iDocinfoSize = m_tAttr.GetLengthBytes();
		if ( iDocinfoSize<0 )
			return false;
		iDocinfoSize = iDocinfoSize / sizeof(DWORD);
		int64_t iRealDocinfoSize = m_iMinMaxIndex ? m_iMinMaxIndex : iDocinfoSize;
		m_iDocinfo = iRealDocinfoSize / iStride;

		if ( !CheckDocsCount ( m_iDocinfo, m_sLastError ) )
			return false;

		if ( iDocinfoSize < iRealDocinfoSize )
		{
			m_sLastError.SetSprintf ( "precomputed chunk size check mismatch" );
			sphLogDebug ( "precomputed chunk size check mismatch (size=" INT64_FMT ", real=" INT64_FMT ", min-max=" INT64_FMT ", count=" INT64_FMT ")",
				iDocinfoSize, iRealDocinfoSize, m_iMinMaxIndex, m_iDocinfo );
			return false;
		}

		m_iDocinfoIndex = ( ( iDocinfoSize - iRealDocinfoSize ) / iStride / 2 ) - 1;
		m_pDocinfoIndex = m_tAttr.GetWritePtr() + m_iMinMaxIndex;

		// prealloc docinfo hash but only if docinfo is big enough (in other words if hash is 8x+ less in size)
		if ( m_tAttr.GetLengthBytes() > ( 32 << DOCINFO_HASH_BITS ) && !m_bDebugCheck )
		{
			if ( !m_tDocinfoHash.Alloc ( ( 1 << DOCINFO_HASH_BITS )+4, m_sLastError ) )
				return false;
		}

		////////////
		// MVA data
		////////////

		if ( m_uVersion>=4 )
		{
			if ( !m_tMva.Setup ( GetIndexFileName("spm").cstr(), m_sLastError, false ) )
				return false;

			if ( m_tMva.GetNumEntries()>INT_MAX )
			{
				m_bArenaProhibit = true;
				sphWarning ( "MVA update disabled (loaded MVA " INT64_FMT ", should be less %d)", m_tMva.GetNumEntries(), INT_MAX );
			}
		}

		///////////////
		// string data
		///////////////

		if ( m_uVersion>=17 && !m_tString.Setup ( GetIndexFileName("sps").cstr(), m_sLastError, true ) )
				return false;
	}


	// prealloc killlist
	if ( m_uVersion>=10 )
	{
		// FIXME!!! m_bId32to64
		if ( !m_tKillList.Setup ( GetIndexFileName("spk").cstr(), m_sLastError, false ) )
			return false;
	}

	// prealloc skiplist
	if ( !m_bDebugCheck && m_bHaveSkips && !m_tSkiplists.Setup ( GetIndexFileName("spe").cstr(), m_sLastError, false ) )
			return false;

	// almost done
	m_bPassedAlloc = true;
	m_iIndexTag = ++m_iIndexTagSeq;

	bool bPersistMVA = sphIsReadable ( GetIndexFileName("mvp").cstr() );
	bool bNoMinMax = ( m_uVersion<20 );
	if ( ( bPersistMVA || bNoMinMax ) && !m_bDebugCheck )
	{
		sphLogDebug ( "'%s' forced to read data at prealloc (persist MVA = %d, no min-max = %d)", m_sIndexName.cstr(), (int)bPersistMVA, (int)bNoMinMax );
		Preread();

		// persist MVA needs valid DocinfoHash
		sphLogDebug ( "Prereading .mvp" );
		if ( !LoadPersistentMVA ( m_sLastError ) )
			return false;

		// build "indexes" for full-scan
		if ( m_uVersion<20 && !PrecomputeMinMax() )
			return false;
	}

	return true;
}


void CSphIndex_VLN::Preread ()
{
	MEMORY ( MEM_INDEX_DISK );

	sphLogDebug ( "CSphIndex_VLN::Preread invoked '%s'", m_sIndexName.cstr() );

	assert ( m_bPassedAlloc );
	if ( m_bPassedRead )
		return;

	///////////////////
	// read everything
	///////////////////

	volatile BYTE uRead = 0; // just need all side-effects
	uRead ^= PrereadMapping ( m_sIndexName.cstr(), "attributes", m_bMlock, m_bOndiskAllAttr, m_tAttr );
	uRead ^= PrereadMapping ( m_sIndexName.cstr(), "MVA", m_bMlock, m_bOndiskPoolAttr, m_tMva );
	uRead ^= PrereadMapping ( m_sIndexName.cstr(), "strings", m_bMlock, m_bOndiskPoolAttr, m_tString );
	uRead ^= PrereadMapping ( m_sIndexName.cstr(), "kill-list", m_bMlock, m_bOndiskAllAttr, m_tKillList );
	uRead ^= PrereadMapping ( m_sIndexName.cstr(), "skip-list", m_bMlock, false, m_tSkiplists );
	uRead ^= PrereadMapping ( m_sIndexName.cstr(), "dictionary", m_bMlock, false, m_tWordlist.m_tBuf );

	//////////////////////
	// precalc everything
	//////////////////////

	// build attributes hash
	if ( m_tAttr.GetLengthBytes() && m_tDocinfoHash.GetLengthBytes() && !m_bDebugCheck )
	{
		sphLogDebug ( "Hashing docinfo" );
		assert ( CheckDocsCount ( m_iDocinfo, m_sLastError ) );
		int iStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
		SphDocID_t uFirst = DOCINFO2ID ( &m_tAttr[0] );
		SphDocID_t uRange = DOCINFO2ID ( &m_tAttr[ ( m_iDocinfo-1)*iStride ] ) - uFirst;
		DWORD iShift = 0;
		while ( uRange>=( 1 << DOCINFO_HASH_BITS ) )
		{
			iShift++;
			uRange >>= 1;
		}

		DWORD * pHash = m_tDocinfoHash.GetWritePtr();
		*pHash++ = iShift;
		*pHash = 0;
		DWORD uLastHash = 0;

		for ( int64_t i=1; i<m_iDocinfo; i++ )
		{
			assert ( DOCINFO2ID ( &m_tAttr[ i*iStride ] )>uFirst
				&& DOCINFO2ID ( &m_tAttr[ ( i-1 )*iStride ] ) < DOCINFO2ID ( &m_tAttr[ i*iStride ] )
				&& "descending document ID found" );
			DWORD uHash = (DWORD)( ( DOCINFO2ID ( &m_tAttr[ i*iStride ] ) - uFirst ) >> iShift );
			if ( uHash==uLastHash )
				continue;

			while ( uLastHash<uHash )
				pHash [ ++uLastHash ] = (DWORD)i;

			uLastHash = uHash;
		}
		pHash [ ++uLastHash ] = (DWORD)m_iDocinfo;
	}

	m_bPassedRead = true;
	sphLogDebug ( "Preread successfully finished, hash=%u", (DWORD)uRead );
	return;
}

void CSphIndex_VLN::SetMemorySettings ( bool bMlock, bool bOndiskAttrs, bool bOndiskPool )
{
	m_bMlock = bMlock;
	m_bOndiskAllAttr = bOndiskAttrs;
	m_bOndiskPoolAttr = ( bOndiskAttrs || bOndiskPool );
}


void CSphIndex_VLN::SetBase ( const char * sNewBase )
{
	m_sFilename = sNewBase;
}


bool CSphIndex_VLN::Rename ( const char * sNewBase )
{
	if ( m_sFilename==sNewBase )
		return true;

	// try to rename everything
	char sFrom [ SPH_MAX_FILENAME_LEN ];
	char sTo [ SPH_MAX_FILENAME_LEN ];

	// +1 for ".spl"
	int iExtCount = sphGetExtCount() + 1;
	const char ** sExts = sphGetExts ( SPH_EXT_TYPE_LOC );
	DWORD uMask = 0;

	int iExt;
	for ( iExt=0; iExt<iExtCount; iExt++ )
	{
		const char * sExt = sExts[iExt];
		if ( !strcmp ( sExt, ".spp" ) && m_uVersion<3 ) // .spp files are v3+
			continue;
		if ( !strcmp ( sExt, ".spm" ) && m_uVersion<4 ) // .spm files are v4+
			continue;
		if ( !strcmp ( sExt, ".spk" ) && m_uVersion<10 ) // .spk files are v10+
			continue;
		if ( !strcmp ( sExt, ".sps" ) && m_uVersion<17 ) // .sps files are v17+
			continue;
		if ( !strcmp ( sExt, ".spe" ) && m_uVersion<31 ) // .spe files are v31+
			continue;

#if !USE_WINDOWS
		if ( !strcmp ( sExt, ".spl" ) && m_iLockFD<0 ) // .spl files are locks
			continue;
#else
		if ( !strcmp ( sExt, ".spl" ) )
		{
			if ( m_iLockFD>=0 )
			{
				::close ( m_iLockFD );
				::unlink ( GetIndexFileName("spl").cstr() );
				sphLogDebug ( "lock %s unlinked, file with ID %d closed", GetIndexFileName("spl").cstr(), m_iLockFD );
				m_iLockFD = -1;
			}
			continue;
		}
#endif

		snprintf ( sFrom, sizeof(sFrom), "%s%s", m_sFilename.cstr(), sExt );
		snprintf ( sTo, sizeof(sTo), "%s%s", sNewBase, sExt );

#if USE_WINDOWS
		::unlink ( sTo );
		sphLogDebug ( "%s unlinked", sTo );
#endif

		if ( ::rename ( sFrom, sTo ) )
		{
			m_sLastError.SetSprintf ( "rename %s to %s failed: %s", sFrom, sTo, strerror(errno) );
			// this is no reason to fail if spl is missing, since it is only lock and no data.
			if ( strcmp ( sExt, ".spl" ) )
				break;
		}
		uMask |= ( 1UL << iExt );
	}

	// are we good?
	if ( iExt==iExtCount )
	{
		SetBase ( sNewBase );
		sphLogDebug ( "Base set to %s", sNewBase );
		return true;
	}

	// if there were errors, rollback
	for ( iExt=0; iExt<iExtCount; iExt++ )
	{
		if (!( uMask & ( 1UL << iExt ) ))
			continue;

		const char * sExt = sExts[iExt];
		snprintf ( sFrom, sizeof(sFrom), "%s%s", sNewBase, sExt );
		snprintf ( sTo, sizeof(sTo), "%s%s", m_sFilename.cstr(), sExt );
		if ( ::rename ( sFrom, sTo ) )
		{
			sphLogDebug ( "Rollback failure when renaming %s to %s", sFrom, sTo );
			// !COMMIT should handle rollback failures somehow
		}
	}
	return false;
}


bool CSphIndex_VLN::IsStarDict () const
{
	return (
		( m_uVersion>=7 && ( m_tSettings.m_iMinPrefixLen>0 || m_tSettings.m_iMinInfixLen>0 ) ) || // v.7 added mangling to infixes
		( m_uVersion==6 && ( m_tSettings.m_iMinPrefixLen>0 ) ) ); // v.6 added mangling to prefixes
}


CSphDict * CSphIndex_VLN::SetupStarDict ( CSphScopedPtr<CSphDict> & tContainer, CSphDict * pPrevDict ) const
{
	// spawn wrapper, and put it in the box
	// wrapper type depends on version; v.8 introduced new mangling rules
	if ( !IsStarDict() )
		return pPrevDict;
	if ( m_uVersion>=8 )
		tContainer = new CSphDictStarV8 ( pPrevDict, m_tSettings.m_iMinPrefixLen>0, m_tSettings.m_iMinInfixLen>0 );
	else
		tContainer = new CSphDictStar ( pPrevDict );

	// FIXME? might wanna verify somehow that the tokenizer has '*' as a character
	return tContainer.Ptr();
}


CSphDict * CSphIndex_VLN::SetupExactDict ( CSphScopedPtr<CSphDict> & tContainer, CSphDict * pPrevDict ) const
{
	if ( m_uVersion<12 || !m_tSettings.m_bIndexExactWords )
		return pPrevDict;

	tContainer = new CSphDictExact ( pPrevDict );
	return tContainer.Ptr();
}


bool CSphIndex_VLN::GetKeywords ( CSphVector <CSphKeywordInfo> & dKeywords,
	const char * szQuery, const GetKeywordsSettings_t & tSettings, CSphString * pError ) const
{
	WITH_QWORD ( this, false, Qword, return DoGetKeywords<Qword> ( dKeywords, szQuery, tSettings, false, pError ) );
	return false;
}

void CSphIndex_VLN::GetSuggest ( const SuggestArgs_t & tArgs, SuggestResult_t & tRes ) const
{
	if ( m_tWordlist.m_tBuf.IsEmpty() || !m_tWordlist.m_dCheckpoints.GetLength() )
		return;

	assert ( !tRes.m_pWordReader );
	tRes.m_pWordReader = new KeywordsBlockReader_c ( m_tWordlist.m_tBuf.GetWritePtr(), m_tWordlist.m_bHaveSkips );
	tRes.m_bHasExactDict = m_tSettings.m_bIndexExactWords;

	sphGetSuggest ( &m_tWordlist, m_tWordlist.m_iInfixCodepointBytes, tArgs, tRes );

	KeywordsBlockReader_c * pReader = (KeywordsBlockReader_c *)tRes.m_pWordReader;
	SafeDelete ( pReader );
	tRes.m_pWordReader = NULL;
}


template < class Qword >
bool CSphIndex_VLN::DoGetKeywords ( CSphVector <CSphKeywordInfo> & dKeywords,
	const char * szQuery, const GetKeywordsSettings_t & tSettings, bool bFillOnly, CSphString * pError ) const
{
	if ( !bFillOnly )
		dKeywords.Resize ( 0 );

	if ( !m_bPassedAlloc )
	{
		if ( pError )
			*pError = "index not preread";
		return false;
	}

	// short-cut if no query or keywords to fill
	if ( ( bFillOnly && !dKeywords.GetLength() ) || ( !bFillOnly && ( !szQuery || !szQuery[0] ) ) )
		return true;

	// TODO: in case of bFillOnly skip tokenizer cloning and setup
	CSphScopedPtr<ISphTokenizer> pTokenizer ( m_pTokenizer->Clone ( SPH_CLONE_INDEX ) ); // avoid race
	pTokenizer->EnableTokenizedMultiformTracking ();

	// need to support '*' and '=' but not the other specials
	// so m_pQueryTokenizer does not work for us, gotta clone and setup one manually
	if ( IsStarDict() )
		pTokenizer->AddPlainChar ( '*' );
	if ( m_tSettings.m_bIndexExactWords )
		pTokenizer->AddPlainChar ( '=' );

	CSphScopedPtr<CSphDict> tDictCloned ( NULL );
	CSphDict * pDictBase = m_pDict;
	if ( pDictBase->HasState() )
		tDictCloned = pDictBase = pDictBase->Clone();

	CSphScopedPtr<CSphDict> tDict ( NULL );
	CSphDict * pDict = SetupStarDict ( tDict, pDictBase );

	CSphScopedPtr<CSphDict> tDict2 ( NULL );
	pDict = SetupExactDict ( tDict2, pDict );

	CSphVector<BYTE> dFiltered;
	CSphScopedPtr<ISphFieldFilter> pFieldFilter ( NULL );
	const BYTE * sModifiedQuery = (const BYTE *)szQuery;
	if ( m_pFieldFilter && szQuery )
	{
		pFieldFilter = m_pFieldFilter->Clone();
		if ( pFieldFilter.Ptr() && pFieldFilter->Apply ( sModifiedQuery, strlen ( (char*)sModifiedQuery ), dFiltered, true ) )
			sModifiedQuery = dFiltered.Begin();
	}

	// FIXME!!! missed bigram, add flags to fold blended parts, show expanded terms

	// prepare for setup
	CSphAutofile tDummy1, tDummy2;

	DiskIndexQwordSetup_c tTermSetup ( tDummy1, tDummy2, m_tSkiplists.GetWritePtr(), NULL );
	tTermSetup.m_pDict = pDict;
	tTermSetup.m_pIndex = this;
	tTermSetup.m_eDocinfo = m_tSettings.m_eDocinfo;

	Qword tQueryWord ( false, false );

	if ( !bFillOnly )
	{
		ExpansionContext_t tExpCtx;
		// query defined options
		tExpCtx.m_iExpansionLimit = ( tSettings.m_iExpansionLimit ? tSettings.m_iExpansionLimit : m_iExpansionLimit );
		bool bExpandWildcards = ( pDict->GetSettings ().m_bWordDict && IsStarDict() && !tSettings.m_bFoldWildcards );

		CSphPlainQueryFilter tAotFilter;
		tAotFilter.m_pTokenizer = pTokenizer.Ptr();
		tAotFilter.m_pDict = pDict;
		tAotFilter.m_pSettings = &m_tSettings;
		tAotFilter.m_pTermSetup = &tTermSetup;
		tAotFilter.m_pQueryWord = &tQueryWord;
		tAotFilter.m_tFoldSettings = tSettings;
		tAotFilter.m_tFoldSettings.m_bFoldWildcards = !bExpandWildcards;

		tExpCtx.m_pWordlist = &m_tWordlist;
		tExpCtx.m_iMinPrefixLen = m_tSettings.m_iMinPrefixLen;
		tExpCtx.m_iMinInfixLen = m_tSettings.m_iMinInfixLen;
		tExpCtx.m_bHasMorphology = m_pDict->HasMorphology();
		tExpCtx.m_bMergeSingles = false;
		tExpCtx.m_eHitless = m_tSettings.m_eHitless;

		pTokenizer->SetBuffer ( sModifiedQuery, strlen ( (const char *)sModifiedQuery) );

		tAotFilter.GetKeywords ( dKeywords, tExpCtx );
	} else
	{
		BYTE sWord[MAX_KEYWORD_BYTES];

		ARRAY_FOREACH ( i, dKeywords )
		{
			CSphKeywordInfo & tInfo = dKeywords[i];
			int iLen = tInfo.m_sTokenized.Length();
			memcpy ( sWord, tInfo.m_sTokenized.cstr(), iLen );
			sWord[iLen] = '\0';

			SphWordID_t iWord = pDict->GetWordID ( sWord );
			if ( iWord )
			{
				tQueryWord.Reset ();
				tQueryWord.m_sWord = tInfo.m_sTokenized;
				tQueryWord.m_sDictWord = (const char*)sWord;
				tQueryWord.m_uWordID = iWord;
				tTermSetup.QwordSetup ( &tQueryWord );

				tInfo.m_iDocs += tQueryWord.m_iDocs;
				tInfo.m_iHits += tQueryWord.m_iHits;
			}
		}
	}

	return true;
}


bool CSphIndex_VLN::FillKeywords ( CSphVector <CSphKeywordInfo> & dKeywords ) const
{
	GetKeywordsSettings_t tSettings;
	tSettings.m_bStats = true;

	WITH_QWORD ( this, false, Qword, return DoGetKeywords<Qword> ( dKeywords, NULL, tSettings, true, NULL ) );
	return false;
}


XQNode_t * CSphIndex_VLN::ExpandPrefix ( XQNode_t * pNode, CSphQueryResultMeta * pResult, CSphScopedPayload * pPayloads, DWORD uQueryDebugFlags ) const
{
	if ( !pNode || !m_pDict->GetSettings().m_bWordDict || ( m_tSettings.m_iMinPrefixLen<=0 && m_tSettings.m_iMinInfixLen<=0 ) )
		return pNode;

	assert ( m_bPassedAlloc );
	assert ( !m_tWordlist.m_tBuf.IsEmpty() );

	ExpansionContext_t tCtx;
	tCtx.m_pWordlist = &m_tWordlist;
	tCtx.m_pResult = pResult;
	tCtx.m_iMinPrefixLen = m_tSettings.m_iMinPrefixLen;
	tCtx.m_iMinInfixLen = m_tSettings.m_iMinInfixLen;
	tCtx.m_iExpansionLimit = m_iExpansionLimit;
	tCtx.m_bHasMorphology = m_pDict->HasMorphology();
	tCtx.m_bMergeSingles = ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_INLINE && ( uQueryDebugFlags & QUERY_DEBUG_NO_PAYLOAD )==0 );
	tCtx.m_pPayloads = pPayloads;
	tCtx.m_eHitless = m_tSettings.m_eHitless;

	pNode = sphExpandXQNode ( pNode, tCtx );
	pNode->Check ( true );

	return pNode;
}


struct CmpPSortersByRandom_fn
{
	inline bool IsLess(const ISphMatchSorter* a, const ISphMatchSorter* b) const
	{
		assert(a);
		assert(b);
		return a->m_bRandomize < b->m_bRandomize;
	}
};



/// one regular query vs many sorters
bool CSphIndex_VLN::MultiQuery ( const CSphQuery * pQuery, CSphQueryResult * pResult,
	int iSorters, ISphMatchSorter ** ppSorters, const CSphMultiQueryArgs & tArgs ) const
{
	assert ( pQuery );
	CSphQueryProfile * pProfile = pResult->m_pProfile;

	MEMORY ( MEM_DISK_QUERY );

	// to avoid the checking of a ppSorters's element for NULL on every next step, just filter out all nulls right here
	CSphVector<ISphMatchSorter*> dSorters;
	dSorters.Reserve ( iSorters );
	for ( int i=0; i<iSorters; i++ )
		if ( ppSorters[i] )
			dSorters.Add ( ppSorters[i] );

	iSorters = dSorters.GetLength();

	// if we have anything to work with
	if ( iSorters==0 )
		return false;

	// non-random at the start, random at the end
	dSorters.Sort ( CmpPSortersByRandom_fn() );

	// fast path for scans
	if ( pQuery->m_sQuery.IsEmpty() )
		return MultiScan ( pQuery, pResult, iSorters, &dSorters[0], tArgs );

	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_DICT_SETUP );

	CSphScopedPtr<CSphDict> tDictCloned ( NULL );
	CSphDict * pDictBase = m_pDict;
	if ( pDictBase->HasState() )
		tDictCloned = pDictBase = pDictBase->Clone();

	CSphScopedPtr<CSphDict> tDict ( NULL );
	CSphDict * pDict = SetupStarDict ( tDict, pDictBase );

	CSphScopedPtr<CSphDict> tDict2 ( NULL );
	pDict = SetupExactDict ( tDict2, pDict );

	CSphVector<BYTE> dFiltered;
	const BYTE * sModifiedQuery = (BYTE *)pQuery->m_sQuery.cstr();

	CSphScopedPtr<ISphFieldFilter> pFieldFilter ( NULL );
	if ( m_pFieldFilter )
	{
		pFieldFilter = m_pFieldFilter->Clone();
		if ( pFieldFilter.Ptr() && pFieldFilter->Apply ( sModifiedQuery, strlen ( (char*)sModifiedQuery ), dFiltered, true ) )
			sModifiedQuery = dFiltered.Begin();
	}

	// parse query
	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_PARSE );

	XQQuery_t tParsed;
	if ( !sphParseExtendedQuery ( tParsed, (const char*)sModifiedQuery, pQuery, m_pQueryTokenizer, &m_tSchema, pDict, m_tSettings ) )
	{
		// FIXME? might wanna reset profile to unknown state
		pResult->m_sError = tParsed.m_sParseError;
		return false;
	}
	if ( !tParsed.m_sParseWarning.IsEmpty() )
		pResult->m_sWarning = tParsed.m_sParseWarning;

	// transform query if needed (quorum transform, etc.)
	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_TRANSFORMS );
	sphTransformExtendedQuery ( &tParsed.m_pRoot, m_tSettings, pQuery->m_bSimplify, this );

	if ( m_bExpandKeywords )
	{
		tParsed.m_pRoot = sphQueryExpandKeywords ( tParsed.m_pRoot, m_tSettings );
		tParsed.m_pRoot->Check ( true );
	}

	// this should be after keyword expansion
	if ( m_tSettings.m_uAotFilterMask )
		TransformAotFilter ( tParsed.m_pRoot, pDict->GetWordforms(), m_tSettings );

	SphWordStatChecker_t tStatDiff;
	tStatDiff.Set ( pResult->m_hWordStats );

	// expanding prefix in word dictionary case
	CSphScopedPayload tPayloads;
	XQNode_t * pPrefixed = ExpandPrefix ( tParsed.m_pRoot, pResult, &tPayloads, pQuery->m_uDebugFlags );
	if ( !pPrefixed )
		return false;
	tParsed.m_pRoot = pPrefixed;

	if ( !sphCheckQueryHeight ( tParsed.m_pRoot, pResult->m_sError ) )
		return false;

	// flag common subtrees
	int iCommonSubtrees = 0;
	if ( m_iMaxCachedDocs && m_iMaxCachedHits )
		iCommonSubtrees = sphMarkCommonSubtrees ( 1, &tParsed );

	tParsed.m_bNeedSZlist = pQuery->m_bZSlist;

	CSphQueryNodeCache tNodeCache ( iCommonSubtrees, m_iMaxCachedDocs, m_iMaxCachedHits );
	bool bResult = ParsedMultiQuery ( pQuery, pResult, iSorters, &dSorters[0], tParsed, pDict, tArgs, &tNodeCache, tStatDiff );

	return bResult;
}


/// many regular queries with one sorter attached to each query.
/// returns true if at least one query succeeded. The failed queries indicated with pResult->m_iMultiplier==-1
bool CSphIndex_VLN::MultiQueryEx ( int iQueries, const CSphQuery * pQueries,
	CSphQueryResult ** ppResults, ISphMatchSorter ** ppSorters, const CSphMultiQueryArgs & tArgs ) const
{
	// ensure we have multiple queries
	assert ( ppResults );
	if ( iQueries==1 )
		return MultiQuery ( pQueries, ppResults[0], 1, ppSorters, tArgs );

	MEMORY ( MEM_DISK_QUERYEX );

	assert ( pQueries );
	assert ( ppSorters );

	CSphScopedPtr<CSphDict> tDictCloned ( NULL );
	CSphDict * pDictBase = m_pDict;
	if ( pDictBase->HasState() )
		tDictCloned = pDictBase = pDictBase->Clone();

	CSphScopedPtr<CSphDict> tDict ( NULL );
	CSphDict * pDict = SetupStarDict ( tDict, pDictBase );

	CSphScopedPtr<CSphDict> tDict2 ( NULL );
	pDict = SetupExactDict ( tDict2, pDict );

	CSphFixedVector<XQQuery_t> dXQ ( iQueries );
	CSphFixedVector<SphWordStatChecker_t> dStatChecker ( iQueries );
	CSphScopedPayload tPayloads;
	bool bResult = false;
	bool bResultScan = false;
	for ( int i=0; i<iQueries; i++ )
	{
		// nothing to do without a sorter
		if ( !ppSorters[i] )
		{
			ppResults[i]->m_iMultiplier = -1; //show that this particular query failed
			continue;
		}

		// fast path for scans
		if ( pQueries[i].m_sQuery.IsEmpty() )
		{
			if ( MultiScan ( pQueries + i, ppResults[i], 1, &ppSorters[i], tArgs ) )
				bResultScan = true;
			else
				ppResults[i]->m_iMultiplier = -1; //show that this particular query failed
			continue;
		}

		ppResults[i]->m_tIOStats.Start();

		// parse query
		if ( sphParseExtendedQuery ( dXQ[i], pQueries[i].m_sQuery.cstr(), &(pQueries[i]), m_pQueryTokenizer, &m_tSchema, pDict, m_tSettings ) )
		{
			// transform query if needed (quorum transform, keyword expansion, etc.)
			sphTransformExtendedQuery ( &dXQ[i].m_pRoot, m_tSettings, pQueries[i].m_bSimplify, this );

			if ( m_bExpandKeywords )
			{
				dXQ[i].m_pRoot = sphQueryExpandKeywords ( dXQ[i].m_pRoot, m_tSettings );
				dXQ[i].m_pRoot->Check ( true );
			}

			// this should be after keyword expansion
			if ( m_tSettings.m_uAotFilterMask )
				TransformAotFilter ( dXQ[i].m_pRoot, pDict->GetWordforms(), m_tSettings );

			dStatChecker[i].Set ( ppResults[i]->m_hWordStats );

			// expanding prefix in word dictionary case
			XQNode_t * pPrefixed = ExpandPrefix ( dXQ[i].m_pRoot, ppResults[i], &tPayloads, pQueries[i].m_uDebugFlags );
			if ( pPrefixed )
			{
				dXQ[i].m_pRoot = pPrefixed;

				if ( sphCheckQueryHeight ( dXQ[i].m_pRoot, ppResults[i]->m_sError ) )
				{
					bResult = true;
				} else
				{
					ppResults[i]->m_iMultiplier = -1;
					SafeDelete ( dXQ[i].m_pRoot );
				}
			} else
			{
				ppResults[i]->m_iMultiplier = -1;
				SafeDelete ( dXQ[i].m_pRoot );
			}
		} else
		{
			ppResults[i]->m_sError = dXQ[i].m_sParseError;
			ppResults[i]->m_iMultiplier = -1;
		}
		if ( !dXQ[i].m_sParseWarning.IsEmpty() )
			ppResults[i]->m_sWarning = dXQ[i].m_sParseWarning;

		ppResults[i]->m_tIOStats.Stop();
	}

	// continue only if we have at least one non-failed
	if ( bResult )
	{
		int iCommonSubtrees = 0;
		if ( m_iMaxCachedDocs && m_iMaxCachedHits )
			iCommonSubtrees = sphMarkCommonSubtrees ( iQueries, &dXQ[0] );

		CSphQueryNodeCache tNodeCache ( iCommonSubtrees, m_iMaxCachedDocs, m_iMaxCachedHits );
		bResult = false;
		for ( int j=0; j<iQueries; j++ )
		{
			// fullscan case
			if ( pQueries[j].m_sQuery.IsEmpty() )
				continue;

			ppResults[j]->m_tIOStats.Start();

			if ( dXQ[j].m_pRoot && ppSorters[j]
					&& ParsedMultiQuery ( &pQueries[j], ppResults[j], 1, &ppSorters[j], dXQ[j], pDict, tArgs, &tNodeCache, dStatChecker[j] ) )
			{
				bResult = true;
				ppResults[j]->m_iMultiplier = iCommonSubtrees ? iQueries : 1;
			} else
			{
				ppResults[j]->m_iMultiplier = -1;
			}

			ppResults[j]->m_tIOStats.Stop();
		}
	}

	return bResult | bResultScan;
}

bool CSphIndex_VLN::ParsedMultiQuery ( const CSphQuery * pQuery, CSphQueryResult * pResult,
	int iSorters, ISphMatchSorter ** ppSorters, const XQQuery_t & tXQ, CSphDict * pDict,
	const CSphMultiQueryArgs & tArgs, CSphQueryNodeCache * pNodeCache, const SphWordStatChecker_t & tStatDiff ) const
{
	assert ( pQuery );
	assert ( pResult );
	assert ( ppSorters );
	assert ( !pQuery->m_sQuery.IsEmpty() && pQuery->m_eMode!=SPH_MATCH_FULLSCAN ); // scans must go through MultiScan()
	assert ( tArgs.m_iTag>=0 );

	// start counting
	int64_t tmQueryStart = sphMicroTimer();

	CSphQueryProfile * pProfile = pResult->m_pProfile;
	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_INIT );

	ScopedThreadPriority_c tPrio ( pQuery->m_bLowPriority );

	///////////////////
	// setup searching
	///////////////////

	// non-ready index, empty response!
	if ( !m_bPassedAlloc )
	{
		pResult->m_sError = "index not preread";
		return false;
	}

	// select the sorter with max schema
	int iMaxSchemaSize = -1;
	int iMaxSchemaIndex = -1;
	for ( int i=0; i<iSorters; i++ )
		if ( ppSorters[i]->GetSchema().GetRowSize() > iMaxSchemaSize )
		{
			iMaxSchemaSize = ppSorters[i]->GetSchema().GetRowSize();
			iMaxSchemaIndex = i;
		}

	// setup calculations and result schema
	CSphQueryContext tCtx ( *pQuery );
	tCtx.m_pProfile = pProfile;
	tCtx.m_pLocalDocs = tArgs.m_pLocalDocs;
	tCtx.m_iTotalDocs = tArgs.m_iTotalDocs;
	if ( !tCtx.SetupCalc ( pResult, ppSorters[iMaxSchemaIndex]->GetSchema(), m_tSchema, m_tMva.GetWritePtr(), m_bArenaProhibit ) )
		return false;

	// set string pool for string on_sort expression fix up
	tCtx.SetStringPool ( m_tString.GetWritePtr() );

	tCtx.m_uPackedFactorFlags = tArgs.m_uPackedFactorFlags;

	// open files
	CSphAutofile tDoclist, tHitlist;
	if ( !m_bKeepFilesOpen )
	{
		if ( pProfile )
			pProfile->Switch ( SPH_QSTATE_OPEN );

		if ( tDoclist.Open ( GetIndexFileName("spd"), SPH_O_READ, pResult->m_sError ) < 0 )
			return false;

		if ( tHitlist.Open ( GetIndexFileName ( m_uVersion>=3 ? "spp" : "spd" ), SPH_O_READ, pResult->m_sError ) < 0 )
			return false;
	}

	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_INIT );

	// setup search terms
	DiskIndexQwordSetup_c tTermSetup ( m_bKeepFilesOpen ? m_tDoclistFile : tDoclist,
		m_bKeepFilesOpen ? m_tHitlistFile : tHitlist,
		m_tSkiplists.GetWritePtr(), pProfile );

	tTermSetup.m_pDict = pDict;
	tTermSetup.m_pIndex = this;
	tTermSetup.m_eDocinfo = m_tSettings.m_eDocinfo;
	tTermSetup.m_uMinDocid = m_uMinDocid;
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
	{
		tTermSetup.m_iInlineRowitems = m_tSchema.GetRowSize();
		tTermSetup.m_pMinRow = m_dMinRow.Begin();
	}
	tTermSetup.m_iDynamicRowitems = ppSorters[iMaxSchemaIndex]->GetSchema().GetDynamicSize();

	if ( pQuery->m_uMaxQueryMsec>0 )
		tTermSetup.m_iMaxTimer = sphMicroTimer() + pQuery->m_uMaxQueryMsec*1000; // max_query_time
	tTermSetup.m_pWarning = &pResult->m_sWarning;
	tTermSetup.m_bSetupReaders = true;
	tTermSetup.m_pCtx = &tCtx;
	tTermSetup.m_pNodeCache = pNodeCache;

	// setup prediction constrain
	CSphQueryStats tQueryStats;
	bool bCollectPredictionCounters = ( pQuery->m_iMaxPredictedMsec>0 );
	int64_t iNanoBudget = (int64_t)(pQuery->m_iMaxPredictedMsec) * 1000000; // from milliseconds to nanoseconds
	tQueryStats.m_pNanoBudget = &iNanoBudget;
	if ( bCollectPredictionCounters )
		tTermSetup.m_pStats = &tQueryStats;

	// bind weights
	tCtx.BindWeights ( pQuery, m_tSchema, pResult->m_sWarning );

	// setup query
	// must happen before index-level reject, in order to build proper keyword stats
	CSphScopedPtr<ISphRanker> pRanker ( sphCreateRanker ( tXQ, pQuery, pResult, tTermSetup, tCtx, ppSorters[iMaxSchemaIndex]->GetSchema() ) );
	if ( !pRanker.Ptr() )
		return false;

	tStatDiff.DumpDiffer ( pResult->m_hWordStats, m_sIndexName.cstr(), pResult->m_sWarning );

	if ( ( tArgs.m_uPackedFactorFlags & SPH_FACTOR_ENABLE ) && pQuery->m_eRanker!=SPH_RANK_EXPR )
		pResult->m_sWarning.SetSprintf ( "packedfactors() and bm25f() requires using an expression ranker" );

	tCtx.SetupExtraData ( pRanker.Ptr(), iSorters==1 ? ppSorters[0] : NULL );

	PoolPtrs_t tMva;
	tMva.m_pMva = m_tMva.GetWritePtr();
	tMva.m_bArenaProhibit = m_bArenaProhibit;
	pRanker->ExtraData ( EXTRA_SET_MVAPOOL, (void**)&tMva );
	pRanker->ExtraData ( EXTRA_SET_STRINGPOOL, (void**)m_tString.GetWritePtr() );

	int iMatchPoolSize = 0;
	for ( int i=0; i<iSorters; i++ )
		iMatchPoolSize += ppSorters[i]->m_iMatchCapacity;

	pRanker->ExtraData ( EXTRA_SET_POOL_CAPACITY, (void**)&iMatchPoolSize );

	// check for the possible integer overflow in m_dPool.Resize
	int64_t iPoolSize = 0;
	if ( pRanker->ExtraData ( EXTRA_GET_POOL_SIZE, (void**)&iPoolSize ) && iPoolSize>INT_MAX )
	{
		pResult->m_sError.SetSprintf ( "ranking factors pool too big (%d Mb), reduce max_matches", (int)( iPoolSize/1024/1024 ) );
		return false;
	}

	// empty index, empty response!
	if ( m_bIsEmpty )
		return true;
	assert ( m_tSettings.m_eDocinfo!=SPH_DOCINFO_EXTERN || !m_tAttr.IsEmpty() ); // check that docinfo is preloaded

	// setup filters
	if ( !tCtx.CreateFilters ( pQuery->m_sQuery.IsEmpty(), &pQuery->m_dFilters, ppSorters[iMaxSchemaIndex]->GetSchema(),
		m_tMva.GetWritePtr(), m_tString.GetWritePtr(), pResult->m_sError, pResult->m_sWarning, pQuery->m_eCollation, m_bArenaProhibit, tArgs.m_dKillList ) )
			return false;

	// check if we can early reject the whole index
	if ( tCtx.m_pFilter && m_iDocinfoIndex )
	{
		DWORD uStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
		DWORD * pMinEntry = const_cast<DWORD*> ( &m_pDocinfoIndex [ m_iDocinfoIndex*uStride*2 ] );
		DWORD * pMaxEntry = pMinEntry + uStride;

		if ( !tCtx.m_pFilter->EvalBlock ( pMinEntry, pMaxEntry ) )
			return true;
	}

	// setup lookup
	tCtx.m_bLookupFilter = ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN ) && pQuery->m_dFilters.GetLength();
	if ( tCtx.m_dCalcFilter.GetLength() || pQuery->m_eRanker==SPH_RANK_EXPR || pQuery->m_eRanker==SPH_RANK_EXPORT )
		tCtx.m_bLookupFilter = true; // suboptimal in case of attr-independent expressions, but we don't care

	tCtx.m_bLookupSort = false;
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && !tCtx.m_bLookupFilter )
		for ( int iSorter=0; iSorter<iSorters && !tCtx.m_bLookupSort; iSorter++ )
			if ( ppSorters[iSorter]->UsesAttrs() )
				tCtx.m_bLookupSort = true;
	if ( tCtx.m_dCalcSort.GetLength() )
		tCtx.m_bLookupSort = true; // suboptimal in case of attr-independent expressions, but we don't care

	// setup sorters vs. MVA
	for ( int i=0; i<iSorters; i++ )
	{
		(ppSorters[i])->SetMVAPool ( m_tMva.GetWritePtr(), m_bArenaProhibit );
		(ppSorters[i])->SetStringPool ( m_tString.GetWritePtr() );
	}

	// setup overrides
	if ( !tCtx.SetupOverrides ( pQuery, pResult, m_tSchema, ppSorters[iMaxSchemaIndex]->GetSchema() ) )
		return false;

	//////////////////////////////////////
	// find and weight matching documents
	//////////////////////////////////////

	bool bFinalLookup = !tCtx.m_bLookupFilter && !tCtx.m_bLookupSort;
	bool bFinalPass = bFinalLookup || tCtx.m_dCalcFinal.GetLength();
	int iMyTag = bFinalPass ? -1 : tArgs.m_iTag;

	switch ( pQuery->m_eMode )
	{
		case SPH_MATCH_ALL:
		case SPH_MATCH_PHRASE:
		case SPH_MATCH_ANY:
		case SPH_MATCH_EXTENDED:
		case SPH_MATCH_EXTENDED2:
		case SPH_MATCH_BOOLEAN:
			MatchExtended ( &tCtx, pQuery, iSorters, ppSorters, pRanker.Ptr(), iMyTag, tArgs.m_iIndexWeight );
			break;

		default:
			sphDie ( "INTERNAL ERROR: unknown matching mode (mode=%d)", pQuery->m_eMode );
	}

	////////////////////
	// cook result sets
	////////////////////

	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_FINALIZE );

	// adjust result sets
	if ( bFinalPass )
	{
		// GotUDF means promise to UDFs that final-stage calls will be evaluated
		// a) over the final, pre-limit result set
		// b) in the final result set order
		bool bGotUDF = false;
		ARRAY_FOREACH_COND ( i, tCtx.m_dCalcFinal, !bGotUDF )
			tCtx.m_dCalcFinal[i].m_pExpr->Command ( SPH_EXPR_GET_UDF, &bGotUDF );

		SphFinalMatchCalc_t tProcessor ( tArgs.m_iTag, bFinalLookup ? this : NULL, tCtx );
		for ( int iSorter=0; iSorter<iSorters; iSorter++ )
		{
			ISphMatchSorter * pTop = ppSorters[iSorter];
			pTop->Finalize ( tProcessor, bGotUDF );
		}
		pResult->m_iBadRows += tProcessor.m_iBadRows;
	}

	pRanker->FinalizeCache ( ppSorters[iMaxSchemaIndex]->GetSchema() );

	// mva and string pools ptrs
	pResult->m_pMva = m_tMva.GetWritePtr();
	pResult->m_pStrings = m_tString.GetWritePtr();
	pResult->m_bArenaProhibit = m_bArenaProhibit;
	pResult->m_iBadRows += tCtx.m_iBadRows;

	// query timer
	int64_t tmWall = sphMicroTimer() - tmQueryStart;
	pResult->m_iQueryTime += (int)( tmWall/1000 );

#if 0
	printf ( "qtm %d, %d, %d, %d, %d\n", int(tmWall), tQueryStats.m_iFetchedDocs,
		tQueryStats.m_iFetchedHits, tQueryStats.m_iSkips, ppSorters[0]->GetTotalCount() );
#endif

	if ( pProfile )
		pProfile->Switch ( SPH_QSTATE_UNKNOWN );

	if ( bCollectPredictionCounters )
	{
		pResult->m_tStats.m_iFetchedDocs += tQueryStats.m_iFetchedDocs;
		pResult->m_tStats.m_iFetchedHits += tQueryStats.m_iFetchedHits;
		pResult->m_tStats.m_iSkips += tQueryStats.m_iSkips;
		pResult->m_bHasPrediction = true;
	}

	return true;
}

//////////////////////////////////////////////////////////////////////////
// INDEX STATUS
//////////////////////////////////////////////////////////////////////////

void CSphIndex_VLN::GetStatus ( CSphIndexStatus* pRes ) const
{
	assert ( pRes );
	if ( !pRes )
		return;
	pRes->m_iRamUse = sizeof(CSphIndex_VLN)
		+ m_dMinRow.GetSizeBytes()
		+ m_dFieldLens.GetSizeBytes()

		+ m_tDocinfoHash.GetLengthBytes()
		+ m_tAttr.GetLengthBytes()
		+ m_tMva.GetLengthBytes()
		+ m_tString.GetLengthBytes()
		+ m_tWordlist.m_tBuf.GetLengthBytes()
		+ m_tKillList.GetLengthBytes()
		+ m_tSkiplists.GetLengthBytes();

	char sFile [ SPH_MAX_FILENAME_LEN ];
	pRes->m_iDiskUse = 0;
	for ( int i=0; i<sphGetExtCount ( m_uVersion ); i++ )
	{
		snprintf ( sFile, sizeof(sFile), "%s%s", m_sFilename.cstr(), sphGetExts ( SPH_EXT_TYPE_CUR, m_uVersion )[i] );
		struct_stat st;
		if ( stat ( sFile, &st )==0 )
			pRes->m_iDiskUse += st.st_size;
	}
}

//////////////////////////////////////////////////////////////////////////
// INDEX CHECKING
//////////////////////////////////////////////////////////////////////////

void CSphIndex_VLN::SetDebugCheck ()
{
	m_bDebugCheck = true;
}

static int sphUnpackStrLength ( CSphReader & tReader )
{
	int v = tReader.GetByte();
	if ( v & 0x80 )
	{
		if ( v & 0x40 )
		{
			v = ( int ( v & 0x3f )<<16 ) + ( int ( tReader.GetByte() )<<8 );
			v += ( tReader.GetByte() ); // MUST be separate statement; cf. sequence point
		} else
		{
			v = ( int ( v & 0x3f )<<8 ) + ( tReader.GetByte() );
		}
	}

	return v;
}

class CSphDocidList
{
public:
	CSphDocidList ()
	{
		m_bRawID = true;
		m_iDocidMin = DOCID_MAX;
		m_iDocidMax = 0;
	}

	~CSphDocidList ()
	{}

	bool Init ( int iRowSize, int64_t iRows, CSphReader & rdAttr, CSphString & sError )
	{
		if ( !iRows )
			return true;

		int iSkip = sizeof ( CSphRowitem ) * iRowSize;

		rdAttr.SeekTo ( 0, sizeof ( CSphRowitem ) * ( DOCINFO_IDSIZE + iRowSize ) );
		m_iDocidMin = rdAttr.GetDocid ();
		rdAttr.SeekTo ( ( iRows-1 ) * sizeof ( CSphRowitem ) * ( DOCINFO_IDSIZE + iRowSize ), sizeof ( CSphRowitem ) * ( DOCINFO_IDSIZE + iRowSize ) );
		m_iDocidMax = rdAttr.GetDocid();
		rdAttr.SeekTo ( 0, sizeof ( CSphRowitem ) * ( DOCINFO_IDSIZE + iRowSize ) );

		if ( m_iDocidMax<m_iDocidMin )
			return true;

		uint64_t uRawBufLenght = sizeof(SphDocID_t) * iRows;
		uint64_t uBitsBufLenght = ( m_iDocidMax - m_iDocidMin ) / 32;
		if ( uRawBufLenght<uBitsBufLenght )
		{
			if ( !m_dDocid.Alloc ( iRows, sError ) )
			{
				sError.SetSprintf ( "unable to allocate doc-id storage: %s", sError.cstr () );
				return false;
			}
		} else
		{
			if ( !m_dBits.Alloc ( ( uBitsBufLenght * sizeof(DWORD) )+1, sError ) )
			{
				sError.SetSprintf ( "unable to allocate doc-id storage: %s", sError.cstr () );
				return false;
			}
			m_bRawID = false;
			memset ( m_dBits.GetWritePtr(), 0, m_dBits.GetLengthBytes() );
		}

		for ( int64_t iRow=0; iRow<iRows && !rdAttr.GetErrorFlag (); iRow++ )
		{
			SphDocID_t uDocid = rdAttr.GetDocid ();
			rdAttr.SkipBytes ( iSkip );

			if ( uDocid<m_iDocidMin || uDocid>m_iDocidMax )
				continue;

			if ( m_bRawID )
				m_dDocid.GetWritePtr()[iRow] = uDocid;
			else
			{
				SphDocID_t uIndex = uDocid - m_iDocidMin;
				DWORD uBit = 1UL<<(uIndex & 31);
				m_dBits.GetWritePtr()[uIndex>>5] |= uBit;
			}
		}

		if ( rdAttr.GetErrorFlag () )
		{
			sError.SetSprintf ( "unable to read attributes: %s", rdAttr.GetErrorMessage().cstr() );
			rdAttr.ResetError();
			return false;
		}

		return true;
	}

	bool HasDocid ( SphDocID_t uDocid )
	{
		if ( uDocid<m_iDocidMin || uDocid>m_iDocidMax )
			return false;

		if ( m_bRawID )
		{
			return ( sphBinarySearch ( m_dDocid.GetWritePtr(), m_dDocid.GetWritePtr () + m_dDocid.GetNumEntries() - 1, uDocid )!=NULL );
		} else
		{
			SphDocID_t uIndex = uDocid - m_iDocidMin;
			DWORD uBit = 1UL<<( uIndex & 31 );

			return ( ( ( m_dBits.GetWritePtr()[uIndex>>5] & uBit ) )!=0 ); // NOLINT
		}
	}

private:
	CSphLargeBuffer<SphDocID_t, false> m_dDocid;
	CSphLargeBuffer<DWORD, false> m_dBits;
	bool m_bRawID;
	SphDocID_t m_iDocidMin;
	SphDocID_t m_iDocidMax;
};


// no strnlen on some OSes (Mac OS)
#if !HAVE_STRNLEN
size_t strnlen ( const char * s, size_t iMaxLen )
{
	if ( !s )
		return 0;

	size_t iRes = 0;
	while ( *s++ && iRes<iMaxLen )
		++iRes;
	return iRes;
}
#endif


#define LOC_FAIL(_args) \
	if ( ++iFails<=FAILS_THRESH ) \
	{ \
		fprintf ( fp, "FAILED, " ); \
		fprintf _args; \
		fprintf ( fp, "\n" ); \
		iFailsPrinted++; \
		\
		if ( iFails==FAILS_THRESH ) \
			fprintf ( fp, "(threshold reached; suppressing further output)\n" ); \
	}


int CSphIndex_VLN::DebugCheck ( FILE * fp )
{
	int64_t tmCheck = sphMicroTimer();
	int64_t iFails = 0;
	int iFailsPrinted = 0;
	const int FAILS_THRESH = 100;

	// check if index is ready
	if ( !m_bPassedAlloc )
		LOC_FAIL(( fp, "index not preread" ));

	bool bProgress = isatty ( fileno ( fp ) )!=0;

	//////////////
	// open files
	//////////////

	CSphString sError;
	CSphAutoreader rdDocs, rdHits;
	CSphAutoreader rdDict;
	CSphAutoreader rdSkips;
	int64_t iSkiplistLen = 0;

	if ( !rdDict.Open ( GetIndexFileName("spi").cstr(), sError ) )
		LOC_FAIL(( fp, "unable to open dictionary: %s", sError.cstr() ));

	if ( !rdDocs.Open ( GetIndexFileName("spd"), sError ) )
		LOC_FAIL(( fp, "unable to open doclist: %s", sError.cstr() ));

	if ( !rdHits.Open ( GetIndexFileName("spp"), sError ) )
		LOC_FAIL(( fp, "unable to open hitlist: %s", sError.cstr() ));

	if ( m_bHaveSkips )
	{
		if ( !rdSkips.Open ( GetIndexFileName ( "spe" ), sError ) )
			LOC_FAIL ( ( fp, "unable to open skiplist: %s", sError.cstr () ) );
		iSkiplistLen = rdSkips.GetFilesize();
	}

	CSphAutoreader rdAttr;
	CSphAutoreader rdString;
	CSphAutoreader rdMva;
	int64_t iStrEnd = 0;
	int64_t iMvaEnd = 0;

	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && !m_tAttr.IsEmpty() )
	{
		fprintf ( fp, "checking rows...\n" );

		if ( !rdAttr.Open ( GetIndexFileName("spa").cstr(), sError ) )
			LOC_FAIL(( fp, "unable to open attributes: %s", sError.cstr() ));

		if ( !rdString.Open ( GetIndexFileName("sps").cstr(), sError ) )
			LOC_FAIL(( fp, "unable to open strings: %s", sError.cstr() ));

		if ( !rdMva.Open ( GetIndexFileName("spm").cstr(), sError ) )
			LOC_FAIL(( fp, "unable to open MVA: %s", sError.cstr() ));
	}

	CSphVector<SphWordID_t> dHitlessWords;
	if ( !LoadHitlessWords ( dHitlessWords ) )
		LOC_FAIL(( fp, "unable to load hitless words: %s", m_sLastError.cstr() ));

	CSphSavedFile tStat;
	const CSphTokenizerSettings & tTokenizerSettings = m_pTokenizer->GetSettings ();
	if ( !tTokenizerSettings.m_sSynonymsFile.IsEmpty() && !STATS::GetFileStats ( tTokenizerSettings.m_sSynonymsFile.cstr(), tStat, &sError ) )
		LOC_FAIL(( fp, "unable to open exceptions '%s': %s", tTokenizerSettings.m_sSynonymsFile.cstr(), sError.cstr() ));

	const CSphDictSettings & tDictSettings = m_pDict->GetSettings ();
	const char * pStop = tDictSettings.m_sStopwords.cstr();
	for ( ;; )
	{
		// find next name start
		while ( pStop && *pStop && isspace(*pStop) ) pStop++;
		if ( !pStop || !*pStop ) break;

		const char * sNameStart = pStop;

		// find next name end
		while ( *pStop && !isspace(*pStop) ) pStop++;

		CSphString sStopFile;
		sStopFile.SetBinary ( sNameStart, pStop-sNameStart );

		if ( !STATS::GetFileStats ( sStopFile.cstr(), tStat, &sError ) )
			LOC_FAIL(( fp, "unable to open stopwords '%s': %s", sStopFile.cstr(), sError.cstr() ));
	}

	if ( !tDictSettings.m_dWordforms.GetLength() )
	{
		ARRAY_FOREACH ( i, tDictSettings.m_dWordforms )
		{
			if ( !STATS::GetFileStats ( tDictSettings.m_dWordforms[i].cstr(), tStat, &sError ) )
				LOC_FAIL(( fp, "unable to open wordforms '%s': %s", tDictSettings.m_dWordforms[i].cstr(), sError.cstr() ));
		}
	}

	////////////////////
	// check dictionary
	////////////////////

	fprintf ( fp, "checking dictionary...\n" );

	SphWordID_t uWordid = 0;
	int64_t iDoclistOffset = 0;
	int iWordsTotal = 0;

	char sWord[MAX_KEYWORD_BYTES], sLastWord[MAX_KEYWORD_BYTES];
	memset ( sWord, 0, sizeof(sWord) );
	memset ( sLastWord, 0, sizeof(sLastWord) );

	const int iWordPerCP = m_uVersion>=21 ? SPH_WORDLIST_CHECKPOINT : 1024;
	const bool bWordDict = m_pDict->GetSettings().m_bWordDict;

	CSphVector<CSphWordlistCheckpoint> dCheckpoints;
	dCheckpoints.Reserve ( m_tWordlist.m_dCheckpoints.GetLength() );
	CSphVector<char> dCheckpointWords;
	dCheckpointWords.Reserve ( m_tWordlist.m_pWords.GetLength() );

	if ( bWordDict && m_uVersion<21 )
		LOC_FAIL(( fp, "dictionary needed index version not less then 21 (readed=%d)"
			, m_uVersion ));

	rdDict.GetByte();
	int iLastSkipsOffset = 0;
	SphOffset_t iWordsEnd = m_tWordlist.m_iWordsEnd;

	while ( rdDict.GetPos()!=iWordsEnd && !m_bIsEmpty )
	{
		// sanity checks
		if ( rdDict.GetPos()>=iWordsEnd )
		{
			LOC_FAIL(( fp, "reading past checkpoints" ));
			break;
		}

		// store current entry pos (for checkpointing later), read next delta
		const int64_t iDictPos = rdDict.GetPos();
		SphWordID_t iDeltaWord = 0;
		if ( bWordDict )
		{
			iDeltaWord = rdDict.GetByte();
		} else
		{
			iDeltaWord = rdDict.UnzipWordid();
		}

		// checkpoint encountered, handle it
		if ( !iDeltaWord )
		{
			rdDict.UnzipOffset();

			if ( ( iWordsTotal%iWordPerCP )!=0 && rdDict.GetPos()!=iWordsEnd )
				LOC_FAIL(( fp, "unexpected checkpoint (pos=" INT64_FMT ", word=%d, words=%d, expected=%d)",
					iDictPos, iWordsTotal, ( iWordsTotal%iWordPerCP ), iWordPerCP ));

			uWordid = 0;
			iDoclistOffset = 0;
			continue;
		}

		SphWordID_t uNewWordid = 0;
		SphOffset_t iNewDoclistOffset = 0;
		int iDocs = 0;
		int iHits = 0;
		bool bHitless = false;

		if ( bWordDict )
		{
			// unpack next word
			// must be in sync with DictEnd()!
			BYTE uPack = (BYTE)iDeltaWord;
			int iMatch, iDelta;
			if ( uPack & 0x80 )
			{
				iDelta = ( ( uPack>>4 ) & 7 ) + 1;
				iMatch = uPack & 15;
			} else
			{
				iDelta = uPack & 127;
				iMatch = rdDict.GetByte();
			}
			const int iLastWordLen = strlen(sLastWord);
			if ( iMatch+iDelta>=(int)sizeof(sLastWord)-1 || iMatch>iLastWordLen )
			{
				LOC_FAIL(( fp, "wrong word-delta (pos=" INT64_FMT ", word=%s, len=%d, begin=%d, delta=%d)",
					iDictPos, sLastWord, iLastWordLen, iMatch, iDelta ));
				rdDict.SkipBytes ( iDelta );
			} else
			{
				rdDict.GetBytes ( sWord+iMatch, iDelta );
				sWord [ iMatch+iDelta ] = '\0';
			}

			iNewDoclistOffset = rdDict.UnzipOffset();
			iDocs = rdDict.UnzipInt();
			iHits = rdDict.UnzipInt();
			int iHint = 0;
			if ( iDocs>=DOCLIST_HINT_THRESH )
			{
				iHint = rdDict.GetByte();
			}
			iHint = DoclistHintUnpack ( iDocs, (BYTE)iHint );

			if ( m_tSettings.m_eHitless==SPH_HITLESS_SOME && ( iDocs & HITLESS_DOC_FLAG )!=0 )
			{
				iDocs = ( iDocs & HITLESS_DOC_MASK );
				bHitless = true;
			}

			const int iNewWordLen = strlen(sWord);

			if ( iNewWordLen==0 )
				LOC_FAIL(( fp, "empty word in dictionary (pos=" INT64_FMT ")",
					iDictPos ));

			if ( iLastWordLen && iNewWordLen )
				if ( sphDictCmpStrictly ( sWord, iNewWordLen, sLastWord, iLastWordLen )<=0 )
					LOC_FAIL(( fp, "word order decreased (pos=" INT64_FMT ", word=%s, prev=%s)",
						iDictPos, sLastWord, sWord ));

			if ( iHint<0 )
				LOC_FAIL(( fp, "invalid word hint (pos=" INT64_FMT ", word=%s, hint=%d)",
					iDictPos, sWord, iHint ));

			if ( iDocs<=0 || iHits<=0 || iHits<iDocs )
				LOC_FAIL(( fp, "invalid docs/hits (pos=" INT64_FMT ", word=%s, docs=" INT64_FMT ", hits=" INT64_FMT ")",
					(int64_t)iDictPos, sWord, (int64_t)iDocs, (int64_t)iHits ));

			memcpy ( sLastWord, sWord, sizeof(sLastWord) );
		} else
		{
			// finish reading the entire entry
			uNewWordid = uWordid + iDeltaWord;
			iNewDoclistOffset = iDoclistOffset + rdDict.UnzipOffset();
			iDocs = rdDict.UnzipInt();
			iHits = rdDict.UnzipInt();
			bHitless = ( dHitlessWords.BinarySearch ( uNewWordid )!=NULL );
			if ( bHitless )
				iDocs = ( iDocs & HITLESS_DOC_MASK );

			if ( uNewWordid<=uWordid )
				LOC_FAIL(( fp, "wordid decreased (pos=" INT64_FMT ", wordid=" UINT64_FMT ", previd=" UINT64_FMT ")",
					(int64_t)iDictPos, (uint64_t)uNewWordid, (uint64_t)uWordid ));

			if ( iNewDoclistOffset<=iDoclistOffset )
				LOC_FAIL(( fp, "doclist offset decreased (pos=" INT64_FMT ", wordid=" UINT64_FMT ")",
					(int64_t)iDictPos, (uint64_t)uNewWordid ));

			if ( iDocs<=0 || iHits<=0 || iHits<iDocs )
				LOC_FAIL(( fp, "invalid docs/hits (pos=" INT64_FMT ", wordid=" UINT64_FMT ", docs=" INT64_FMT ", hits=" INT64_FMT ", hitless=%s)",
					(int64_t)iDictPos, (uint64_t)uNewWordid, (int64_t)iDocs, (int64_t)iHits, ( bHitless?"true":"false" ) ));
		}

		// skiplist
		if ( m_bHaveSkips && iDocs>SPH_SKIPLIST_BLOCK && !bHitless )
		{
			int iSkipsOffset = rdDict.UnzipInt();
			if ( !bWordDict && iSkipsOffset<iLastSkipsOffset )
				LOC_FAIL(( fp, "descending skiplist pos (last=%d, cur=%d, wordid=%llu)",
					iLastSkipsOffset, iSkipsOffset, UINT64 ( uNewWordid ) ));
			iLastSkipsOffset = iSkipsOffset;
		}

		// update stats, add checkpoint
		if ( ( iWordsTotal%iWordPerCP )==0 )
		{
			CSphWordlistCheckpoint & tCP = dCheckpoints.Add();
			tCP.m_iWordlistOffset = iDictPos;

			if ( bWordDict )
			{
				const int iLen = strlen ( sWord );
				char * sArenaWord = dCheckpointWords.AddN ( iLen + 1 );
				memcpy ( sArenaWord, sWord, iLen );
				sArenaWord[iLen] = '\0';
				tCP.m_uWordID = sArenaWord - dCheckpointWords.Begin();
			} else
				tCP.m_uWordID = uNewWordid;
		}

		// TODO add back infix checking

		uWordid = uNewWordid;
		iDoclistOffset = iNewDoclistOffset;
		iWordsTotal++;
	}

	// check the checkpoints
	if ( dCheckpoints.GetLength()!=m_tWordlist.m_dCheckpoints.GetLength() )
		LOC_FAIL(( fp, "checkpoint count mismatch (read=%d, calc=%d)",
			(int)m_tWordlist.m_dCheckpoints.GetLength(), (int)dCheckpoints.GetLength() ));

	m_tWordlist.DebugPopulateCheckpoints();
	for ( int i=0; i < Min ( dCheckpoints.GetLength(), (int)m_tWordlist.m_dCheckpoints.GetLength() ); i++ )
	{
		CSphWordlistCheckpoint tRefCP = dCheckpoints[i];
		const CSphWordlistCheckpoint & tCP = m_tWordlist.m_dCheckpoints[i];
		const int iLen = bWordDict ? strlen ( tCP.m_sWord ) : 0;
		if ( bWordDict )
			tRefCP.m_sWord = dCheckpointWords.Begin() + tRefCP.m_uWordID;
		if ( bWordDict && ( tRefCP.m_sWord[0]=='\0' || tCP.m_sWord[0]=='\0' ) )
		{
			LOC_FAIL(( fp, "empty checkpoint %d (read_word=%s, read_len=%u, readpos=" INT64_FMT ", calc_word=%s, calc_len=%u, calcpos=" INT64_FMT ")",
				i, tCP.m_sWord, (DWORD)strlen ( tCP.m_sWord ), (int64_t)tCP.m_iWordlistOffset,
					tRefCP.m_sWord, (DWORD)strlen ( tRefCP.m_sWord ), (int64_t)tRefCP.m_iWordlistOffset ));

		} else if ( sphCheckpointCmpStrictly ( tCP.m_sWord, iLen, tCP.m_uWordID, bWordDict, tRefCP )
			|| tRefCP.m_iWordlistOffset!=tCP.m_iWordlistOffset )
		{
			if ( bWordDict )
			{
				LOC_FAIL(( fp, "checkpoint %d differs (read_word=%s, readpos=" INT64_FMT ", calc_word=%s, calcpos=" INT64_FMT ")",
					i,
					tCP.m_sWord,
					(int64_t)tCP.m_iWordlistOffset,
					tRefCP.m_sWord,
					(int64_t)tRefCP.m_iWordlistOffset ));
			} else
			{
				LOC_FAIL(( fp, "checkpoint %d differs (readid=" UINT64_FMT ", readpos=" INT64_FMT ", calcid=" UINT64_FMT ", calcpos=" INT64_FMT ")",
					i,
					(uint64_t)tCP.m_uWordID,
					(int64_t)tCP.m_iWordlistOffset,
					(uint64_t)tRefCP.m_uWordID,
					(int64_t)tRefCP.m_iWordlistOffset ));
			}
		}
	}

	dCheckpoints.Reset();
	dCheckpointWords.Reset();

	///////////////////////
	// check docs and hits
	///////////////////////

	fprintf ( fp, "checking data...\n" );

	CSphScopedPtr<CSphDocidList> tDoclist ( new CSphDocidList );
	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && !tDoclist->Init ( m_tSchema.GetRowSize (), m_iDocinfo, rdAttr, sError ) )
		LOC_FAIL ( ( fp, "%s", sError.cstr () ) );

	int64_t iDocsSize = rdDocs.GetFilesize();

	rdDict.SeekTo ( 1, READ_NO_SIZE_HINT );
	rdDocs.SeekTo ( 1, READ_NO_SIZE_HINT );
	rdHits.SeekTo ( 1, READ_NO_SIZE_HINT );

	uWordid = 0;
	iDoclistOffset = 0;
	int iDictDocs, iDictHits;
	bool bHitless = false;

	int iWordsChecked = 0;
	while ( rdDict.GetPos()<iWordsEnd )
	{
		bHitless = false;
		SphWordID_t iDeltaWord = 0;
		if ( bWordDict )
		{
			iDeltaWord = rdDict.GetByte();
		} else
		{
			iDeltaWord = rdDict.UnzipWordid();
		}
		if ( !iDeltaWord )
		{
			rdDict.UnzipOffset();

			uWordid = 0;
			iDoclistOffset = 0;
			continue;
		}

		if ( bWordDict )
		{
			// unpack next word
			// must be in sync with DictEnd()!
			BYTE uPack = (BYTE)iDeltaWord;

			int iMatch, iDelta;
			if ( uPack & 0x80 )
			{
				iDelta = ( ( uPack>>4 ) & 7 ) + 1;
				iMatch = uPack & 15;
			} else
			{
				iDelta = uPack & 127;
				iMatch = rdDict.GetByte();
			}
			const int iLastWordLen = strlen(sWord);
			if ( iMatch+iDelta>=(int)sizeof(sWord)-1 || iMatch>iLastWordLen )
			{
				rdDict.SkipBytes ( iDelta );
			} else
			{
				rdDict.GetBytes ( sWord+iMatch, iDelta );
				sWord [ iMatch+iDelta ] = '\0';
			}

			iDoclistOffset = rdDict.UnzipOffset();
			iDictDocs = rdDict.UnzipInt();
			iDictHits = rdDict.UnzipInt();
			if ( iDictDocs>=DOCLIST_HINT_THRESH )
				rdDict.GetByte();

			if ( m_tSettings.m_eHitless==SPH_HITLESS_SOME && ( iDictDocs & HITLESS_DOC_FLAG ) )
			{
				iDictDocs = ( iDictDocs & HITLESS_DOC_MASK );
				bHitless = true;
			}
		} else
		{
			// finish reading the entire entry
			uWordid = uWordid + iDeltaWord;
			bHitless = ( dHitlessWords.BinarySearch ( uWordid )!=NULL );
			iDoclistOffset = iDoclistOffset + rdDict.UnzipOffset();
			iDictDocs = rdDict.UnzipInt();
			if ( bHitless )
				iDictDocs = ( iDictDocs & HITLESS_DOC_MASK );
			iDictHits = rdDict.UnzipInt();
		}

		// FIXME? verify skiplist content too
		int iSkipsOffset = 0;
		if ( m_bHaveSkips && iDictDocs>SPH_SKIPLIST_BLOCK && !bHitless )
			iSkipsOffset = rdDict.UnzipInt();

		// check whether the offset is as expected
		if ( iDoclistOffset!=rdDocs.GetPos() )
		{
			if ( !bWordDict )
				LOC_FAIL(( fp, "unexpected doclist offset (wordid=" UINT64_FMT "(%s)(%d), dictpos=" INT64_FMT ", doclistpos=" INT64_FMT ")",
					(uint64_t)uWordid, sWord, iWordsChecked, iDoclistOffset, (int64_t)rdDocs.GetPos() ));

			if ( iDoclistOffset>=iDocsSize || iDoclistOffset<0 )
			{
				LOC_FAIL(( fp, "unexpected doclist offset, off the file (wordid=" UINT64_FMT "(%s)(%d), dictpos=" INT64_FMT ", doclistsize=" INT64_FMT ")",
					(uint64_t)uWordid, sWord, iWordsChecked, iDoclistOffset, iDocsSize ));
				iWordsChecked++;
				continue;
			} else
				rdDocs.SeekTo ( iDoclistOffset, READ_NO_SIZE_HINT );
		}

		// create and manually setup doclist reader
		DiskIndexQwordTraits_c * pQword = NULL;
		DWORD uInlineHits = ( m_tSettings.m_eHitFormat==SPH_HIT_FORMAT_INLINE );
		DWORD uInlineDocinfo = ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE );
		switch ( ( uInlineHits<<1 ) | uInlineDocinfo )
		{
		case 0: { typedef DiskIndexQword_c < false, false, false > T; pQword = new T ( false, false ); break; }
		case 1: { typedef DiskIndexQword_c < false, true, false > T; pQword = new T ( false, false ); break; }
		case 2: { typedef DiskIndexQword_c < true, false, false > T; pQword = new T ( false, false ); break; }
		case 3: { typedef DiskIndexQword_c < true, true, false > T; pQword = new T ( false, false ); break; }
		}
		if ( !pQword )
			sphDie ( "INTERNAL ERROR: impossible qword settings" );

		pQword->m_tDoc.Reset ( m_tSchema.GetDynamicSize() );
		pQword->m_iMinID = m_uMinDocid;
		pQword->m_tDoc.m_uDocID = m_uMinDocid;
		if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
		{
			pQword->m_iInlineAttrs = m_tSchema.GetDynamicSize();
			pQword->m_pInlineFixup = m_dMinRow.Begin();
		} else
		{
			pQword->m_iInlineAttrs = 0;
			pQword->m_pInlineFixup = NULL;
		}
		pQword->m_iDocs = 0;
		pQword->m_iHits = 0;
		pQword->m_rdDoclist.SetFile ( rdDocs.GetFD(), rdDocs.GetFilename().cstr() );
		pQword->m_rdDoclist.SeekTo ( rdDocs.GetPos(), READ_NO_SIZE_HINT );
		pQword->m_rdHitlist.SetFile ( rdHits.GetFD(), rdHits.GetFilename().cstr() );
		pQword->m_rdHitlist.SeekTo ( rdHits.GetPos(), READ_NO_SIZE_HINT );

		CSphRowitem * pInlineStorage = NULL;
		if ( pQword->m_iInlineAttrs )
			pInlineStorage = new CSphRowitem [ pQword->m_iInlineAttrs ];

		// loop the doclist
		SphDocID_t uLastDocid = 0;
		int iDoclistDocs = 0;
		int iDoclistHits = 0;
		int iHitlistHits = 0;

		bHitless |= ( m_tSettings.m_eHitless==SPH_HITLESS_ALL ||
			( m_tSettings.m_eHitless==SPH_HITLESS_SOME && dHitlessWords.BinarySearch ( uWordid ) ) );
		pQword->m_bHasHitlist = !bHitless;

		CSphVector<SkiplistEntry_t> dDoclistSkips;
		for ( ;; )
		{
			// skiplist state is saved just *before* decoding those boundary entries
			if ( m_bHaveSkips && ( iDoclistDocs & ( SPH_SKIPLIST_BLOCK-1 ) )==0 )
			{
				SkiplistEntry_t & tBlock = dDoclistSkips.Add();
				tBlock.m_iBaseDocid = pQword->m_tDoc.m_uDocID;
				tBlock.m_iOffset = pQword->m_rdDoclist.GetPos();
				tBlock.m_iBaseHitlistPos = pQword->m_uHitPosition;
			}

			// FIXME? this can fail on a broken entry (eg fieldid over 256)
			const CSphMatch & tDoc = pQword->GetNextDoc ( pInlineStorage );
			if ( !tDoc.m_uDocID )
				break;

			// checks!
			if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN )
			{
				SphDocID_t uDocID = tDoc.m_uDocID;
				if ( !tDoclist->HasDocid ( uDocID ) )
				{
					LOC_FAIL(( fp, "row not found (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ")",
						uint64_t(uWordid), sWord, tDoc.m_uDocID ));
				}
			}

			if ( tDoc.m_uDocID<=uLastDocid )
				LOC_FAIL(( fp, "docid decreased (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", lastid=" DOCID_FMT ")",
					uint64_t(uWordid), sWord, tDoc.m_uDocID, uLastDocid ));

			uLastDocid = tDoc.m_uDocID;
			iDoclistDocs++;
			iDoclistHits += pQword->m_uMatchHits;

			// check position in case of regular (not-inline) hit
			if (!( pQword->m_iHitlistPos>>63 ))
			{
				if ( !bWordDict && pQword->m_iHitlistPos!=pQword->m_rdHitlist.GetPos() )
					LOC_FAIL(( fp, "unexpected hitlist offset (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", expected=" INT64_FMT ", actual=" INT64_FMT ")",
						(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID,
						(int64_t)pQword->m_iHitlistPos, (int64_t)pQword->m_rdHitlist.GetPos() ));
			}

			// aim
			pQword->SeekHitlist ( pQword->m_iHitlistPos );

			// loop the hitlist
			int iDocHits = 0;
			FieldMask_t dFieldMask;
			dFieldMask.UnsetAll();
			Hitpos_t uLastHit = EMPTY_HIT;

			while ( !bHitless )
			{
				Hitpos_t uHit = pQword->GetNextHit();
				if ( uHit==EMPTY_HIT )
					break;

				if ( !( uLastHit<uHit ) )
					LOC_FAIL(( fp, "hit entries sorting order decreased (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", hit=%u, last=%u)",
							(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, uHit, uLastHit ));

				if ( HITMAN::GetField ( uLastHit )==HITMAN::GetField ( uHit ) )
				{
					if ( !( HITMAN::GetPos ( uLastHit )<HITMAN::GetPos ( uHit ) ) )
						LOC_FAIL(( fp, "hit decreased (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", hit=%u, last=%u)",
								(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, HITMAN::GetPos ( uHit ), HITMAN::GetPos ( uLastHit ) ));
					if ( HITMAN::IsEnd ( uLastHit ) )
						LOC_FAIL(( fp, "multiple tail hits (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", hit=0x%x, last=0x%x)",
								(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, uHit, uLastHit ));
				} else
				{
					if ( !( HITMAN::GetField ( uLastHit )<HITMAN::GetField ( uHit ) ) )
						LOC_FAIL(( fp, "hit field decreased (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", hit field=%u, last field=%u)",
								(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, HITMAN::GetField ( uHit ), HITMAN::GetField ( uLastHit ) ));
				}

				uLastHit = uHit;

				int iField = HITMAN::GetField ( uHit );
				if ( iField<0 || iField>=SPH_MAX_FIELDS )
				{
					LOC_FAIL(( fp, "hit field out of bounds (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", field=%d)",
						(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, iField ));

				} else if ( iField>=m_tSchema.m_dFields.GetLength() )
				{
					LOC_FAIL(( fp, "hit field out of schema (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", field=%d)",
						(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, iField ));
				} else
				{
					dFieldMask.Set(iField);
				}

				iDocHits++; // to check doclist entry
				iHitlistHits++; // to check dictionary entry
			}

			// check hit count
			if ( iDocHits!=(int)pQword->m_uMatchHits && !bHitless )
				LOC_FAIL(( fp, "doc hit count mismatch (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ", doclist=%d, hitlist=%d)",
					(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID, pQword->m_uMatchHits, iDocHits ));

			if ( GetMatchSchema().m_dFields.GetLength()>32 )
				pQword->CollectHitMask();

			// check the mask
			if ( memcmp ( dFieldMask.m_dMask, pQword->m_dQwordFields.m_dMask, sizeof(dFieldMask.m_dMask) ) && !bHitless )
				LOC_FAIL(( fp, "field mask mismatch (wordid=" UINT64_FMT "(%s), docid=" DOCID_FMT ")",
					(uint64_t)uWordid, sWord, pQword->m_tDoc.m_uDocID ));

			// update my hitlist reader
			rdHits.SeekTo ( pQword->m_rdHitlist.GetPos(), READ_NO_SIZE_HINT );
		}

		// do checks
		if ( iDictDocs!=iDoclistDocs )
			LOC_FAIL(( fp, "doc count mismatch (wordid=" UINT64_FMT "(%s), dict=%d, doclist=%d, hitless=%s)",
				uint64_t(uWordid), sWord, iDictDocs, iDoclistDocs, ( bHitless?"true":"false" ) ));

		if ( ( iDictHits!=iDoclistHits || iDictHits!=iHitlistHits ) && !bHitless )
			LOC_FAIL(( fp, "hit count mismatch (wordid=" UINT64_FMT "(%s), dict=%d, doclist=%d, hitlist=%d)",
				uint64_t(uWordid), sWord, iDictHits, iDoclistHits, iHitlistHits ));

		while ( m_bHaveSkips && iDoclistDocs>SPH_SKIPLIST_BLOCK && !bHitless )
		{
			if ( iSkipsOffset<=0 || iSkipsOffset>iSkiplistLen )
			{
				LOC_FAIL(( fp, "invalid skiplist offset (wordid=%llu(%s), off=%d, max=" INT64_FMT ")",
					UINT64 ( uWordid ), sWord, iSkipsOffset, iSkiplistLen ));
				break;
			}

			// boundary adjustment
			if ( ( iDoclistDocs & ( SPH_SKIPLIST_BLOCK-1 ) )==0 )
				dDoclistSkips.Pop();

			SkiplistEntry_t t;
			t.m_iBaseDocid = m_uMinDocid;
			t.m_iOffset = iDoclistOffset;
			t.m_iBaseHitlistPos = 0;

			// hint is: dDoclistSkips * ZIPPED( sizeof(int64_t) * 3 ) == dDoclistSkips * 8
			rdSkips.SeekTo ( iSkipsOffset, dDoclistSkips.GetLength ()*8 );
			int i = 0;
			while ( ++i<dDoclistSkips.GetLength() )
			{
				const SkiplistEntry_t & r = dDoclistSkips[i];

				uint64_t uDocidDelta = rdSkips.UnzipOffset ();
				uint64_t uOff = rdSkips.UnzipOffset ();
				uint64_t uPosDelta = rdSkips.UnzipOffset ();

				if ( rdSkips.GetErrorFlag () )
				{
					LOC_FAIL ( ( fp, "skiplist reading error (wordid=%llu(%s), exp=%d, got=%d, error='%s')",
						UINT64 ( uWordid ), sWord, i, (int)dDoclistSkips.GetLength (), rdSkips.GetErrorMessage ().cstr () ) );
					rdSkips.ResetError ();
					break;
				}

				t.m_iBaseDocid += SPH_SKIPLIST_BLOCK + (SphDocID_t)uDocidDelta;
				t.m_iOffset += 4*SPH_SKIPLIST_BLOCK + uOff;
				t.m_iBaseHitlistPos += uPosDelta;
				if ( t.m_iBaseDocid!=r.m_iBaseDocid
					|| t.m_iOffset!=r.m_iOffset ||
					t.m_iBaseHitlistPos!=r.m_iBaseHitlistPos )
				{
					LOC_FAIL(( fp, "skiplist entry %d mismatch (wordid=%llu(%s), exp={%llu, %llu, %llu}, got={%llu, %llu, %llu})",
						i, UINT64 ( uWordid ), sWord,
						UINT64 ( r.m_iBaseDocid ), UINT64 ( r.m_iOffset ), UINT64 ( r.m_iBaseHitlistPos ),
						UINT64 ( t.m_iBaseDocid ), UINT64 ( t.m_iOffset ), UINT64 ( t.m_iBaseHitlistPos ) ));
					break;
				}
			}
			break;
		}

		// move my reader instance forward too
		rdDocs.SeekTo ( pQword->m_rdDoclist.GetPos(), READ_NO_SIZE_HINT );

		// cleanup
		SafeDelete ( pInlineStorage );
		SafeDelete ( pQword );

		// progress bar
		if ( (++iWordsChecked)%1000==0 && bProgress )
		{
			fprintf ( fp, "%d/%d\r", iWordsChecked, iWordsTotal );
			fflush ( fp );
		}
	}

	tDoclist = NULL;

	///////////////////////////
	// check rows (attributes)
	///////////////////////////

	if ( m_tSettings.m_eDocinfo==SPH_DOCINFO_EXTERN && !m_tAttr.IsEmpty() )
	{
		fprintf ( fp, "checking rows...\n" );

		// sizes and counts
		int64_t iRowsTotal = m_iDocinfo;
		DWORD uStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();

		int64_t iAllRowsTotal = iRowsTotal;
		iAllRowsTotal += (m_iDocinfoIndex+1)*2; // should had been fixed up to v.20 by the loader

		if ( iAllRowsTotal*uStride!=(int64_t)m_tAttr.GetNumEntries() )
			LOC_FAIL(( fp, "rowitems count mismatch (expected=" INT64_FMT ", loaded=" INT64_FMT ")",
				iAllRowsTotal*uStride, (int64_t)m_tAttr.GetNumEntries() ));

		iStrEnd = rdString.GetFilesize();
		iMvaEnd = rdMva.GetFilesize();
		CSphFixedVector<DWORD> dRow ( uStride );
		CSphVector<DWORD> dMva;
		rdAttr.SeekTo ( 0, sizeof ( dRow[0] ) * dRow.GetLength() );

		// extract rowitem indexes for MVAs etc
		// (ie. attr types that we can and will run additional checks on)
		CSphVector<int> dMvaItems;
		CSphVector<CSphAttrLocator> dFloatItems;
		CSphVector<CSphAttrLocator> dStrItems;
		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
			if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
			{
				if ( tAttr.m_tLocator.m_iBitCount!=ROWITEM_BITS )
				{
					LOC_FAIL(( fp, "unexpected MVA bitcount (attr=%d, expected=%d, got=%d)",
						i, ROWITEM_BITS, tAttr.m_tLocator.m_iBitCount ));
					continue;
				}
				if ( ( tAttr.m_tLocator.m_iBitOffset % ROWITEM_BITS )!=0 )
				{
					LOC_FAIL(( fp, "unaligned MVA bitoffset (attr=%d, bitoffset=%d)",
						i, tAttr.m_tLocator.m_iBitOffset ));
					continue;
				}
				if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
				dMvaItems.Add ( tAttr.m_tLocator.m_iBitOffset/ROWITEM_BITS );
			} else if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_FLOAT )
				dFloatItems.Add	( tAttr.m_tLocator );
			else if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_STRING || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_JSON )
				dStrItems.Add ( tAttr.m_tLocator );
		}
		int iMva64 = dMvaItems.GetLength();
		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i);
			if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
				dMvaItems.Add ( tAttr.m_tLocator.m_iBitOffset/ROWITEM_BITS );
		}

		// walk string data, build a list of acceptable start offsets
		// must be sorted by construction
		CSphVector<DWORD> dStringOffsets;
		if ( m_tString.GetNumEntries()>1 )
		{
			rdString.SeekTo ( 1, READ_NO_SIZE_HINT );
			while ( rdString.GetPos()<iStrEnd )
			{
				int64_t iLastPos = rdString.GetPos();
				const int iLen = sphUnpackStrLength ( rdString );

				// 4 bytes must be enough to encode string length, hence pCur+4
				if ( rdString.GetPos()+iLen>iStrEnd || rdString.GetPos()>iLastPos+4 )
				{
					LOC_FAIL(( fp, "string length out of bounds (offset=" INT64_FMT ", len=%d)", iLastPos, iLen ));
					break;
				}

				dStringOffsets.Add ( (DWORD)iLastPos );
				rdString.SkipBytes ( iLen );
			}
		}

		// loop the rows
		int iOrphan = 0;
		SphDocID_t uLastID = 0;

		for ( int64_t iRow=0; iRow<iRowsTotal; iRow++ )
		{
			// fetch the row
			rdAttr.GetBytes ( dRow.Begin(), sizeof(dRow[0])*dRow.GetLength() );
			SphDocID_t uCurID = DOCINFO2ID ( dRow.Begin() );

			// check that ids are ascending
			bool bIsSpaValid = ( uLastID<uCurID );
			if ( !bIsSpaValid )
				LOC_FAIL(( fp, "docid decreased (row=" INT64_FMT ", id=" DOCID_FMT ", lastid=" DOCID_FMT ")",
					iRow, uCurID, uLastID ));

			uLastID = uCurID;

			///////////////////////////
			// check MVAs
			///////////////////////////

			if ( dMvaItems.GetLength() )
			{
				bool bMvaFix = false;
				DWORD uMvaSpaFixed = 0;
				const CSphRowitem * pAttrs = DOCINFO2ATTRS ( dRow.Begin() );
				bool bHasValues = false;
				bool bHasArena = false;
				ARRAY_FOREACH ( iItem, dMvaItems )
				{
					const DWORD uOffset = pAttrs[dMvaItems[iItem]];
					bHasValues |= ( uOffset!=0 );
					bool bArena = ( ( uOffset & MVA_ARENA_FLAG )!=0 ) && !m_bArenaProhibit;
					bHasArena |= bArena;

					if ( uOffset && !bArena && uOffset>=iMvaEnd )
					{
						bIsSpaValid = false;
						LOC_FAIL(( fp, "MVA index out of bounds (row=" INT64_FMT ", mvaattr=%d, docid=" DOCID_FMT ", index=%u)",
							iRow, (int)iItem, uLastID, uOffset ));
					}

					if ( uOffset && !bArena && uOffset<iMvaEnd && !bMvaFix )
					{
						uMvaSpaFixed = uOffset - sizeof(SphDocID_t) / sizeof(DWORD);
						bMvaFix = true;
					}
				}

				// MVAs ptr recovery from previous errors only if current spa record is valid
				if ( rdMva.GetPos()!=SphOffset_t(sizeof(DWORD)*uMvaSpaFixed) && bIsSpaValid && bMvaFix )
					rdMva.SeekTo ( sizeof(DWORD)*uMvaSpaFixed, READ_NO_SIZE_HINT );

				bool bLastIDChecked = false;
				SphDocID_t uLastMvaID = 0;
				while ( rdMva.GetPos()<iMvaEnd )
				{
					// current row does not reference any MVA values
					// lets mark it as checked and bail
					if ( !bHasValues )
					{
						bLastIDChecked = true;
						break;
					}

					int64_t iLastPos = rdMva.GetPos();
					const SphDocID_t uMvaID = rdMva.GetDocid();
					if ( uMvaID>uLastID )
						break;

					if ( bLastIDChecked && uLastID==uMvaID )
						LOC_FAIL(( fp, "duplicate docid found (row=" INT64_FMT ", docid expected=" DOCID_FMT ", got=" DOCID_FMT ", index=" INT64_FMT ")",
							iRow, uLastID, uMvaID, iLastPos ));

					if ( uMvaID<uLastMvaID )
						LOC_FAIL(( fp, "MVA docid decreased (row=" INT64_FMT ", spa docid=" DOCID_FMT ", last MVA docid=" DOCID_FMT ", MVA docid=" DOCID_FMT ", index=" INT64_FMT ")",
							iRow, uLastID, uLastMvaID, uMvaID, iLastPos ));

					bool bIsMvaCorrect = ( uLastMvaID<=uMvaID && uMvaID<=uLastID );
					uLastMvaID = uMvaID;
					bool bWasArena = false;
					int iLastEmpty = INT_MAX;

					// loop MVAs
					ARRAY_FOREACH_COND ( iItem, dMvaItems, bIsMvaCorrect )
					{
						const DWORD uSpaOffset = pAttrs[dMvaItems[iItem]];
						bool bArena = ( ( uSpaOffset & MVA_ARENA_FLAG )!=0 ) && !m_bArenaProhibit;
						bWasArena |= bArena;

						// zero offset means empty MVA in rt index, however plain index stores offset to zero length
						if ( !uSpaOffset || bArena )
						{
							iLastEmpty = iItem;
							continue;
						}

						// where also might be updated mva with zero length
						if ( bWasArena || ( iLastEmpty==iItem-1 ) )
							rdMva.SeekTo ( sizeof(DWORD)*uSpaOffset, READ_NO_SIZE_HINT );
						bWasArena = false;
						iLastEmpty = INT_MAX;

						// check offset (index)
						if ( uMvaID==uLastID && bIsSpaValid && rdMva.GetPos()!=int(sizeof(DWORD))*uSpaOffset )
						{
							LOC_FAIL(( fp, "unexpected MVA docid (row=" INT64_FMT ", mvaattr=%d, docid expected=" DOCID_FMT ", got=" DOCID_FMT ", expected=" INT64_FMT ", got=%u)",
								iRow, (int)iItem, uLastID, uMvaID, rdMva.GetPos()/sizeof(DWORD), uSpaOffset ));
							// it's unexpected but it's our best guess
							// but do fix up only once, to prevent infinite loop
							if ( !bLastIDChecked )
								rdMva.SeekTo ( sizeof(DWORD)*uSpaOffset, READ_NO_SIZE_HINT );
						}

						if ( rdMva.GetPos()>=iMvaEnd )
						{
							LOC_FAIL(( fp, "MVA index out of bounds (row=" INT64_FMT ", mvaattr=%d, docid expected=" DOCID_FMT ", got=" DOCID_FMT ", index=" INT64_FMT ")",
								iRow, (int)iItem, uLastID, uMvaID, rdMva.GetPos()/sizeof(DWORD) ));
							bIsMvaCorrect = false;
							continue;
						}

						// check values
						DWORD uValues = rdMva.GetDword();

						if ( rdMva.GetPos()+int(sizeof(DWORD))*uValues-1>=iMvaEnd )
						{
							LOC_FAIL(( fp, "MVA count out of bounds (row=" INT64_FMT ", mvaattr=%d, docid expected=" DOCID_FMT ", got=" DOCID_FMT ", count=%u)",
								iRow, (int)iItem, uLastID, uMvaID, uValues ));
							rdMva.SeekTo ( rdMva.GetPos() + sizeof(DWORD)*uValues, READ_NO_SIZE_HINT );
							bIsMvaCorrect = false;
							continue;
						}

						dMva.Resize ( uValues );
						rdMva.GetBytes ( dMva.Begin(), sizeof(DWORD)*uValues );

						// check that values are ascending
						for ( DWORD uVal=(iItem>=iMva64 ? 2 : 1); uVal<uValues && bIsMvaCorrect; )
						{
							int64_t iPrev, iCur;
							if ( iItem>=iMva64 )
							{
								iPrev = MVA_UPSIZE ( dMva.Begin() + uVal - 2 );
								iCur = MVA_UPSIZE ( dMva.Begin() + uVal );
								uVal += 2;
							} else
							{
								iPrev = dMva[uVal-1];
								iCur = dMva[uVal];
								uVal++;
							}

							if ( iCur<=iPrev )
							{
								LOC_FAIL(( fp, "unsorted MVA values (row=" INT64_FMT ", mvaattr=%d, docid expected=" DOCID_FMT ", got=" DOCID_FMT ", val[%u]=%u, val[%u]=%u)",
									iRow, (int)iItem, uLastID, uMvaID, ( iItem>=iMva64 ? uVal-2 : uVal-1 ), (unsigned int)iPrev, uVal, (unsigned int)iCur ));
								bIsMvaCorrect = false;
							}

							uVal += ( iItem>=iMva64 ? 2 : 1 );
						}
					}

					if ( !bIsMvaCorrect )
						break;

					// orphan only ON no errors && ( not matched ids || ids matched multiply times )
					if ( bIsMvaCorrect && ( uMvaID!=uLastID || ( uMvaID==uLastID && bLastIDChecked ) ) )
						iOrphan++;

					bLastIDChecked |= ( uLastID==uMvaID );
				}

				if ( !bLastIDChecked && bHasValues && !bHasArena )
					LOC_FAIL(( fp, "missed or damaged MVA (row=" INT64_FMT ", docid expected=" DOCID_FMT ")",
						iRow, uLastID ));
			}

			///////////////////////////
			// check floats
			///////////////////////////

			ARRAY_FOREACH ( iItem, dFloatItems )
			{
				const CSphRowitem * pAttrs = DOCINFO2ATTRS ( dRow.Begin() );
				const DWORD uValue = (DWORD)sphGetRowAttr ( pAttrs, dFloatItems[ iItem ] );
				const DWORD uExp = ( uValue >> 23 ) & 0xff;
				const DWORD uMantissa = uValue & 0x003fffff;

				// check normalized
				if ( uExp==0 && uMantissa!=0 )
					LOC_FAIL(( fp, "float attribute value is unnormalized (row=" INT64_FMT ", attr=%d, id=" DOCID_FMT ", raw=0x%x, value=%f)",
						iRow, (int)iItem, uLastID, uValue, sphDW2F ( uValue ) ));

				// check +-inf
				if ( uExp==0xff && uMantissa==0 )
					LOC_FAIL(( fp, "float attribute is infinity (row=" INT64_FMT ", attr=%d, id=" DOCID_FMT ", raw=0x%x, value=%f)",
						iRow, (int)iItem, uLastID, uValue, sphDW2F ( uValue ) ));
			}

			/////////////////
			// check strings
			/////////////////

			ARRAY_FOREACH ( iItem, dStrItems )
			{
				const CSphRowitem * pAttrs = DOCINFO2ATTRS ( dRow.Begin() );

				const DWORD uOffset = (DWORD)sphGetRowAttr ( pAttrs, dStrItems[ iItem ] );
				if ( uOffset>=iStrEnd )
				{
					LOC_FAIL(( fp, "string offset out of bounds (row=" INT64_FMT ", stringattr=%d, docid=" DOCID_FMT ", index=%u)",
						iRow, (int)iItem, uLastID, uOffset ));
					continue;
				}

				if ( !uOffset )
					continue;

				rdString.SeekTo ( uOffset, READ_NO_SIZE_HINT );
				const int iLen = sphUnpackStrLength ( rdString );

				// check that length is sane
				if ( rdString.GetPos()+iLen-1>=iStrEnd )
				{
					LOC_FAIL(( fp, "string length out of bounds (row=" INT64_FMT ", stringattr=%d, docid=" DOCID_FMT ", index=%u)",
						iRow, (int)iItem, uLastID, uOffset ));
					continue;
				}

				// check that offset is one of the good ones
				// (that is, that we don't point in the middle of some other data)
				if ( !dStringOffsets.BinarySearch ( uOffset ) )
				{
					LOC_FAIL(( fp, "string offset is not a string start (row=" INT64_FMT ", stringattr=%d, docid=" DOCID_FMT ", offset=%u)",
						iRow, (int)iItem, uLastID, uOffset ));
				}
			}

			// progress bar
			if ( iRow%1000==0 && bProgress )
			{
				fprintf ( fp, INT64_FMT"/" INT64_FMT "\r", iRow, iRowsTotal );
				fflush ( fp );
			}
		}

		if ( iOrphan )
			fprintf ( fp, "WARNING: %d orphaned MVA entries were found\n", iOrphan );

		///////////////////////////
		// check blocks index
		///////////////////////////

		fprintf ( fp, "checking attribute blocks index...\n" );

		// check size
		const int64_t iTempDocinfoIndex = ( m_iDocinfo+DOCINFO_INDEX_FREQ-1 ) / DOCINFO_INDEX_FREQ;
		if ( iTempDocinfoIndex!=m_iDocinfoIndex )
			LOC_FAIL(( fp, "block count differs (expected=" INT64_FMT ", got=" INT64_FMT ")",
				iTempDocinfoIndex, m_iDocinfoIndex ));

		const DWORD uMinMaxStride = DOCINFO_IDSIZE + m_tSchema.GetRowSize();
		const DWORD * pDocinfoIndexMax = m_pDocinfoIndex + ( m_iDocinfoIndex+1 )*uMinMaxStride*2;

		rdAttr.SeekTo ( 0, sizeof ( dRow[0] ) * dRow.GetLength() );

		for ( int64_t iIndexEntry=0; iIndexEntry<m_iDocinfo; iIndexEntry++ )
		{
			const int64_t iBlock = iIndexEntry / DOCINFO_INDEX_FREQ;

			// we have to do some checks in border cases, for example: when move from 1st to 2nd block
			const int64_t iPrevEntryBlock = ( iIndexEntry-1 )/DOCINFO_INDEX_FREQ;
			const bool bIsBordersCheckTime = ( iPrevEntryBlock!=iBlock );

			rdAttr.GetBytes ( dRow.Begin(), sizeof(dRow[0]) * dRow.GetLength() );
			const SphDocID_t uDocID = DOCINFO2ID ( dRow.Begin() );

			const DWORD * pMinEntry = m_pDocinfoIndex + iBlock * uMinMaxStride * 2;
			const DWORD * pMaxEntry = pMinEntry + uMinMaxStride;
			const DWORD * pMinAttrs = DOCINFO2ATTRS ( pMinEntry );
			const DWORD * pMaxAttrs = pMinAttrs + uMinMaxStride;

			// check docid vs global range
			if ( pMaxEntry+uMinMaxStride > pDocinfoIndexMax )
				LOC_FAIL(( fp, "unexpected block index end (row=" INT64_FMT ", docid=" DOCID_FMT ", block=" INT64_FMT ", max=" INT64_FMT ", cur=" INT64_FMT ")",
					iIndexEntry, uDocID, iBlock, int64_t ( pDocinfoIndexMax-m_pDocinfoIndex ), int64_t ( pMaxEntry+uMinMaxStride-m_pDocinfoIndex ) ));

			// check attribute location vs global range
			if ( pMaxAttrs+uMinMaxStride > pDocinfoIndexMax )
				LOC_FAIL(( fp, "attribute position out of blocks index (row=" INT64_FMT ", docid=" DOCID_FMT ", block=" INT64_FMT ", expected<" INT64_FMT ", got=" INT64_FMT ")",
					iIndexEntry, uDocID, iBlock, int64_t ( pDocinfoIndexMax-m_pDocinfoIndex ), int64_t ( pMaxAttrs+uMinMaxStride-m_pDocinfoIndex ) ));

			const SphDocID_t uMinDocID = DOCINFO2ID ( pMinEntry );
			const SphDocID_t uMaxDocID = DOCINFO2ID ( pMaxEntry );

			// checks is docid min max range valid
			if ( uMinDocID > uMaxDocID && bIsBordersCheckTime )
				LOC_FAIL(( fp, "invalid docid range (row=" INT64_FMT ", block=" INT64_FMT ", min=" DOCID_FMT ", max=" DOCID_FMT ")",
					iIndexEntry, iBlock, uMinDocID, uMaxDocID ));

			// checks docid vs blocks range
			if ( uDocID < uMinDocID || uDocID > uMaxDocID )
				LOC_FAIL(( fp, "unexpected docid range (row=" INT64_FMT ", docid=" DOCID_FMT ", block=" INT64_FMT ", min=" DOCID_FMT ", max=" DOCID_FMT ")",
					iIndexEntry, uDocID, iBlock, uMinDocID, uMaxDocID ));

			bool bIsFirstMva = true;
			bool bWasArenaMva = false;

			// check values vs blocks range
			const DWORD * pSpaRow = DOCINFO2ATTRS ( dRow.Begin() );
			for ( int iItem=0; iItem<m_tSchema.GetAttrsCount(); iItem++ )
			{
				const CSphColumnInfo & tCol = m_tSchema.GetAttr(iItem);

				switch ( tCol.m_eAttrType )
				{
				case ESphAttr::SPH_ATTR_INTEGER:
				case ESphAttr::SPH_ATTR_TIMESTAMP:
				case ESphAttr::SPH_ATTR_BOOL:
				case ESphAttr::SPH_ATTR_BIGINT:
					{
						const SphAttr_t uVal = sphGetRowAttr ( pSpaRow, tCol.m_tLocator );
						const SphAttr_t uMin = sphGetRowAttr ( pMinAttrs, tCol.m_tLocator );
						const SphAttr_t uMax = sphGetRowAttr ( pMaxAttrs, tCol.m_tLocator );

						// checks is attribute min max range valid
						if ( uMin > uMax && bIsBordersCheckTime )
							LOC_FAIL(( fp, "invalid attribute range (row=" INT64_FMT ", block=" INT64_FMT ", min=" INT64_FMT ", max=" INT64_FMT ")",
								iIndexEntry, iBlock, uMin, uMax ));

						if ( uVal < uMin || uVal > uMax )
							LOC_FAIL(( fp, "unexpected attribute value (row=" INT64_FMT ", attr=%u, docid=" DOCID_FMT ", block=" INT64_FMT ", value=0x" UINT64_FMT ", min=0x" UINT64_FMT ", max=0x" UINT64_FMT ")",
								iIndexEntry, iItem, uDocID, iBlock, uint64_t(uVal), uint64_t(uMin), uint64_t(uMax) ));
					}
					break;

				case ESphAttr::SPH_ATTR_FLOAT:
					{
						const float fVal = sphDW2F ( (DWORD)sphGetRowAttr ( pSpaRow, tCol.m_tLocator ) );
						const float fMin = sphDW2F ( (DWORD)sphGetRowAttr ( pMinAttrs, tCol.m_tLocator ) );
						const float fMax = sphDW2F ( (DWORD)sphGetRowAttr ( pMaxAttrs, tCol.m_tLocator ) );

						// checks is attribute min max range valid
						if ( fMin > fMax && bIsBordersCheckTime )
							LOC_FAIL(( fp, "invalid attribute range (row=" INT64_FMT ", block=" INT64_FMT ", min=%f, max=%f)",
								iIndexEntry, iBlock, fMin, fMax ));

						if ( fVal < fMin || fVal > fMax )
							LOC_FAIL(( fp, "unexpected attribute value (row=" INT64_FMT ", attr=%u, docid=" DOCID_FMT ", block=" INT64_FMT ", value=%f, min=%f, max=%f)",
								iIndexEntry, iItem, uDocID, iBlock, fVal, fMin, fMax ));
					}
					break;

				case ESphAttr::SPH_ATTR_UINT32SET:
					{
						const DWORD uMin = (DWORD)sphGetRowAttr ( pMinAttrs, tCol.m_tLocator );
						const DWORD uMax = (DWORD)sphGetRowAttr ( pMaxAttrs, tCol.m_tLocator );

						// checks is MVA attribute min max range valid
						if ( uMin > uMax && bIsBordersCheckTime && uMin!=0xffffffff && uMax!=0 )
							LOC_FAIL(( fp, "invalid MVA range (row=" INT64_FMT ", block=" INT64_FMT ", min=0x%x, max=0x%x)",
							iIndexEntry, iBlock, uMin, uMax ));

						SphAttr_t uOff = sphGetRowAttr ( pSpaRow, tCol.m_tLocator );
						if ( !uOff || ( uOff & MVA_ARENA_FLAG )!=0 )
						{
							bWasArenaMva |= ( ( uOff & MVA_ARENA_FLAG )!=0 );
							break;
						}

						SphDocID_t uMvaDocID = 0;
						if ( bIsFirstMva && !bWasArenaMva )
						{
							bIsFirstMva = false;
							rdMva.SeekTo ( sizeof(DWORD) * uOff - sizeof(SphDocID_t), READ_NO_SIZE_HINT );
							uMvaDocID = rdMva.GetDocid();
						} else
						{
							rdMva.SeekTo ( sizeof(DWORD) * uOff, READ_NO_SIZE_HINT );
						}

						if ( uOff>=iMvaEnd )
							break;

						if ( uMvaDocID && uMvaDocID!=uDocID && !bWasArenaMva )
						{
							LOC_FAIL(( fp, "unexpected MVA docid (row=" INT64_FMT ", mvaattr=%d, expected=" DOCID_FMT ", got=" DOCID_FMT ", block=" INT64_FMT ", index=%u)",
								iIndexEntry, iItem, uDocID, uMvaDocID, iBlock, (DWORD)uOff ));
							break;
						}

						// check values
						const DWORD uValues = rdMva.GetDword();
						if ( uOff+uValues>iMvaEnd )
							break;

						dMva.Resize ( uValues );
						rdMva.GetBytes ( dMva.Begin(), sizeof ( dMva[0] ) * uValues );

						for ( DWORD iVal=0; iVal<uValues; iVal++ )
						{
							const DWORD uVal = dMva[iVal];
							if ( uVal < uMin || uVal > uMax )
								LOC_FAIL(( fp, "unexpected MVA value (row=" INT64_FMT ", attr=%u, docid=" DOCID_FMT ", block=" INT64_FMT ", index=%u, value=0x%x, min=0x%x, max=0x%x)",
									iIndexEntry, iItem, uDocID, iBlock, iVal, (DWORD)uVal, (DWORD)uMin, (DWORD)uMax ));
						}
					}
					break;

				default:
					break;
				}
			}

			// progress bar
			if ( iIndexEntry%1000==0 && bProgress )
			{
				fprintf ( fp, INT64_FMT"/" INT64_FMT "\r", iIndexEntry, m_iDocinfo );
				fflush ( fp );
			}
		}
	}

	///////////////////////////
	// check kill-list
	///////////////////////////

	fprintf ( fp, "checking kill-list...\n" );

	// check that ids are ascending
	for ( DWORD uID=1; uID<m_tKillList.GetNumEntries(); uID++ )
		if ( m_tKillList[uID]<=m_tKillList[uID-1] )
			LOC_FAIL(( fp, "unsorted kill-list values (val[%d]=%d, val[%d]=%d)",
				uID-1, (DWORD)m_tKillList[uID-1], uID, (DWORD)m_tKillList[uID] ));

	///////////////////////////
	// all finished
	///////////////////////////

	// well, no known kinds of failures, maybe some unknown ones
	tmCheck = sphMicroTimer() - tmCheck;
	if ( !iFails )
		fprintf ( fp, "check passed" );
	else if ( iFails!=iFailsPrinted )
		fprintf ( fp, "check FAILED, %d of " INT64_FMT " failures reported", iFailsPrinted, iFails );
	else
		fprintf ( fp, "check FAILED, " INT64_FMT " failures reported", iFails );
	fprintf ( fp, ", %d.%d sec elapsed\n", (int)(tmCheck/1000000), (int)((tmCheck/100000)%10) );

	return (int)Min ( iFails, 255 ); // this is the exitcode; so cap it
} // NOLINT function length


CSphIndex* sphCreateIndexPhrase(const char* szIndexName, const char* sFilename)
{
	return new CSphIndex_VLN(szIndexName, sFilename);
}

}