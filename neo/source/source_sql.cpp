#include "neo/source/source_sql.h"
#include "neo/io/io.h"
#include "neo/dict/dict.h"
#include "neo/core/die.h"
#include "neo/utility/inline_misc.h"

namespace NEO {

CSphSourceParams_SQL::CSphSourceParams_SQL ()
	: m_iRangeStep ( 1024 )
	, m_iRefRangeStep ( 1024 )
	, m_bPrintQueries ( false )
	, m_iRangedThrottle ( 0 )
	, m_iMaxFileBufferSize ( 0 )
	, m_eOnFileFieldError ( FFE_IGNORE_FIELD )
	, m_iPort ( 0 )
{
}


const char * const CSphSource_SQL::MACRO_VALUES [ CSphSource_SQL::MACRO_COUNT ] =
{
	"$start",
	"$end"
};


CSphSource_SQL::CSphSource_SQL ( const char * sName )
	: CSphSource_Document	( sName )
	, m_bSqlConnected		( false )
	, m_uMinID				( 0 )
	, m_uMaxID				( 0 )
	, m_uCurrentID			( 0 )
	, m_uMaxFetchedID		( 0 )
	, m_iMultiAttr			( -1 )
	, m_iSqlFields			( 0 )
	, m_bCanUnpack			( false )
	, m_bUnpackFailed		( false )
	, m_bUnpackOverflow		( false )
	, m_iJoinedHitField		( -1 )
	, m_iJoinedHitID		( 0 )
	, m_iJoinedHitPos		( 0 )
{
}


bool CSphSource_SQL::Setup ( const CSphSourceParams_SQL & tParams )
{
	// checks
	assert ( !tParams.m_sQuery.IsEmpty() );

	m_tParams = tParams;

	// defaults
	#define LOC_FIX_NULL(_arg) if ( !m_tParams._arg.cstr() ) m_tParams._arg = "";
	LOC_FIX_NULL ( m_sHost );
	LOC_FIX_NULL ( m_sUser );
	LOC_FIX_NULL ( m_sPass );
	LOC_FIX_NULL ( m_sDB );
	#undef LOC_FIX_NULL

	#define LOC_FIX_QARRAY(_arg) \
		ARRAY_FOREACH ( i, m_tParams._arg ) \
			if ( m_tParams._arg[i].IsEmpty() ) \
				m_tParams._arg.Remove ( i-- );
	LOC_FIX_QARRAY ( m_dQueryPre );
	LOC_FIX_QARRAY ( m_dQueryPost );
	LOC_FIX_QARRAY ( m_dQueryPostIndex );
	#undef LOC_FIX_QARRAY

	// build and store default DSN for error reporting
	char sBuf [ 1024 ];
	snprintf ( sBuf, sizeof(sBuf), "sql://%s:***@%s:%d/%s",
		m_tParams.m_sUser.cstr(), m_tParams.m_sHost.cstr(),
		m_tParams.m_iPort, m_tParams.m_sDB.cstr() );
	m_sSqlDSN = sBuf;

	if ( m_tParams.m_iMaxFileBufferSize > 0 )
		m_iMaxFileBufferSize = m_tParams.m_iMaxFileBufferSize;
	m_eOnFileFieldError = m_tParams.m_eOnFileFieldError;

	return true;
}

const char * SubstituteParams ( const char * sQuery, const char * const * dMacroses, const char ** dValues, int iMcount )
{
	// OPTIMIZE? things can be precalculated
	const char * sCur = sQuery;
	int iLen = 0;
	while ( *sCur )
	{
		if ( *sCur=='$' )
		{
			int i;
			for ( i=0; i<iMcount; i++ )
				if ( strncmp ( dMacroses[i], sCur, strlen ( dMacroses[i] ) )==0 )
				{
					sCur += strlen ( dMacroses[i] );
					iLen += strlen ( dValues[i] );
					break;
				}
			if ( i<iMcount )
				continue;
		}

		sCur++;
		iLen++;
	}
	iLen++; // trailing zero

	// do interpolation
	char * sRes = new char [ iLen ];
	sCur = sQuery;

	char * sDst = sRes;
	while ( *sCur )
	{
		if ( *sCur=='$' )
		{
			int i;
			for ( i=0; i<iMcount; i++ )
				if ( strncmp ( dMacroses[i], sCur, strlen ( dMacroses[i] ) )==0 )
				{
					strcpy ( sDst, dValues[i] ); // NOLINT
					sCur += strlen ( dMacroses[i] );
					sDst += strlen ( dValues[i] );
					break;
				}
			if ( i<iMcount )
				continue;
		}
		*sDst++ = *sCur++;
	}
	*sDst++ = '\0';
	assert ( sDst-sRes==iLen );
	return sRes;
}


bool CSphSource_SQL::RunQueryStep ( const char * sQuery, CSphString & sError )
{
	sError = "";

	if ( m_tParams.m_iRangeStep<=0 )
		return false;
	if ( m_uCurrentID>m_uMaxID )
		return false;

	static const int iBufSize = 32;
	const char * sRes = NULL;

	sphSleepMsec ( m_tParams.m_iRangedThrottle );

	//////////////////////////////////////////////
	// range query with $start/$end interpolation
	//////////////////////////////////////////////

	assert ( m_uMinID>0 );
	assert ( m_uMaxID>0 );
	assert ( m_uMinID<=m_uMaxID );
	assert ( sQuery );

	char sValues [ MACRO_COUNT ] [ iBufSize ];
	const char * pValues [ MACRO_COUNT ];
	SphDocID_t uNextID = Min ( m_uCurrentID + (SphDocID_t)m_tParams.m_iRangeStep - 1, m_uMaxID );
	snprintf ( sValues[0], iBufSize, DOCID_FMT, m_uCurrentID );
	snprintf ( sValues[1], iBufSize, DOCID_FMT, uNextID );
	pValues[0] = sValues[0];
	pValues[1] = sValues[1];
	g_iIndexerCurrentRangeMin = m_uCurrentID;
	g_iIndexerCurrentRangeMax = uNextID;
	m_uCurrentID = 1 + uNextID;

	sRes = SubstituteParams ( sQuery, MACRO_VALUES, pValues, MACRO_COUNT );

	// run query
	SqlDismissResult ();
	bool bRes = SqlQuery ( sRes );

	if ( !bRes )
		sError.SetSprintf ( "sql_range_query: %s (DSN=%s)", SqlError(), m_sSqlDSN.cstr() );

	SafeDeleteArray ( sRes );
	return bRes;
}

static bool HookConnect ( const char* szCommand )
{
	FILE * pPipe = popen ( szCommand, "r" );
	if ( !pPipe )
		return false;
	pclose ( pPipe );
	return true;
}

inline static const char* skipspace ( const char* pBuf, const char* pBufEnd )
{
	assert ( pBuf );
	assert ( pBufEnd );

	while ( (pBuf<pBufEnd) && isspace ( *pBuf ) )
		++pBuf;
	return pBuf;
}

inline static const char* scannumber ( const char* pBuf, const char* pBufEnd, SphDocID_t* pRes )
{
	assert ( pBuf );
	assert ( pBufEnd );
	assert ( pRes );

	if ( pBuf<pBufEnd )
	{
		*pRes = 0;
		// FIXME! could check for overflow
		while ( isdigit ( *pBuf ) && pBuf<pBufEnd )
			(*pRes) = 10*(*pRes) + (int)( (*pBuf++)-'0' );
	}
	return pBuf;
}

static bool HookQueryRange ( const char* szCommand, SphDocID_t* pMin, SphDocID_t* pMax )
{
	FILE * pPipe = popen ( szCommand, "r" );
	if ( !pPipe )
		return false;

	const int MAX_BUF_SIZE = 1024;
	char dBuf [MAX_BUF_SIZE];
	int iRead = (int)fread ( dBuf, 1, MAX_BUF_SIZE, pPipe );
	pclose ( pPipe );
	const char* pStart = dBuf;
	const char* pEnd = pStart + iRead;
	// leading whitespace and 1-st number
	pStart = skipspace ( pStart, pEnd );
	pStart = scannumber ( pStart, pEnd, pMin );
	// whitespace and 2-nd number
	pStart = skipspace ( pStart, pEnd );
	scannumber ( pStart, pEnd, pMax );
	return true;
}

static bool HookPostIndex ( const char* szCommand, SphDocID_t uLastIndexed )
{
	const char * sMacro = "$maxid";
	char sValue[32];
	const char* pValue = sValue;
	snprintf ( sValue, sizeof(sValue), DOCID_FMT, uLastIndexed );

	const char * pCmd = SubstituteParams ( szCommand, &sMacro, &pValue, 1 );

	FILE * pPipe = popen ( pCmd, "r" );
	SafeDeleteArray ( pCmd );
	if ( !pPipe )
		return false;
	pclose ( pPipe );
	return true;
}

/// connect to SQL server
bool CSphSource_SQL::Connect ( CSphString & sError )
{
	// do not connect twice
	if ( m_bSqlConnected )
		return true;

	// try to connect
	if ( !SqlConnect() )
	{
		sError.SetSprintf ( "sql_connect: %s (DSN=%s)", SqlError(), m_sSqlDSN.cstr() );
		return false;
	}

	m_tHits.m_dData.Reserve ( m_iMaxHits );

	// all good
	m_bSqlConnected = true;
	if ( !m_tParams.m_sHookConnect.IsEmpty() && !HookConnect ( m_tParams.m_sHookConnect.cstr() ) )
	{
		sError.SetSprintf ( "hook_connect: runtime error %s when running external hook", strerror(errno) );
		return false;
	}
	return true;
}


#define LOC_ERROR(_msg,_arg)			{ sError.SetSprintf ( _msg, _arg ); return false; }
#define LOC_ERROR2(_msg,_arg,_arg2)		{ sError.SetSprintf ( _msg, _arg, _arg2 ); return false; }

/// setup them ranges (called both for document range-queries and MVA range-queries)
bool CSphSource_SQL::SetupRanges ( const char * sRangeQuery, const char * sQuery, const char * sPrefix, CSphString & sError, ERangesReason iReason )
{
	// check step
	if ( m_tParams.m_iRangeStep<=0 )
		LOC_ERROR ( "sql_range_step=" INT64_FMT ": must be non-zero positive", m_tParams.m_iRangeStep );

	if ( m_tParams.m_iRangeStep<128 )
		sphWarn ( "sql_range_step=" INT64_FMT ": too small; might hurt indexing performance!", m_tParams.m_iRangeStep );

	// check query for macros
	for ( int i=0; i<MACRO_COUNT; i++ )
		if ( !strstr ( sQuery, MACRO_VALUES[i] ) )
			LOC_ERROR2 ( "%s: macro '%s' not found in match fetch query", sPrefix, MACRO_VALUES[i] );

	// run query
	if ( !SqlQuery ( sRangeQuery ) )
	{
		sError.SetSprintf ( "%s: range-query failed: %s (DSN=%s)", sPrefix, SqlError(), m_sSqlDSN.cstr() );
		return false;
	}

	// fetch min/max
	int iCols = SqlNumFields ();
	if ( iCols!=2 )
		LOC_ERROR2 ( "%s: expected 2 columns (min_id/max_id), got %d", sPrefix, iCols );

	if ( !SqlFetchRow() )
	{
		sError.SetSprintf ( "%s: range-query fetch failed: %s (DSN=%s)", sPrefix, SqlError(), m_sSqlDSN.cstr() );
		return false;
	}

	if ( ( SqlColumn(0)==NULL || !SqlColumn(0)[0] ) && ( SqlColumn(1)==NULL || !SqlColumn(1)[0] ) )
	{
		// the source seems to be empty; workaround
		m_uMinID = 1;
		m_uMaxID = 1;

	} else
	{
		// get and check min/max id
		const char * sCol0 = SqlColumn(0);
		const char * sCol1 = SqlColumn(1);
		m_uMinID = sphToDocid ( sCol0 );
		m_uMaxID = sphToDocid ( sCol1 );
		if ( !sCol0 ) sCol0 = "(null)";
		if ( !sCol1 ) sCol1 = "(null)";

		if ( m_uMinID<=0 )
			LOC_ERROR ( "sql_query_range: min_id='%s': must be positive 32/64-bit unsigned integer", sCol0 );
		if ( m_uMaxID<=0 )
			LOC_ERROR ( "sql_query_range: max_id='%s': must be positive 32/64-bit unsigned integer", sCol1 );
		if ( m_uMinID>m_uMaxID )
			LOC_ERROR2 ( "sql_query_range: min_id='%s', max_id='%s': min_id must be less than max_id", sCol0, sCol1 );
	}

	SqlDismissResult ();

	if ( iReason==SRE_DOCS && ( !m_tParams.m_sHookQueryRange.IsEmpty() ) )
	{
		if ( !HookQueryRange ( m_tParams.m_sHookQueryRange.cstr(), &m_uMinID, &m_uMaxID ) )
			LOC_ERROR ( "hook_query_range: runtime error %s when running external hook", strerror(errno) );
		if ( m_uMinID<=0 )
			LOC_ERROR ( "hook_query_range: min_id=" DOCID_FMT ": must be positive 32/64-bit unsigned integer", m_uMinID );
		if ( m_uMaxID<=0 )
			LOC_ERROR ( "hook_query_range: max_id=" DOCID_FMT ": must be positive 32/64-bit unsigned integer", m_uMaxID );
		if ( m_uMinID>m_uMaxID )
			LOC_ERROR2 ( "hook_query_range: min_id=" DOCID_FMT ", max_id=" DOCID_FMT ": min_id must be less than max_id", m_uMinID, m_uMaxID );
	}

	return true;
}


/// issue main rows fetch query
bool CSphSource_SQL::IterateStart ( CSphString & sError )
{
	assert ( m_bSqlConnected );

	m_iNullIds = false;
	m_iMaxIds = false;

	// run pre-queries
	ARRAY_FOREACH ( i, m_tParams.m_dQueryPre )
	{
		if ( !SqlQuery ( m_tParams.m_dQueryPre[i].cstr() ) )
		{
			sError.SetSprintf ( "sql_query_pre[%d]: %s (DSN=%s)", i, SqlError(), m_sSqlDSN.cstr() );
			SqlDisconnect ();
			return false;
		}
		SqlDismissResult ();
	}

	for ( ;; )
	{
		m_tParams.m_iRangeStep = 0;

		// issue first fetch query
		if ( !m_tParams.m_sQueryRange.IsEmpty() )
		{
			m_tParams.m_iRangeStep = m_tParams.m_iRefRangeStep;
			// run range-query; setup ranges
			if ( !SetupRanges ( m_tParams.m_sQueryRange.cstr(), m_tParams.m_sQuery.cstr(), "sql_query_range: ", sError, SRE_DOCS ) )
				return false;

			// issue query
			m_uCurrentID = m_uMinID;
			if ( !RunQueryStep ( m_tParams.m_sQuery.cstr(), sError ) )
				return false;
		} else
		{
			// normal query; just issue
			if ( !SqlQuery ( m_tParams.m_sQuery.cstr() ) )
			{
				sError.SetSprintf ( "sql_query: %s (DSN=%s)", SqlError(), m_sSqlDSN.cstr() );
				return false;
			}
		}
		break;
	}

	// some post-query setup
	m_tSchema.Reset();

	for ( int i=0; i<SPH_MAX_FIELDS; i++ )
		m_dUnpack[i] = SPH_UNPACK_NONE;

	m_iSqlFields = SqlNumFields(); // for rowdump
	int iCols = SqlNumFields() - 1; // skip column 0, which must be the id

	CSphVector<bool> dFound;
	dFound.Resize ( m_tParams.m_dAttrs.GetLength() );
	ARRAY_FOREACH ( i, dFound )
		dFound[i] = false;

	const bool bWordDict = m_pDict->GetSettings().m_bWordDict;

	// map plain attrs from SQL
	for ( int i=0; i<iCols; i++ )
	{
		const char * sName = SqlFieldName ( i+1 );
		if ( !sName )
			LOC_ERROR ( "column number %d has no name", i+1 );

		CSphColumnInfo tCol ( sName );
		ARRAY_FOREACH ( j, m_tParams.m_dAttrs )
			if ( !strcasecmp ( tCol.m_sName.cstr(), m_tParams.m_dAttrs[j].m_sName.cstr() ) )
		{
			const CSphColumnInfo & tAttr = m_tParams.m_dAttrs[j];

			tCol.m_eAttrType = tAttr.m_eAttrType;
			assert ( tCol.m_eAttrType!=ESphAttr::SPH_ATTR_NONE );

			if ( ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET ) && tAttr.m_eSrc!=SPH_ATTRSRC_FIELD )
				LOC_ERROR ( "multi-valued attribute '%s' of wrong source-type found in query; must be 'field'", tAttr.m_sName.cstr() );

			tCol = tAttr;
			dFound[j] = true;
			break;
		}

		ARRAY_FOREACH ( j, m_tParams.m_dFileFields )
		{
			if ( !strcasecmp ( tCol.m_sName.cstr(), m_tParams.m_dFileFields[j].cstr() ) )
				tCol.m_bFilename = true;
		}

		tCol.m_iIndex = i+1;
		tCol.m_eWordpart = GetWordpart ( tCol.m_sName.cstr(), bWordDict );

		if ( tCol.m_eAttrType==ESphAttr::SPH_ATTR_NONE || tCol.m_bIndexed )
		{
			m_tSchema.m_dFields.Add ( tCol );
			ARRAY_FOREACH ( k, m_tParams.m_dUnpack )
			{
				CSphUnpackInfo & tUnpack = m_tParams.m_dUnpack[k];
				if ( tUnpack.m_sName==tCol.m_sName )
				{
					if ( !m_bCanUnpack )
					{
						sError.SetSprintf ( "this source does not support column unpacking" );
						return false;
					}
					int iIndex = m_tSchema.m_dFields.GetLength() - 1;
					if ( iIndex < SPH_MAX_FIELDS )
					{
						m_dUnpack[iIndex] = tUnpack.m_eFormat;
						m_dUnpackBuffers[iIndex].Resize ( SPH_UNPACK_BUFFER_SIZE );
					}
					break;
				}
			}
		}

		if ( tCol.m_eAttrType!=ESphAttr::SPH_ATTR_NONE )
		{
			if ( CSphSchema::IsReserved ( tCol.m_sName.cstr() ) )
				LOC_ERROR ( "%s is not a valid attribute name", tCol.m_sName.cstr() );

			m_tSchema.AddAttr ( tCol, true ); // all attributes are dynamic at indexing time
		}
	}

	// map multi-valued attrs
	ARRAY_FOREACH ( i, m_tParams.m_dAttrs )
	{
		const CSphColumnInfo & tAttr = m_tParams.m_dAttrs[i];
		if ( ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET ) && tAttr.m_eSrc!=SPH_ATTRSRC_FIELD )
		{
			CSphColumnInfo tMva;
			tMva = tAttr;
			tMva.m_iIndex = m_tSchema.GetAttrsCount();

			if ( CSphSchema::IsReserved ( tMva.m_sName.cstr() ) )
				LOC_ERROR ( "%s is not a valid attribute name", tMva.m_sName.cstr() );

			m_tSchema.AddAttr ( tMva, true ); // all attributes are dynamic at indexing time
			dFound[i] = true;
		}
	}

	// warn if some attrs went unmapped
	ARRAY_FOREACH ( i, dFound )
		if ( !dFound[i] )
			sphWarn ( "attribute '%s' not found - IGNORING", m_tParams.m_dAttrs[i].m_sName.cstr() );

	// joined fields
	m_iPlainFieldsLength = m_tSchema.m_dFields.GetLength();

	ARRAY_FOREACH ( i, m_tParams.m_dJoinedFields )
	{
		CSphColumnInfo tCol;
		tCol.m_iIndex = -1;
		tCol.m_sName = m_tParams.m_dJoinedFields[i].m_sName;
		tCol.m_sQuery = m_tParams.m_dJoinedFields[i].m_sQuery;
		tCol.m_bPayload = m_tParams.m_dJoinedFields[i].m_bPayload;
		tCol.m_eSrc = m_tParams.m_dJoinedFields[i].m_sRanged.IsEmpty() ? SPH_ATTRSRC_QUERY : SPH_ATTRSRC_RANGEDQUERY;
		tCol.m_sQueryRange = m_tParams.m_dJoinedFields[i].m_sRanged;
		tCol.m_eWordpart = GetWordpart ( tCol.m_sName.cstr(), bWordDict );
		m_tSchema.m_dFields.Add ( tCol );
	}

	// auto-computed length attributes
	if ( !AddAutoAttrs ( sError ) )
		return false;

	// alloc storage
	AllocDocinfo();

	// check it
	if ( m_tSchema.m_dFields.GetLength()>SPH_MAX_FIELDS )
		LOC_ERROR2 ( "too many fields (fields=%d, max=%d)",
			m_tSchema.m_dFields.GetLength(), SPH_MAX_FIELDS );

	// log it
	if ( m_fpDumpRows )
	{
		const char * sTable = m_tSchema.m_sName.cstr();

		time_t iNow = time ( NULL );
		fprintf ( m_fpDumpRows, "#\n# === source %s ts %d\n# %s#\n", sTable, (int)iNow, ctime ( &iNow ) );
		ARRAY_FOREACH ( i, m_tSchema.m_dFields )
			fprintf ( m_fpDumpRows, "# field %d: %s\n",(int) i, m_tSchema.m_dFields[i].m_sName.cstr() );

		for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
		{
			const CSphColumnInfo & tCol = m_tSchema.GetAttr(i);
			fprintf ( m_fpDumpRows, "# %s = %s # attr %d\n", sphTypeDirective ( tCol.m_eAttrType ), tCol.m_sName.cstr(), i );
		}

		fprintf ( m_fpDumpRows, "#\n\nDROP TABLE IF EXISTS rows_%s;\nCREATE TABLE rows_%s (\n  id VARCHAR(32) NOT NULL,\n",
			sTable, sTable );
		for ( int i=1; i<m_iSqlFields; i++ )
			fprintf ( m_fpDumpRows, "  %s VARCHAR(4096) NOT NULL,\n", SqlFieldName(i) );
		fprintf ( m_fpDumpRows, "  KEY(id) );\n\n" );
	}

	return true;
}

#undef LOC_ERROR
#undef LOC_ERROR2
#undef LOC_SQL_ERROR


void CSphSource_SQL::Disconnect ()
{
	SafeDeleteArray ( m_pReadFileBuffer );
	m_tHits.m_dData.Reset();

	if ( m_iNullIds )
		sphWarn ( "source %s: skipped %d document(s) with zero/NULL ids", m_tSchema.m_sName.cstr(), m_iNullIds );

	if ( m_iMaxIds )
		sphWarn ( "source %s: skipped %d document(s) with DOCID_MAX ids", m_tSchema.m_sName.cstr(), m_iMaxIds );

	m_iNullIds = 0;
	m_iMaxIds = 0;

	if ( m_bSqlConnected )
		SqlDisconnect ();
	m_bSqlConnected = false;
}


BYTE ** CSphSource_SQL::NextDocument ( CSphString & sError )
{
	assert ( m_bSqlConnected );

	// get next non-zero-id row
	do
	{
		// try to get next row
		bool bGotRow = SqlFetchRow ();

		// when the party's over...
		while ( !bGotRow )
		{
			// is that an error?
			if ( SqlIsError() )
			{
				sError.SetSprintf ( "sql_fetch_row: %s", SqlError() );
				m_tDocInfo.m_uDocID = 1; // 0 means legal eof
				return NULL;
			}

			// maybe we can do next step yet?
			if ( !RunQueryStep ( m_tParams.m_sQuery.cstr(), sError ) )
			{
				// if there's a message, there's an error
				// otherwise, we're just over
				if ( !sError.IsEmpty() )
				{
					m_tDocInfo.m_uDocID = 1; // 0 means legal eof
					return NULL;
				}

			} else
			{
				// step went fine; try to fetch
				bGotRow = SqlFetchRow ();
				continue;
			}

			SqlDismissResult ();

			// ok, we're over
			ARRAY_FOREACH ( i, m_tParams.m_dQueryPost )
			{
				if ( !SqlQuery ( m_tParams.m_dQueryPost[i].cstr() ) )
				{
					sphWarn ( "sql_query_post[%d]: error=%s, query=%s",
						i, SqlError(), m_tParams.m_dQueryPost[i].cstr() );
					break;
				}
				SqlDismissResult ();
			}

			m_tDocInfo.m_uDocID = 0; // 0 means legal eof
			return NULL;
		}

		// get him!
		m_tDocInfo.m_uDocID = VerifyID ( sphToDocid ( SqlColumn(0) ) );
		m_uMaxFetchedID = Max ( m_uMaxFetchedID, m_tDocInfo.m_uDocID );
	} while ( !m_tDocInfo.m_uDocID );

	// cleanup attrs
	for ( int i=0; i<m_tSchema.GetRowSize(); i++ )
		m_tDocInfo.m_pDynamic[i] = 0;

	// split columns into fields and attrs
	for ( int i=0; i<m_iPlainFieldsLength; i++ )
	{
		// get that field
		#if USE_ZLIB
		if ( m_dUnpack[i]!=SPH_UNPACK_NONE )
		{
			DWORD uUnpackedLen = 0;
			m_dFields[i] = (BYTE*) SqlUnpackColumn ( i, uUnpackedLen, m_dUnpack[i] );
			m_dFieldLengths[i] = (int)uUnpackedLen;
			continue;
		}
		#endif
		m_dFields[i] = (BYTE*) SqlColumn ( m_tSchema.m_dFields[i].m_iIndex );
		m_dFieldLengths[i] = SqlColumnLength ( m_tSchema.m_dFields[i].m_iIndex );
	}

	for ( int i=0; i<m_tSchema.GetAttrsCount(); i++ )
	{
		const CSphColumnInfo & tAttr = m_tSchema.GetAttr(i); // shortcut

		if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET )
		{
			int uOff = 0;
			if ( tAttr.m_eSrc==SPH_ATTRSRC_FIELD )
			{
				uOff = ParseFieldMVA ( m_dMva, SqlColumn ( tAttr.m_iIndex ), tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET );
			}
			m_tDocInfo.SetAttr ( tAttr.m_tLocator, uOff );
			continue;
		}

		switch ( tAttr.m_eAttrType )
		{
			case ESphAttr::SPH_ATTR_STRING:
			case ESphAttr::SPH_ATTR_JSON:
				// memorize string, fixup NULLs
				m_dStrAttrs[i] = SqlColumn ( tAttr.m_iIndex );
				if ( !m_dStrAttrs[i].cstr() )
					m_dStrAttrs[i] = "";

				m_tDocInfo.SetAttr ( tAttr.m_tLocator, 0 );
				break;

			case ESphAttr::SPH_ATTR_FLOAT:
				m_tDocInfo.SetAttrFloat ( tAttr.m_tLocator, sphToFloat ( SqlColumn ( tAttr.m_iIndex ) ) ); // FIXME? report conversion errors maybe?
				break;

			case ESphAttr::SPH_ATTR_BIGINT:
				m_tDocInfo.SetAttr ( tAttr.m_tLocator, sphToInt64 ( SqlColumn ( tAttr.m_iIndex ) ) ); // FIXME? report conversion errors maybe?
				break;

			case ESphAttr::SPH_ATTR_TOKENCOUNT:
				// reset, and the value will be filled by IterateHits()
				m_tDocInfo.SetAttr ( tAttr.m_tLocator, 0 );
				break;

			default:
				// just store as uint by default
				m_tDocInfo.SetAttr ( tAttr.m_tLocator, sphToDword ( SqlColumn ( tAttr.m_iIndex ) ) ); // FIXME? report conversion errors maybe?
				break;
		}
	}

	// log it
	if ( m_fpDumpRows )
	{
		fprintf ( m_fpDumpRows, "INSERT INTO rows_%s VALUES (", m_tSchema.m_sName.cstr() );
		for ( int i=0; i<m_iSqlFields; i++ )
		{
			if ( i )
				fprintf ( m_fpDumpRows, ", " );
			FormatEscaped ( m_fpDumpRows, SqlColumn(i) );
		}
		fprintf ( m_fpDumpRows, ");\n" );
	}

	return m_dFields;
}


const int * CSphSource_SQL::GetFieldLengths() const
{
	return m_dFieldLengths;
}


void CSphSource_SQL::PostIndex ()
{
	if ( ( !m_tParams.m_dQueryPostIndex.GetLength() ) && m_tParams.m_sHookPostIndex.IsEmpty() )
		return;

	assert ( !m_bSqlConnected );

	const char * sSqlError = NULL;
	if ( m_tParams.m_dQueryPostIndex.GetLength() )
	{
#define LOC_SQL_ERROR(_msg) { sSqlError = _msg; break; }

		for ( ;; )
		{
			if ( !SqlConnect () )
				LOC_SQL_ERROR ( "mysql_real_connect" );

			ARRAY_FOREACH ( i, m_tParams.m_dQueryPostIndex )
			{
				char * sQuery = sphStrMacro ( m_tParams.m_dQueryPostIndex[i].cstr(), "$maxid", m_uMaxFetchedID );
				bool bRes = SqlQuery ( sQuery );
				delete [] sQuery;

				if ( !bRes )
					LOC_SQL_ERROR ( "sql_query_post_index" );

				SqlDismissResult ();
			}

			break;
		}

		if ( sSqlError )
			sphWarn ( "%s: %s (DSN=%s)", sSqlError, SqlError(), m_sSqlDSN.cstr() );

#undef LOC_SQL_ERROR

		SqlDisconnect ();
	}
	if ( !m_tParams.m_sHookPostIndex.IsEmpty() && !HookPostIndex ( m_tParams.m_sHookPostIndex.cstr(), m_uMaxFetchedID ) )
	{
		sphWarn ( "hook_post_index: runtime error %s when running external hook", strerror(errno) );
	}
}


bool CSphSource_SQL::IterateMultivaluedStart ( int iAttr, CSphString & sError )
{
	if ( iAttr<0 || iAttr>=m_tSchema.GetAttrsCount() )
		return false;

	m_iMultiAttr = iAttr;
	const CSphColumnInfo & tAttr = m_tSchema.GetAttr(iAttr);

	if ( !(tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET ) )
		return false;

	CSphString sPrefix;
	switch ( tAttr.m_eSrc )
	{
	case SPH_ATTRSRC_FIELD:
		return false;

	case SPH_ATTRSRC_QUERY:
		// run simple query
		if ( !SqlQuery ( tAttr.m_sQuery.cstr() ) )
		{
			sError.SetSprintf ( "multi-valued attr '%s' query failed: %s", tAttr.m_sName.cstr(), SqlError() );
			return false;
		}
		break;

	case SPH_ATTRSRC_RANGEDQUERY:
			m_tParams.m_iRangeStep = m_tParams.m_iRefRangeStep;

			// setup ranges
			sPrefix.SetSprintf ( "multi-valued attr '%s' ranged query: ", tAttr.m_sName.cstr() );
			if ( !SetupRanges ( tAttr.m_sQueryRange.cstr(), tAttr.m_sQuery.cstr(), sPrefix.cstr(), sError, SRE_MVA ) )
				return false;

			// run first step (in order to report errors)
			m_uCurrentID = m_uMinID;
			if ( !RunQueryStep ( tAttr.m_sQuery.cstr(), sError ) )
				return false;

			break;

	default:
		sError.SetSprintf ( "INTERNAL ERROR: unknown multi-valued attr source type %d", tAttr.m_eSrc );
		return false;
	}

	// check fields count
	if ( SqlNumFields()!=2 )
	{
		sError.SetSprintf ( "multi-valued attr '%s' query returned %d fields (expected 2)", tAttr.m_sName.cstr(), SqlNumFields() );
		SqlDismissResult ();
		return false;
	}
	return true;
}


bool CSphSource_SQL::IterateMultivaluedNext ()
{
	const CSphColumnInfo & tAttr = m_tSchema.GetAttr ( m_iMultiAttr );

	assert ( m_bSqlConnected );
	assert ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET || tAttr.m_eAttrType==ESphAttr::SPH_ATTR_INT64SET );

	// fetch next row
	bool bGotRow = SqlFetchRow ();
	while ( !bGotRow )
	{
		if ( SqlIsError() )
			sphDie ( "sql_fetch_row: %s", SqlError() ); // FIXME! this should be reported

		if ( tAttr.m_eSrc!=SPH_ATTRSRC_RANGEDQUERY )
		{
			SqlDismissResult();
			return false;
		}

		CSphString sTmp;
		if ( !RunQueryStep ( tAttr.m_sQuery.cstr(), sTmp ) ) // FIXME! this should be reported
			return false;

		bGotRow = SqlFetchRow ();
		continue;
	}

	// return that tuple or offset to storage for MVA64 value
	m_tDocInfo.m_uDocID = sphToDocid ( SqlColumn(0) );
	m_dMva.Resize ( 0 );
	if ( tAttr.m_eAttrType==ESphAttr::SPH_ATTR_UINT32SET )
		m_dMva.Add ( sphToDword ( SqlColumn(1) ) );
	else
		sphAddMva64 ( m_dMva, sphToInt64 ( SqlColumn(1) ) );

	return true;
}


bool CSphSource_SQL::IterateKillListStart ( CSphString & sError )
{
	if ( m_tParams.m_sQueryKilllist.IsEmpty () )
		return false;

	if ( !SqlQuery ( m_tParams.m_sQueryKilllist.cstr () ) )
	{
		sError.SetSprintf ( "killlist query failed: %s", SqlError() );
		return false;
	}

	return true;
}


bool CSphSource_SQL::IterateKillListNext ( SphDocID_t & uDocId )
{
	if ( SqlFetchRow () )
		uDocId = sphToDocid ( SqlColumn(0) );
	else
	{
		if ( SqlIsError() )
			sphDie ( "sql_query_killlist: %s", SqlError() ); // FIXME! this should be reported
		else
		{
			SqlDismissResult ();
			return false;
		}
	}

	return true;
}


void CSphSource_SQL::ReportUnpackError ( int iIndex, int iError )
{
	if ( !m_bUnpackFailed )
	{
		m_bUnpackFailed = true;
		sphWarn ( "failed to unpack column '%s', error=%d, docid=" DOCID_FMT, SqlFieldName(iIndex), iError, m_tDocInfo.m_uDocID );
	}
}


#if !USE_ZLIB

const char * CSphSource_SQL::SqlUnpackColumn ( int iFieldIndex, DWORD & uUnpackedLen, ESphUnpackFormat )
{
	int iIndex = m_tSchema.m_dFields[iFieldIndex].m_iIndex;
	uUnpackedLen = SqlColumnLength(iIndex);
	return SqlColumn(iIndex);
}

#else

const char * CSphSource_SQL::SqlUnpackColumn ( int iFieldIndex, DWORD & uUnpackedLen, ESphUnpackFormat eFormat )
{
	int iIndex = m_tSchema.m_dFields[iFieldIndex].m_iIndex;
	const char * pData = SqlColumn(iIndex);

	if ( pData==NULL )
		return NULL;

	int iPackedLen = SqlColumnLength(iIndex);
	if ( iPackedLen<=0 )
		return NULL;


	CSphVector<char> & tBuffer = m_dUnpackBuffers[iFieldIndex];
	switch ( eFormat )
	{
		case SPH_UNPACK_MYSQL_COMPRESS:
		{
			if ( iPackedLen<=4 )
			{
				if ( !m_bUnpackFailed )
				{
					m_bUnpackFailed = true;
					sphWarn ( "failed to unpack '%s', invalid column size (size=%d), "
						"docid=" DOCID_FMT, SqlFieldName(iIndex), iPackedLen, m_tDocInfo.m_uDocID );
				}
				return NULL;
			}

			unsigned long uSize = 0;
			for ( int i=0; i<4; i++ )
				uSize += ((unsigned long)((BYTE)pData[i])) << ( 8*i );
			uSize &= 0x3FFFFFFF;

			if ( uSize > m_tParams.m_uUnpackMemoryLimit )
			{
				if ( !m_bUnpackOverflow )
				{
					m_bUnpackOverflow = true;
					sphWarn ( "failed to unpack '%s', column size limit exceeded (size=%d),"
						" docid=" DOCID_FMT, SqlFieldName(iIndex), (int)uSize, m_tDocInfo.m_uDocID );
				}
				return NULL;
			}

			int iResult;
			tBuffer.Resize ( uSize + 1 );
			unsigned long uLen = iPackedLen-4;
			iResult = uncompress ( (Bytef *)tBuffer.Begin(), &uSize, (Bytef *)pData + 4, uLen );
			if ( iResult==Z_OK )
			{
				uUnpackedLen = uSize;
				tBuffer[uSize] = 0;
				return &tBuffer[0];
			} else
				ReportUnpackError ( iIndex, iResult );
			return NULL;
		}

		case SPH_UNPACK_ZLIB:
		{
			char * sResult = 0;
			int iBufferOffset = 0;
			int iResult;

			z_stream tStream;
			tStream.zalloc = Z_NULL;
			tStream.zfree = Z_NULL;
			tStream.opaque = Z_NULL;
			tStream.avail_in = iPackedLen;
			tStream.next_in = (Bytef *)SqlColumn(iIndex);

			iResult = inflateInit ( &tStream );
			if ( iResult!=Z_OK )
				return NULL;

			for ( ;; )
			{
				tStream.next_out = (Bytef *)&tBuffer[iBufferOffset];
				tStream.avail_out = tBuffer.GetLength() - iBufferOffset - 1;

				iResult = inflate ( &tStream, Z_NO_FLUSH );
				if ( iResult==Z_STREAM_END )
				{
					tBuffer [ tStream.total_out ] = 0;
					uUnpackedLen = tStream.total_out;
					sResult = &tBuffer[0];
					break;
				} else if ( iResult==Z_OK )
				{
					assert ( tStream.avail_out==0 );

					tBuffer.Resize ( tBuffer.GetLength()*2 );
					iBufferOffset = tStream.total_out;
				} else
				{
					ReportUnpackError ( iIndex, iResult );
					break;
				}
			}

			inflateEnd ( &tStream );
			return sResult;
		}

		case SPH_UNPACK_NONE:
			return pData;
	}
	return NULL;
}
#endif // USE_ZLIB


ISphHits * CSphSource_SQL::IterateJoinedHits ( CSphString & sError )
{
	// iterating of joined hits happens after iterating hits from main query
	// so we may be sure at this moment no new IDs will be put in m_dAllIds
	if ( !m_bIdsSorted )
	{
		m_dAllIds.Uniq();
		m_bIdsSorted = true;
	}
	m_tHits.m_dData.Resize ( 0 );

	// eof check
	if ( m_iJoinedHitField>=m_tSchema.m_dFields.GetLength() )
	{
		m_tDocInfo.m_uDocID = 0;
		return &m_tHits;
	}

	bool bProcessingRanged = true;

	// my fetch loop
	while ( m_iJoinedHitField<m_tSchema.m_dFields.GetLength() )
	{
		if ( m_tState.m_bProcessingHits || SqlFetchRow() )
		{
			// next row
			m_tDocInfo.m_uDocID = sphToDocid ( SqlColumn(0) ); // FIXME! handle conversion errors and zero/max values?

			// lets skip joined document totally if there was no such document ID returned by main query
			if ( !m_dAllIds.BinarySearch ( m_tDocInfo.m_uDocID ) )
				continue;

			// field start? restart ids
			if ( !m_iJoinedHitID )
				m_iJoinedHitID = m_tDocInfo.m_uDocID;

			// docid asc requirement violated? report an error
			if ( m_iJoinedHitID>m_tDocInfo.m_uDocID )
			{
				sError.SetSprintf ( "joined field '%s': query MUST return document IDs in ASC order",
					m_tSchema.m_dFields[m_iJoinedHitField].m_sName.cstr() );
				return NULL;
			}

			// next document? update tracker, reset position
			if ( m_iJoinedHitID<m_tDocInfo.m_uDocID )
			{
				m_iJoinedHitID = m_tDocInfo.m_uDocID;
				m_iJoinedHitPos = 0;
			}

			if ( !m_tState.m_bProcessingHits )
			{
				m_tState = CSphBuildHitsState_t();
				m_tState.m_iField = m_iJoinedHitField;
				m_tState.m_iStartField = m_iJoinedHitField;
				m_tState.m_iEndField = m_iJoinedHitField+1;

				if ( m_tSchema.m_dFields[m_iJoinedHitField].m_bPayload )
					m_tState.m_iStartPos = sphToDword ( SqlColumn(2) );
				else
					m_tState.m_iStartPos = m_iJoinedHitPos;
			}

			// build those hits
			BYTE * dText[] = { (BYTE *)SqlColumn(1) };
			m_tState.m_dFields = dText;
			m_tState.m_dFieldLengths.Resize(1);
			m_tState.m_dFieldLengths[0] = SqlColumnLength(1);

			BuildHits ( sError, true );

			// update current position
			if ( !m_tSchema.m_dFields[m_iJoinedHitField].m_bPayload && !m_tState.m_bProcessingHits && m_tHits.Length() )
				m_iJoinedHitPos = HITMAN::GetPos ( m_tHits.Last()->m_uWordPos );

			if ( m_tState.m_bProcessingHits )
				break;
		} else if ( SqlIsError() )
		{
			// error while fetching row
			sError = SqlError();
			return NULL;

		} else
		{
			int iLastField = m_iJoinedHitField;
			bool bRanged = ( m_iJoinedHitField>=m_iPlainFieldsLength && m_iJoinedHitField<m_tSchema.m_dFields.GetLength()
				&& m_tSchema.m_dFields[m_iJoinedHitField].m_eSrc==SPH_ATTRSRC_RANGEDQUERY );

			// current field is over, continue to next field
			if ( m_iJoinedHitField<0 )
				m_iJoinedHitField = m_iPlainFieldsLength;
			else if ( !bRanged || !bProcessingRanged )
				m_iJoinedHitField++;

			// eof check
			if ( m_iJoinedHitField>=m_tSchema.m_dFields.GetLength() )
			{
				m_tDocInfo.m_uDocID = ( m_tHits.Length() ? 1 : 0 ); // to eof or not to eof
				SqlDismissResult ();
				return &m_tHits;
			}

			SqlDismissResult ();

			bProcessingRanged = false;
			bool bCheckNumFields = true;
			CSphColumnInfo & tJoined = m_tSchema.m_dFields[m_iJoinedHitField];

			// start fetching next field
			if ( tJoined.m_eSrc!=SPH_ATTRSRC_RANGEDQUERY )
			{
				if ( !SqlQuery ( tJoined.m_sQuery.cstr() ) )
				{
					sError = SqlError();
					return NULL;
				}
			} else
			{
				m_tParams.m_iRangeStep = m_tParams.m_iRefRangeStep;

				// setup ranges for next field
				if ( iLastField!=m_iJoinedHitField )
				{
					CSphString sPrefix;
					sPrefix.SetSprintf ( "joined field '%s' ranged query: ", tJoined.m_sName.cstr() );
					if ( !SetupRanges ( tJoined.m_sQueryRange.cstr(), tJoined.m_sQuery.cstr(), sPrefix.cstr(), sError, SRE_JOINEDHITS ) )
						return NULL;

					m_uCurrentID = m_uMinID;
				}

				// run first step (in order to report errors)
				bool bRes = RunQueryStep ( tJoined.m_sQuery.cstr(), sError );
				bProcessingRanged = bRes; // select next documents in range or loop once to process next field
				bCheckNumFields = bRes;

				if ( !sError.IsEmpty() )
					return NULL;
			}

			const int iExpected = m_tSchema.m_dFields[m_iJoinedHitField].m_bPayload ? 3 : 2;
			if ( bCheckNumFields && SqlNumFields()!=iExpected )
			{
				const char * sName = m_tSchema.m_dFields[m_iJoinedHitField].m_sName.cstr();
				sError.SetSprintf ( "joined field '%s': query MUST return exactly %d columns, got %d", sName, iExpected, SqlNumFields() );
				return NULL;
			}

			m_iJoinedHitID = 0;
			m_iJoinedHitPos = 0;
		}
	}

	return &m_tHits;
}

void SqlAttrsConfigure(CSphSourceParams_SQL& tParams, const CSphVariant* pHead,
	ESphAttr eAttrType, const char* sSourceName, bool bIndexedAttr)
{
	for (const CSphVariant* pCur = pHead; pCur; pCur = pCur->m_pNext)
	{
		CSphColumnInfo tCol(pCur->cstr(), eAttrType);
		char* pColon = strchr(const_cast<char*> (tCol.m_sName.cstr()), ':');
		if (pColon)
		{
			*pColon = '\0';

			if (eAttrType == ESphAttr::SPH_ATTR_INTEGER)
			{
				int iBits = strtol(pColon + 1, NULL, 10);
				if (iBits <= 0 || iBits > ROWITEM_BITS)
				{
					fprintf(stdout, "WARNING: source '%s': attribute '%s': invalid bitcount=%d (bitcount ignored)\n",
						sSourceName, tCol.m_sName.cstr(), iBits);
					iBits = -1;
				}
				tCol.m_tLocator.m_iBitCount = iBits;

			}
			else
			{
				fprintf(stdout, "WARNING: source '%s': attribute '%s': bitcount is only supported for integer types\n",
					sSourceName, tCol.m_sName.cstr());
			}
		}
		tParams.m_dAttrs.Add(tCol);
		if (bIndexedAttr)
			tParams.m_dAttrs.Last().m_bIndexed = true;
	}
}


}