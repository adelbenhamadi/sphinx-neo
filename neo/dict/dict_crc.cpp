#include "neo/platform/compat.h"
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/dict/dict_crc.h"
#include "neo/sphinx/xstemmer.h"
#include "neo/sphinx/xutility.h"

#include "neo/utility/log.h"
#include "neo/io/fnv64.h"
#include "neo/io/io_stats.h"
#include "neo/io/crc32.h"
#include "neo/int/scoped_pointer.h"
#include "neo/tools/charset.h"
#include "neo/core/word_list.h"
#include "neo/tokenizer/tokenizer_settings.h"
#include "neo/tokenizer/tokenizer.h"

#include <sys/stat.h>
#include <fcntl.h>


namespace NEO {

	

/*static*/ const int MAX_REPORT_LEN = 1024;


/// morphology
enum
{
	SPH_MORPH_STEM_EN,
	SPH_MORPH_STEM_RU_UTF8,
	SPH_MORPH_STEM_CZ,
	SPH_MORPH_STEM_AR_UTF8,
	SPH_MORPH_SOUNDEX,
	SPH_MORPH_METAPHONE_UTF8,
	SPH_MORPH_AOTLEMMER_BASE,
	SPH_MORPH_AOTLEMMER_RU_UTF8 = SPH_MORPH_AOTLEMMER_BASE,
	SPH_MORPH_AOTLEMMER_EN,
	SPH_MORPH_AOTLEMMER_DE_UTF8,
	SPH_MORPH_AOTLEMMER_BASE_ALL,
	SPH_MORPH_AOTLEMMER_RU_ALL = SPH_MORPH_AOTLEMMER_BASE_ALL,
	SPH_MORPH_AOTLEMMER_EN_ALL,
	SPH_MORPH_AOTLEMMER_DE_ALL,
	SPH_MORPH_LIBSTEMMER_FIRST,
	SPH_MORPH_LIBSTEMMER_LAST = SPH_MORPH_LIBSTEMMER_FIRST + 64
};

/////////////////////

CSphVector<CSphWordforms*> CSphTemplateDictTraits::m_dWordformContainers;

CSphTemplateDictTraits::CSphTemplateDictTraits ()
	: m_iStopwords	( 0 )
	, m_pStopwords	( NULL )
	, m_dStopwordContainer ( 0 )
	, m_bDisableWordforms ( false )
	, m_pWordforms	( NULL )
{
}

CSphTemplateDictTraits::~CSphTemplateDictTraits ()
{
#if USE_LIBSTEMMER
	ARRAY_FOREACH ( i, m_dStemmers )
		sb_stemmer_delete ( m_dStemmers[i] );
#endif

	if ( m_pWordforms )
		--m_pWordforms->m_iRefCount;
}

SphWordID_t CSphTemplateDictTraits::FilterStopword ( SphWordID_t uID ) const
{
	if ( !m_iStopwords )
		return uID;

	// OPTIMIZE: binary search is not too good, could do some hashing instead
	SphWordID_t * pStart = m_pStopwords;
	SphWordID_t * pEnd = m_pStopwords + m_iStopwords - 1;
	do
	{
		if ( uID==*pStart || uID==*pEnd )
			return 0;

		if ( uID<*pStart || uID>*pEnd )
			return uID;

		SphWordID_t * pMid = pStart + (pEnd-pStart)/2;
		if ( uID==*pMid )
			return 0;

		if ( uID<*pMid )
			pEnd = pMid;
		else
			pStart = pMid;
	} while ( pEnd-pStart>1 );

	return uID;
}


int CSphTemplateDictTraits::ParseMorphology ( const char * sMorph, CSphString & sMessage )
{
	int iRes = ST_OK;
	for ( const char * sStart=sMorph; ; )
	{
		while ( *sStart && ( sphIsSpace ( *sStart ) || *sStart==',' ) )
			++sStart;
		if ( !*sStart )
			break;

		const char * sWordStart = sStart;
		while ( *sStart && !sphIsSpace ( *sStart ) && *sStart!=',' )
			++sStart;

		if ( sStart > sWordStart )
		{
			switch ( InitMorph ( sWordStart, sStart - sWordStart, sMessage ) )
			{
				case ST_ERROR:		return ST_ERROR;
				case ST_WARNING:	iRes = ST_WARNING;
				default:			break;
			}
		}
	}
	return iRes;
}


int CSphTemplateDictTraits::InitMorph ( const char * szMorph, int iLength, CSphString & sMessage )
{
	if ( iLength==0 )
		return ST_OK;

	if ( iLength==4 && !strncmp ( szMorph, "none", iLength ) )
		return ST_OK;

	if ( iLength==7 && !strncmp ( szMorph, "stem_en", iLength ) )
	{
		if ( m_dMorph.Contains ( SPH_MORPH_AOTLEMMER_EN ) )
		{
			sMessage.SetSprintf ( "stem_en and lemmatize_en clash" );
			return ST_ERROR;
		}

		if ( m_dMorph.Contains ( SPH_MORPH_AOTLEMMER_EN_ALL ) )
		{
			sMessage.SetSprintf ( "stem_en and lemmatize_en_all clash" );
			return ST_ERROR;
		}

		stem_en_init ();
		return AddMorph ( SPH_MORPH_STEM_EN );
	}

	if ( iLength==7 && !strncmp ( szMorph, "stem_ru", iLength ) )
	{
		if ( m_dMorph.Contains ( SPH_MORPH_AOTLEMMER_RU_UTF8 ) )
		{
			sMessage.SetSprintf ( "stem_ru and lemmatize_ru clash" );
			return ST_ERROR;
		}

		if ( m_dMorph.Contains ( SPH_MORPH_AOTLEMMER_RU_ALL ) )
		{
			sMessage.SetSprintf ( "stem_ru and lemmatize_ru_all clash" );
			return ST_ERROR;
		}

		stem_ru_init ();
		return AddMorph ( SPH_MORPH_STEM_RU_UTF8 );
	}

	for ( int j=0; j<AOT_LENGTH; ++j )
	{
		char buf[20];
		char buf_all[20];
		sprintf ( buf, "lemmatize_%s", AOT_LANGUAGES[j] ); // NOLINT
		sprintf ( buf_all, "lemmatize_%s_all", AOT_LANGUAGES[j] ); // NOLINT

		if ( iLength==12 && !strncmp ( szMorph, buf, iLength ) )
		{
			if ( j==AOT_RU && m_dMorph.Contains ( SPH_MORPH_STEM_RU_UTF8 ) )
			{
				sMessage.SetSprintf ( "stem_ru and lemmatize_ru clash" );
				return ST_ERROR;
			}

			if ( j==AOT_EN && m_dMorph.Contains ( SPH_MORPH_STEM_EN ) )
			{
				sMessage.SetSprintf ( "stem_en and lemmatize_en clash" );
				return ST_ERROR;
			}

			// no test for SPH_MORPH_STEM_DE since we doesn't have it.

			if ( m_dMorph.Contains ( SPH_MORPH_AOTLEMMER_BASE_ALL+j ) )
			{
				sMessage.SetSprintf ( "%s and %s clash", buf, buf_all );
				return ST_ERROR;
			}

			CSphString sDictFile;
			sDictFile.SetSprintf ( "%s/%s.pak", g_sLemmatizerBase.cstr(), AOT_LANGUAGES[j] );
			if ( !sphAotInit ( sDictFile, sMessage, j ) )
				return ST_ERROR;

			// add manually instead of AddMorph(), because we need to update that fingerprint
			int iMorph = j + SPH_MORPH_AOTLEMMER_BASE;
			if ( j==AOT_RU )
				iMorph = SPH_MORPH_AOTLEMMER_RU_UTF8;
			else if ( j==AOT_DE )
				iMorph = SPH_MORPH_AOTLEMMER_DE_UTF8;

			if ( !m_dMorph.Contains ( iMorph ) )
			{
				if ( m_sMorphFingerprint.IsEmpty() )
					m_sMorphFingerprint.SetSprintf ( "%s:%08x"
						, sphAotDictinfo(j).m_sName.cstr()
						, sphAotDictinfo(j).m_iValue );
				else
					m_sMorphFingerprint.SetSprintf ( "%s;%s:%08x"
					, m_sMorphFingerprint.cstr()
					, sphAotDictinfo(j).m_sName.cstr()
					, sphAotDictinfo(j).m_iValue );
				m_dMorph.Add ( iMorph );
			}
			return ST_OK;
		}

		if ( iLength==16 && !strncmp ( szMorph, buf_all, iLength ) )
		{
			if ( j==AOT_RU && ( m_dMorph.Contains ( SPH_MORPH_STEM_RU_UTF8 ) ) )
			{
				sMessage.SetSprintf ( "stem_ru and lemmatize_ru_all clash" );
				return ST_ERROR;
			}

			if ( m_dMorph.Contains ( SPH_MORPH_AOTLEMMER_BASE+j ) )
			{
				sMessage.SetSprintf ( "%s and %s clash", buf, buf_all );
				return ST_ERROR;
			}

			CSphString sDictFile;
			sDictFile.SetSprintf ( "%s/%s.pak", g_sLemmatizerBase.cstr(), AOT_LANGUAGES[j] );
			if ( !sphAotInit ( sDictFile, sMessage, j ) )
				return ST_ERROR;

			return AddMorph ( SPH_MORPH_AOTLEMMER_BASE_ALL+j );
		}
	}

	if ( iLength==7 && !strncmp ( szMorph, "stem_cz", iLength ) )
	{
		stem_cz_init ();
		return AddMorph ( SPH_MORPH_STEM_CZ );
	}

	if ( iLength==7 && !strncmp ( szMorph, "stem_ar", iLength ) )
		return AddMorph ( SPH_MORPH_STEM_AR_UTF8 );

	if ( iLength==9 && !strncmp ( szMorph, "stem_enru", iLength ) )
	{
		stem_en_init ();
		stem_ru_init ();
		AddMorph ( SPH_MORPH_STEM_EN );
		return AddMorph ( SPH_MORPH_STEM_RU_UTF8 );
	}

	if ( iLength==7 && !strncmp ( szMorph, "soundex", iLength ) )
		return AddMorph ( SPH_MORPH_SOUNDEX );

	if ( iLength==9 && !strncmp ( szMorph, "metaphone", iLength ) )
		return AddMorph ( SPH_MORPH_METAPHONE_UTF8 );

#if USE_LIBSTEMMER
	const int LIBSTEMMER_LEN = 11;
	const int MAX_ALGO_LENGTH = 64;
	if ( iLength > LIBSTEMMER_LEN && iLength - LIBSTEMMER_LEN < MAX_ALGO_LENGTH && !strncmp ( szMorph, "libstemmer_", LIBSTEMMER_LEN ) )
	{
		CSphString sAlgo;
		sAlgo.SetBinary ( szMorph+LIBSTEMMER_LEN, iLength - LIBSTEMMER_LEN );

		sb_stemmer * pStemmer = NULL;

		pStemmer = sb_stemmer_new ( sAlgo.cstr(), "UTF_8" );

		if ( !pStemmer )
		{
			sMessage.SetSprintf ( "unknown stemmer libstemmer_%s; skipped", sAlgo.cstr() );
			return ST_WARNING;
		}

		AddMorph ( SPH_MORPH_LIBSTEMMER_FIRST + m_dStemmers.GetLength () );
		ARRAY_FOREACH ( i, m_dStemmers )
		{
			if ( m_dStemmers[i]==pStemmer )
			{
				sb_stemmer_delete ( pStemmer );
				return ST_OK;
			}
		}

		m_dStemmers.Add ( pStemmer );
		m_dDescStemmers.Add ( sAlgo );
		return ST_OK;
	}
#endif

	if ( iLength==11 && !strncmp ( szMorph, "rlp_chinese", iLength ) )
		return ST_OK;

	if ( iLength==19 && !strncmp ( szMorph, "rlp_chinese_batched", iLength ) )
		return ST_OK;

	sMessage.SetBinary ( szMorph, iLength );
	sMessage.SetSprintf ( "unknown stemmer %s; skipped", sMessage.cstr() );
	return ST_WARNING;
}


int CSphTemplateDictTraits::AddMorph ( int iMorph )
{
	if ( !m_dMorph.Contains ( iMorph ) )
		m_dMorph.Add ( iMorph );
	return ST_OK;
}


void CSphTemplateDictTraits::ApplyStemmers ( BYTE * pWord ) const
{
	// try wordforms
	if ( m_pWordforms && m_pWordforms->ToNormalForm ( pWord, true, m_bDisableWordforms ) )
		return;

	// check length
	if ( m_tSettings.m_iMinStemmingLen<=1 || sphUTF8Len ( (const char*)pWord )>=m_tSettings.m_iMinStemmingLen )
	{
		// try stemmers
		ARRAY_FOREACH ( i, m_dMorph )
			if ( StemById ( pWord, m_dMorph[i] ) )
				break;
	}

	if ( m_pWordforms && m_pWordforms->m_bHavePostMorphNF )
		m_pWordforms->ToNormalForm ( pWord, false, m_bDisableWordforms );
}

const CSphMultiformContainer * CSphTemplateDictTraits::GetMultiWordforms () const
{
	return m_pWordforms ? m_pWordforms->m_pMultiWordforms : NULL;
}

uint64_t CSphTemplateDictTraits::GetSettingsFNV () const
{
	uint64_t uHash = (uint64_t)m_pWordforms;

	if ( m_pStopwords )
		uHash = sphFNV64 ( m_pStopwords, m_iStopwords*sizeof(*m_pStopwords), uHash );

	uHash = sphFNV64 ( &m_tSettings.m_iMinStemmingLen, sizeof(m_tSettings.m_iMinStemmingLen), uHash );
	DWORD uFlags = 0;
	if ( m_tSettings.m_bWordDict )
		uFlags |= 1<<0;
	if ( m_tSettings.m_bStopwordsUnstemmed )
		uFlags |= 1<<2;
	uHash = sphFNV64 ( &uFlags, sizeof(uFlags), uHash );

	uHash = sphFNV64 ( m_dMorph.Begin(), m_dMorph.GetLength()*sizeof(m_dMorph[0]), uHash );
#if USE_LIBSTEMMER
	ARRAY_FOREACH ( i, m_dDescStemmers )
		uHash = sphFNV64 ( m_dDescStemmers[i].cstr(), m_dDescStemmers[i].Length(), uHash );
#endif

	return uHash;
}


CSphDict * CSphTemplateDictTraits::CloneBase ( CSphTemplateDictTraits * pDict ) const
{
	assert ( pDict );
	pDict->m_tSettings = m_tSettings;
	pDict->m_iStopwords = m_iStopwords;
	pDict->m_pStopwords = m_pStopwords;
	pDict->m_dSWFileInfos = m_dSWFileInfos;
	pDict->m_dWFFileInfos = m_dWFFileInfos;
	pDict->m_pWordforms = m_pWordforms;
	if ( m_pWordforms )
		m_pWordforms->m_iRefCount++;

	pDict->m_dMorph = m_dMorph;
#if USE_LIBSTEMMER
	assert ( m_dDescStemmers.GetLength()==m_dStemmers.GetLength() );
	pDict->m_dDescStemmers = m_dDescStemmers;
	ARRAY_FOREACH ( i, m_dDescStemmers )
	{
		pDict->m_dStemmers.Add ( sb_stemmer_new ( m_dDescStemmers[i].cstr(), "UTF_8" ) );
		assert ( pDict->m_dStemmers.Last() );
	}
#endif

	return pDict;
}

bool CSphTemplateDictTraits::HasState() const
{
#if !USE_LIBSTEMMER
	return false;
#else
	return ( m_dDescStemmers.GetLength()>0 );
#endif
}

/////////////////////////////////////////////////////////////////////////////

template<>
SphWordID_t CCRCEngine<true>::DoCrc ( const BYTE * pWord )
{
	return sphCRC32 ( pWord );
}


template<>
SphWordID_t CCRCEngine<false>::DoCrc ( const BYTE * pWord )
{
	return (SphWordID_t) sphFNV64 ( pWord );
}


template<>
SphWordID_t CCRCEngine<true>::DoCrc ( const BYTE * pWord, int iLen )
{
	return sphCRC32 ( pWord, iLen );
}


template<>
SphWordID_t CCRCEngine<false>::DoCrc ( const BYTE * pWord, int iLen )
{
	return (SphWordID_t) sphFNV64 ( pWord, iLen );
}


template < bool CRC32DICT >
SphWordID_t CSphDictCRC<CRC32DICT>::GetWordID ( BYTE * pWord )
{
	// apply stopword filter before stemmers
	if ( GetSettings().m_bStopwordsUnstemmed && !FilterStopword ( tHASH::DoCrc ( pWord ) ) )
		return 0;

	// skip stemmers for magic words
	if ( pWord[0]>=0x20 )
		ApplyStemmers ( pWord );

	// stemmer might squeeze out the word
	if ( !pWord[0] )
		return 0;

	return GetSettings().m_bStopwordsUnstemmed
		? tHASH::DoCrc ( pWord )
		: FilterStopword ( tHASH::DoCrc ( pWord ) );
}


template < bool CRC32DICT >
SphWordID_t CSphDictCRC<CRC32DICT>::GetWordID ( const BYTE * pWord, int iLen, bool bFilterStops )
{
	SphWordID_t uId = tHASH::DoCrc ( pWord, iLen );
	return bFilterStops ? FilterStopword ( uId ) : uId;
}


template < bool CRC32DICT >
SphWordID_t CSphDictCRC<CRC32DICT>::GetWordIDWithMarkers ( BYTE * pWord )
{
	ApplyStemmers ( pWord + 1 );
	SphWordID_t uWordId = tHASH::DoCrc ( pWord + 1 );
	int iLength = strlen ( (const char *)(pWord + 1) );
	pWord [iLength + 1] = MAGIC_WORD_TAIL;
	pWord [iLength + 2] = '\0';
	return FilterStopword ( uWordId ) ? tHASH::DoCrc ( pWord ) : 0;
}


template < bool CRC32DICT >
SphWordID_t CSphDictCRC<CRC32DICT>::GetWordIDNonStemmed ( BYTE * pWord )
{
	// this method can generally receive both '=stopword' with a marker and 'stopword' without it
	// so for filtering stopwords, let's handle both
	int iOff = ( pWord[0]<' ' );
	SphWordID_t uWordId = tHASH::DoCrc ( pWord+iOff );
	if ( !FilterStopword ( uWordId ) )
		return 0;

	return tHASH::DoCrc ( pWord );
}


template < bool CRC32DICT >
bool CSphDictCRC<CRC32DICT>::IsStopWord ( const BYTE * pWord ) const
{
	return FilterStopword ( tHASH::DoCrc ( pWord ) )==0;
}


//////////////////////////////////////////////////////////////////////////
SphWordID_t CSphDictTemplate::GetWordID ( BYTE * pWord )
{
	// apply stopword filter before stemmers
	if ( GetSettings().m_bStopwordsUnstemmed && !FilterStopword ( DoCrc ( pWord ) ) )
		return 0;

	// skip stemmers for magic words
	if ( pWord[0]>=0x20 )
		ApplyStemmers ( pWord );

	return GetSettings().m_bStopwordsUnstemmed
		? DoCrc ( pWord )
		: FilterStopword ( DoCrc ( pWord ) );
}


SphWordID_t CSphDictTemplate::GetWordID ( const BYTE * pWord, int iLen, bool bFilterStops )
{
	SphWordID_t uId = DoCrc ( pWord, iLen );
	return bFilterStops ? FilterStopword ( uId ) : uId;
}


SphWordID_t CSphDictTemplate::GetWordIDWithMarkers ( BYTE * pWord )
{
	ApplyStemmers ( pWord + 1 );
	// stemmer might squeeze out the word
	if ( !pWord[1] )
		return 0;
	SphWordID_t uWordId = DoCrc ( pWord + 1 );
	int iLength = strlen ( (const char *)(pWord + 1) );
	pWord [iLength + 1] = MAGIC_WORD_TAIL;
	pWord [iLength + 2] = '\0';
	return FilterStopword ( uWordId ) ? DoCrc ( pWord ) : 0;
}


SphWordID_t CSphDictTemplate::GetWordIDNonStemmed ( BYTE * pWord )
{
	SphWordID_t uWordId = DoCrc ( pWord + 1 );
	if ( !FilterStopword ( uWordId ) )
		return 0;

	return DoCrc ( pWord );
}

bool CSphDictTemplate::IsStopWord ( const BYTE * pWord ) const
{
	return FilterStopword ( DoCrc ( pWord ) )==0;
}

//////////////////////////////////////////////////////////////////////////


void CSphTemplateDictTraits::LoadStopwords ( const char * sFiles, const ISphTokenizer * pTokenizer )
{
	assert ( !m_pStopwords );
	assert ( !m_iStopwords );

	// tokenize file list
	if ( !sFiles || !*sFiles )
		return;

	m_dSWFileInfos.Resize ( 0 );

	CSphScopedPtr<ISphTokenizer> tTokenizer ( pTokenizer->Clone ( SPH_CLONE_INDEX ) );
	CSphFixedVector<char> dList ( 1+strlen(sFiles) );
	strcpy ( dList.Begin(), sFiles ); // NOLINT

	char * pCur = dList.Begin();
	char * sName = NULL;

	CSphVector<SphWordID_t> dStop;

	for ( ;; )
	{
		// find next name start
		while ( *pCur && isspace(*pCur) ) pCur++;
		if ( !*pCur ) break;
		sName = pCur;

		// find next name end
		while ( *pCur && !isspace(*pCur) ) pCur++;
		if ( *pCur ) *pCur++ = '\0';

		BYTE * pBuffer = NULL;

		CSphSavedFile tInfo;
		tInfo.m_sFilename = sName;
		STATS::GetFileStats ( sName, tInfo, NULL );
		m_dSWFileInfos.Add ( tInfo );

		// open file
		struct_stat st;
		if ( stat ( sName, &st )==0 )
			pBuffer = new BYTE [(size_t)st.st_size];
		else
		{
			sphWarn ( "stopwords: failed to get file size for '%s'", sName );
			continue;
		}

		FILE * fp = fopen ( sName, "rb" );
		if ( !fp )
		{
			sphWarn ( "failed to load stopwords from '%s'", sName );
			SafeDeleteArray ( pBuffer );
			continue;
		}

		// tokenize file
		int iLength = (int)fread ( pBuffer, 1, (size_t)st.st_size, fp );

		BYTE * pToken;
		tTokenizer->SetBuffer ( pBuffer, iLength );
		while ( ( pToken = tTokenizer->GetToken() )!=NULL )
			if ( m_tSettings.m_bStopwordsUnstemmed )
				dStop.Add ( GetWordIDNonStemmed ( pToken ) );
			else
				dStop.Add ( GetWordID ( pToken ) );

		// close file
		fclose ( fp );

		SafeDeleteArray ( pBuffer );
	}

	// sort stopwords
	dStop.Uniq();

	// store IDs
	if ( dStop.GetLength() )
	{
		m_dStopwordContainer.Reset ( dStop.GetLength() );
		ARRAY_FOREACH ( i, dStop )
			m_dStopwordContainer[i] = dStop[i];

		m_iStopwords = m_dStopwordContainer.GetLength ();
		m_pStopwords = m_dStopwordContainer.Begin();
	}
}


void CSphTemplateDictTraits::LoadStopwords ( const CSphVector<SphWordID_t> & dStopwords )
{
	m_dStopwordContainer.Reset ( dStopwords.GetLength() );
	ARRAY_FOREACH ( i, dStopwords )
		m_dStopwordContainer[i] = dStopwords[i];

	m_iStopwords = m_dStopwordContainer.GetLength ();
	m_pStopwords = m_dStopwordContainer.Begin();
}


void CSphTemplateDictTraits::WriteStopwords ( CSphWriter & tWriter )
{
	tWriter.PutDword ( (DWORD)m_iStopwords );
	for ( int i = 0; i < m_iStopwords; i++ )
		tWriter.ZipOffset ( m_pStopwords[i] );
}


void CSphTemplateDictTraits::SweepWordformContainers ( const CSphVector<CSphSavedFile> & dFiles )
{
	for ( int i = 0; i < m_dWordformContainers.GetLength (); )
	{
		CSphWordforms * WC = m_dWordformContainers[i];
		if ( WC->m_iRefCount==0 && !WC->IsEqual ( dFiles ) )
		{
			delete WC;
			m_dWordformContainers.Remove ( i );
		} else
			++i;
	}
}




void AddStringToReport ( CSphString & sReport, const CSphString & sString, bool bLast )
{
	int iLen = sReport.Length();
	if ( iLen + sString.Length() + 2 > MAX_REPORT_LEN )
		return;

	char * szReport = (char *)sReport.cstr();
	strcat ( szReport, sString.cstr() );	// NOLINT
	iLen += sString.Length();
	if ( bLast )
		szReport[iLen] = '\0';
	else
	{
		szReport[iLen] = ' ';
		szReport[iLen+1] = '\0';
	}
}


void ConcatReportStrings ( const CSphTightVector<CSphString> & dStrings, CSphString & sReport )
{
	sReport.Reserve ( MAX_REPORT_LEN );
	*(char *)sReport.cstr() = '\0';

	ARRAY_FOREACH ( i, dStrings )
		AddStringToReport ( sReport, dStrings[i], i==dStrings.GetLength()-1 );
}


void ConcatReportStrings ( const CSphTightVector<CSphNormalForm> & dStrings, CSphString & sReport )
{
	sReport.Reserve ( MAX_REPORT_LEN );
	*(char *)sReport.cstr() = '\0';

	ARRAY_FOREACH ( i, dStrings )
		AddStringToReport ( sReport, dStrings[i].m_sForm, i==dStrings.GetLength()-1 );
}


CSphWordforms * CSphTemplateDictTraits::GetWordformContainer ( const CSphVector<CSphSavedFile> & dFileInfos,
	const CSphVector<CSphString> * pEmbedded, const ISphTokenizer * pTokenizer, const char * sIndex )
{
	uint64_t uTokenizerFNV = pTokenizer->GetSettingsFNV();
	ARRAY_FOREACH ( i, m_dWordformContainers )
		if ( m_dWordformContainers[i]->IsEqual ( dFileInfos ) )
		{
			CSphWordforms * pContainer = m_dWordformContainers[i];
			if ( uTokenizerFNV==pContainer->m_uTokenizerFNV )
				return pContainer;

			CSphTightVector<CSphString> dErrorReport;
			ARRAY_FOREACH ( j, dFileInfos )
				dErrorReport.Add ( dFileInfos[j].m_sFilename );

			CSphString sAllFiles;
			ConcatReportStrings ( dErrorReport, sAllFiles );
			sphWarning ( "index '%s': wordforms file '%s' is shared with index '%s', "
				"but tokenizer settings are different",
				sIndex, sAllFiles.cstr(), pContainer->m_sIndexName.cstr() );
		}

	CSphWordforms * pContainer = LoadWordformContainer ( dFileInfos, pEmbedded, pTokenizer, sIndex );
	if ( pContainer )
		m_dWordformContainers.Add ( pContainer );

	return pContainer;
}


struct CmpMultiforms_fn
{
	inline bool IsLess ( const CSphMultiform * pA, const CSphMultiform * pB ) const
	{
		assert ( pA && pB );
		if ( pA->m_iFileId==pB->m_iFileId )
			return pA->m_dTokens.GetLength() > pB->m_dTokens.GetLength();

		return pA->m_iFileId > pB->m_iFileId;
	}
};


void CSphTemplateDictTraits::AddWordform ( CSphWordforms * pContainer, char * sBuffer, int iLen,
	ISphTokenizer * pTokenizer, const char * szFile, const CSphVector<int> & dBlended, int iFileId )
{
	CSphVector<CSphString> dTokens;

	bool bSeparatorFound = false;
	bool bAfterMorphology = false;

	// parse the line
	pTokenizer->SetBuffer ( (BYTE*)sBuffer, iLen );

	bool bFirstToken = true;
	bool bStopwordsPresent = false;
	bool bCommentedWholeLine = false;

	BYTE * pFrom = NULL;
	while ( ( pFrom = pTokenizer->GetToken () )!=NULL )
	{
		if ( *pFrom=='#' )
		{
			bCommentedWholeLine = bFirstToken;
			break;
		}

		if ( *pFrom=='~' && bFirstToken )
		{
			bAfterMorphology = true;
			bFirstToken = false;
			continue;
		}

		bFirstToken = false;

		if ( *pFrom=='>' )
		{
			bSeparatorFound = true;
			break;
		}

		if ( *pFrom=='=' && *pTokenizer->GetBufferPtr()=='>' )
		{
			pTokenizer->GetToken();
			bSeparatorFound = true;
			break;
		}

		if ( GetWordID ( pFrom, strlen ( (const char*)pFrom ), true ) )
			dTokens.Add ( (const char*)pFrom );
		else
			bStopwordsPresent = true;
	}

	if ( !dTokens.GetLength() )
	{
		if ( !bCommentedWholeLine )
			sphWarning ( "index '%s': all source tokens are stopwords (wordform='%s', file='%s'). IGNORED.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );
		return;
	}

	if ( !bSeparatorFound )
	{
		sphWarning ( "index '%s': no wordform separator found (wordform='%s', file='%s'). IGNORED.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );
		return;
	}

	BYTE * pTo = pTokenizer->GetToken ();
	if ( !pTo )
	{
		sphWarning ( "index '%s': no destination token found (wordform='%s', file='%s'). IGNORED.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );
		return;
	}

	if ( *pTo=='#' )
	{
		sphWarning ( "index '%s': misplaced comment (wordform='%s', file='%s'). IGNORED.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );
		return;
	}

	CSphVector<CSphNormalForm> dDestTokens;
	bool bFirstDestIsStop = !GetWordID ( pTo, strlen ( (const char*)pTo ), true );
	CSphNormalForm & tForm = dDestTokens.Add();
	tForm.m_sForm = (const char *)pTo;
	tForm.m_iLengthCP = pTokenizer->GetLastTokenLen();

	// what if we have more than one word in the right part?
	const BYTE * pDestToken;
	while ( ( pDestToken = pTokenizer->GetToken() )!=NULL )
	{
		bool bStop = ( !GetWordID ( pDestToken, strlen ( (const char*)pDestToken ), true ) );
		if ( !bStop )
		{
			CSphNormalForm & tForm = dDestTokens.Add();
			tForm.m_sForm = (const char *)pDestToken;
			tForm.m_iLengthCP = pTokenizer->GetLastTokenLen();
		}

		bStopwordsPresent |= bStop;
	}

	// we can have wordforms with 1 destination token that is a stopword
	if ( dDestTokens.GetLength()>1 && bFirstDestIsStop )
		dDestTokens.Remove(0);

	if ( !dDestTokens.GetLength() )
	{
		sphWarning ( "index '%s': destination token is a stopword (wordform='%s', file='%s'). IGNORED.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );
		return;
	}

	if ( bStopwordsPresent )
		sphWarning ( "index '%s': wordform contains stopwords (wordform='%s'). Fix your wordforms file '%s'.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );

	// we disabled all blended, so we need to filter them manually
	bool bBlendedPresent = false;
	if ( dBlended.GetLength() )
		ARRAY_FOREACH ( i, dDestTokens )
		{
			int iCode;
			const BYTE * pBuf = (const BYTE *) dDestTokens[i].m_sForm.cstr();
			while ( ( iCode = sphUTF8Decode ( pBuf ) )>0 && !bBlendedPresent )
				bBlendedPresent = ( dBlended.BinarySearch ( iCode )!=NULL );
		}

	if ( bBlendedPresent )
		sphWarning ( "invalid mapping (destination contains blended characters) (wordform='%s'). Fix your wordforms file '%s'.", sBuffer, szFile );

	if ( bBlendedPresent && dDestTokens.GetLength()>1 )
	{
		sphWarning ( "blended characters are not allowed with multiple destination tokens (wordform='%s', file='%s'). IGNORED.", sBuffer, szFile );
		return;
	}

	if ( dTokens.GetLength()>1 || dDestTokens.GetLength()>1 )
	{
		CSphMultiform * pMultiWordform = new CSphMultiform;
		pMultiWordform->m_iFileId = iFileId;
		pMultiWordform->m_dNormalForm.Resize ( dDestTokens.GetLength() );
		ARRAY_FOREACH ( i, dDestTokens )
			pMultiWordform->m_dNormalForm[i] = dDestTokens[i];

		for ( int i = 1; i < dTokens.GetLength(); i++ )
			pMultiWordform->m_dTokens.Add ( dTokens[i] );

		if ( !pContainer->m_pMultiWordforms )
			pContainer->m_pMultiWordforms = new CSphMultiformContainer;

		CSphMultiforms ** pWordforms = pContainer->m_pMultiWordforms->m_Hash ( dTokens[0] );
		if ( pWordforms )
		{
			ARRAY_FOREACH ( iMultiform, (*pWordforms)->m_pForms )
			{
				CSphMultiform * pStoredMF = (*pWordforms)->m_pForms[iMultiform];
				if ( pStoredMF->m_dTokens.GetLength()==pMultiWordform->m_dTokens.GetLength() )
				{
					bool bSameTokens = true;
					ARRAY_FOREACH_COND ( iToken, pStoredMF->m_dTokens, bSameTokens )
						if ( pStoredMF->m_dTokens[iToken]!=pMultiWordform->m_dTokens[iToken] )
							bSameTokens = false;

					if ( bSameTokens )
					{
						CSphString sStoredTokens, sStoredForms;
						ConcatReportStrings ( pStoredMF->m_dTokens, sStoredTokens );
						ConcatReportStrings ( pStoredMF->m_dNormalForm, sStoredForms );
						sphWarning ( "index '%s': duplicate wordform found - overridden ( current='%s', old='%s %s > %s' ). Fix your wordforms file '%s'.",
							pContainer->m_sIndexName.cstr(), sBuffer, dTokens[0].cstr(), sStoredTokens.cstr(), sStoredForms.cstr(), szFile );

						pStoredMF->m_dNormalForm.Resize ( pMultiWordform->m_dNormalForm.GetLength() );
						ARRAY_FOREACH ( iForm, pMultiWordform->m_dNormalForm )
							pStoredMF->m_dNormalForm[iForm] = pMultiWordform->m_dNormalForm[iForm];

						pStoredMF->m_iFileId = iFileId;

						SafeDelete ( pMultiWordform );
						break; // otherwise, we crash next turn
					}
				}
			}

			if ( pMultiWordform )
			{
				(*pWordforms)->m_pForms.Add ( pMultiWordform );

				// sort forms by files and length
				// but do not sort if we're loading embedded
				if ( iFileId>=0 )
					(*pWordforms)->m_pForms.Sort ( CmpMultiforms_fn() );

				(*pWordforms)->m_iMinTokens = Min ( (*pWordforms)->m_iMinTokens,(int) pMultiWordform->m_dTokens.GetLength () );
				(*pWordforms)->m_iMaxTokens = Max ( (*pWordforms)->m_iMaxTokens,(int) pMultiWordform->m_dTokens.GetLength () );
				pContainer->m_pMultiWordforms->m_iMaxTokens = Max ( pContainer->m_pMultiWordforms->m_iMaxTokens, (*pWordforms)->m_iMaxTokens );
			}
		} else
		{
			CSphMultiforms * pNewWordforms = new CSphMultiforms;
			pNewWordforms->m_pForms.Add ( pMultiWordform );
			pNewWordforms->m_iMinTokens = pMultiWordform->m_dTokens.GetLength ();
			pNewWordforms->m_iMaxTokens = pMultiWordform->m_dTokens.GetLength ();
			pContainer->m_pMultiWordforms->m_iMaxTokens = Max ( pContainer->m_pMultiWordforms->m_iMaxTokens, pNewWordforms->m_iMaxTokens );
			pContainer->m_pMultiWordforms->m_Hash.Add ( pNewWordforms, dTokens[0] );
		}

		// let's add destination form to regular wordform to keep destination from being stemmed
		// FIXME!!! handle multiple destination tokens and ~flag for wordforms
		if ( !bAfterMorphology && dDestTokens.GetLength()==1 && !pContainer->m_dHash.Exists ( dDestTokens[0].m_sForm ) )
		{
			CSphStoredNF tForm;
			tForm.m_sWord = dDestTokens[0].m_sForm;
			tForm.m_bAfterMorphology = bAfterMorphology;
			pContainer->m_bHavePostMorphNF |= bAfterMorphology;
			if ( !pContainer->m_dNormalForms.GetLength()
				|| pContainer->m_dNormalForms.Last().m_sWord!=dDestTokens[0].m_sForm
				|| pContainer->m_dNormalForms.Last().m_bAfterMorphology!=bAfterMorphology )
				pContainer->m_dNormalForms.Add ( tForm );

			pContainer->m_dHash.Add ( pContainer->m_dNormalForms.GetLength()-1, dDestTokens[0].m_sForm );
		}
	} else
	{
		if ( bAfterMorphology )
		{
			BYTE pBuf [16+3*SPH_MAX_WORD_LEN];
			memcpy ( pBuf, dTokens[0].cstr(), dTokens[0].Length()+1 );
			ApplyStemmers ( pBuf );
			dTokens[0] = (char *)pBuf;
		}

		// check wordform that source token is a new token or has same destination token
		int * pRefTo = pContainer->m_dHash ( dTokens[0] );
		assert ( !pRefTo || ( *pRefTo>=0 && *pRefTo<pContainer->m_dNormalForms.GetLength() ) );
		if ( pRefTo )
		{
			// replace with a new wordform
			if ( pContainer->m_dNormalForms[*pRefTo].m_sWord!=dDestTokens[0].m_sForm || pContainer->m_dNormalForms[*pRefTo].m_bAfterMorphology!=bAfterMorphology )
			{
				CSphStoredNF & tRefTo = pContainer->m_dNormalForms[*pRefTo];
				sphWarning ( "index '%s': duplicate wordform found - overridden ( current='%s', old='%s%s > %s' ). Fix your wordforms file '%s'.",
					pContainer->m_sIndexName.cstr(), sBuffer, tRefTo.m_bAfterMorphology ? "~" : "", dTokens[0].cstr(), tRefTo.m_sWord.cstr(), szFile );

				tRefTo.m_sWord = dDestTokens[0].m_sForm;
				tRefTo.m_bAfterMorphology = bAfterMorphology;
				pContainer->m_bHavePostMorphNF |= bAfterMorphology;
			} else
				sphWarning ( "index '%s': duplicate wordform found ( '%s' ). Fix your wordforms file '%s'.", pContainer->m_sIndexName.cstr(), sBuffer, szFile );
		} else
		{
			CSphStoredNF tForm;
			tForm.m_sWord = dDestTokens[0].m_sForm;
			tForm.m_bAfterMorphology = bAfterMorphology;
			pContainer->m_bHavePostMorphNF |= bAfterMorphology;
			if ( !pContainer->m_dNormalForms.GetLength()
				|| pContainer->m_dNormalForms.Last().m_sWord!=dDestTokens[0].m_sForm
				|| pContainer->m_dNormalForms.Last().m_bAfterMorphology!=bAfterMorphology)
				pContainer->m_dNormalForms.Add ( tForm );

			pContainer->m_dHash.Add ( pContainer->m_dNormalForms.GetLength()-1, dTokens[0] );
		}
	}
}


CSphWordforms * CSphTemplateDictTraits::LoadWordformContainer ( const CSphVector<CSphSavedFile> & dFileInfos,
	const CSphVector<CSphString> * pEmbeddedWordforms, const ISphTokenizer * pTokenizer, const char * sIndex )
{
	// allocate it
	CSphWordforms * pContainer = new CSphWordforms();
	pContainer->m_dFiles = dFileInfos;
	pContainer->m_uTokenizerFNV = pTokenizer->GetSettingsFNV();
	pContainer->m_sIndexName = sIndex;

	CSphScopedPtr<ISphTokenizer> pMyTokenizer ( pTokenizer->Clone ( SPH_CLONE_INDEX ) );
	const CSphTokenizerSettings & tSettings = pMyTokenizer->GetSettings();
	CSphVector<int> dBlended;

	// get a list of blend chars and set add them to the tokenizer as simple chars
	if ( tSettings.m_sBlendChars.Length() )
	{
		CSphVector<char> dNewCharset;
		dNewCharset.Resize ( tSettings.m_sCaseFolding.Length() );
		memcpy ( dNewCharset.Begin(), tSettings.m_sCaseFolding.cstr(), dNewCharset.GetLength() );

		CSphVector<CSphRemapRange> dRemaps;
		CSphCharsetDefinitionParser tParser;
		if ( tParser.Parse ( tSettings.m_sBlendChars.cstr(), dRemaps ) )
			ARRAY_FOREACH ( i, dRemaps )
				for ( int j = dRemaps[i].m_iStart; j<=dRemaps[i].m_iEnd; j++ )
				{
					dNewCharset.Add ( ',' );
					dNewCharset.Add ( ' ' );
					dNewCharset.Add ( char(j) );
					dBlended.Add ( j );
				}

		dNewCharset.Add(0);

		// sort dBlended for binary search
		dBlended.Sort ();

		CSphString sError;
		pMyTokenizer->SetCaseFolding ( dNewCharset.Begin(), sError );

		// disable blend chars
		pMyTokenizer->SetBlendChars ( NULL, sError );
	}

	// add wordform-specific specials
	pMyTokenizer->AddSpecials ( "#=>~" );

	if ( pEmbeddedWordforms )
	{
		CSphTightVector<CSphString> dFilenames;
		dFilenames.Resize ( dFileInfos.GetLength() );
		ARRAY_FOREACH ( i, dFileInfos )
			dFilenames[i] = dFileInfos[i].m_sFilename;

		CSphString sAllFiles;
		ConcatReportStrings ( dFilenames, sAllFiles );

		ARRAY_FOREACH ( i, (*pEmbeddedWordforms) )
			AddWordform ( pContainer, (char*)(*pEmbeddedWordforms)[i].cstr(),
				(*pEmbeddedWordforms)[i].Length(), pMyTokenizer.Ptr(), sAllFiles.cstr(), dBlended, -1 );
	} else
	{
		char sBuffer [ 6*SPH_MAX_WORD_LEN + 512 ]; // enough to hold 2 UTF-8 words, plus some whitespace overhead

		ARRAY_FOREACH ( i, dFileInfos )
		{
			CSphAutoreader rdWordforms;
			const char * szFile = dFileInfos[i].m_sFilename.cstr();
			CSphString sError;
			if ( !rdWordforms.Open ( szFile, sError ) )
			{
				sphWarning ( "index '%s': %s", sIndex, sError.cstr() );
				return NULL;
			}

			int iLen;
			while ( ( iLen = rdWordforms.GetLine ( sBuffer, sizeof(sBuffer) ) )>=0 )
				AddWordform ( pContainer, sBuffer, iLen, pMyTokenizer.Ptr(), szFile, dBlended, i );
		}
	}

	return pContainer;
}


bool CSphTemplateDictTraits::LoadWordforms ( const CSphVector<CSphString> & dFiles,
	const CSphEmbeddedFiles * pEmbedded, const ISphTokenizer * pTokenizer, const char * sIndex )
{
	if ( pEmbedded )
	{
		m_dWFFileInfos.Resize ( pEmbedded->m_dWordformFiles.GetLength() );
		ARRAY_FOREACH ( i, m_dWFFileInfos )
			m_dWFFileInfos[i] = pEmbedded->m_dWordformFiles[i];
	} else
	{
		m_dWFFileInfos.Reserve ( dFiles.GetLength() );
		CSphSavedFile tFile;
		ARRAY_FOREACH ( i, dFiles )
			if ( !dFiles[i].IsEmpty() )
			{
				if (STATS::GetFileStats ( dFiles[i].cstr(), tFile, NULL ) )
					m_dWFFileInfos.Add ( tFile );
				else
					sphWarning ( "index '%s': wordforms file '%s' not found", sIndex, dFiles[i].cstr() );
			}
	}

	if ( !m_dWFFileInfos.GetLength() )
		return false;

	SweepWordformContainers ( m_dWFFileInfos );

	m_pWordforms = GetWordformContainer ( m_dWFFileInfos, pEmbedded ? &(pEmbedded->m_dWordforms) : NULL, pTokenizer, sIndex );
	if ( m_pWordforms )
	{
		m_pWordforms->m_iRefCount++;
		if ( m_pWordforms->m_bHavePostMorphNF && !m_dMorph.GetLength() )
			sphWarning ( "index '%s': wordforms contain post-morphology normal forms, but no morphology was specified", sIndex );
	}

	return !!m_pWordforms;
}


void CSphTemplateDictTraits::WriteWordforms ( CSphWriter & tWriter )
{
	if ( !m_pWordforms )
	{
		tWriter.PutDword(0);
		return;
	}

	int nMultiforms = 0;
	if ( m_pWordforms->m_pMultiWordforms )
	{
		CSphMultiformContainer::CSphMultiformHash & tHash = m_pWordforms->m_pMultiWordforms->m_Hash;
		tHash.IterateStart();
		while ( tHash.IterateNext() )
		{
			CSphMultiforms * pMF = tHash.IterateGet();
			nMultiforms += pMF ? pMF->m_pForms.GetLength() : 0;
		}
	}

	tWriter.PutDword ( m_pWordforms->m_dHash.GetLength()+nMultiforms );
	m_pWordforms->m_dHash.IterateStart();
	while ( m_pWordforms->m_dHash.IterateNext() )
	{
		const CSphString & sKey = m_pWordforms->m_dHash.IterateGetKey();
		int iIndex = m_pWordforms->m_dHash.IterateGet();
		CSphString sLine;
		sLine.SetSprintf ( "%s%s > %s", m_pWordforms->m_dNormalForms[iIndex].m_bAfterMorphology ? "~" : "",
			sKey.cstr(), m_pWordforms->m_dNormalForms[iIndex].m_sWord.cstr() );
		tWriter.PutString ( sLine );
	}

	if ( m_pWordforms->m_pMultiWordforms )
	{
		CSphMultiformContainer::CSphMultiformHash & tHash = m_pWordforms->m_pMultiWordforms->m_Hash;
		tHash.IterateStart();
		while ( tHash.IterateNext() )
		{
			const CSphString & sKey = tHash.IterateGetKey();
			CSphMultiforms * pMF = tHash.IterateGet();
			if ( !pMF )
				continue;

			ARRAY_FOREACH ( i, pMF->m_pForms )
			{
				CSphString sLine, sTokens, sForms;
				ConcatReportStrings ( pMF->m_pForms[i]->m_dTokens, sTokens );
				ConcatReportStrings ( pMF->m_pForms[i]->m_dNormalForm, sForms );

				sLine.SetSprintf ( "%s %s > %s", sKey.cstr(), sTokens.cstr(), sForms.cstr() );
				tWriter.PutString ( sLine );
			}
		}
	}
}


int CSphTemplateDictTraits::SetMorphology ( const char * szMorph, CSphString & sMessage )
{
	m_dMorph.Reset ();
#if USE_LIBSTEMMER
	ARRAY_FOREACH ( i, m_dStemmers )
		sb_stemmer_delete ( m_dStemmers[i] );
	m_dStemmers.Reset ();
#endif

	if ( !szMorph )
		return ST_OK;

	CSphString sOption = szMorph;
	sOption.ToLower ();

	CSphString sError;
	int iRes = ParseMorphology ( sOption.cstr(), sMessage );
	if ( iRes==ST_WARNING && sMessage.IsEmpty() )
		sMessage.SetSprintf ( "invalid morphology option %s; skipped", sOption.cstr() );
	return iRes;
}


bool CSphTemplateDictTraits::HasMorphology() const
{
	return ( m_dMorph.GetLength()>0 );
}


/// common id-based stemmer
bool CSphTemplateDictTraits::StemById ( BYTE * pWord, int iStemmer ) const
{
	char szBuf [ MAX_KEYWORD_BYTES ];

	// safe quick strncpy without (!) padding and with a side of strlen
	char * p = szBuf;
	char * pMax = szBuf + sizeof(szBuf) - 1;
	BYTE * pLastSBS = NULL;
	while ( *pWord && p<pMax )
	{
		pLastSBS = ( *pWord )<0x80 ? pWord : pLastSBS;
		*p++ = *pWord++;
	}
	int iLen = p - szBuf;
	*p = '\0';
	pWord -= iLen;

	switch ( iStemmer )
	{
	case SPH_MORPH_STEM_EN:
		stem_en ( pWord, iLen );
		break;

	case SPH_MORPH_STEM_RU_UTF8:
		// skip stemming in case of SBC at the end of the word
		if ( pLastSBS && ( pLastSBS-pWord+1 )>=iLen )
			break;

		// stem only UTF8 tail
		if ( !pLastSBS )
		{
			stem_ru_utf8 ( (WORD*)pWord );
		} else
		{
			stem_ru_utf8 ( (WORD *)( pLastSBS+1 ) );
		}
		break;

	case SPH_MORPH_STEM_CZ:
		stem_cz ( pWord );
		break;

	case SPH_MORPH_STEM_AR_UTF8:
		stem_ar_utf8 ( pWord );
		break;

	case SPH_MORPH_SOUNDEX:
		stem_soundex ( pWord );
		break;

	case SPH_MORPH_METAPHONE_UTF8:
		stem_dmetaphone ( pWord );
		break;

	case SPH_MORPH_AOTLEMMER_RU_UTF8:
		sphAotLemmatizeRuUTF8 ( pWord );
		break;

	case SPH_MORPH_AOTLEMMER_EN:
		sphAotLemmatize ( pWord, AOT_EN );
		break;

	case SPH_MORPH_AOTLEMMER_DE_UTF8:
		sphAotLemmatizeDeUTF8 ( pWord );
		break;

	case SPH_MORPH_AOTLEMMER_RU_ALL:
	case SPH_MORPH_AOTLEMMER_EN_ALL:
	case SPH_MORPH_AOTLEMMER_DE_ALL:
		// do the real work somewhere else
		// this is mostly for warning suppressing and making some features like
		// index_exact_words=1 vs expand_keywords=1 work
		break;

	default:
#if USE_LIBSTEMMER
		if ( iStemmer>=SPH_MORPH_LIBSTEMMER_FIRST && iStemmer<SPH_MORPH_LIBSTEMMER_LAST )
		{
			sb_stemmer * pStemmer = m_dStemmers [iStemmer - SPH_MORPH_LIBSTEMMER_FIRST];
			assert ( pStemmer );

			const sb_symbol * sStemmed = sb_stemmer_stem ( pStemmer, (sb_symbol*)pWord, strlen ( (const char*)pWord ) );
			int iLen = sb_stemmer_length ( pStemmer );

			memcpy ( pWord, sStemmed, iLen );
			pWord[iLen] = '\0';
		} else
			return false;

	break;
#else
		return false;
#endif
	}

	return strcmp ( (char *)pWord, szBuf )!=0;
}

///////////////////////////


void CSphDiskDictTraits::DictBegin ( CSphAutofile & , CSphAutofile & tDict, int, ThrottleState_t * pThrottle )
{
	m_wrDict.CloseFile ();
	m_wrDict.SetFile ( tDict, NULL, m_sWriterError );
	m_wrDict.SetThrottle ( pThrottle );
	m_wrDict.PutByte ( 1 );
}

bool CSphDiskDictTraits::DictEnd ( DictHeader_t * pHeader, int, CSphString & sError, ThrottleState_t * )
{
	// flush wordlist checkpoints
	pHeader->m_iDictCheckpointsOffset = m_wrDict.GetPos();
	pHeader->m_iDictCheckpoints = m_dCheckpoints.GetLength();
	ARRAY_FOREACH ( i, m_dCheckpoints )
	{
		assert ( m_dCheckpoints[i].m_iWordlistOffset );
		m_wrDict.PutOffset ( m_dCheckpoints[i].m_uWordID );
		m_wrDict.PutOffset ( m_dCheckpoints[i].m_iWordlistOffset );
	}

	// done
	m_wrDict.CloseFile ();
	if ( m_wrDict.IsError() )
		sError = m_sWriterError;
	return !m_wrDict.IsError();
}

void CSphDiskDictTraits::DictEntry ( const CSphDictEntry & tEntry )
{
	// insert wordlist checkpoint
	if ( ( m_iEntries % SPH_WORDLIST_CHECKPOINT )==0 )
	{
		if ( m_iEntries ) // but not the 1st entry
		{
			assert ( tEntry.m_iDoclistOffset > m_iLastDoclistPos );
			m_wrDict.ZipInt ( 0 ); // indicate checkpoint
			m_wrDict.ZipOffset ( tEntry.m_iDoclistOffset - m_iLastDoclistPos ); // store last length
		}

		// restart delta coding, once per SPH_WORDLIST_CHECKPOINT entries
		m_iLastWordID = 0;
		m_iLastDoclistPos = 0;

		// begin new wordlist entry
		assert ( m_wrDict.GetPos()<=UINT_MAX );

		CSphWordlistCheckpoint & tCheckpoint = m_dCheckpoints.Add();
		tCheckpoint.m_uWordID = tEntry.m_uWordID;
		tCheckpoint.m_iWordlistOffset = m_wrDict.GetPos();
	}

	assert ( tEntry.m_iDoclistOffset>m_iLastDoclistPos );
	m_wrDict.ZipOffset ( tEntry.m_uWordID - m_iLastWordID ); // FIXME! slow with 32bit wordids
	m_wrDict.ZipOffset ( tEntry.m_iDoclistOffset - m_iLastDoclistPos );

	m_iLastWordID = tEntry.m_uWordID;
	m_iLastDoclistPos = tEntry.m_iDoclistOffset;

	assert ( tEntry.m_iDocs );
	assert ( tEntry.m_iHits );
	m_wrDict.ZipInt ( tEntry.m_iDocs );
	m_wrDict.ZipInt ( tEntry.m_iHits );

	// write skiplist location info, if any
	if ( tEntry.m_iDocs > SPH_SKIPLIST_BLOCK )
		m_wrDict.ZipOffset ( tEntry.m_iSkiplistOffset );

	m_iEntries++;
}

void CSphDiskDictTraits::DictEndEntries ( SphOffset_t iDoclistOffset )
{
	assert ( iDoclistOffset>=m_iLastDoclistPos );
	m_wrDict.ZipInt ( 0 ); // indicate checkpoint
	m_wrDict.ZipOffset ( iDoclistOffset - m_iLastDoclistPos ); // store last doclist length
}

//////////////////////////


CSphDict* sphCreateDictionaryCRC(const CSphDictSettings& tSettings,
	const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex,
	CSphString& sError)
{
	CSphDict* pDict = NULL;
	if_const(USE_64BIT)
		pDict = new CSphDictCRC<false>();
	else
	pDict = new CSphDictCRC<true>();

	if (!pDict)
		return NULL;
	return SetupDictionary(pDict, tSettings, pFiles, pTokenizer, sIndex, sError);
}



CSphDict* sphCreateDictionaryTemplate(const CSphDictSettings& tSettings,
	const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex,
	CSphString& sError)
{
	CSphDict* pDict = new CSphDictTemplate();
	if (!pDict)
		return NULL;
	return SetupDictionary(pDict, tSettings, pFiles, pTokenizer, sIndex, sError);
}

}