
//#include "neo/sphinx.h"
#include "neo/sphinx/xutility.h"
//#include "neo/sphinxint.h"
#include "neo/sphinx/xplugin.h"
#include "neo/sphinx/xrlp.h"

#include "neo/core/globals.h"
#include "neo/io/io.h"
#include "neo/utility/log.h"
#include "neo/core/die.h"
#include "neo/tokenizer/tokenizer_settings.h"
#include "neo/index/index_settings.h"
#include "neo/source/base.h"
#include "neo/index/ft_index.h"
#include "neo/dict/dict_crc.h"
#include "neo/tools/config_parser.h"
#include "neo/core/version.h"

#include <ctype.h>
#include <fcntl.h>

#include <cerrno>

#if HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#if USE_WINDOWS
#include <io.h> // for ::open on windows
#include <dbghelp.h>
#pragma comment(linker, "/defaultlib:dbghelp.lib")
#pragma message("Automatically linking with dbghelp.lib")
#else
#include <sys/wait.h>
#include <signal.h>
#include <glob.h>

#ifdef HAVE_DLOPEN
#include <dlfcn.h>
#endif // HAVE_DLOPEN

#ifndef HAVE_DLERROR
#define dlerror() ""
#endif // HAVE_DLERROR

#endif

namespace NEO {

//////////////////////////////////////////////////////////////////////////
// STRING FUNCTIONS
//////////////////////////////////////////////////////////////////////////

void sphSplit ( CSphVector<CSphString> & dOut, const char * sIn )
{
	if ( !sIn )
		return;

	const char * p = (char*)sIn;
	while ( *p )
	{
		// skip non-alphas
		while ( (*p) && !sphIsAlpha(*p) )
			p++;
		if ( !(*p) )
			break;

		// this is my next token
		assert ( sphIsAlpha(*p) );
		const char * sNext = p;
		while ( sphIsAlpha(*p) )
			p++;
		if ( sNext!=p )
			dOut.Add().SetBinary ( sNext, int (p-sNext) );
	}
}


void sphSplit ( CSphVector<CSphString> & dOut, const char * sIn, const char * sBounds )
{
	if ( !sIn )
		return;

	const char * p = (char*)sIn;
	while ( *p )
	{
		// skip until the first non-boundary character
		const char * sNext = p;
		while ( *p && !strchr ( sBounds, *p ) )
			p++;

		// add the token, skip the char
		dOut.Add().SetBinary ( sNext, int (p-sNext) );
		if ( *p=='\0' )
			break;

		p++;
	}
}

template < typename T1, typename T2 >
static bool sphWildcardMatchRec ( const T1 * sString, const T2 * sPattern )
{
	if ( !sString || !sPattern )
		return false;

	const T1 * s = sString;
	const T2 * p = sPattern;
	while ( *s )
	{
		switch ( *p )
		{
		case '\\':
			// escaped char, strict match the next one literally
			p++;
			if ( *s++!=*p++ )
				return false;
			break;

		case '?':
			// match any character
			s++;
			p++;
			break;

		case '%':
			// gotta match either 0 or 1 characters
			// well, lets look ahead and see what we need to match next
			p++;

			// just a shortcut, %* can be folded to just *
			if ( *p=='*' )
				break;

			// plain char after a hash? check the non-ambiguous cases
			if ( !sphIsWild(*p) )
			{
				if ( s[0]!=*p )
				{
					// hash does not match 0 chars
					// check if we can match 1 char, or it's a no-match
					if ( s[1]!=*p )
						return false;
					s++;
					break;
				} else
				{
					// hash matches 0 chars
					// check if we could ambiguously match 1 char too, though
					if ( s[1]!=*p )
						break;
					// well, fall through to "scan both options" route
				}
			}

			// could not decide yet
			// so just recurse both options
			if ( sphWildcardMatchRec ( s, p ) || sphWildcardMatchRec ( s+1, p ) )
				return true;
			return false;

		case '*':
			// skip all the extra stars and question marks
			for ( p++; *p=='*' || *p=='?'; p++ )
				if ( *p=='?' )
				{
					s++;
					if ( !*s )
						return p[1]=='\0';
				}

			// short-circuit trailing star
			if ( !*p )
				return true;

			// so our wildcard expects a real character
			// scan forward for its occurrences and recurse
			for ( ;; )
			{
				if ( !*s )
					return false;
				if ( *s==*p && sphWildcardMatchRec ( s+1, p+1 ) )
					return true;
				s++;
			}
			break;

		default:
			// default case, strict match
			if ( *s++!=*p++ )
				return false;
			break;
		}
	}

	// eliminate trailing stars
	while ( *p=='*' )
		p++;

	// string done
	// pattern should be either done too, or a trailing star, or a trailing hash
	return p[0]=='\0'
		|| ( p[0]=='*' && p[1]=='\0' )
		|| ( p[0]=='%' && p[1]=='\0' );
}

template < typename T1, typename T2 >
static bool sphWildcardMatchDP ( const T1 * sString, const T2 * sPattern )
{
	assert ( sString && sPattern && *sString && *sPattern );

	const T1 * s = sString;
	const T2 * p = sPattern;
	bool bEsc = false;
	int iEsc = 0;

	const int iBufCount = 2;
	const int iBufLenMax = SPH_MAX_WORD_LEN*3+4+1;
	int dTmp [iBufCount][iBufLenMax];
	dTmp[0][0] = 1;
	dTmp[1][0] = 0;
	for ( int i=0; i<iBufLenMax; i++ )
		dTmp[0][i] = 1;

	while ( *p )
	{
		// count, flag and skip escape char
		if ( *p=='\\' )
		{
			iEsc++;
			p++;
			bEsc = true;
			continue;
		}

		s = sString;
		int iPattern = int (p - sPattern) + 1 - iEsc;
		int iPrev = ( iPattern + 1 ) % iBufCount;
		int iCur = iPattern % iBufCount;

		// check the 1st wildcard
		if ( !bEsc && ( *p=='*' || *p=='%' ) )
		{
			dTmp[iCur][0] = dTmp[iPrev][0];

		} else
		{
			dTmp[iCur][0] = 0;
		}

		while ( *s )
		{
			int j = int (s - sString) + 1;
			if ( !bEsc && *p=='*' )
			{
				dTmp[iCur][j] = dTmp[iPrev][j-1] || dTmp[iCur][j-1] || dTmp[iPrev][j];
			} else if ( !bEsc && *p=='%' )
			{
				dTmp[iCur][j] = dTmp[iPrev][j-1] || dTmp[iPrev][j];
			} else if ( *p==*s || ( !bEsc && *p=='?' ) )
			{
				dTmp[iCur][j] = dTmp[iPrev][j-1];
			} else
			{
				dTmp[iCur][j] = 0;
			}
			s++;
		}
		p++;
		bEsc = false;
	}

	return ( dTmp[( p-sPattern-iEsc ) % iBufCount][s-sString]!=0 );
}


template < typename T1, typename T2 >
bool sphWildcardMatchSpec ( const T1 * sString, const T2 * sPattern )
{
	int iLen = 0;
	int iStars = 0;
	const T2 * p = sPattern;
	while ( *p )
	{
		iStars += ( *p=='*' );
		iLen++;
		p++;
	}

	if ( iStars>10 || ( iStars>5 && iLen>17 ) )
		return sphWildcardMatchDP ( sString, sPattern );
	else
		return sphWildcardMatchRec ( sString, sPattern );
}


bool sphWildcardMatch ( const char * sString, const char * sPattern, const int * pPattern )
{
	if ( !sString || !sPattern || !*sString || !*sPattern )
		return false;

	// there are basically 4 codepaths, because both string and pattern may or may not contain utf-8 chars
	// pPattern and pString are pointers to unpacked utf-8, pPattern can be precalculated (default is NULL)

	int dString [ SPH_MAX_WORD_LEN + 1 ];
	const int * pString = ( sphIsUTF8 ( sString ) && sphUTF8ToWideChar ( sString, dString, SPH_MAX_WORD_LEN ) ) ? dString : NULL;

	if ( !pString && !pPattern )
		return sphWildcardMatchSpec ( sString, sPattern ); // ascii vs ascii

	if ( pString && !pPattern )
		return sphWildcardMatchSpec ( pString, sPattern ); // utf-8 vs ascii

	if ( !pString && pPattern )
		return sphWildcardMatchSpec ( sString, pPattern ); // ascii vs utf-8

	if ( pString && pPattern )
		return sphWildcardMatchSpec ( pString, pPattern ); // utf-8 vs utf-8

	return false;
}


void sphConfTokenizer ( const CSphConfigSection & hIndex, CSphTokenizerSettings & tSettings )
{
	tSettings.m_iNgramLen = Max ( hIndex.GetInt ( "ngram_len" ), 0 );

	if ( hIndex ( "ngram_chars" ) )
	{
		if ( tSettings.m_iNgramLen )
			tSettings.m_iType = TOKENIZER_NGRAM;
		else
			sphWarning ( "ngram_chars specified, but ngram_len=0; IGNORED" );
	}

	tSettings.m_sCaseFolding = hIndex.GetStr ( "charset_table" );
	tSettings.m_iMinWordLen = Max ( hIndex.GetInt ( "min_word_len", 1 ), 1 );
	tSettings.m_sNgramChars = hIndex.GetStr ( "ngram_chars" );
	tSettings.m_sSynonymsFile = hIndex.GetStr ( "exceptions" ); // new option name
	tSettings.m_sIgnoreChars = hIndex.GetStr ( "ignore_chars" );
	tSettings.m_sBlendChars = hIndex.GetStr ( "blend_chars" );
	tSettings.m_sBlendMode = hIndex.GetStr ( "blend_mode" );
	tSettings.m_sIndexingPlugin = hIndex.GetStr ( "index_token_filter" );

	// phrase boundaries
	int iBoundaryStep = Max ( hIndex.GetInt ( "phrase_boundary_step" ), -1 );
	if ( iBoundaryStep!=0 )
		tSettings.m_sBoundary = hIndex.GetStr ( "phrase_boundary" );
}

void sphConfDictionary ( const CSphConfigSection & hIndex, CSphDictSettings & tSettings )
{
	tSettings.m_sMorphology = hIndex.GetStr ( "morphology" );
	tSettings.m_sStopwords = hIndex.GetStr ( "stopwords" );
	tSettings.m_iMinStemmingLen = hIndex.GetInt ( "min_stemming_len", 1 );
	tSettings.m_bStopwordsUnstemmed = hIndex.GetInt ( "stopwords_unstemmed" )!=0;

	for ( CSphVariant * pWordforms = hIndex("wordforms"); pWordforms; pWordforms = pWordforms->m_pNext )
	{
		if ( !pWordforms->cstr() || !*pWordforms->cstr() )
			continue;

		CSphVector<CSphString> dFilesFound;

#if USE_WINDOWS
		WIN32_FIND_DATA tFFData;
		const char * sLastSlash = NULL;
		for ( const char * s = pWordforms->cstr(); *s; s++ )
			if ( *s=='/' || *s=='\\' )
				sLastSlash = s;

		CSphString sPath;
		if ( sLastSlash )
			sPath = pWordforms->strval().SubString ( 0, sLastSlash - pWordforms->cstr() + 1 );

		HANDLE hFind = FindFirstFile ( pWordforms->cstr(), &tFFData );
		if ( hFind!=INVALID_HANDLE_VALUE )
		{
			if ( !sPath.IsEmpty() )
			{
				dFilesFound.Resize ( dFilesFound.GetLength()+1 );
				dFilesFound.Last().SetSprintf ( "%s%s", sPath.cstr(), tFFData.cFileName );
			} else
				dFilesFound.Add ( tFFData.cFileName );

			while ( FindNextFile ( hFind, &tFFData )!=0 )
			{
				if ( !sPath.IsEmpty() )
				{
					dFilesFound.Resize ( dFilesFound.GetLength()+1 );
					dFilesFound.Last().SetSprintf ( "%s%s", sPath.cstr(), tFFData.cFileName );
				} else
					dFilesFound.Add ( tFFData.cFileName );
			}

			FindClose ( hFind );
		}
#else
		glob_t tGlob;
		glob ( pWordforms->cstr(), GLOB_MARK | GLOB_NOSORT, NULL, &tGlob );
		if ( tGlob.gl_pathv )
			for ( int i = 0; i < (int)tGlob.gl_pathc; i++ )
			{
				const char * szPathName = tGlob.gl_pathv[i];
				if ( !szPathName )
					continue;

				size_t iLen = strlen ( szPathName );
				if ( !iLen || szPathName[iLen-1]=='/' )
					continue;

				dFilesFound.Add ( szPathName );
			}

		globfree ( &tGlob );
#endif

		dFilesFound.Uniq();
		ARRAY_FOREACH ( i, dFilesFound )
			tSettings.m_dWordforms.Add ( dFilesFound[i] );
	}

	if ( hIndex("dict") )
	{
		tSettings.m_bWordDict = true; // default to keywords
		if ( hIndex["dict"]=="crc" )
		{
			sphWarning ( "dict=crc deprecated, use dict=keywords instead" );
			tSettings.m_bWordDict = false;

		} else if ( hIndex["dict"]!="keywords" )
			fprintf ( stdout, "WARNING: unknown dict=%s, defaulting to keywords\n", hIndex["dict"].cstr() );
	}
}

#if USE_RE2
bool sphConfFieldFilter ( const CSphConfigSection & hIndex, CSphFieldFilterSettings & tSettings, CSphString & )
{
	// regular expressions
	tSettings.m_dRegexps.Resize ( 0 );
	for ( CSphVariant * pFilter = hIndex("regexp_filter"); pFilter; pFilter = pFilter->m_pNext )
		tSettings.m_dRegexps.Add ( pFilter->cstr() );

	return tSettings.m_dRegexps.GetLength() > 0;
}
#else
bool sphConfFieldFilter ( const CSphConfigSection & hIndex, CSphFieldFilterSettings &, CSphString & sError )
{
	if ( hIndex ( "regexp_filter" ) )
		sError.SetSprintf ( "regexp_filter specified but no regexp support compiled" );

	return false;
}
#endif

const char * sphBigramName ( ESphBigram eType )
{
	switch ( eType )
	{
		case SPH_BIGRAM_ALL:
			return "all";

		case SPH_BIGRAM_FIRSTFREQ:
			return "first_freq";

		case SPH_BIGRAM_BOTHFREQ:
			return "both_freq";

		case SPH_BIGRAM_NONE:
		default:
			return "none";
	}
}

bool sphConfIndex ( const CSphConfigSection & hIndex, CSphIndexSettings & tSettings, CSphString & sError )
{
	// misc settings
	tSettings.m_iMinPrefixLen = Max ( hIndex.GetInt ( "min_prefix_len" ), 0 );
	tSettings.m_iMinInfixLen = Max ( hIndex.GetInt ( "min_infix_len" ), 0 );
	tSettings.m_iMaxSubstringLen = Max ( hIndex.GetInt ( "max_substring_len" ), 0 );
	tSettings.m_iBoundaryStep = Max ( hIndex.GetInt ( "phrase_boundary_step" ), -1 );
	tSettings.m_bIndexExactWords = hIndex.GetInt ( "index_exact_words" )!=0;
	tSettings.m_iOvershortStep = Min ( Max ( hIndex.GetInt ( "overshort_step", 1 ), 0 ), 1 );
	tSettings.m_iStopwordStep = Min ( Max ( hIndex.GetInt ( "stopword_step", 1 ), 0 ), 1 );
	tSettings.m_iEmbeddedLimit = hIndex.GetSize ( "embedded_limit", 16384 );
	tSettings.m_bIndexFieldLens = hIndex.GetInt ( "index_field_lengths" )!=0;
	tSettings.m_sIndexTokenFilter = hIndex.GetStr ( "index_token_filter" );

	// prefix/infix fields
	CSphString sFields;

	sFields = hIndex.GetStr ( "prefix_fields" );
	sFields.ToLower();
	sphSplit ( tSettings.m_dPrefixFields, sFields.cstr() );

	sFields = hIndex.GetStr ( "infix_fields" );
	sFields.ToLower();
	sphSplit ( tSettings.m_dInfixFields, sFields.cstr() );

	if ( tSettings.m_iMinPrefixLen==0 && tSettings.m_dPrefixFields.GetLength()!=0 )
	{
		fprintf ( stdout, "WARNING: min_prefix_len=0, prefix_fields ignored\n" );
		tSettings.m_dPrefixFields.Reset();
	}

	if ( tSettings.m_iMinInfixLen==0 && tSettings.m_dInfixFields.GetLength()!=0 )
	{
		fprintf ( stdout, "WARNING: min_infix_len=0, infix_fields ignored\n" );
		tSettings.m_dInfixFields.Reset();
	}

	// the only way we could have both prefixes and infixes enabled is when specific field subsets are configured
	if ( tSettings.m_iMinInfixLen>0 && tSettings.m_iMinPrefixLen>0
		&& ( !tSettings.m_dPrefixFields.GetLength() || !tSettings.m_dInfixFields.GetLength() ) )
	{
		sError.SetSprintf ( "prefixes and infixes can not both be enabled on all fields" );
		return false;
	}

	tSettings.m_dPrefixFields.Uniq();
	tSettings.m_dInfixFields.Uniq();

	ARRAY_FOREACH ( i, tSettings.m_dPrefixFields )
		if ( tSettings.m_dInfixFields.Contains ( tSettings.m_dPrefixFields[i] ) )
	{
		sError.SetSprintf ( "field '%s' marked both as prefix and infix", tSettings.m_dPrefixFields[i].cstr() );
		return false;
	}

	if ( tSettings.m_iMaxSubstringLen && tSettings.m_iMaxSubstringLen<tSettings.m_iMinInfixLen )
	{
		sError.SetSprintf ( "max_substring_len=%d is less than min_infix_len=%d", tSettings.m_iMaxSubstringLen, tSettings.m_iMinInfixLen );
		return false;
	}

	if ( tSettings.m_iMaxSubstringLen && tSettings.m_iMaxSubstringLen<tSettings.m_iMinPrefixLen )
	{
		sError.SetSprintf ( "max_substring_len=%d is less than min_prefix_len=%d", tSettings.m_iMaxSubstringLen, tSettings.m_iMinPrefixLen );
		return false;
	}

	bool bWordDict = ( strcmp ( hIndex.GetStr ( "dict", "keywords" ), "keywords" )==0 );
	if ( !bWordDict )
		sphWarning ( "dict=crc deprecated, use dict=keywords instead" );

	if ( hIndex("type") && hIndex["type"]=="rt" && ( tSettings.m_iMinInfixLen>0 || tSettings.m_iMinPrefixLen>0 ) && !bWordDict )
	{
		sError.SetSprintf ( "RT indexes support prefixes and infixes with only dict=keywords" );
		return false;
	}

	if ( bWordDict && tSettings.m_iMaxSubstringLen>0 )
	{
		sError.SetSprintf ( "max_substring_len can not be used with dict=keywords" );
		return false;
	}

	// html stripping
	if ( hIndex ( "html_strip" ) )
	{
		tSettings.m_bHtmlStrip = hIndex.GetInt ( "html_strip" )!=0;
		tSettings.m_sHtmlIndexAttrs = hIndex.GetStr ( "html_index_attrs" );
		tSettings.m_sHtmlRemoveElements = hIndex.GetStr ( "html_remove_elements" );
	}

	// docinfo
	tSettings.m_eDocinfo = SPH_DOCINFO_EXTERN;
	if ( hIndex("docinfo") )
	{
		if ( hIndex["docinfo"]=="none" )		tSettings.m_eDocinfo = SPH_DOCINFO_NONE;
		else if ( hIndex["docinfo"]=="inline" )	tSettings.m_eDocinfo = SPH_DOCINFO_INLINE;
		else if ( hIndex["docinfo"]=="extern" )	tSettings.m_eDocinfo = SPH_DOCINFO_EXTERN;
		else
			fprintf ( stdout, "WARNING: unknown docinfo=%s, defaulting to extern\n", hIndex["docinfo"].cstr() );

		if ( tSettings.m_eDocinfo==SPH_DOCINFO_INLINE )
			fprintf ( stdout, "WARNING: docinfo=inline is deprecated, use ondisk_attrs=1 instead\n" );

		if ( tSettings.m_eDocinfo==SPH_DOCINFO_INLINE && tSettings.m_bIndexFieldLens )
		{
			sError.SetSprintf ( "index_field_lengths must be disabled for docinfo=inline" );
			return false;
		}
	}

	// hit format
	// TODO! add the description into documentation.
	tSettings.m_eHitFormat = SPH_HIT_FORMAT_INLINE;
	if ( hIndex("hit_format") )
	{
		if ( hIndex["hit_format"]=="plain" )		tSettings.m_eHitFormat = SPH_HIT_FORMAT_PLAIN;
		else if ( hIndex["hit_format"]=="inline" )	tSettings.m_eHitFormat = SPH_HIT_FORMAT_INLINE;
		else
			fprintf ( stdout, "WARNING: unknown hit_format=%s, defaulting to inline\n", hIndex["hit_format"].cstr() );
	}

	// hit-less indices
	if ( hIndex("hitless_words") )
	{
		const CSphString & sValue = hIndex["hitless_words"].strval();
		if ( sValue=="all" )
		{
			tSettings.m_eHitless = SPH_HITLESS_ALL;
		} else
		{
			tSettings.m_eHitless = SPH_HITLESS_SOME;
			tSettings.m_sHitlessFiles = sValue;
		}
	}

	// sentence and paragraph indexing
	tSettings.m_bIndexSP = ( hIndex.GetInt ( "index_sp" )!=0 );
	tSettings.m_sZones = hIndex.GetStr ( "index_zones" );

	// bigrams
	tSettings.m_eBigramIndex = SPH_BIGRAM_NONE;
	if ( hIndex("bigram_index") )
	{
		CSphString s = hIndex["bigram_index"].strval();
		s.ToLower();
		if ( s=="all" )
			tSettings.m_eBigramIndex = SPH_BIGRAM_ALL;
		else if ( s=="first_freq" )
			tSettings.m_eBigramIndex = SPH_BIGRAM_FIRSTFREQ;
		else if ( s=="both_freq" )
			tSettings.m_eBigramIndex = SPH_BIGRAM_BOTHFREQ;
		else
		{
			sError.SetSprintf ( "unknown bigram_index=%s (must be all, first_freq, or both_freq)", s.cstr() );
			return false;
		}
	}

	tSettings.m_sBigramWords = hIndex.GetStr ( "bigram_freq_words" );
	tSettings.m_sBigramWords.Trim();

	bool bEmptyOk = tSettings.m_eBigramIndex==SPH_BIGRAM_NONE || tSettings.m_eBigramIndex==SPH_BIGRAM_ALL;
	if ( bEmptyOk!=tSettings.m_sBigramWords.IsEmpty() )
	{
		sError.SetSprintf ( "bigram_index=%s, bigram_freq_words must%s be empty", hIndex["bigram_index"].cstr(),
			bEmptyOk ? "" : " not" );
		return false;
	}

	// aot
	CSphVector<CSphString> dMorphs;
	sphSplit ( dMorphs, hIndex.GetStr ( "morphology" ) );

	tSettings.m_uAotFilterMask = 0;
	for ( int j=0; j<AOT_LENGTH; ++j )
	{
		char buf_all[20];
		sprintf ( buf_all, "lemmatize_%s_all", AOT_LANGUAGES[j] ); //NOLINT
		ARRAY_FOREACH ( i, dMorphs )
			if ( dMorphs[i]==buf_all )
			{
				tSettings.m_uAotFilterMask |= (1UL) << j;
				break;
			}
	}

	bool bPlainRLP = ARRAY_ANY ( bPlainRLP, dMorphs, dMorphs[_any]=="rlp_chinese" );
	bool bBatchedRLP = ARRAY_ANY ( bBatchedRLP, dMorphs, dMorphs[_any]=="rlp_chinese_batched" );

	if ( bPlainRLP && bBatchedRLP )
	{
		fprintf ( stdout, "WARNING: both rlp_chinese and rlp_chinese_batched options specified; switching to rlp_chinese\n" );
		bBatchedRLP = false;
	}

	tSettings.m_eChineseRLP = bPlainRLP ? SPH_RLP_PLAIN : ( bBatchedRLP ? SPH_RLP_BATCHED : SPH_RLP_NONE );
	tSettings.m_sRLPContext = hIndex.GetStr ( "rlp_context" );

	if ( !sphRLPCheckConfig ( tSettings, sError ) )
		fprintf ( stdout, "WARNING: %s\n", sError.cstr() );

	// all good
	return true;
}


bool sphFixupIndexSettings ( CSphIndex * pIndex, const CSphConfigSection & hIndex, CSphString & sError, bool bTemplateDict )
{
	bool bTokenizerSpawned = false;

	if ( !pIndex->GetTokenizer () )
	{
		CSphTokenizerSettings tSettings;
		sphConfTokenizer ( hIndex, tSettings );

		ISphTokenizer * pTokenizer = ISphTokenizer::Create ( tSettings, NULL, sError );
		if ( !pTokenizer )
			return false;

		bTokenizerSpawned = true;
		pIndex->SetTokenizer ( pTokenizer );
	}

	if ( !pIndex->GetDictionary () )
	{
		CSphDict * pDict = NULL;
		CSphDictSettings tSettings;
		if ( bTemplateDict )
		{
			sphConfDictionary ( hIndex, tSettings );
			pDict = sphCreateDictionaryTemplate ( tSettings, NULL, pIndex->GetTokenizer (), pIndex->GetName(), sError );
			CSphIndexSettings tIndexSettings = pIndex->GetSettings();
			tIndexSettings.m_uAotFilterMask = sphParseMorphAot ( tSettings.m_sMorphology.cstr() );
			pIndex->Setup ( tIndexSettings );
		} else
		{
			sphConfDictionary ( hIndex, tSettings );
			pDict = sphCreateDictionaryCRC ( tSettings, NULL, pIndex->GetTokenizer (), pIndex->GetName(), sError );
		}
		if ( !pDict )
		{
			sphWarning ( "index '%s': %s", pIndex->GetName(), sError.cstr() );
			return false;
		}

		pIndex->SetDictionary ( pDict );
	}

	if ( bTokenizerSpawned )
	{
		pIndex->SetTokenizer ( ISphTokenizer::CreateMultiformFilter ( pIndex->LeakTokenizer(),
			pIndex->GetDictionary()->GetMultiWordforms () ) );
	}

	pIndex->SetupQueryTokenizer();

	if ( !pIndex->IsStripperInited () )
	{
		CSphIndexSettings tSettings = pIndex->GetSettings ();

		if ( hIndex ( "html_strip" ) )
		{
			tSettings.m_bHtmlStrip = hIndex.GetInt ( "html_strip" )!=0;
			tSettings.m_sHtmlIndexAttrs = hIndex.GetStr ( "html_index_attrs" );
			tSettings.m_sHtmlRemoveElements = hIndex.GetStr ( "html_remove_elements" );
		}
		tSettings.m_sZones = hIndex.GetStr ( "index_zones" );

		pIndex->Setup ( tSettings );
	}

	if ( !pIndex->GetFieldFilter() )
	{
		ISphFieldFilter * pFieldFilter = NULL;
		CSphFieldFilterSettings tFilterSettings;
		if ( sphConfFieldFilter ( hIndex, tFilterSettings, sError ) )
			pFieldFilter = sphCreateRegexpFilter ( tFilterSettings, sError );

		sphSpawnRLPFilter ( pFieldFilter, pIndex->GetSettings(), pIndex->GetTokenizer()->GetSettings(), pIndex->GetName(), sError );
		if ( !sError.IsEmpty () )
			sphWarning ( "index '%s': %s", pIndex->GetName(), sError.cstr() );

		pIndex->SetFieldFilter ( pFieldFilter );
	}

	// exact words fixup, needed for RT indexes
	// cloned from indexer, remove somehow?
	CSphDict * pDict = pIndex->GetDictionary();
	assert ( pDict );

	CSphIndexSettings tSettings = pIndex->GetSettings ();
	bool bNeedExact = ( pDict->HasMorphology() || pDict->GetWordformsFileInfos().GetLength() );
	if ( tSettings.m_bIndexExactWords && !bNeedExact )
	{
		tSettings.m_bIndexExactWords = false;
		pIndex->Setup ( tSettings );
		fprintf ( stdout, "WARNING: no morphology, index_exact_words=1 has no effect, ignoring\n" );
	}

	if ( pDict->GetSettings().m_bWordDict && pDict->HasMorphology() &&
		( tSettings.m_iMinPrefixLen || tSettings.m_iMinInfixLen ) && !tSettings.m_bIndexExactWords )
	{
		tSettings.m_bIndexExactWords = true;
		pIndex->Setup ( tSettings );
		fprintf ( stdout, "WARNING: dict=keywords and prefixes and morphology enabled, forcing index_exact_words=1\n" );
	}

	pIndex->PostSetup();
	return true;
}

//////////////////////////////////////////////////////////////////////////

const char * sphLoadConfig ( const char * sOptConfig, bool bQuiet, CSphConfigParser & cp )
{
	// fallback to defaults if there was no explicit config specified
	while ( !sOptConfig )
	{
#ifdef SYSCONFDIR
		sOptConfig = SYSCONFDIR "/sphinx.conf";
		if ( sphIsReadable ( sOptConfig ) )
			break;
#endif

		sOptConfig = "./sphinx.conf";
		if ( sphIsReadable ( sOptConfig ) )
			break;

		sOptConfig = NULL;
		break;
	}

	if ( !sOptConfig )
		sphDie ( "no readable config file (looked in "
#ifdef SYSCONFDIR
		SYSCONFDIR "/sphinx.conf, "
#endif
		"./sphinx.conf)" );

	if ( !bQuiet )
		fprintf ( stdout, "using config file '%s'...\n", sOptConfig );

	// load config
	if ( !cp.Parse ( sOptConfig ) )
		sphDie ( "failed to parse config file '%s'", sOptConfig );

	CSphConfig & hConf = cp.m_tConf;
	if ( !hConf ( "index" ) )
		sphDie ( "no indexes found in config file '%s'", sOptConfig );

	return sOptConfig;
}

//////////////////////////////////////////////////////////////////////////


void sphSetJsonOptions(bool bStrict, bool bAutoconvNumbers, bool bKeynamesToLowercase)
{
	g_bJsonStrict = bStrict;
	g_bJsonAutoconvNumbers = bAutoconvNumbers;
	g_bJsonKeynamesToLowercase = bKeynamesToLowercase;
}


//////////////////////////////////////////////////////////////////////////
// CRASH REPORTING
//////////////////////////////////////////////////////////////////////////

template <typename Uint>
static void UItoA ( char** ppOutput, Uint uVal, int iBase=10, int iWidth=0, int iPrec=0, const char cFill=' ' )
{
	assert ( ppOutput );
	assert ( *ppOutput );

	const char cDigits[] = "0123456789abcdef";

	if ( iWidth && iPrec )
	{
		iPrec = iWidth;
		iWidth = 0;
	}

	if ( !uVal )
	{
		if ( !iPrec && !iWidth )
			*(*ppOutput)++ = cDigits[0];
		else
		{
			while ( iPrec-- )
				*(*ppOutput)++ = cDigits[0];
			if ( iWidth )
			{
				while ( --iWidth )
					*(*ppOutput)++ = cFill;
				*(*ppOutput)++ = cDigits[0];
			}
		}
		return;
	}

	const BYTE uMaxIndex = 31; // 20 digits for MAX_INT64 in decimal; let it be 31 (32 digits max).
	char CBuf[uMaxIndex+1];
	char *pRes = &CBuf[uMaxIndex];
	char *& pOutput = *ppOutput;

	while ( uVal )
	{
		*pRes-- = cDigits [ uVal % iBase ];
		uVal /= iBase;
	}

	BYTE uLen = (BYTE)( uMaxIndex - (pRes-CBuf) );

	if ( iWidth )
		while ( uLen < iWidth )
		{
			*pOutput++ = cFill;
			iWidth--;
		}

	if ( iPrec )
	{
		while ( uLen < iPrec )
		{
			*pOutput++=cDigits[0];
			iPrec--;
		}
		iPrec = uLen-iPrec;
	}

	while ( pRes < CBuf+uMaxIndex-iPrec )
		*pOutput++ = *++pRes;
}


static int sphVSprintf ( char * pOutput, const char * sFmt, va_list ap )
{
	enum eStates { SNORMAL, SPERCENT, SHAVEFILL, SINWIDTH, SINPREC };
	eStates state = SNORMAL;
	size_t iPrec = 0;
	size_t iWidth = 0;
	char cFill = ' ';
	const char * pBegin = pOutput;
	bool bHeadingSpace = true;

	char c;
	while ( ( c = *sFmt++ )!=0 )
	{
		// handle percent
		if ( c=='%' )
		{
			if ( state==SNORMAL )
			{
				state = SPERCENT;
				iPrec = 0;
				iWidth = 0;
				cFill = ' ';
			} else
			{
				state = SNORMAL;
				*pOutput++ = c;
			}
			continue;
		}

		// handle regular chars
		if ( state==SNORMAL )
		{
			*pOutput++ = c;
			continue;
		}

		// handle modifiers
		switch ( c )
		{
		case '0':
			if ( state==SPERCENT )
			{
				cFill = '0';
				state = SHAVEFILL;
				break;
			}
		case '1': case '2': case '3':
		case '4': case '5': case '6':
		case '7': case '8': case '9':
			if ( state==SPERCENT || state==SHAVEFILL )
			{
				state = SINWIDTH;
				iWidth = c - '0';
			} else if ( state==SINWIDTH )
				iWidth = iWidth * 10 + c - '0';
			else if ( state==SINPREC )
				iPrec = iPrec * 10 + c - '0';
			break;

		case '-':
			if ( state==SPERCENT )
				bHeadingSpace = false;
			else
				state = SNORMAL; // FIXME? means that bad/unhandled syntax with dash will be just ignored
			break;

		case '.':
			state = SINPREC;
			iPrec = 0;
			break;

		case 's': // string
			{
				const char * pValue = va_arg ( ap, const char * );
				if ( !pValue )
					pValue = "(null)";
				size_t iValue = strlen ( pValue );

				if ( iWidth && bHeadingSpace )
					while ( iValue < iWidth-- )
						*pOutput++ = ' ';

				if ( iPrec && iPrec < iValue )
					while ( iPrec-- )
						*pOutput++ = *pValue++;
				else
					while ( *pValue )
						*pOutput++ = *pValue++;

				if ( iWidth && !bHeadingSpace )
					while ( iValue < iWidth-- )
						*pOutput++ = ' ';

				state = SNORMAL;
				break;
			}

		case 'p': // pointer
			{
				void * pValue = va_arg ( ap, void * );
				uint64_t uValue = uint64_t ( pValue );
				UItoA ( &pOutput, uValue, 16, iWidth, iPrec, cFill );
				state = SNORMAL;
				break;
			}

		case 'x': // hex integer
		case 'd': // decimal integer
			{
				DWORD uValue = va_arg ( ap, DWORD );
				UItoA ( &pOutput, uValue, ( c=='x' ) ? 16 : 10, iWidth, iPrec, cFill );
				state = SNORMAL;
				break;
			}

		case 'l': // decimal int64
			{
				int64_t iValue = va_arg ( ap, int64_t );
				UItoA ( &pOutput, iValue, 10, iWidth, iPrec, cFill );
				state = SNORMAL;
				break;
			}

		default:
			state = SNORMAL;
			*pOutput++ = c;
		}
	}

	// final zero to EOL
	*pOutput++ = '\n';
	return int ( pOutput - pBegin );
}


bool sphWrite ( int iFD, const void * pBuf, size_t iSize )
{
	return ( iSize==(size_t)::write ( iFD, pBuf, iSize ) );
}


static char g_sSafeInfoBuf [ 1024 ];

void sphSafeInfo ( int iFD, const char * sFmt, ... )
{
	if ( iFD<0 || !sFmt )
		return;

	va_list ap;
	va_start ( ap, sFmt );
	int iLen = sphVSprintf ( g_sSafeInfoBuf, sFmt, ap ); // FIXME! make this vsnprintf
	va_end ( ap );
	sphWrite ( iFD, g_sSafeInfoBuf, size_t (iLen) );
}


int sphSafeInfo ( char * pBuf, const char * sFmt, ... )
{
	va_list ap;
	va_start ( ap, sFmt );
	int iLen = sphVSprintf ( pBuf, sFmt, ap ); // FIXME! make this vsnprintf
	va_end ( ap );
	return iLen;
}


#if !USE_WINDOWS

#define SPH_BACKTRACE_ADDR_COUNT 128
#define SPH_BT_BINARY_NAME 2
#define SPH_BT_ADDRS 3
static void * g_pBacktraceAddresses [SPH_BACKTRACE_ADDR_COUNT];
static char g_pBacktrace[4096];
static const char g_sSourceTail[] = "> source.txt\n";
static const char * g_pArgv[128] = { "addr2line", "-e", "./searchd", "0x0", NULL };
static CSphString g_sBinaryName;

#if HAVE_BACKTRACE & HAVE_BACKTRACE_SYMBOLS
const char * DoBacktrace ( int iDepth, int iSkip )
{
	if ( !iDepth || iDepth > SPH_BACKTRACE_ADDR_COUNT )
		iDepth = SPH_BACKTRACE_ADDR_COUNT;
	iDepth = backtrace ( g_pBacktraceAddresses, iDepth );
	char ** ppStrings = backtrace_symbols ( g_pBacktraceAddresses, iDepth );
	if ( !ppStrings )
		return NULL;
	char * pDst = g_pBacktrace;
	for ( int i=iSkip; i<iDepth; ++i )
	{
		const char * pStr = ppStrings[i];
		do
			*pDst++ = *pStr++;
		while (*pStr);
		*pDst++='\n';
	}
	*pDst = '\0';
	free ( ppStrings );
	return g_pBacktrace; ///< sorry, no backtraces on Windows...
}
#else
const char * DoBacktrace ( int, int )
{
	return nullptr; ///< sorry, no backtraces...
}
#endif

void sphBacktrace ( int iFD, bool bSafe )
{
	if ( iFD<0 )
		return;

	sphSafeInfo ( iFD, "-------------- backtrace begins here ---------------" );
#ifdef COMPILER
	sphSafeInfo ( iFD, "Program compiled with " COMPILER );
#endif

#ifdef CONFIGURE_FLAGS
	sphSafeInfo ( iFD, "Configured with flags: " CONFIGURE_FLAGS );
#endif

#ifdef OS_UNAME
	sphSafeInfo ( iFD, "Host OS is " OS_UNAME );
#endif

	bool bOk = true;

	void * pMyStack = NULL;
	int iStackSize = 0;
	if ( !bSafe )
	{
		pMyStack = sphMyStack();
		iStackSize = g_iThreadStackSize;
	}
	sphSafeInfo ( iFD, "Stack bottom = 0x%p, thread stack size = 0x%x", pMyStack, iStackSize );

	while ( pMyStack && !bSafe )
	{
		sphSafeInfo ( iFD, "Trying manual backtrace:" );
		BYTE ** pFramePointer = NULL;

		int iFrameCount = 0;
		int iReturnFrameCount = sphIsLtLib() ? 2 : 1;

#ifdef __i386__
#define SIGRETURN_FRAME_OFFSET 17
		__asm __volatile__ ( "movl %%ebp,%0":"=r"(pFramePointer):"r"(pFramePointer) );
#endif

#ifdef __x86_64__
#define SIGRETURN_FRAME_OFFSET 23
		__asm __volatile__ ( "movq %%rbp,%0":"=r"(pFramePointer):"r"(pFramePointer) );
#endif

#ifndef SIGRETURN_FRAME_OFFSET
#define SIGRETURN_FRAME_OFFSET 0
#endif

		if ( !pFramePointer )
		{
			sphSafeInfo ( iFD, "Frame pointer is null, manual backtrace failed (did you build with -fomit-frame-pointer?)" );
			break;
		}

		if ( !pMyStack || (BYTE*) pMyStack > (BYTE*) &pFramePointer )
		{
			int iRound = Min ( 65536, iStackSize );
			pMyStack = (void *) ( ( (size_t) &pFramePointer + iRound ) & ~(size_t)65535 );
			sphSafeInfo ( iFD, "Something wrong with thread stack, manual backtrace may be incorrect (fp=0x%p)", pFramePointer );

			if ( pFramePointer > (BYTE**) pMyStack || pFramePointer < (BYTE**) pMyStack - iStackSize )
			{
				sphSafeInfo ( iFD, "Wrong stack limit or frame pointer, manual backtrace failed (fp=0x%p, stack=0x%p, stacksize=0x%x)",
					pFramePointer, pMyStack, iStackSize );
				break;
			}
		}

		sphSafeInfo ( iFD, "Stack looks OK, attempting backtrace." );

		BYTE** pNewFP = NULL;
		while ( pFramePointer < (BYTE**) pMyStack )
		{
			pNewFP = (BYTE**) *pFramePointer;
			sphSafeInfo ( iFD, "0x%p", iFrameCount==iReturnFrameCount ? *(pFramePointer + SIGRETURN_FRAME_OFFSET) : *(pFramePointer + 1) );

			bOk = pNewFP > pFramePointer;
			if ( !bOk ) break;

			pFramePointer = pNewFP;
			iFrameCount++;
		}

		if ( !bOk )
			sphSafeInfo ( iFD, "Something wrong in frame pointers, manual backtrace failed (fp=%p)", pNewFP );

		break;
	}

	int iDepth = 0;
#if HAVE_BACKTRACE
	sphSafeInfo ( iFD, "Trying system backtrace:" );
	iDepth = backtrace ( g_pBacktraceAddresses, SPH_BACKTRACE_ADDR_COUNT );
	if ( iDepth>0 )
		bOk = true;
#if HAVE_BACKTRACE_SYMBOLS
	sphSafeInfo ( iFD, "begin of system symbols:" );
	backtrace_symbols_fd ( g_pBacktraceAddresses, iDepth, iFD );
#elif !HAVE_BACKTRACE_SYMBOLS
	sphSafeInfo ( iFD, "begin of manual symbols:" );
	for ( int i=0; i<iDepth; i++ )
		sphSafeInfo ( iFD, "%p", g_pBacktraceAddresses[i] );
#endif // HAVE_BACKTRACE_SYMBOLS
#endif // !HAVE_BACKTRACE

	sphSafeInfo ( iFD, "-------------- backtrace ends here ---------------" );

	if ( bOk )
		sphSafeInfo ( iFD, "Please, create a bug report in our bug tracker (http://sphinxsearch.com/bugs) and attach there:\n"
							"a) searchd log, b) searchd binary, c) searchd symbols.\n"
							"Look into the chapter 'Reporting bugs' in the documentation\n"
							"(/usr/share/doc/sphinx/sphinx.txt or http://sphinxsearch.com/docs/current.html#reporting-bugs)" );

	// convert all BT addresses to source code lines
	int iCount = Min ( iDepth, (int)( sizeof(g_pArgv)/sizeof(g_pArgv[0]) - SPH_BT_ADDRS - 1 ) );
	sphSafeInfo ( iFD, "--- BT to source lines (depth %d): ---", iCount );
	char * pCur = g_pBacktrace;
	for ( int i=0; i<iCount; i++ )
	{
		// early our on strings buffer overrun
		if ( pCur>=g_pBacktrace+sizeof(g_pBacktrace)-48 )
		{
			iCount = i;
			break;
		}
		g_pArgv[i+SPH_BT_ADDRS] = pCur;
		pCur += sphSafeInfo ( pCur, "0x%x", g_pBacktraceAddresses[i] );
		*(pCur-1) = '\0'; // make null terminated string from EOL string
	}
	g_pArgv[iCount+SPH_BT_ADDRS] = NULL;

	int iChild = fork();

	if ( iChild==0 )
	{
		// map stdout to log file
		if ( iFD!=1 )
		{
			close ( 1 );
			dup2 ( iFD, 1 );
		}

		execvp ( g_pArgv[0], const_cast<char **> ( g_pArgv ) ); // using execvp instead execv to auto find addr2line in directories

		// if we here - execvp failed, ask user to do conversion manually
		sphSafeInfo ( iFD, "conversion failed (error '%s'):\n"
			"  1. Run the command provided below over the crashed binary (for example, '%s'):\n"
			"  2. Attach the source.txt to the bug report.", strerror ( errno ), g_pArgv[SPH_BT_BINARY_NAME] );

		int iColumn = 0;
		for ( int i=0; g_pArgv[i]!=NULL; i++ )
		{
			const char * s = g_pArgv[i];
			while ( *s )
				s++;
			size_t iLen = s-g_pArgv[i];
			sphWrite ( iFD, g_pArgv[i], iLen );
			sphWrite ( iFD, " ", 1 );
			int iWas = iColumn % 80;
			iColumn += iLen;
			int iNow = iColumn % 80;
			if ( iNow<iWas )
				sphWrite ( iFD, "\n", 1 );
		}
		sphWrite ( iFD, g_sSourceTail, sizeof(g_sSourceTail)-1 );
		exit ( 1 );

	} else
	if ( iChild==-1 )
	{
		sphSafeInfo ( iFD, "fork for running execvp failed: [%d] %s", errno, strerror(errno) );
		return;
	}

	int iStatus, iResult;
	do
	{
		// can be interrupted by pretty much anything (e.g. SIGCHLD from other searchd children)
		iResult = waitpid ( iChild, &iStatus, 0 );

		// they say this can happen if child exited and SIGCHLD was ignored
		// a cleaner one would be to temporary handle it here, but can we be bothered
		if ( iResult==-1 && errno==ECHILD )
		{
			iResult = iChild;
			iStatus = 0;
		}

		if ( iResult==-1 && errno!=EINTR )
		{
			sphSafeInfo ( iFD, "waitpid() failed: [%d] %s", errno, strerror(errno) );
			return;
		}
	} while ( iResult!=iChild );

	sphSafeInfo ( iFD, "--- BT to source lines finished ---" );
}

void sphBacktraceSetBinaryName ( const char * sName )
{
	g_sBinaryName = sName;
	g_pArgv[SPH_BT_BINARY_NAME] = g_sBinaryName.cstr();
}

#else // USE_WINDOWS

const char * DoBacktrace ( int, int )
{
	return NULL; ///< sorry, no backtraces on Windows...
}

void sphBacktrace ( EXCEPTION_POINTERS * pExc, const char * sFile )
{
	if ( !pExc || !sFile || !(*sFile) )
	{
		sphInfo ( "can't generate minidump" );
		return;
	}

	HANDLE hFile = CreateFile ( sFile, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0 );
	if ( hFile==INVALID_HANDLE_VALUE )
	{
		sphInfo ( "can't create minidump file '%s'", sFile );
		return;
	}

	MINIDUMP_EXCEPTION_INFORMATION tExcInfo;
	tExcInfo.ExceptionPointers = pExc;
	tExcInfo.ClientPointers = FALSE;
	tExcInfo.ThreadId = GetCurrentThreadId();

	bool bDumped = ( MiniDumpWriteDump ( GetCurrentProcess(), GetCurrentProcessId(), hFile, MiniDumpNormal, &tExcInfo, 0, 0 )==TRUE );
	CloseHandle ( hFile );

	if ( !bDumped )
		sphInfo ( "can't dump minidump" );
}

void sphBacktraceSetBinaryName ( const char * )
{
}

#endif // USE_WINDOWS


static bool g_bUnlinkOld = true;
void sphSetUnlinkOld ( bool bUnlink )
{
	g_bUnlinkOld = bUnlink;
}


void sphUnlinkIndex ( const char * sName, bool bForce )
{
	if ( !( g_bUnlinkOld || bForce ) )
		return;

	char sFileName[SPH_MAX_FILENAME_LEN];

	// +1 is for .mvp
	for ( int i=0; i<sphGetExtCount()+1; i++ )
	{
		snprintf ( sFileName, sizeof(sFileName), "%s%s", sName, sphGetExts ( SPH_EXT_TYPE_CUR )[i] );
		// 'mvp' is optional file
		if ( ::unlink ( sFileName ) && errno!=ENOENT )
			sphWarning ( "unlink failed (file '%s', error '%s'", sFileName, strerror(errno) );
	}
}


void sphCheckDuplicatePaths ( const CSphConfig & hConf )
{
	CSphOrderedHash < CSphString, CSphString, CSphStrHashFunc, 256 > hPaths;
	hConf["index"].IterateStart ();
	while ( hConf["index"].IterateNext() )
	{
		CSphConfigSection & hIndex = hConf["index"].IterateGet ();
		if ( hIndex ( "path" ) )
		{
			const CSphString & sIndex = hConf["index"].IterateGetKey ();
			if ( hPaths ( hIndex["path"].strval() ) )
				sphDie ( "duplicate paths: index '%s' has the same path as '%s'.\n", sIndex.cstr(), hPaths[hIndex["path"].strval()].cstr() );
			hPaths.Add ( sIndex, hIndex["path"].strval() );
		}
	}
}


void sphConfigureCommon ( const CSphConfig & hConf )
{
	if ( !hConf("common") || !hConf["common"]("common") )
		return;

	CSphConfigSection & hCommon = hConf["common"]["common"];
	g_sLemmatizerBase = hCommon.GetStr ( "lemmatizer_base" );
	sphConfigureRLP ( hCommon );

	g_bProgressiveMerge = ( hCommon.GetInt ( "progressive_merge", 1 )!=0 );

	bool bJsonStrict = false;
	bool bJsonAutoconvNumbers;
	bool bJsonKeynamesToLowercase = false;

	if ( hCommon("on_json_attr_error") )
	{
		const CSphString & sVal = hCommon["on_json_attr_error"].strval();
		if ( sVal=="ignore_attr" )
			bJsonStrict = false;
		else if ( sVal=="fail_index" )
			bJsonStrict = true;
		else
			sphDie ( "unknown on_json_attr_error value (must be one of ignore_attr, fail_index)" );
	}

	if ( hCommon("json_autoconv_keynames") )
	{
		const CSphString & sVal = hCommon["json_autoconv_keynames"].strval();
		if ( sVal=="lowercase" )
			bJsonKeynamesToLowercase = true;
		else
			sphDie ( "unknown json_autoconv_keynames value (must be 'lowercase')" );
	}

	bJsonAutoconvNumbers = ( hCommon.GetInt ( "json_autoconv_numbers", 0 )!=0 );
	sphSetJsonOptions ( bJsonStrict, bJsonAutoconvNumbers, bJsonKeynamesToLowercase );

	if ( hCommon("plugin_dir") )
		sphPluginInit ( hCommon["plugin_dir"].cstr() );
}

bool sphIsChineseCode ( int iCode )
{
	return ( ( iCode>=0x2E80 && iCode<=0x2EF3 ) ||	// CJK radicals
		( iCode>=0x2F00 && iCode<=0x2FD5 ) ||	// Kangxi radicals
		( iCode>=0x3000 && iCode<=0x303F ) ||	// CJK Symbols and Punctuation
		( iCode>=0x3105 && iCode<=0x312D ) ||	// Bopomofo
		( iCode>=0x31C0 && iCode<=0x31E3 ) ||	// CJK strokes
		( iCode>=0x3400 && iCode<=0x4DB5 ) ||	// CJK Ideograph Extension A
		( iCode>=0x4E00 && iCode<=0x9FFF ) ||	// Ideograph
		( iCode>=0xF900 && iCode<=0xFAD9 ) ||	// compatibility ideographs
		( iCode>=0xFF00 && iCode<=0xFFEF ) ||	// Halfwidth and fullwidth forms
		( iCode>=0x20000 && iCode<=0x2FA1D ) );	// CJK Ideograph Extensions B/C/D, and compatibility ideographs
}

bool sphDetectChinese ( const BYTE * szBuffer, int iLength )
{
	if ( !szBuffer || !iLength )
		return false;

	const BYTE * pBuffer = szBuffer;
	while ( pBuffer<szBuffer+iLength )
	{
		int iCode = sphUTF8Decode ( pBuffer );
		if ( sphIsChineseCode ( iCode ) )
			return true;
	}

	return false;
}


static const char * g_dRankerNames[] =
{
	"proximity_bm25",
	"bm25",
	"none",
	"wordcount",
	"proximity",
	"matchany",
	"fieldmask",
	"sph04",
	"expr",
	"export",
	NULL
};


const char * sphGetRankerName ( ESphRankMode eRanker )
{
	if ( eRanker<SPH_RANK_PROXIMITY_BM25 || eRanker>=SPH_RANK_TOTAL )
		return NULL;

	return g_dRankerNames[eRanker];
}

#if HAVE_DLOPEN

CSphDynamicLibrary::CSphDynamicLibrary ( const char * sPath )
	: m_bReady ( false )
	, m_pLibrary ( nullptr )
{
	m_pLibrary = dlopen ( sPath, RTLD_NOW | RTLD_GLOBAL );
	if ( !m_pLibrary )
		sphLogDebug ( "dlopen(%s) failed", sPath );
	else
		sphLogDebug ( "dlopen(%s)=%p", sPath, m_pLibrary );
}

bool CSphDynamicLibrary::LoadSymbols ( const char** sNames, void*** pppFuncs, int iNum )
{
	if ( !m_pLibrary )
		return false;

	if ( m_bReady )
		return true;

	for ( int i=0; i<iNum; ++i )
	{
		void* pResult = dlsym ( m_pLibrary, sNames[i] );
		if ( !pResult )
		{
			sphLogDebug ( "Symbol %s not found", sNames[i] );
			return false;
		}
		// yes, it is void*** - triple pointer.
		// void* is the legacy pointer (to the function, in this case).
		// void** is the variable where we store the pointer to the function.
		// that is to cast all different pointers to legacy void*.
		// we put the addresses to these variables into array, and it adds
		// one more level of indirection. void*** actually is void**[]
		*pppFuncs[i] = pResult;
	}
	m_bReady = true;
	return true;
};

#else

CSphDynamicLibrary::CSphDynamicLibrary ( const char * ) {};
bool CSphDynamicLibrary::LoadSymbols ( const char **, void ***, int ) { return false; }

#endif


void RebalanceWeights ( const CSphFixedVector<int64_t> & dTimers, WORD * pWeights )
{
	assert ( dTimers.GetLength () );
	float fSum = 0.0;
	int iCounters = 0;

	// weights are proportional to frequencies (inverse to timers)
	CSphFixedVector<float> dFrequencies ( dTimers.GetLength() );
	ARRAY_FOREACH ( i, dTimers )
	if ( dTimers[i]>0 )
	{
		dFrequencies[i] = (float)1000/dTimers[i];
		fSum += dFrequencies[i];
		++iCounters;
	}

	// no statistics, all timers bad, keep previous weights
	if ( fSum<=0 )
		return;

	// in case of mirror without response still set small probability to it
	const float fEmptiesPercent = 0.1f;
	int iEmpties = dTimers.GetLength() - iCounters;

	// balance weights
	int64_t iCheck = 0;
	ARRAY_FOREACH ( i, dFrequencies )
	{
		// mirror weight is inverse of timer \ query time
		float fWeight = dFrequencies[i] / fSum;

		// subtract coef-empty percent to get sum eq to 1.0
		if ( iEmpties )
			fWeight = fWeight - fWeight * fEmptiesPercent;

		// mirror without response
		if ( !dTimers[i] )
			fWeight = fEmptiesPercent / iEmpties;
		else if ( iCounters==1 ) // case when only one mirror has valid counter
			fWeight = 1.0f - fEmptiesPercent;

		int iWeight = int ( fWeight * 65535.0f );
		assert ( iWeight>=0 && iWeight<=65535 );
		pWeights[i] = (WORD)iWeight;
		iCheck += pWeights[i];
	}
	assert ( iCheck<=65535 );
}


}

//
// $Id$
//
