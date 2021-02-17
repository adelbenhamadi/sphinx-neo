
#include "neo/int/scoped_pointer.h"
#include "neo/int/throttle_state.h"
#include "neo/tokenizer/enums.h"
#include "neo/tokenizer/tokenizer_settings.h"
#include "neo/tokenizer/tokenizer.h"
#include "neo/tokenizer/form.h"
#include "neo/io/io_stats.h"
#include "neo/io/fnv64.h"
#include "neo/io/file.h"
#include "neo/io/autofile.h"
#include "neo/utility/log.h"
#include "neo/tools/charset.h"
#include "neo/tools/utf8_tools.h"

#include "neo/sphinx/xutility.h"
#include "neo/sphinx/xplugin.h"


namespace NEO {

	const char* SPHINX_DEFAULT_UTF8_TABLE = "0..9, A..Z->a..z, _, a..z, U+410..U+42F->U+430..U+44F, U+430..U+44F, U+401->U+451, U+451";

void FillStoredTokenInfo ( StoredToken_t & tToken, const BYTE * sToken, ISphTokenizer * pTokenizer )
{
	assert ( sToken && pTokenizer );
	strncpy ( (char *)tToken.m_sToken, (const char *)sToken, sizeof(tToken.m_sToken) );
	tToken.m_szTokenStart = pTokenizer->GetTokenStart ();
	tToken.m_szTokenEnd = pTokenizer->GetTokenEnd ();
	tToken.m_iOvershortCount = pTokenizer->GetOvershortCount ();
	tToken.m_iTokenLen = pTokenizer->GetLastTokenLen ();
	tToken.m_pBufferPtr = pTokenizer->GetBufferPtr ();
	tToken.m_pBufferEnd = pTokenizer->GetBufferEnd();
	tToken.m_bBoundary = pTokenizer->GetBoundary ();
	tToken.m_bSpecial = pTokenizer->WasTokenSpecial ();
	tToken.m_bBlended = pTokenizer->TokenIsBlended();
	tToken.m_bBlendedPart = pTokenizer->TokenIsBlendedPart();
}


CSphTokenizerSettings::CSphTokenizerSettings ()
	: m_iType				( TOKENIZER_UTF8 )
	, m_iMinWordLen			( 1 )
	, m_iNgramLen			( 0 )
{
}


ISphTokenizer::ISphTokenizer ()
	: m_iLastTokenLen ( 0 )
	, m_bTokenBoundary ( false )
	, m_bBoundary ( false )
	, m_bWasSpecial ( false )
	, m_bWasSynonym ( false )
	, m_bEscaped ( false )
	, m_iOvershortCount ( 0 )
	, m_eTokenMorph ( SPH_TOKEN_MORPH_RAW )
	, m_bBlended ( false )
	, m_bNonBlended ( true )
	, m_bBlendedPart ( false )
	, m_bBlendAdd ( false )
	, m_uBlendVariants ( BLEND_TRIM_NONE )
	, m_uBlendVariantsPending ( 0 )
	, m_bBlendSkipPure ( false )
	, m_bShortTokenFilter ( false )
	, m_bDetectSentences ( false )
	, m_bPhrase ( false )
{}


bool ISphTokenizer::SetCaseFolding ( const char * sConfig, CSphString & sError )
{
	CSphVector<CSphRemapRange> dRemaps;
	CSphCharsetDefinitionParser tParser;
	if ( !tParser.Parse ( sConfig, dRemaps ) )
	{
		sError = tParser.GetLastError();
		return false;
	}

	const int MIN_CODE = 0x21;
	ARRAY_FOREACH ( i, dRemaps )
	{
		CSphRemapRange & tMap = dRemaps[i];

		if ( tMap.m_iStart<MIN_CODE || tMap.m_iStart>=m_tLC.MAX_CODE )
		{
			sphWarning ( "wrong character mapping start specified: U+%x, should be between U+%x and U+%x (inclusive); CLAMPED",
				tMap.m_iStart, MIN_CODE, m_tLC.MAX_CODE-1 );
			tMap.m_iStart = Min ( Max ( tMap.m_iStart, MIN_CODE ), m_tLC.MAX_CODE-1 );
		}

		if ( tMap.m_iEnd<MIN_CODE || tMap.m_iEnd>=m_tLC.MAX_CODE )
		{
			sphWarning ( "wrong character mapping end specified: U+%x, should be between U+%x and U+%x (inclusive); CLAMPED",
				tMap.m_iEnd, MIN_CODE, m_tLC.MAX_CODE-1 );
			tMap.m_iEnd = Min ( Max ( tMap.m_iEnd, MIN_CODE ), m_tLC.MAX_CODE-1 );
		}

		if ( tMap.m_iRemapStart<MIN_CODE || tMap.m_iRemapStart>=m_tLC.MAX_CODE )
		{
			sphWarning ( "wrong character remapping start specified: U+%x, should be between U+%x and U+%x (inclusive); CLAMPED",
				tMap.m_iRemapStart, MIN_CODE, m_tLC.MAX_CODE-1 );
			tMap.m_iRemapStart = Min ( Max ( tMap.m_iRemapStart, MIN_CODE ), m_tLC.MAX_CODE-1 );
		}

		auto iRemapEnd = tMap.m_iRemapStart+tMap.m_iEnd-tMap.m_iStart;
		if ( iRemapEnd<MIN_CODE || iRemapEnd>=m_tLC.MAX_CODE )
		{
			sphWarning ( "wrong character remapping end specified: U+%x, should be between U+%x and U+%x (inclusive); IGNORED",
				iRemapEnd, MIN_CODE, m_tLC.MAX_CODE-1 );
			dRemaps.Remove(i);
			i--;
		}
	}

	m_tLC.Reset ();
	m_tLC.AddRemaps ( dRemaps, 0 );
	return true;
}


void ISphTokenizer::AddPlainChar ( char c )
{
	CSphVector<CSphRemapRange> dTmp ( 1 );
	dTmp[0].m_iStart = dTmp[0].m_iEnd = dTmp[0].m_iRemapStart = c;
	m_tLC.AddRemaps ( dTmp, 0 );
}


void ISphTokenizer::AddSpecials ( const char * sSpecials )
{
	m_tLC.AddSpecials ( sSpecials );
}


void ISphTokenizer::Setup ( const CSphTokenizerSettings & tSettings )
{
	m_tSettings = tSettings;
}


ISphTokenizer * ISphTokenizer::Create ( const CSphTokenizerSettings & tSettings, const CSphEmbeddedFiles * pFiles, CSphString & sError )
{
	CSphScopedPtr<ISphTokenizer> pTokenizer ( NULL );

	switch ( tSettings.m_iType )
	{
		case TOKENIZER_UTF8:	pTokenizer = sphCreateUTF8Tokenizer (); break;
		case TOKENIZER_NGRAM:	pTokenizer = sphCreateUTF8NgramTokenizer (); break;
		default:
			sError.SetSprintf ( "failed to create tokenizer (unknown charset type '%d')", tSettings.m_iType );
			return NULL;
	}

	pTokenizer->Setup ( tSettings );

	if ( !tSettings.m_sCaseFolding.IsEmpty () && !pTokenizer->SetCaseFolding ( tSettings.m_sCaseFolding.cstr (), sError ) )
	{
		sError.SetSprintf ( "'charset_table': %s", sError.cstr() );
		return NULL;
	}

	if ( !tSettings.m_sSynonymsFile.IsEmpty () && !pTokenizer->LoadSynonyms ( tSettings.m_sSynonymsFile.cstr (),
		pFiles && pFiles->m_bEmbeddedSynonyms ? pFiles : NULL, sError ) )
	{
		sError.SetSprintf ( "'synonyms': %s", sError.cstr() );
		return NULL;
	}

	if ( !tSettings.m_sBoundary.IsEmpty () && !pTokenizer->SetBoundary ( tSettings.m_sBoundary.cstr (), sError ) )
	{
		sError.SetSprintf ( "'phrase_boundary': %s", sError.cstr() );
		return NULL;
	}

	if ( !tSettings.m_sIgnoreChars.IsEmpty () && !pTokenizer->SetIgnoreChars ( tSettings.m_sIgnoreChars.cstr (), sError ) )
	{
		sError.SetSprintf ( "'ignore_chars': %s", sError.cstr() );
		return NULL;
	}

	if ( !tSettings.m_sBlendChars.IsEmpty () && !pTokenizer->SetBlendChars ( tSettings.m_sBlendChars.cstr (), sError ) )
	{
		sError.SetSprintf ( "'blend_chars': %s", sError.cstr() );
		return NULL;
	}

	if ( !pTokenizer->SetBlendMode ( tSettings.m_sBlendMode.cstr (), sError ) )
	{
		sError.SetSprintf ( "'blend_mode': %s", sError.cstr() );
		return NULL;
	}

	pTokenizer->SetNgramLen ( tSettings.m_iNgramLen );

	if ( !tSettings.m_sNgramChars.IsEmpty () && !pTokenizer->SetNgramChars ( tSettings.m_sNgramChars.cstr (), sError ) )
	{
		sError.SetSprintf ( "'ngram_chars': %s", sError.cstr() );
		return NULL;
	}

	return pTokenizer.LeakPtr ();
}


ISphTokenizer * ISphTokenizer::CreateMultiformFilter ( ISphTokenizer * pTokenizer, const CSphMultiformContainer * pContainer )
{
	if ( !pContainer )
		return pTokenizer;
	return new CSphMultiformTokenizer ( pTokenizer, pContainer );
}


ISphTokenizer * ISphTokenizer::CreateBigramFilter ( ISphTokenizer * pTokenizer, ESphBigram eBigramIndex, const CSphString & sBigramWords, CSphString & sError )
{
	assert ( pTokenizer );

	if ( eBigramIndex==SPH_BIGRAM_NONE )
		return pTokenizer;

	CSphVector<CSphString> dFreq;
	if ( eBigramIndex!=SPH_BIGRAM_ALL )
	{
		const BYTE * pTok = NULL;
		pTokenizer->SetBuffer ( (const BYTE*)sBigramWords.cstr(),(int) sBigramWords.Length() );
		while ( ( pTok = pTokenizer->GetToken() )!=NULL )
			dFreq.Add ( (const char*)pTok );

		if ( !dFreq.GetLength() )
		{
			SafeDelete ( pTokenizer );
			sError.SetSprintf ( "bigram_freq_words does not contain any valid words" );
			return NULL;
		}
	}

	return new CSphBigramTokenizer ( pTokenizer, eBigramIndex, dFreq );
}


class PluginFilterTokenizer_c : public CSphTokenFilter
{
protected:
	const PluginTokenFilter_c *	m_pFilter;		//plugin descriptor
	CSphString					m_sOptions;		//options string for the plugin init()
	void *						m_pUserdata;	//userdata returned from by the plugin init()
	bool						m_bGotExtra;	//are we looping through extra tokens?
	int							m_iPosDelta;	//position delta for the current token, see comments in GetToken()
	bool						m_bWasBlended;	//whether the last raw token was blended

public:
	PluginFilterTokenizer_c ( ISphTokenizer * pTok, const PluginTokenFilter_c * pFilter, const char * sOptions )
		: CSphTokenFilter ( pTok )
		, m_pFilter ( pFilter )
		, m_sOptions ( sOptions )
		, m_pUserdata ( NULL )
		, m_bGotExtra ( false )
		, m_iPosDelta ( 0 )
		, m_bWasBlended ( false )
	{
		assert ( m_pTokenizer );
		assert ( m_pFilter );
		m_pFilter->AddRef();
		// FIXME!!! handle error in constructor \ move to setup?
		CSphString sError;
		SetFilterSchema ( CSphSchema(), sError );
	}

	~PluginFilterTokenizer_c()
	{
		if ( m_pFilter->m_fnDeinit )
			m_pFilter->m_fnDeinit ( m_pUserdata );
		m_pFilter->Release();
	}

	ISphTokenizer * Clone ( ESphTokenizerClone eMode ) const
	{
		ISphTokenizer * pTok = m_pTokenizer->Clone ( eMode );
		return new PluginFilterTokenizer_c ( pTok, m_pFilter, m_sOptions.cstr() );
	}

	virtual bool SetFilterSchema ( const CSphSchema & s, CSphString & sError )
	{
		if ( m_pUserdata && m_pFilter->m_fnDeinit )
			m_pFilter->m_fnDeinit ( m_pUserdata );

		CSphVector<const char*> dFields;
		ARRAY_FOREACH ( i, s.m_dFields )
			dFields.Add ( s.m_dFields[i].m_sName.cstr() );

		char sErrBuf[SPH_UDF_ERROR_LEN+1];
		if ( m_pFilter->m_fnInit ( &m_pUserdata,(int) dFields.GetLength(), dFields.Begin(), m_sOptions.cstr(), sErrBuf )==0 )
			return true;
		sError = sErrBuf;
		return false;
	}

	virtual bool SetFilterOptions ( const char * sOptions, CSphString & sError )
	{
		char sErrBuf[SPH_UDF_ERROR_LEN+1];
		if ( m_pFilter->m_fnBeginDocument ( m_pUserdata, sOptions, sErrBuf )==0 )
			return true;
		sError = sErrBuf;
		return false;
	}

	virtual void BeginField ( int iField )
	{
		if ( m_pFilter->m_fnBeginField )
			m_pFilter->m_fnBeginField ( m_pUserdata, iField );
	}

	virtual BYTE * GetToken ()
	{
		// we have two principal states here
		// a) have pending extra tokens, keep looping and returning those
		// b) no extras, keep pushing until plugin returns anything
		//
		// we also have to handle position deltas, and that story is a little tricky
		// positions are not assigned in the tokenizer itself (we might wanna refactor that)
		// however, tokenizer has some (partial) control over the keyword positions, too
		// when it skips some too-short tokens, it returns a non-zero value via GetOvershortCount()
		// when it returns a blended token, it returns true via TokenIsBlended()
		// so while the default position delta is 1, overshorts can increase it by N,
		// and blended flag can decrease it by 1, and that's under tokenizer's control
		//
		// so for the plugins, we simplify (well i hope!) this complexity a little
		// we compute a proper position delta here, pass it, and let the plugin modify it
		// we report all tokens as regular, and return the delta via GetOvershortCount()

		// state (a), just loop the pending extras
		if ( m_bGotExtra )
		{
			m_iPosDelta = 1; // default delta is 1
			BYTE * pTok = (BYTE*) m_pFilter->m_fnGetExtraToken ( m_pUserdata, &m_iPosDelta );
			if ( pTok )
				return pTok;
			m_bGotExtra = false;
		}

		// state (b), push raw tokens, return results
		for ( ;; )
		{
			// get next raw token, handle field end
			BYTE * pRaw = m_pTokenizer->GetToken();
			if ( !pRaw )
			{
				// no more hits? notify plugin of a field end,
				// and check if there are pending tokens
				m_bGotExtra = 0;
				if ( m_pFilter->m_fnEndField )
					if ( !m_pFilter->m_fnEndField ( m_pUserdata ) )
						return NULL;

				// got them, start fetching
				m_bGotExtra = true;
				return (BYTE*)m_pFilter->m_fnGetExtraToken ( m_pUserdata, &m_iPosDelta );
			}

			// compute proper position delta
			m_iPosDelta = ( m_bWasBlended ? 0 : 1 ) + m_pTokenizer->GetOvershortCount();
			m_bWasBlended = m_pTokenizer->TokenIsBlended();

			// push raw token to plugin, return a processed one, if any
			int iExtra = 0;
			BYTE * pTok = (BYTE*)m_pFilter->m_fnPushToken ( m_pUserdata, (char*)pRaw, &iExtra, &m_iPosDelta );
			m_bGotExtra = ( iExtra!=0 );
			if ( pTok )
				return pTok;
		}
	}

	virtual int GetOvershortCount()
	{
		return m_iPosDelta-1;
	}

	virtual bool TokenIsBlended() const
	{
		return false;
	}
};


ISphTokenizer * ISphTokenizer::CreatePluginFilter ( ISphTokenizer * pTokenizer, const CSphString & sSpec, CSphString & sError )
{
	CSphVector<CSphString> dPlugin; // dll, filtername, options
	if ( !sphPluginParseSpec ( sSpec, dPlugin, sError ) )
		return NULL;

	if ( !dPlugin.GetLength() )
		return pTokenizer;

	const PluginDesc_c * p = sphPluginAcquire ( dPlugin[0].cstr(), PLUGIN_INDEX_TOKEN_FILTER, dPlugin[1].cstr(), sError );
	if ( !p )
	{
		sError.SetSprintf ( "INTERNAL ERROR: plugin %s:%s loaded ok but lookup fails", dPlugin[0].cstr(), dPlugin[1].cstr() );
		return NULL;
	}
	ISphTokenizer * pPluginTokenizer = new PluginFilterTokenizer_c ( pTokenizer, (const PluginTokenFilter_c *)p, dPlugin[2].cstr() );
	p->Release(); // plugin got owned by filter no need to leak counter
	return pPluginTokenizer;
}


bool ISphTokenizer::AddSpecialsSPZ ( const char * sSpecials, const char * sDirective, CSphString & sError )
{
	for ( int i=0; sSpecials[i]; i++ )
	{
		int iCode =(int) m_tLC.ToLower ( sSpecials[i] );
		if ( iCode & ( FLAG_CODEPOINT_NGRAM | FLAG_CODEPOINT_BOUNDARY | FLAG_CODEPOINT_IGNORE ) )
		{
			sError.SetSprintf ( "%s requires that character '%c' is not in ngram_chars, phrase_boundary, or ignore_chars",
				sDirective, sSpecials[i] );
			return false;
		}
	}

	AddSpecials ( sSpecials );
	return true;
}


bool ISphTokenizer::EnableSentenceIndexing ( CSphString & sError )
{
	const char sSpecials[] = { '.', '?', '!', MAGIC_CODE_PARAGRAPH, 0 };

	if ( !AddSpecialsSPZ ( sSpecials, "index_sp", sError ) )
		return false;

	m_bDetectSentences = true;
	return true;
}


bool ISphTokenizer::EnableZoneIndexing ( CSphString & sError )
{
	static const char sSpecials[] = { MAGIC_CODE_ZONE, 0 };
	return AddSpecialsSPZ ( sSpecials, "index_zones", sError );
}

uint64_t ISphTokenizer::GetSettingsFNV () const
{
	uint64_t uHash = m_tLC.GetFNV();

	DWORD uFlags = 0;
	if ( m_bBlendSkipPure )
		uFlags |= 1<<1;
	if ( m_bShortTokenFilter )
		uFlags |= 1<<2;
	uHash = sphFNV64 ( &uFlags, sizeof(uFlags), uHash );
	uHash = sphFNV64 ( &m_uBlendVariants, sizeof(m_uBlendVariants), uHash );

	uHash = sphFNV64 ( &m_tSettings.m_iType, sizeof(m_tSettings.m_iType), uHash );
	uHash = sphFNV64 ( &m_tSettings.m_iMinWordLen, sizeof(m_tSettings.m_iMinWordLen), uHash );
	uHash = sphFNV64 ( &m_tSettings.m_iNgramLen, sizeof(m_tSettings.m_iNgramLen), uHash );

	return uHash;
}

//////////////////////////////////////////////////////////////////////////

CSphTokenizerBase::CSphTokenizerBase ()
	: m_pBuffer		( NULL )
	, m_pBufferMax	( NULL )
	, m_pCur		( NULL )
	, m_pTokenStart ( NULL )
	, m_pTokenEnd	( NULL )
	, m_iAccum		( 0 )
	, m_pExc		( NULL )
	, m_bHasBlend	( false )
	, m_pBlendStart		( NULL )
	, m_pBlendEnd		( NULL )
	, m_eMode ( SPH_CLONE_INDEX )
{
	m_pAccum = m_sAccum;
}


CSphTokenizerBase::~CSphTokenizerBase()
{
	SafeDelete ( m_pExc );
}


bool CSphTokenizerBase::SetCaseFolding ( const char * sConfig, CSphString & sError )
{
	assert ( m_eMode!=SPH_CLONE_QUERY_LIGHTWEIGHT );
	if ( m_pExc )
	{
		sError = "SetCaseFolding() must not be called after LoadSynonyms()";
		return false;
	}
	m_bHasBlend = false;
	return ISphTokenizer::SetCaseFolding ( sConfig, sError );
}


bool CSphTokenizerBase::SetBlendChars ( const char * sConfig, CSphString & sError )
{
	assert ( m_eMode!=SPH_CLONE_QUERY_LIGHTWEIGHT );
	m_bHasBlend = ISphTokenizer::SetBlendChars ( sConfig, sError );
	return m_bHasBlend;
}


bool CSphTokenizerBase::LoadSynonyms ( const char * sFilename, const CSphEmbeddedFiles * pFiles, CSphString & sError )
{
	assert ( m_eMode!=SPH_CLONE_QUERY_LIGHTWEIGHT );

	ExceptionsTrieGen_c g;
	if ( pFiles )
	{
		m_tSynFileInfo = pFiles->m_tSynonymFile;
		ARRAY_FOREACH ( i, pFiles->m_dSynonyms )
		{
			if ( !g.ParseLine ( (char*)pFiles->m_dSynonyms[i].cstr(), sError ) )
				sphWarning ( "%s line %d: %s", pFiles->m_tSynonymFile.m_sFilename.cstr(), i, sError.cstr() );
		}
	} else
	{
		if ( !sFilename || !*sFilename )
			return true;

		STATS::GetFileStats ( sFilename, m_tSynFileInfo, NULL );

		CSphAutoreader tReader;
		if ( !tReader.Open ( sFilename, sError ) )
			return false;

		char sBuffer[1024];
		int iLine = 0;
		while ( tReader.GetLine ( sBuffer, sizeof(sBuffer) )>=0 )
		{
			iLine++;
			if ( !g.ParseLine ( sBuffer, sError ) )
				sphWarning ( "%s line %d: %s", sFilename, iLine, sError.cstr() );
		}
	}

	m_pExc = g.Build();
	return true;
}


void CSphTokenizerBase::WriteSynonyms ( CSphWriter & tWriter )
{
	if ( m_pExc )
		m_pExc->Export ( tWriter );
	else
		tWriter.PutDword ( 0 );
}


void CSphTokenizerBase::CloneBase ( const CSphTokenizerBase * pFrom, ESphTokenizerClone eMode )
{
	m_eMode = eMode;
	m_pExc = NULL;
	if ( pFrom->m_pExc )
	{
		m_pExc = new ExceptionsTrie_c();
		*m_pExc = *pFrom->m_pExc;
	}
	m_tSettings = pFrom->m_tSettings;
	m_bHasBlend = pFrom->m_bHasBlend;
	m_uBlendVariants = pFrom->m_uBlendVariants;
	m_bBlendSkipPure = pFrom->m_bBlendSkipPure;
	m_bShortTokenFilter = ( eMode!=SPH_CLONE_INDEX );

	switch ( eMode )
	{
		case SPH_CLONE_INDEX:
			assert ( pFrom->m_eMode==SPH_CLONE_INDEX );
			m_tLC = pFrom->m_tLC;
			break;

		case SPH_CLONE_QUERY:
		{
			assert ( pFrom->m_eMode==SPH_CLONE_INDEX || pFrom->m_eMode==SPH_CLONE_QUERY );
			m_tLC = pFrom->m_tLC;

			CSphVector<CSphRemapRange> dRemaps;
			CSphRemapRange Range;
			Range.m_iStart = Range.m_iEnd = Range.m_iRemapStart = '\\';
			dRemaps.Add ( Range );
			m_tLC.AddRemaps ( dRemaps, FLAG_CODEPOINT_SPECIAL );

			m_uBlendVariants = BLEND_TRIM_NONE;
			break;
		}

		case SPH_CLONE_QUERY_LIGHTWEIGHT:
		{
			// FIXME? avoid double lightweight clones, too?
			assert ( pFrom->m_eMode!=SPH_CLONE_INDEX );
			assert ( pFrom->m_tLC.ToLower ( '\\' ) & FLAG_CODEPOINT_SPECIAL );

			// lightweight tokenizer clone
			// copy 3 KB of lowercaser chunk pointers, but do NOT copy the table data
			SafeDeleteArray ( m_tLC.m_pData );
			m_tLC.m_iChunks = 0;
			m_tLC.m_pData = NULL;
			for ( int i=0; i<CSphLowercaser::CHUNK_COUNT; i++ )
				m_tLC.m_pChunk[i] = pFrom->m_tLC.m_pChunk[i];
			break;
		}
	}
}

uint64_t CSphTokenizerBase::GetSettingsFNV () const
{
	uint64_t uHash = ISphTokenizer::GetSettingsFNV();

	DWORD uFlags = 0;
	if ( m_bHasBlend )
		uFlags |= 1<<0;
	uHash = sphFNV64 ( &uFlags, sizeof(uFlags), uHash );

	return uHash;
}


void CSphTokenizerBase::SetBufferPtr ( const char * sNewPtr )
{
	assert ( (BYTE*)sNewPtr>=m_pBuffer && (BYTE*)sNewPtr<=m_pBufferMax );
	m_pCur = Min ( m_pBufferMax, Max ( m_pBuffer, (const BYTE*)sNewPtr ) );
	m_iAccum = 0;
	m_pAccum = m_sAccum;
	m_pTokenStart = m_pTokenEnd = NULL;
	m_pBlendStart = m_pBlendEnd = NULL;
}


int CSphTokenizerBase2::SkipBlended()
{
	if ( !m_pBlendEnd )
		return 0;

	const BYTE * pMax = m_pBufferMax;
	m_pBufferMax = m_pBlendEnd;

	// loop until the blended token end
	int iBlended = 0; // how many blended subtokens we have seen so far
	int iAccum = 0; // how many non-blended chars in a row we have seen so far
	while ( m_pCur < m_pBufferMax )
	{
		int iCode = GetCodepoint();
		if ( iCode=='\\' )
			iCode = GetCodepoint(); // no boundary check, GetCP does it
		iCode =(int) m_tLC.ToLower ( iCode ); // no -1 check, ToLower does it
		if ( iCode<0 )
			iCode = 0;
		if ( iCode & FLAG_CODEPOINT_BLEND )
			iCode = 0;
		if ( iCode & MASK_CODEPOINT )
		{
			iAccum++;
			continue;
		}
		if ( iAccum>=m_tSettings.m_iMinWordLen )
			iBlended++;
		iAccum = 0;
	}
	if ( iAccum>=m_tSettings.m_iMinWordLen )
		iBlended++;

	m_pBufferMax = pMax;
	return iBlended;
}


/// adjusts blending magic when we're about to return a token (any token)
/// returns false if current token should be skipped, true otherwise
bool CSphTokenizerBase::BlendAdjust ( const BYTE * pCur )
{
	// check if all we got is a bunch of blended characters (pure-blended case)
	if ( m_bBlended && !m_bNonBlended )
	{
		// we either skip this token, or pretend it was normal
		// in both cases, clear the flag
		m_bBlended = false;

		// do we need to skip it?
		if ( m_bBlendSkipPure )
		{
			m_pBlendStart = NULL;
			return false;
		}
	}
	m_bNonBlended = false;

	// adjust buffer pointers
	if ( m_bBlended && m_pBlendStart )
	{
		// called once per blended token, on processing start
		// at this point, full blended token is in the accumulator
		// and we're about to return it
		m_pCur = m_pBlendStart;
		m_pBlendEnd = pCur;
		m_pBlendStart = NULL;
		m_bBlendedPart = true;
	} else if ( pCur>=m_pBlendEnd )
	{
		// tricky bit, as at this point, token we're about to return
		// can either be a blended subtoken, or the next one
		m_bBlendedPart = ( m_pTokenStart!=NULL ) && ( m_pTokenStart<m_pBlendEnd );
		m_pBlendEnd = NULL;
		m_pBlendStart = NULL;
	} else if ( !m_pBlendEnd )
	{
		// we aren't re-parsing blended; so clear the "blended subtoken" flag
		m_bBlendedPart = false;
	}
	return true;
}


static inline void CopySubstring ( BYTE * pDst, const BYTE * pSrc, int iLen )
{
	while ( iLen-->0 && *pSrc )
		*pDst++ = *pSrc++;
	*pDst++ = '\0';
}


BYTE * CSphTokenizerBase2::GetBlendedVariant ()
{
	// we can get called on several occasions
	// case 1, a new blended token was just accumulated
	if ( m_bBlended && !m_bBlendAdd )
	{
		// fast path for the default case (trim_none)
		if ( m_uBlendVariants==BLEND_TRIM_NONE )
			return m_sAccum;

		// analyze the full token, find non-blended bounds
		m_iBlendNormalStart = -1;
		m_iBlendNormalEnd = -1;

		// OPTIMIZE? we can skip this based on non-blended flag from adjust
		const BYTE * p = m_sAccum;
		while ( *p )
		{
			int iLast = (int)( p-m_sAccum );
			int iCode = sphUTF8Decode(p);
			if (!( m_tLC.ToLower ( iCode ) & FLAG_CODEPOINT_BLEND ))
			{
				m_iBlendNormalEnd = (int)( p-m_sAccum );
				if ( m_iBlendNormalStart<0 )
					m_iBlendNormalStart = iLast;
			}
		}

		// build todo mask
		// check and revert a few degenerate cases
		m_uBlendVariantsPending = m_uBlendVariants;
		if ( m_uBlendVariantsPending & BLEND_TRIM_BOTH )
		{
			if ( m_iBlendNormalStart<0 )
			{
				// no heading blended; revert BOTH to TAIL
				m_uBlendVariantsPending &= ~BLEND_TRIM_BOTH;
				m_uBlendVariantsPending |= BLEND_TRIM_TAIL;
			} else if ( m_iBlendNormalEnd<0 )
			{
				// no trailing blended; revert BOTH to HEAD
				m_uBlendVariantsPending &= ~BLEND_TRIM_BOTH;
				m_uBlendVariantsPending |= BLEND_TRIM_HEAD;
			}
		}
		if ( m_uBlendVariantsPending & BLEND_TRIM_HEAD )
		{
			// either no heading blended, or pure blended; revert HEAD to NONE
			if ( m_iBlendNormalStart<=0 )
			{
				m_uBlendVariantsPending &= ~BLEND_TRIM_HEAD;
				m_uBlendVariantsPending |= BLEND_TRIM_NONE;
			}
		}
		if ( m_uBlendVariantsPending & BLEND_TRIM_TAIL )
		{
			// either no trailing blended, or pure blended; revert TAIL to NONE
			if ( m_iBlendNormalEnd<=0 || m_sAccum[m_iBlendNormalEnd]==0 )
			{
				m_uBlendVariantsPending &= ~BLEND_TRIM_TAIL;
				m_uBlendVariantsPending |= BLEND_TRIM_NONE;
			}
		}

		// ok, we are going to return a few variants after all, flag that
		// OPTIMIZE? add fast path for "single" variants?
		m_bBlendAdd = true;
		assert ( m_uBlendVariantsPending );

		// we also have to stash the original blended token
		// because accumulator contents may get trashed by caller (say, when stemming)
		strncpy ( (char*)m_sAccumBlend, (char*)m_sAccum, sizeof(m_sAccumBlend) );
	}

	// case 2, caller is checking for pending variants, have we even got any?
	if ( !m_bBlendAdd )
		return NULL;

	// handle trim_none
	// this MUST be the first handler, so that we could avoid copying below, and just return the original accumulator
	if ( m_uBlendVariantsPending & BLEND_TRIM_NONE )
	{
		m_uBlendVariantsPending &= ~BLEND_TRIM_NONE;
		m_bBlended = true;
		return m_sAccum;
	}

	// handle trim_all
	if ( m_uBlendVariantsPending & BLEND_TRIM_ALL )
	{
		m_uBlendVariantsPending &= ~BLEND_TRIM_ALL;
		m_bBlended = true;
		const BYTE * pSrc = m_sAccumBlend;
		BYTE * pDst = m_sAccum;
		while ( *pSrc )
		{
			int iCode = sphUTF8Decode ( pSrc );
			if ( !( m_tLC.ToLower ( iCode ) & FLAG_CODEPOINT_BLEND ) )
				pDst += sphUTF8Encode ( pDst, ( iCode & MASK_CODEPOINT ) );
		}
		*pDst = '\0';

		return m_sAccum;
	}

	// handle trim_both
	if ( m_uBlendVariantsPending & BLEND_TRIM_BOTH )
	{
		m_uBlendVariantsPending &= ~BLEND_TRIM_BOTH;
		if ( m_iBlendNormalStart<0 )
			m_uBlendVariantsPending |= BLEND_TRIM_TAIL; // no heading blended; revert BOTH to TAIL
		else if ( m_iBlendNormalEnd<0 )
			m_uBlendVariantsPending |= BLEND_TRIM_HEAD; // no trailing blended; revert BOTH to HEAD
		else
		{
			assert ( m_iBlendNormalStart<m_iBlendNormalEnd );
			CopySubstring ( m_sAccum, m_sAccumBlend+m_iBlendNormalStart, m_iBlendNormalEnd-m_iBlendNormalStart );
			m_bBlended = true;
			return m_sAccum;
		}
	}

	// handle TRIM_HEAD
	if ( m_uBlendVariantsPending & BLEND_TRIM_HEAD )
	{
		m_uBlendVariantsPending &= ~BLEND_TRIM_HEAD;
		if ( m_iBlendNormalStart>=0 )
		{
			// FIXME! need we check for overshorts?
			CopySubstring ( m_sAccum, m_sAccumBlend+m_iBlendNormalStart, sizeof(m_sAccum) );
			m_bBlended = true;
			return m_sAccum;
		}
	}

	// handle TRIM_TAIL
	if ( m_uBlendVariantsPending & BLEND_TRIM_TAIL )
	{
		m_uBlendVariantsPending &= ~BLEND_TRIM_TAIL;
		if ( m_iBlendNormalEnd>0 )
		{
			// FIXME! need we check for overshorts?
			CopySubstring ( m_sAccum, m_sAccumBlend, m_iBlendNormalEnd );
			m_bBlended = true;
			return m_sAccum;
		}
	}

	// all clear, no more variants to go
	m_bBlendAdd = false;
	return NULL;
}


static inline bool IsCapital ( int iCh )
{
	return iCh>='A' && iCh<='Z';
}


static inline bool IsWhitespace ( BYTE c )
{
	return ( c=='\0' || c==' ' || c=='\t' || c=='\r' || c=='\n' );
}


static inline bool IsWhitespace ( int c )
{
	return ( c=='\0' || c==' ' || c=='\t' || c=='\r' || c=='\n' );
}


static inline bool IsBoundary ( BYTE c, bool bPhrase )
{
	// FIXME? sorta intersects with specials
	// then again, a shortened-down list (more strict syntax) is reasonble here too
	return IsWhitespace(c) || c=='"' || ( !bPhrase && ( c=='(' || c==')' || c=='|' ) );
}


static inline bool IsPunctuation ( int c )
{
	return ( c>=33 && c<=47 ) || ( c>=58 && c<=64 ) || ( c>=91 && c<=96 ) || ( c>=123 && c<=126 );
}


int CSphTokenizerBase::CodepointArbitrationI ( int iCode )
{
	if ( !m_bDetectSentences )
		return iCode;

	// detect sentence boundaries
	// FIXME! should use charset_table (or add a new directive) and support languages other than English
	int iSymbol = iCode & MASK_CODEPOINT;
	if ( iSymbol=='?' || iSymbol=='!' )
	{
		// definitely a sentence boundary
		return MAGIC_CODE_SENTENCE | FLAG_CODEPOINT_SPECIAL;
	}

	if ( iSymbol=='.' )
	{
		// inline dot ("in the U.K and"), not a boundary
		bool bInwordDot = ( sphIsAlpha ( m_pCur[0] ) || m_pCur[0]==',' );

		// followed by a small letter or an opening paren, not a boundary
		// FIXME? might want to scan for more than one space
		// Yoyodine Inc. exists ...
		// Yoyodine Inc. (the company) ..
		bool bInphraseDot = ( sphIsSpace ( m_pCur[0] )
			&& ( ( 'a'<=m_pCur[1] && m_pCur[1]<='z' )
				|| ( m_pCur[1]=='(' && 'a'<=m_pCur[2] && m_pCur[2]<='z' ) ) );

		// preceded by something that looks like a middle name, opening first name, salutation
		bool bMiddleName = false;
		switch ( m_iAccum )
		{
			case 1:
				// 1-char capital letter
				// example: J. R. R. Tolkien, who wrote Hobbit ...
				// example: John D. Doe ...
				bMiddleName = IsCapital ( m_pCur[-2] );
				break;
			case 2:
				// 2-char token starting with a capital
				if ( IsCapital ( m_pCur[-3] ) )
				{
					// capital+small
					// example: Known as Mr. Doe ...
					if ( !IsCapital ( m_pCur[-2] ) )
						bMiddleName = true;

					// known capital+capital (MR, DR, MS)
					if (
						( m_pCur[-3]=='M' && m_pCur[-2]=='R' ) ||
						( m_pCur[-3]=='M' && m_pCur[-2]=='S' ) ||
						( m_pCur[-3]=='D' && m_pCur[-2]=='R' ) )
							bMiddleName = true;
				}
				break;
			case 3:
				// preceded by a known 3-byte token (MRS, DRS)
				// example: Survived by Mrs. Doe ...
				if ( ( m_sAccum[0]=='m' || m_sAccum[0]=='d' ) && m_sAccum[1]=='r' && m_sAccum[2]=='s' )
					bMiddleName = true;
				break;
		}

		if ( !bInwordDot && !bInphraseDot && !bMiddleName )
		{
			// sentence boundary
			return MAGIC_CODE_SENTENCE | FLAG_CODEPOINT_SPECIAL;
		} else
		{
			// just a character
			if ( ( iCode & MASK_FLAGS )==FLAG_CODEPOINT_SPECIAL )
				return 0; // special only, not dual? then in this context, it is a separator
			else
				return iCode & ~( FLAG_CODEPOINT_SPECIAL | FLAG_CODEPOINT_DUAL ); // perhaps it was blended, so return the original code
		}
	}

	// pass-through
	return iCode;
}


int CSphTokenizerBase::CodepointArbitrationQ ( int iCode, bool bWasEscaped, BYTE uNextByte )
{
	if ( iCode & FLAG_CODEPOINT_NGRAM )
		return iCode; // ngrams are handled elsewhere

	int iSymbol = iCode & MASK_CODEPOINT;

	// codepoints can't be blended and special at the same time
	if ( ( iCode & FLAG_CODEPOINT_BLEND ) && ( iCode & FLAG_CODEPOINT_SPECIAL ) )
	{
		bool bBlend =
			bWasEscaped || // escaped characters should always act as blended
			( m_bPhrase && !sphIsModifier ( iSymbol ) && iSymbol!='"' ) || // non-modifier special inside phrase
			( m_iAccum && ( iSymbol=='@' || iSymbol=='/' || iSymbol=='-' ) ); // some specials in the middle of a token

		// clear special or blend flags
		iCode &= bBlend
			? ~( FLAG_CODEPOINT_DUAL | FLAG_CODEPOINT_SPECIAL )
			: ~( FLAG_CODEPOINT_DUAL | FLAG_CODEPOINT_BLEND );
	}

	// escaped specials are not special
	// dash and dollar inside the word are not special (however, single opening modifier is not a word!)
	// non-modifier specials within phrase are not special
	bool bDashInside = ( m_iAccum && iSymbol=='-' && !( m_iAccum==1 && sphIsModifier ( m_sAccum[0] ) ));
	if ( iCode & FLAG_CODEPOINT_SPECIAL )
		if ( bWasEscaped
			|| bDashInside
			|| ( m_iAccum && iSymbol=='$' && !IsBoundary ( uNextByte, m_bPhrase ) )
			|| ( m_bPhrase && iSymbol!='"' && !sphIsModifier ( iSymbol ) ) )
	{
		if ( iCode & FLAG_CODEPOINT_DUAL )
			iCode &= ~( FLAG_CODEPOINT_SPECIAL | FLAG_CODEPOINT_DUAL );
		else
			iCode = 0;
	}

	// if we didn't remove special by now, it must win
	if ( iCode & FLAG_CODEPOINT_DUAL )
	{
		assert ( iCode & FLAG_CODEPOINT_SPECIAL );
		iCode = iSymbol | FLAG_CODEPOINT_SPECIAL;
	}

	// ideally, all conflicts must be resolved here
	// well, at least most
	assert ( sphBitCount ( iCode & MASK_FLAGS )<=1 );
	return iCode;
}

#if !USE_WINDOWS
#define __forceinline inline
#endif

static __forceinline bool IsSeparator ( int iFolded, bool bFirst )
{
	// eternal separator
	if ( iFolded<0 || ( iFolded & MASK_CODEPOINT )==0 )
		return true;

	// just a codepoint
	if (!( iFolded & MASK_FLAGS ))
		return false;

	// any magic flag, besides dual
	if (!( iFolded & FLAG_CODEPOINT_DUAL ))
		return true;

	// FIXME? n-grams currently also set dual
	if ( iFolded & FLAG_CODEPOINT_NGRAM )
		return true;

	// dual depends on position
	return bFirst;
}

// handles escaped specials that are not in the character set
// returns true if the codepoint should be processed as a simple codepoint,
// returns false if it should be processed as a whitespace
// for example: aaa\!bbb => aaa bbb
static inline bool Special2Simple ( int & iCodepoint )
{
	if ( ( iCodepoint & FLAG_CODEPOINT_DUAL ) || !( iCodepoint & FLAG_CODEPOINT_SPECIAL ) )
	{
		iCodepoint &= ~( FLAG_CODEPOINT_SPECIAL | FLAG_CODEPOINT_DUAL );
		return true;
	}

	return false;
}

static bool CheckRemap ( CSphString & sError, const CSphVector<CSphRemapRange> & dRemaps, const char * sSource, bool bCanRemap, const CSphLowercaser & tLC )
{
	// check
	ARRAY_FOREACH ( i, dRemaps )
	{
		const CSphRemapRange & r = dRemaps[i];

		if ( !bCanRemap && r.m_iStart!=r.m_iRemapStart )
		{
			sError.SetSprintf ( "%s characters must not be remapped (map-from=U+%x, map-to=U+%x)",
								sSource, r.m_iStart, r.m_iRemapStart );
			return false;
		}

		for ( size_t j=r.m_iStart; j<=r.m_iEnd; j++ )
		{
			if ( tLC.ToLower ( j ) )
			{
				sError.SetSprintf ( "%s characters must not be referenced anywhere else (code=U+%x)", sSource, j );
				return false;
			}
		}

		if ( bCanRemap )
		{
			for ( size_t j=r.m_iRemapStart; j<=r.m_iRemapStart + r.m_iEnd - r.m_iStart; j++ )
			{
				if ( tLC.ToLower ( j ) )
				{
					sError.SetSprintf ( "%s characters must not be referenced anywhere else (code=U+%x)", sSource, j );
					return false;
				}
			}
		}
	}

	return true;
}


bool ISphTokenizer::RemapCharacters ( const char * sConfig, DWORD uFlags, const char * sSource, bool bCanRemap, CSphString & sError )
{
	// parse
	CSphVector<CSphRemapRange> dRemaps;
	CSphCharsetDefinitionParser tParser;
	if ( !tParser.Parse ( sConfig, dRemaps ) )
	{
		sError = tParser.GetLastError();
		return false;
	}

	if ( !CheckRemap ( sError, dRemaps, sSource, bCanRemap, m_tLC ) )
		return false;

	// add mapping
	m_tLC.AddRemaps ( dRemaps, uFlags );
	return true;
}

bool ISphTokenizer::SetBoundary ( const char * sConfig, CSphString & sError )
{
	return RemapCharacters ( sConfig, FLAG_CODEPOINT_BOUNDARY, "phrase boundary", false, sError );
}

bool ISphTokenizer::SetIgnoreChars ( const char * sConfig, CSphString & sError )
{
	return RemapCharacters ( sConfig, FLAG_CODEPOINT_IGNORE, "ignored", false, sError );
}

bool ISphTokenizer::SetBlendChars ( const char * sConfig, CSphString & sError )
{
	return sConfig ? RemapCharacters ( sConfig, FLAG_CODEPOINT_BLEND, "blend", true, sError ) : false;
}


static bool sphStrncmp ( const char * sCheck, int iCheck, const char * sRef )
{
	return ( iCheck==(int)strlen(sRef) && memcmp ( sCheck, sRef, iCheck )==0 );
}


bool ISphTokenizer::SetBlendMode ( const char * sMode, CSphString & sError )
{
	if ( !sMode || !*sMode )
	{
		m_uBlendVariants = BLEND_TRIM_NONE;
		m_bBlendSkipPure = false;
		return true;
	}

	m_uBlendVariants = 0;
	const char * p = sMode;
	while ( *p )
	{
		while ( !sphIsAlpha(*p) )
			p++;
		if ( !*p )
			break;

		const char * sTok = p;
		while ( sphIsAlpha(*p) )
			p++;
		if ( sphStrncmp ( sTok, (int)(p - sTok), "trim_none" ) )
			m_uBlendVariants |= BLEND_TRIM_NONE;
		else if ( sphStrncmp ( sTok, (int)(p - sTok), "trim_head" ) )
			m_uBlendVariants |= BLEND_TRIM_HEAD;
		else if ( sphStrncmp ( sTok, (int)(p - sTok), "trim_tail" ) )
			m_uBlendVariants |= BLEND_TRIM_TAIL;
		else if ( sphStrncmp ( sTok, (int)(p - sTok), "trim_both" ) )
			m_uBlendVariants |= BLEND_TRIM_BOTH;
		else if ( sphStrncmp ( sTok, (int)(p - sTok), "trim_all" ) )
			m_uBlendVariants |= BLEND_TRIM_ALL;
		else if ( sphStrncmp ( sTok, (int)( p-sTok), "skip_pure" ) )
			m_bBlendSkipPure = true;
		else
		{
			sError.SetSprintf ( "unknown blend_mode option near '%s'", sTok );
			return false;
		}
	}

	if ( !m_uBlendVariants )
	{
		sError.SetSprintf ( "blend_mode must define at least one variant to index" );
		m_uBlendVariants = BLEND_TRIM_NONE;
		m_bBlendSkipPure = false;
		return false;
	}
	return true;
}

/////////////////////////////////////////////////////////////////////////////

template < bool IS_QUERY >
CSphTokenizer_UTF8<IS_QUERY>::CSphTokenizer_UTF8 ()
{
	CSphString sTmp;
	SetCaseFolding ( SPHINX_DEFAULT_UTF8_TABLE, sTmp );
	m_bHasBlend = false;
}


template < bool IS_QUERY >
void CSphTokenizer_UTF8<IS_QUERY>::SetBuffer ( const BYTE * sBuffer, int iLength )
{
	// check that old one is over and that new length is sane
	assert ( iLength>=0 );

	// set buffer
	m_pBuffer = sBuffer;
	m_pBufferMax = sBuffer + iLength;
	m_pCur = sBuffer;
	m_pTokenStart = m_pTokenEnd = NULL;
	m_pBlendStart = m_pBlendEnd = NULL;

	m_iOvershortCount = 0;
	m_bBoundary = m_bTokenBoundary = false;
}


template < bool IS_QUERY >
BYTE * CSphTokenizer_UTF8<IS_QUERY>::GetToken ()
{
	m_bWasSpecial = false;
	m_bBlended = false;
	m_iOvershortCount = 0;
	m_bTokenBoundary = false;
	m_bWasSynonym = false;

	return m_bHasBlend
		? DoGetToken<IS_QUERY,true>()
		: DoGetToken<IS_QUERY,false>();
}


bool CSphTokenizerBase2::CheckException ( const BYTE * pStart, const BYTE * pCur, bool bQueryMode )
{
	assert ( m_pExc );
	assert ( pStart );

	// at this point [pStart,pCur) is our regular tokenization candidate,
	// and pCur is pointing at what normally is considered separtor
	//
	// however, it might be either a full exception (if we're really lucky)
	// or (more likely) an exception prefix, so lets check for that
	//
	// interestingly enough, note that our token might contain a full exception
	// as a prefix, for instance [USAF] token vs [USA] exception; but in that case
	// we still need to tokenize regularly, because even exceptions need to honor
	// word boundaries

	// lets begin with a special (hopefully fast) check for the 1st byte
	const BYTE * p = pStart;
	if ( m_pExc->GetFirst ( *p )<0 )
		return false;

	// consume all the (character data) bytes until the first separator
	int iNode = 0;
	while ( p<pCur )
	{
		if ( bQueryMode && *p=='\\' )
		{
			p++;
			continue;;
		}
		iNode =(int) m_pExc->GetNext ((size_t) iNode, *p++ );
		if ( iNode<0 )
			return false;
	}

	const BYTE * pMapEnd = NULL; // the longest exception found so far is [pStart,pMapEnd)
	const BYTE * pMapTo = NULL; // the destination mapping

	// now, we got ourselves a valid exception prefix, so lets keep consuming more bytes,
	// ie. until further separators, and keep looking for a full exception match
	while ( iNode>=0 )
	{
		// in query mode, ignore quoting slashes
		if ( bQueryMode && *p=='\\' )
		{
			p++;
			continue;
		}

		// decode one more codepoint, check if it is a separator
		bool bSep = true;
		bool bSpace = sphIsSpace(*p); // okay despite utf-8, cause hard whitespace is all ascii-7

		const BYTE * q = p;
		if ( p<m_pBufferMax )
			bSep = IsSeparator ( (int) m_tLC.ToLower ( sphUTF8Decode(q) ), false ); // FIXME? sometimes they ARE first

		// there is a separator ahead, so check if we have a full match
		if ( bSep && m_pExc->GetMapping(iNode) )
		{
			pMapEnd = p;
			pMapTo = m_pExc->GetMapping(iNode);
		}

		// eof? bail
		if ( p>=m_pBufferMax )
			break;

		// not eof? consume those bytes
		if ( bSpace )
		{
			// and fold (hard) whitespace while we're at it!
			while ( sphIsSpace(*p) )
				p++;
			iNode =(int) m_pExc->GetNext ((size_t) iNode, ' ' );
		} else
		{
			// just consume the codepoint, byte-by-byte
			while ( p<q && iNode>=0 )
				iNode =(int) m_pExc->GetNext ((size_t) iNode, *p++ );
		}

		// we just consumed a separator, so check for a full match again
		if ( iNode>=0 && bSep && m_pExc->GetMapping(iNode) )
		{
			pMapEnd = p;
			pMapTo = m_pExc->GetMapping(iNode);
		}
	}

	// found anything?
	if ( !pMapTo )
		return false;

	strncpy ( (char*)m_sAccum, (char*)pMapTo, sizeof(m_sAccum) );
	m_pCur = pMapEnd;
	m_pTokenStart = pStart;
	m_pTokenEnd = pMapEnd;
	m_iLastTokenLen =(int) strlen ( (char*)m_sAccum );

	m_bWasSynonym = true;
	return true;
}


static inline bool ShortTokenFilter(BYTE* pToken, int iLen)
{
	return pToken[0] == '*' || (iLen > 0 && pToken[iLen - 1] == '*');
}


template < bool IS_QUERY, bool IS_BLEND >
BYTE * CSphTokenizerBase2::DoGetToken ()
{
	// return pending blending variants
	if_const ( IS_BLEND )
	{
		BYTE * pVar = GetBlendedVariant ();
		if ( pVar )
			return pVar;
		m_bBlendedPart = ( m_pBlendEnd!=NULL );
	}

	// in query mode, lets capture (soft-whitespace hard-whitespace) sequences and adjust overshort counter
	// sample queries would be (one NEAR $$$) or (one | $$$ two) where $ is not a valid character
	bool bGotNonToken = ( !IS_QUERY || m_bPhrase ); // only do this in query mode, never in indexing mode, never within phrases
	bool bGotSoft = false; // hey Beavis he said soft huh huhhuh

	m_pTokenStart = NULL;
	for ( ;; )
	{
		// get next codepoint
		const BYTE * const pCur = m_pCur; // to redo special char, if there's a token already

		int iCodePoint;
		int iCode;
		if ( pCur<m_pBufferMax && *pCur<128 )
		{
			iCodePoint = *m_pCur++;
			iCode = m_tLC.m_pChunk[0][iCodePoint];
		} else
		{
			iCodePoint = GetCodepoint(); // advances m_pCur
			iCode =(int) m_tLC.ToLower ( iCodePoint );
		}

		// handle escaping
		bool bWasEscaped = ( IS_QUERY && iCodePoint=='\\' ); // whether current codepoint was escaped
		if ( bWasEscaped )
		{
			iCodePoint = GetCodepoint();
			iCode =(int) m_tLC.ToLower ( iCodePoint );
			if ( !Special2Simple ( iCode ) )
				iCode = 0;
		}

		// handle eof
		if ( iCode<0 )
		{
			FlushAccum ();

			// suddenly, exceptions
			if ( m_pExc && m_pTokenStart && CheckException ( m_pTokenStart, pCur, IS_QUERY ) )
				return m_sAccum;

			// skip trailing short word
			if ( m_iLastTokenLen<m_tSettings.m_iMinWordLen )
			{
				if ( !m_bShortTokenFilter || !ShortTokenFilter ( m_sAccum, m_iLastTokenLen ) )
				{
					if ( m_iLastTokenLen )
						m_iOvershortCount++;
					m_iLastTokenLen = 0;
					if_const ( IS_BLEND )
						BlendAdjust ( pCur );
					return NULL;
				}
			}

			// keep token end here as BlendAdjust might change m_pCur
			m_pTokenEnd = m_pCur;

			// return trailing word
			if_const ( IS_BLEND && !BlendAdjust ( pCur ) )
				return NULL;
			if_const ( IS_BLEND && m_bBlended )
				return GetBlendedVariant();
			return m_sAccum;
		}

		// handle all the flags..
		if_const ( IS_QUERY )
			iCode = CodepointArbitrationQ ( iCode, bWasEscaped, *m_pCur );
		else if ( m_bDetectSentences )
			iCode = CodepointArbitrationI ( iCode );

		// handle ignored chars
		if ( iCode & FLAG_CODEPOINT_IGNORE )
			continue;

		// handle blended characters
		if_const ( IS_BLEND && ( iCode & FLAG_CODEPOINT_BLEND ) )
		{
			if ( m_pBlendEnd )
				iCode = 0;
			else
			{
				m_bBlended = true;
				m_pBlendStart = m_iAccum ? m_pTokenStart : pCur;
			}
		}

		// handle soft-whitespace-only tokens
		if ( !bGotNonToken && !m_iAccum )
		{
			if ( !bGotSoft )
			{
				// detect opening soft whitespace
				if ( ( iCode==0 && !IsWhitespace ( iCodePoint ) && !IsPunctuation ( iCodePoint ) )
					|| ( ( iCode & FLAG_CODEPOINT_BLEND ) && !m_iAccum ) )
				{
					bGotSoft = true;
				}
			} else
			{
				// detect closing hard whitespace or special
				// (if there was anything meaningful in the meantime, we must never get past the outer if!)
				if ( IsWhitespace ( iCodePoint ) || ( iCode & FLAG_CODEPOINT_SPECIAL ) )
				{
					m_iOvershortCount++;
					bGotNonToken = true;
				}
			}
		}

		// handle whitespace and boundary
		if ( m_bBoundary && ( iCode==0 ) )
		{
			m_bTokenBoundary = true;
			m_iBoundaryOffset = pCur - m_pBuffer - 1;
		}
		m_bBoundary = ( iCode & FLAG_CODEPOINT_BOUNDARY )!=0;

		// handle separator (aka, most likely a token!)
		if ( iCode==0 || m_bBoundary )
		{
			FlushAccum ();

			// suddenly, exceptions
			if ( m_pExc && CheckException ( m_pTokenStart ? m_pTokenStart : pCur, pCur, IS_QUERY ) )
				return m_sAccum;

			if_const ( IS_BLEND && !BlendAdjust ( pCur ) )
				continue;

			if ( m_iLastTokenLen<m_tSettings.m_iMinWordLen
				&& !( m_bShortTokenFilter && ShortTokenFilter ( m_sAccum, m_iLastTokenLen ) ) )
			{
				if ( m_iLastTokenLen )
					m_iOvershortCount++;
				continue;
			} else
			{
				m_pTokenEnd = pCur;
				if_const ( IS_BLEND && m_bBlended )
					return GetBlendedVariant();
				return m_sAccum;
			}
		}

		// handle specials
		if ( iCode & FLAG_CODEPOINT_SPECIAL )
		{
			// skip short words preceding specials
			if ( m_iAccum<m_tSettings.m_iMinWordLen )
			{
				m_sAccum[m_iAccum] = '\0';

				if ( !m_bShortTokenFilter || !ShortTokenFilter ( m_sAccum, m_iAccum ) )
				{
					if ( m_iAccum )
						m_iOvershortCount++;

					FlushAccum ();
				}
			}

			if ( m_iAccum==0 )
			{
				m_bNonBlended = m_bNonBlended || ( !( iCode & FLAG_CODEPOINT_BLEND ) && !( iCode & FLAG_CODEPOINT_SPECIAL ) );
				m_bWasSpecial = !( iCode & FLAG_CODEPOINT_NGRAM );
				m_pTokenStart = pCur;
				m_pTokenEnd = m_pCur;
				AccumCodepoint ( iCode & MASK_CODEPOINT ); // handle special as a standalone token
			} else
			{
				m_pCur = pCur; // we need to flush current accum and then redo special char again
				m_pTokenEnd = pCur;
			}

			FlushAccum ();

			// suddenly, exceptions
			if ( m_pExc && CheckException ( m_pTokenStart, pCur, IS_QUERY ) )
				return m_sAccum;

			if_const ( IS_BLEND )
			{
				if ( !BlendAdjust ( pCur ) )
					continue;
				if ( m_bBlended )
					return GetBlendedVariant();
			}
			return m_sAccum;
		}

		if ( m_iAccum==0 )
			m_pTokenStart = pCur;

		// tricky bit
		// heading modifiers must not (!) affected blended status
		// eg. we want stuff like '=-' (w/o apostrophes) thrown away when pure_blend is on
		if_const ( IS_BLEND )
			if_const (!( IS_QUERY && !m_iAccum && sphIsModifier ( iCode & MASK_CODEPOINT ) ) )
				m_bNonBlended = m_bNonBlended || !( iCode & FLAG_CODEPOINT_BLEND );

		// just accumulate
		// manual inlining of utf8 encoder gives us a few extra percent
		// which is important here, this is a hotspot
		if ( m_iAccum<SPH_MAX_WORD_LEN && ( m_pAccum-m_sAccum+SPH_MAX_UTF8_BYTES<=(int)sizeof(m_sAccum) ) )
		{
			iCode &= MASK_CODEPOINT;
			m_iAccum++;
			SPH_UTF8_ENCODE ( m_pAccum, iCode );
		}
	}
}


void CSphTokenizerBase2::FlushAccum ()
{
	assert ( m_pAccum-m_sAccum < (int)sizeof(m_sAccum) );
	m_iLastTokenLen = m_iAccum;
	*m_pAccum = 0;
	m_iAccum = 0;
	m_pAccum = m_sAccum;
}


template < bool IS_QUERY >
ISphTokenizer * CSphTokenizer_UTF8<IS_QUERY>::Clone ( ESphTokenizerClone eMode ) const
{
	CSphTokenizerBase * pClone;
	if ( eMode!=SPH_CLONE_INDEX )
		pClone = new CSphTokenizer_UTF8<true>();
	else
		pClone = new CSphTokenizer_UTF8<false>();
	pClone->CloneBase ( this, eMode );
	return pClone;
}


template < bool IS_QUERY >
int CSphTokenizer_UTF8<IS_QUERY>::GetCodepointLength ( int iCode ) const
{
	if ( iCode<128 )
		return 1;

	int iBytes = 0;
	while ( iCode & 0x80 )
	{
		iBytes++;
		iCode <<= 1;
	}

	assert ( iBytes>=2 && iBytes<=4 );
	return iBytes;
}

/////////////////////////////////////////////////////////////////////////////

template < bool IS_QUERY >
bool CSphTokenizer_UTF8Ngram<IS_QUERY>::SetNgramChars ( const char * sConfig, CSphString & sError )
{
	assert ( this->m_eMode!=SPH_CLONE_QUERY_LIGHTWEIGHT );
	CSphVector<CSphRemapRange> dRemaps;
	CSphCharsetDefinitionParser tParser;
	if ( !tParser.Parse ( sConfig, dRemaps ) )
	{
		sError = tParser.GetLastError();
		return false;
	}

	if ( !CheckRemap ( sError, dRemaps, "ngram", true, this->m_tLC ) )
		return false;

	// gcc braindamage requires this
	this->m_tLC.AddRemaps ( dRemaps, FLAG_CODEPOINT_NGRAM | FLAG_CODEPOINT_SPECIAL ); // !COMMIT support other n-gram lengths than 1
	m_sNgramCharsStr = sConfig;
	return true;
}


template < bool IS_QUERY >
void CSphTokenizer_UTF8Ngram<IS_QUERY>::SetNgramLen ( int iLen )
{
	assert ( this->m_eMode!=SPH_CLONE_QUERY_LIGHTWEIGHT );
	assert ( iLen>0 );
	m_iNgramLen = iLen;
}


template < bool IS_QUERY >
BYTE * CSphTokenizer_UTF8Ngram<IS_QUERY>::GetToken ()
{
	// !COMMIT support other n-gram lengths than 1
	assert ( m_iNgramLen==1 );
	return CSphTokenizer_UTF8<IS_QUERY>::GetToken ();
}

/////////////////////////////////////////////////////////////////////////

bool sphParseCharset ( const char * sCharset, CSphVector<CSphRemapRange> & dRemaps )
{
	CSphCharsetDefinitionParser tParser;
	return tParser.Parse ( sCharset, dRemaps );
}

//////////////////////////////////////////////////////////////////////////

CSphMultiformTokenizer::CSphMultiformTokenizer ( ISphTokenizer * pTokenizer, const CSphMultiformContainer * pContainer )
	: CSphTokenFilter ( pTokenizer )
	, m_pMultiWordforms ( pContainer )
	, m_iStart	( 0 )
	, m_iOutputPending ( -1 )
	, m_pCurrentForm ( NULL )
	, m_bBuildMultiform	( false )
{
	assert ( pTokenizer && pContainer );
	m_dStoredTokens.Reserve ( pContainer->m_iMaxTokens + 6 ); // max form tokens + some blended tokens
	m_sTokenizedMultiform[0] = '\0';
}


CSphMultiformTokenizer::~CSphMultiformTokenizer ()
{
	SafeDelete ( m_pTokenizer );
}


BYTE * CSphMultiformTokenizer::GetToken ()
{
	if ( m_iOutputPending > -1 && m_pCurrentForm )
	{
		if ( ++m_iOutputPending>=m_pCurrentForm->m_dNormalForm.GetLength() )
		{
			m_iOutputPending = -1;
			m_pCurrentForm = NULL;
		} else
		{
			StoredToken_t & tStart = m_dStoredTokens[m_iStart];
			strncpy ( (char *)tStart.m_sToken, m_pCurrentForm->m_dNormalForm[m_iOutputPending].m_sForm.cstr(), sizeof(tStart.m_sToken) );

			tStart.m_iTokenLen = m_pCurrentForm->m_dNormalForm[m_iOutputPending].m_iLengthCP;
			tStart.m_bBoundary = false;
			tStart.m_bSpecial = false;
			tStart.m_bBlended = false;
			tStart.m_bBlendedPart = false;
			return tStart.m_sToken;
		}
	}

	m_sTokenizedMultiform[0] = '\0';
	m_iStart++;

	if ( m_iStart>=m_dStoredTokens.GetLength() )
	{
		m_iStart = 0;
		m_dStoredTokens.Resize ( 0 );
		const BYTE * pToken = m_pTokenizer->GetToken();
		if ( !pToken )
			return NULL;

		FillStoredTokenInfo ( m_dStoredTokens.Add(), pToken, m_pTokenizer );
		while ( m_dStoredTokens.Last().m_bBlended || m_dStoredTokens.Last().m_bBlendedPart )
		{
			pToken = m_pTokenizer->GetToken ();
			if ( !pToken )
				break;

			FillStoredTokenInfo ( m_dStoredTokens.Add(), pToken, m_pTokenizer );
		}
	}

	CSphMultiforms ** pWordforms = NULL;
	int iTokensGot = 1;
	bool bBlended = false;

	// check multi-form
	// only blended parts checked for multi-form with blended
	// in case ALL blended parts got transformed primary blended got replaced by normal form
	// otherwise blended tokens provided as is
	if ( m_dStoredTokens[m_iStart].m_bBlended || m_dStoredTokens[m_iStart].m_bBlendedPart )
	{
		if ( m_dStoredTokens[m_iStart].m_bBlended && m_iStart+1<m_dStoredTokens.GetLength() && m_dStoredTokens[m_iStart+1].m_bBlendedPart )
		{
			pWordforms = m_pMultiWordforms->m_Hash ( (const char *)m_dStoredTokens[m_iStart+1].m_sToken );
			if ( pWordforms )
			{
				bBlended = true;
				for ( int i=m_iStart+2; i<m_dStoredTokens.GetLength(); i++ )
				{
					// break out on blended over or got completely different blended
					if ( m_dStoredTokens[i].m_bBlended || !m_dStoredTokens[i].m_bBlendedPart )
						break;

					iTokensGot++;
				}
			}
		}
	} else
	{
		pWordforms = m_pMultiWordforms->m_Hash ( (const char *)m_dStoredTokens[m_iStart].m_sToken );
		if ( pWordforms )
		{
			int iTokensNeed = (*pWordforms)->m_iMaxTokens + 1;
			int iCur = m_iStart;
			bool bGotBlended = false;

			// collect up ahead to multi-form tokens or all blended tokens
			while ( iTokensGot<iTokensNeed || bGotBlended )
			{
				iCur++;
				if ( iCur>=m_dStoredTokens.GetLength() )
				{
					// fetch next token
					const BYTE* pToken = m_pTokenizer->GetToken ();
					if ( !pToken )
						break;

					FillStoredTokenInfo ( m_dStoredTokens.Add(), pToken, m_pTokenizer );
				}

				bool bCurBleneded = ( m_dStoredTokens[iCur].m_bBlended || m_dStoredTokens[iCur].m_bBlendedPart );
				if ( bGotBlended && !bCurBleneded )
					break;

				bGotBlended = bCurBleneded;
				// count only regular tokens; can not fold mixed (regular+blended) tokens to form
				iTokensGot += ( bGotBlended ? 0 : 1 );
			}
		}
	}

	if ( !pWordforms || iTokensGot<(*pWordforms)->m_iMinTokens+1 )
		return m_dStoredTokens[m_iStart].m_sToken;

	int iStartToken = m_iStart + ( bBlended ? 1 : 0 );
	ARRAY_FOREACH ( i, (*pWordforms)->m_pForms )
	{
		const CSphMultiform * pCurForm = (*pWordforms)->m_pForms[i];
		int iFormTokCount =(int) pCurForm->m_dTokens.GetLength();

		if ( iTokensGot<iFormTokCount+1 || ( bBlended && iTokensGot!=iFormTokCount+1 ) )
			continue;

		int iForm = 0;
		for ( ; iForm<iFormTokCount; iForm++ )
		{
			const StoredToken_t & tTok = m_dStoredTokens[(size_t) (iStartToken + 1 + iForm)];
			const char * szStored = (const char*)tTok.m_sToken;
			const char * szNormal = pCurForm->m_dTokens[iForm].cstr ();

			if ( *szNormal!=*szStored || strcasecmp ( szNormal, szStored ) )
				break;
		}

		// early out - no destination form detected
		if ( iForm!=iFormTokCount )
			continue;

		// tokens after folded form are valid tail that should be processed next time
		if ( m_bBuildMultiform )
		{
			BYTE * pOut = m_sTokenizedMultiform;
			BYTE * pMax = pOut + sizeof(m_sTokenizedMultiform);
			for ( int j=0; j<iFormTokCount+1 && pOut<pMax; j++ )
			{
				const StoredToken_t & tTok = m_dStoredTokens[iStartToken+j];
				const BYTE * sTok = tTok.m_sToken;
				if ( j && pOut<pMax )
					*pOut++ = ' ';
				while ( *sTok && pOut<pMax )
					*pOut++ = *sTok++;
			}
			*pOut = '\0';
			*(pMax-1) = '\0';
		}

		if ( !bBlended )
		{
			// fold regular tokens to form
			const StoredToken_t & tStart = m_dStoredTokens[m_iStart];
			StoredToken_t & tEnd = m_dStoredTokens[m_iStart+iFormTokCount];
			m_iStart += iFormTokCount;

			strncpy ( (char *)tEnd.m_sToken, pCurForm->m_dNormalForm[0].m_sForm.cstr(), sizeof(tEnd.m_sToken) );
			tEnd.m_szTokenStart = tStart.m_szTokenStart;
			tEnd.m_iTokenLen = pCurForm->m_dNormalForm[0].m_iLengthCP;

			tEnd.m_bBoundary = false;
			tEnd.m_bSpecial = false;
			tEnd.m_bBlended = false;
			tEnd.m_bBlendedPart = false;

			if ( pCurForm->m_dNormalForm.GetLength()>1 )
			{
				m_iOutputPending = 0;
				m_pCurrentForm = pCurForm;
			}
		} else
		{
			// replace blended by form
			// FIXME: add multiple destination token support here (if needed)
			assert ( pCurForm->m_dNormalForm.GetLength()==1 );
			StoredToken_t & tDst = m_dStoredTokens[m_iStart];
			strncpy ( (char *)tDst.m_sToken, pCurForm->m_dNormalForm[0].m_sForm.cstr(), sizeof(tDst.m_sToken) );
			tDst.m_iTokenLen = pCurForm->m_dNormalForm[0].m_iLengthCP;
		}
		break;
	}

	return m_dStoredTokens[m_iStart].m_sToken;
}


ISphTokenizer * CSphMultiformTokenizer::Clone ( ESphTokenizerClone eMode ) const
{
	ISphTokenizer * pClone = m_pTokenizer->Clone ( eMode );
	return CreateMultiformFilter ( pClone, m_pMultiWordforms );
}


void CSphMultiformTokenizer::SetBufferPtr ( const char * sNewPtr )
{
	m_iStart = 0;
	m_iOutputPending = -1;
	m_pCurrentForm = NULL;
	m_dStoredTokens.Resize ( 0 );
	m_pTokenizer->SetBufferPtr ( sNewPtr );
}

void CSphMultiformTokenizer::SetBuffer ( const BYTE * sBuffer, int iLength )
{
	m_pTokenizer->SetBuffer ( sBuffer, iLength );
	SetBufferPtr ( (const char *)sBuffer );
}

uint64_t CSphMultiformTokenizer::GetSettingsFNV () const
{
	uint64_t uHash = CSphTokenFilter::GetSettingsFNV();
	uHash ^= (uint64_t)m_pMultiWordforms;
	return uHash;
}


int CSphMultiformTokenizer::SkipBlended ()
{
	bool bGotBlended = ( m_iStart<m_dStoredTokens.GetLength() &&
		( m_dStoredTokens[m_iStart].m_bBlended || m_dStoredTokens[m_iStart].m_bBlendedPart ) );
	if ( !bGotBlended )
		return 0;

	int iWasStart = m_iStart;
	for ( int iTok=m_iStart+1; iTok<m_dStoredTokens.GetLength() && m_dStoredTokens[iTok].m_bBlendedPart && !m_dStoredTokens[iTok].m_bBlended; iTok++ )
		m_iStart = iTok;

	return m_iStart-iWasStart;
}

bool CSphMultiformTokenizer::WasTokenMultiformDestination ( bool & bHead, int & iDestCount ) const
{
	if ( m_iOutputPending>-1 && m_pCurrentForm && m_pCurrentForm->m_dNormalForm.GetLength()>1 && m_iOutputPending<m_pCurrentForm->m_dNormalForm.GetLength() )
	{
		bHead = ( m_iOutputPending==0 );
		iDestCount =(int) m_pCurrentForm->m_dNormalForm.GetLength();
		return true;
	} else
	{
		return false;
	}
}

//file.cpp

ISphTokenizer * sphCreateUTF8Tokenizer ()
{
	return new CSphTokenizer_UTF8<false> ();
}

ISphTokenizer * sphCreateUTF8NgramTokenizer ()
{
	return new CSphTokenizer_UTF8Ngram<false> ();
}

bool LoadTokenizerSettings ( CSphReader & tReader, CSphTokenizerSettings & tSettings,
	CSphEmbeddedFiles & tEmbeddedFiles, DWORD uVersion, CSphString & sWarning )
{
	if ( uVersion<9 )
		return true;

	tSettings.m_iType = tReader.GetByte ();
	if ( tSettings.m_iType!=TOKENIZER_UTF8 && tSettings.m_iType!=TOKENIZER_NGRAM )
	{
		sWarning = "can't load an old index with SBCS tokenizer";
		return false;
	}

	tSettings.m_sCaseFolding = tReader.GetString ();
	tSettings.m_iMinWordLen = tReader.GetDword ();
	tEmbeddedFiles.m_bEmbeddedSynonyms = false;
	if ( uVersion>=30 )
	{
		tEmbeddedFiles.m_bEmbeddedSynonyms = !!tReader.GetByte();
		if ( tEmbeddedFiles.m_bEmbeddedSynonyms )
		{
			int nSynonyms = (int)tReader.GetDword();
			tEmbeddedFiles.m_dSynonyms.Resize ( nSynonyms );
			ARRAY_FOREACH ( i, tEmbeddedFiles.m_dSynonyms )
				tEmbeddedFiles.m_dSynonyms[i] = tReader.GetString();
		}
	}

	tSettings.m_sSynonymsFile = tReader.GetString ();
	ReadFileInfo ( tReader, tSettings.m_sSynonymsFile.cstr (),
		tEmbeddedFiles.m_tSynonymFile, tEmbeddedFiles.m_bEmbeddedSynonyms ? NULL : &sWarning );
	tSettings.m_sBoundary = tReader.GetString ();
	tSettings.m_sIgnoreChars = tReader.GetString ();
	tSettings.m_iNgramLen = tReader.GetDword ();
	tSettings.m_sNgramChars = tReader.GetString ();
	if ( uVersion>=15 )
		tSettings.m_sBlendChars = tReader.GetString ();
	if ( uVersion>=24 )
		tSettings.m_sBlendMode = tReader.GetString();

	return true;
}


/// gets called from and MUST be in sync with RtIndex_t::SaveDiskHeader()!
/// note that SaveDiskHeader() occasionaly uses some PREVIOUS format version!
void SaveTokenizerSettings ( CSphWriter & tWriter, ISphTokenizer * pTokenizer, int iEmbeddedLimit )
{
	assert ( pTokenizer );

	const CSphTokenizerSettings & tSettings = pTokenizer->GetSettings ();
	tWriter.PutByte ( tSettings.m_iType );
	tWriter.PutString ( tSettings.m_sCaseFolding.cstr () );
	tWriter.PutDword ( tSettings.m_iMinWordLen );

	bool bEmbedSynonyms = pTokenizer->GetSynFileInfo ().m_uSize<=(SphOffset_t)iEmbeddedLimit;
	tWriter.PutByte ( bEmbedSynonyms ? 1 : 0 );
	if ( bEmbedSynonyms )
		pTokenizer->WriteSynonyms ( tWriter );

	tWriter.PutString ( tSettings.m_sSynonymsFile.cstr () );
	WriteFileInfo ( tWriter, pTokenizer->GetSynFileInfo () );
	tWriter.PutString ( tSettings.m_sBoundary.cstr () );
	tWriter.PutString ( tSettings.m_sIgnoreChars.cstr () );
	tWriter.PutDword ( tSettings.m_iNgramLen );
	tWriter.PutString ( tSettings.m_sNgramChars.cstr () );
	tWriter.PutString ( tSettings.m_sBlendChars.cstr () );
	tWriter.PutString ( tSettings.m_sBlendMode.cstr () );
}


DWORD sphParseMorphAot(const char* sMorphology)
{
	if (!sMorphology || !*sMorphology)
		return 0;

	CSphVector<CSphString> dMorphs;
	sphSplit(dMorphs, sMorphology);

	DWORD uAotFilterMask = 0;
	for (int j = 0; j < AOT_LENGTH; ++j)
	{
		char buf_all[20];
		sprintf(buf_all, "lemmatize_%s_all", AOT_LANGUAGES[j]); // NOLINT
		ARRAY_FOREACH(i, dMorphs)
		{
			if (dMorphs[i] == buf_all)
			{
				uAotFilterMask |= (1UL) << j;
				break;
			}
		}
	}

	return uAotFilterMask;
}

}
