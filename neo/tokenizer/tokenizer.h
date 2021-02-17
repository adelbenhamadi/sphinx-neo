#pragma once
#include "neo/core/config.h"
#include "neo/core/exception.h"
#include "neo/int/types.h"
#include "neo/io/file.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
//#include "neo/source/schema.h"
#include "neo/tokenizer/enums.h"
#include "neo/dict/dict_settings.h"
#include "neo/index/index_settings.h"
#include "neo/tokenizer/tokenizer_settings.h"
#include "neo/tokenizer/form.h"

#include "neo/utility/hash.h"
#include "neo/tools/utf8_tools.h"
#include "neo/tools/lower_caser.h"
#include "neo/io/fnv64.h"

namespace NEO {

	extern const char* SPHINX_DEFAULT_UTF8_TABLE;
	/// generic tokenizer
	class ISphTokenizer
	{
	public:
		/// trivial ctor
		ISphTokenizer();

		/// trivial dtor
		virtual							~ISphTokenizer() {}

	public:
		/// set new translation table
		/// returns true on success, false on failure
		virtual bool					SetCaseFolding(const char* sConfig, CSphString& sError);

		/// add additional character as valid (with folding to itself)
		virtual void					AddPlainChar(char c);

		/// add special chars to translation table
		/// updates lowercaser so that these remap to -1
		virtual void					AddSpecials(const char* sSpecials);

		/// set ignored characters
		virtual bool					SetIgnoreChars(const char* sIgnored, CSphString& sError);

		/// set n-gram characters (for CJK n-gram indexing)
		virtual bool					SetNgramChars(const char*, CSphString&) { return true; }

		/// set n-gram length (for CJK n-gram indexing)
		virtual void					SetNgramLen(int) {}

		/// load synonyms list
		virtual bool					LoadSynonyms(const char* sFilename, const CSphEmbeddedFiles* pFiles, CSphString& sError) = 0;

		/// write synonyms to file
		virtual void					WriteSynonyms(CSphWriter& tWriter) = 0;

		/// set phrase boundary chars
		virtual bool					SetBoundary(const char* sConfig, CSphString& sError);

		/// set blended characters
		virtual bool					SetBlendChars(const char* sConfig, CSphString& sError);

		/// set blended tokens processing mode
		virtual bool					SetBlendMode(const char* sMode, CSphString& sError);

		/// setup tokenizer using given settings
		virtual void					Setup(const CSphTokenizerSettings& tSettings);

		/// create a tokenizer using the given settings
		static ISphTokenizer* Create(const CSphTokenizerSettings& tSettings, const CSphEmbeddedFiles* pFiles, CSphString& sError);

		/// create a token filter
		static ISphTokenizer* CreateMultiformFilter(ISphTokenizer* pTokenizer, const CSphMultiformContainer* pContainer);

		/// create a token filter
		static ISphTokenizer* CreateBigramFilter(ISphTokenizer* pTokenizer, ESphBigram eBigramIndex, const CSphString& sBigramWords, CSphString& sError);

		/// create a plugin filter
		/// sSspec is a library, name, and options specification string, eg "myplugins.dll:myfilter1:arg1=123"
		static ISphTokenizer* CreatePluginFilter(ISphTokenizer* pTokenizer, const CSphString& sSpec, CSphString& sError);

		/// save tokenizer settings to a stream
		virtual const CSphTokenizerSettings& GetSettings() const { return m_tSettings; }

		/// get synonym file info
		virtual const CSphSavedFile& GetSynFileInfo() const { return m_tSynFileInfo; }

	public:
		/// pass next buffer
		virtual void					SetBuffer(const BYTE* sBuffer, int iLength) = 0;

		/// set current index schema (only intended for the token filter plugins)
		virtual bool					SetFilterSchema(const CSphSchema&, CSphString&) { return true; }

		/// set per-document options from INSERT
		virtual bool					SetFilterOptions(const char*, CSphString&) { return true; }

		/// notify tokenizer that we now begin indexing a field with a given number (only intended for the token filter plugins)
		virtual void					BeginField(int) {}

		/// get next token
		virtual BYTE* GetToken() = 0;

		/// calc codepoint length
		virtual int						GetCodepointLength(int iCode) const = 0;

		/// get max codepoint length
		virtual int						GetMaxCodepointLength() const = 0;

		/// enable indexing-time sentence boundary detection, and paragraph indexing
		virtual bool					EnableSentenceIndexing(CSphString& sError);

		/// enable zone indexing
		virtual bool					EnableZoneIndexing(CSphString& sError);

		/// enable tokenized multiform tracking
		virtual void					EnableTokenizedMultiformTracking() {}

		/// get last token length, in codepoints
		virtual int						GetLastTokenLen() const { return m_iLastTokenLen; }

		/// get last token boundary flag (true if there was a boundary before the token)
		virtual bool					GetBoundary() { return m_bTokenBoundary; }

		/// get byte offset of the last boundary character
		virtual int						GetBoundaryOffset() { return m_iBoundaryOffset; }

		/// was last token a special one?
		virtual bool					WasTokenSpecial() { return m_bWasSpecial; }

		virtual bool					WasTokenSynonym() const { return m_bWasSynonym; }

		/// get amount of overshort keywords skipped before this token
		virtual int						GetOvershortCount() { return (!m_bBlended && m_bBlendedPart ? 0 : m_iOvershortCount); }

		/// get original tokenized multiform (if any); NULL means there was none
		virtual BYTE* GetTokenizedMultiform() { return NULL; }

		/// was last token a part of multi-wordforms destination
		/// head parameter might be useful to distinguish between sequence of different multi-wordforms
		virtual bool					WasTokenMultiformDestination(bool& bHead, int& iDestCount) const = 0;

		/// check whether this token is a generated morphological guess
		ESphTokenMorph					GetTokenMorph() const { return m_eTokenMorph; }

		virtual bool					TokenIsBlended() const { return m_bBlended; }
		virtual bool					TokenIsBlendedPart() const { return m_bBlendedPart; }
		virtual int						SkipBlended() { return 0; }

	public:
		/// spawn a clone of my own
		virtual ISphTokenizer* Clone(ESphTokenizerClone eMode) const = 0;

		/// start buffer point of last token
		virtual const char* GetTokenStart() const = 0;

		/// end buffer point of last token (exclusive, ie. *GetTokenEnd() is already NOT part of a token!)
		virtual const char* GetTokenEnd() const = 0;

		/// current buffer ptr
		virtual const char* GetBufferPtr() const = 0;

		/// buffer end
		virtual const char* GetBufferEnd() const = 0;

		/// set new buffer ptr (must be within current bounds)
		virtual void					SetBufferPtr(const char* sNewPtr) = 0;

		/// get settings hash
		virtual uint64_t				GetSettingsFNV() const;

		/// get (readonly) lowercaser
		const CSphLowercaser& GetLowercaser() const { return m_tLC; }

	protected:
		virtual bool					RemapCharacters(const char* sConfig, DWORD uFlags, const char* sSource, bool bCanRemap, CSphString& sError);
		virtual bool					AddSpecialsSPZ(const char* sSpecials, const char* sDirective, CSphString& sError);

	protected:
		static const int				MAX_SYNONYM_LEN = 1024;	///< max synonyms map-from length, bytes

		static const BYTE				BLEND_TRIM_NONE = 1;
		static const BYTE				BLEND_TRIM_HEAD = 2;
		static const BYTE				BLEND_TRIM_TAIL = 4;
		static const BYTE				BLEND_TRIM_BOTH = 8;
		static const BYTE				BLEND_TRIM_ALL = 16;

		CSphLowercaser					m_tLC;						///< my lowercaser
		int								m_iLastTokenLen;			///< last token length, in codepoints
		bool							m_bTokenBoundary;			///< last token boundary flag (true after boundary codepoint followed by separator)
		bool							m_bBoundary;				///< boundary flag (true immediately after boundary codepoint)
		int								m_iBoundaryOffset;			///< boundary character offset (in bytes)
		bool							m_bWasSpecial;				///< special token flag
		bool							m_bWasSynonym;				///< last token is a synonym token
		bool							m_bEscaped;					///< backslash handling flag
		int								m_iOvershortCount;			///< skipped overshort tokens count
		ESphTokenMorph					m_eTokenMorph;				///< whether last token was a generated morphological guess

		bool							m_bBlended;					///< whether last token (as in just returned from GetToken()) was blended
		bool							m_bNonBlended;				///< internal, whether there were any normal chars in that blended token
		bool							m_bBlendedPart;				///< whether last token is a normal subtoken of a blended token
		bool							m_bBlendAdd;				///< whether we have more pending blended variants (of current accumulator) to return
		BYTE							m_uBlendVariants;			///< mask of blended variants as requested by blend_mode (see BLEND_TRIM_xxx flags)
		BYTE							m_uBlendVariantsPending;	///< mask of pending blended variants (we clear bits as we return variants)
		bool							m_bBlendSkipPure;			///< skip purely blended tokens

		bool							m_bShortTokenFilter;		///< short token filter flag
		bool							m_bDetectSentences;			///< should we detect sentence boundaries?

		CSphTokenizerSettings			m_tSettings;				///< tokenizer settings
		CSphSavedFile					m_tSynFileInfo;				///< synonyms file info

	public:
		bool							m_bPhrase;
	};

	/// parse charset table
	bool					sphParseCharset(const char* sCharset, CSphVector<CSphRemapRange>& dRemaps);

	/// create UTF-8 tokenizer
	ISphTokenizer* sphCreateUTF8Tokenizer();

	/// create UTF-8 tokenizer with n-grams support (for CJK n-gram indexing)
	ISphTokenizer* sphCreateUTF8NgramTokenizer();



	class CSphTokenizerBase : public ISphTokenizer
	{
	public:
		CSphTokenizerBase();
		~CSphTokenizerBase();

		virtual bool			SetCaseFolding(const char* sConfig, CSphString& sError);
		virtual bool			LoadSynonyms(const char* sFilename, const CSphEmbeddedFiles* pFiles, CSphString& sError);
		virtual void			WriteSynonyms(CSphWriter& tWriter);
		virtual void			CloneBase(const CSphTokenizerBase* pFrom, ESphTokenizerClone eMode);

		virtual const char* GetTokenStart() const { return (const char*)m_pTokenStart; }
		virtual const char* GetTokenEnd() const { return (const char*)m_pTokenEnd; }
		virtual const char* GetBufferPtr() const { return (const char*)m_pCur; }
		virtual const char* GetBufferEnd() const { return (const char*)m_pBufferMax; }
		virtual void			SetBufferPtr(const char* sNewPtr);
		virtual uint64_t		GetSettingsFNV() const;

		virtual bool			SetBlendChars(const char* sConfig, CSphString& sError);
		virtual bool			WasTokenMultiformDestination(bool&, int&) const { return false; }

	public:
		// lightweight clones must impose a lockdown on some methods
		// (specifically those that change the lowercaser data table)

		virtual void AddPlainChar(char c)
		{
			assert(m_eMode != SPH_CLONE_QUERY_LIGHTWEIGHT);
			ISphTokenizer::AddPlainChar(c);
		}

		virtual void AddSpecials(const char* sSpecials)
		{
			assert(m_eMode != SPH_CLONE_QUERY_LIGHTWEIGHT);
			ISphTokenizer::AddSpecials(sSpecials);
		}

		virtual void Setup(const CSphTokenizerSettings& tSettings)
		{
			assert(m_eMode != SPH_CLONE_QUERY_LIGHTWEIGHT);
			ISphTokenizer::Setup(tSettings);
		}

		virtual bool RemapCharacters(const char* sConfig, DWORD uFlags, const char* sSource, bool bCanRemap, CSphString& sError)
		{
			assert(m_eMode != SPH_CLONE_QUERY_LIGHTWEIGHT);
			return ISphTokenizer::RemapCharacters(sConfig, uFlags, sSource, bCanRemap, sError);
		}

	protected:
		bool	BlendAdjust(const BYTE* pPosition);
		int		CodepointArbitrationI(int iCodepoint);
		int		CodepointArbitrationQ(int iCodepoint, bool bWasEscaped, BYTE uNextByte);

	protected:
		const BYTE* m_pBuffer;							//my buffer
		const BYTE* m_pBufferMax;						//max buffer ptr, exclusive (ie. this ptr is invalid, but every ptr below is ok)
		const BYTE* m_pCur;								//current position
		const BYTE* m_pTokenStart;						//last token start point
		const BYTE* m_pTokenEnd;						//last token end point

		BYTE				m_sAccum[3 * SPH_MAX_WORD_LEN + 3];	//folded token accumulator
		BYTE* m_pAccum;							//current accumulator position
		int					m_iAccum;							//boundary token size

		BYTE				m_sAccumBlend[3 * SPH_MAX_WORD_LEN + 3];	//blend-acc, an accumulator copy for additional blended variants
		int					m_iBlendNormalStart;					//points to first normal char in the accumulators (might be NULL)
		int					m_iBlendNormalEnd;						//points just past (!) last normal char in the accumulators (might be NULL)

		ExceptionsTrie_c* m_pExc;								//exceptions trie, if any

		bool				m_bHasBlend;
		const BYTE* m_pBlendStart;
		const BYTE* m_pBlendEnd;

		ESphTokenizerClone	m_eMode;
	};


	/// methods that get specialized with regards to charset type
	/// aka GetCodepoint() decoder and everything that depends on it
	class CSphTokenizerBase2 : public CSphTokenizerBase
	{
	protected:
		/// get codepoint
		inline int GetCodepoint()
		{
			while (m_pCur < m_pBufferMax)
			{
				int iCode = sphUTF8Decode(m_pCur);
				if (iCode >= 0)
					return iCode; // successful decode
			}
			return -1; // eof
		}

		/// accum codepoint
		inline void AccumCodepoint(int iCode)
		{
			assert(iCode > 0);
			assert(m_iAccum >= 0);

			// throw away everything which is over the token size
			bool bFit = (m_iAccum < SPH_MAX_WORD_LEN);
			bFit &= (m_pAccum - m_sAccum + SPH_MAX_UTF8_BYTES <= (int)sizeof(m_sAccum));

			if (bFit)
			{
				m_pAccum += sphUTF8Encode(m_pAccum, iCode);
				assert(m_pAccum >= m_sAccum && m_pAccum < m_sAccum + sizeof(m_sAccum));
				m_iAccum++;
			}
		}

	protected:
		BYTE* GetBlendedVariant();
		bool			CheckException(const BYTE* pStart, const BYTE* pCur, bool bQueryMode);

		template < bool IS_QUERY, bool IS_BLEND >
		BYTE* DoGetToken();

		void						FlushAccum();

	public:
		virtual int		SkipBlended();
	};


	/// UTF-8 tokenizer
	template < bool IS_QUERY >
	class CSphTokenizer_UTF8 : public CSphTokenizerBase2
	{
	public:
		CSphTokenizer_UTF8();
		virtual void				SetBuffer(const BYTE* sBuffer, int iLength);
		virtual BYTE* GetToken();
		virtual ISphTokenizer* Clone(ESphTokenizerClone eMode) const;
		virtual int					GetCodepointLength(int iCode) const;
		virtual int					GetMaxCodepointLength() const { return m_tLC.GetMaxCodepointLength(); }
	};


	/// UTF-8 tokenizer with n-grams
	template < bool IS_QUERY >
	class CSphTokenizer_UTF8Ngram : public CSphTokenizer_UTF8<IS_QUERY>
	{
	public:
		CSphTokenizer_UTF8Ngram() : m_iNgramLen(1) {}

	public:
		virtual bool		SetNgramChars(const char* sConfig, CSphString& sError);
		virtual void		SetNgramLen(int iLen);
		virtual BYTE* GetToken();

	protected:
		int					m_iNgramLen;
		CSphString			m_sNgramCharsStr;
	};


	struct StoredToken_t
	{
		BYTE			m_sToken[3 * SPH_MAX_WORD_LEN + 4];
		// tokenized state
		const char* m_szTokenStart;
		const char* m_szTokenEnd;
		const char* m_pBufferPtr;
		const char* m_pBufferEnd;
		int				m_iTokenLen;
		int				m_iOvershortCount;
		bool			m_bBoundary;
		bool			m_bSpecial;
		bool			m_bBlended;
		bool			m_bBlendedPart;
	};


	/// token filter base (boring proxy stuff)
	class CSphTokenFilter : public ISphTokenizer
	{
	protected:
		ISphTokenizer* m_pTokenizer;

	public:
		explicit						CSphTokenFilter(ISphTokenizer* pTokenizer) : m_pTokenizer(pTokenizer) {}
		~CSphTokenFilter() { SafeDelete(m_pTokenizer); }

		virtual bool					SetCaseFolding(const char* sConfig, CSphString& sError) { return m_pTokenizer->SetCaseFolding(sConfig, sError); }
		virtual void					AddPlainChar(char c) { m_pTokenizer->AddPlainChar(c); }
		virtual void					AddSpecials(const char* sSpecials) { m_pTokenizer->AddSpecials(sSpecials); }
		virtual bool					SetIgnoreChars(const char* sIgnored, CSphString& sError) { return m_pTokenizer->SetIgnoreChars(sIgnored, sError); }
		virtual bool					SetNgramChars(const char* sConfig, CSphString& sError) { return m_pTokenizer->SetNgramChars(sConfig, sError); }
		virtual void					SetNgramLen(int iLen) { m_pTokenizer->SetNgramLen(iLen); }
		virtual bool					LoadSynonyms(const char* sFilename, const CSphEmbeddedFiles* pFiles, CSphString& sError) { return m_pTokenizer->LoadSynonyms(sFilename, pFiles, sError); }
		virtual void					WriteSynonyms(CSphWriter& tWriter) { return m_pTokenizer->WriteSynonyms(tWriter); }
		virtual bool					SetBoundary(const char* sConfig, CSphString& sError) { return m_pTokenizer->SetBoundary(sConfig, sError); }
		virtual void					Setup(const CSphTokenizerSettings& tSettings) { m_pTokenizer->Setup(tSettings); }
		virtual const CSphTokenizerSettings& GetSettings() const { return m_pTokenizer->GetSettings(); }
		virtual const CSphSavedFile& GetSynFileInfo() const { return m_pTokenizer->GetSynFileInfo(); }
		virtual bool					EnableSentenceIndexing(CSphString& sError) { return m_pTokenizer->EnableSentenceIndexing(sError); }
		virtual bool					EnableZoneIndexing(CSphString& sError) { return m_pTokenizer->EnableZoneIndexing(sError); }
		virtual int						SkipBlended() { return m_pTokenizer->SkipBlended(); }

		virtual int						GetCodepointLength(int iCode) const { return m_pTokenizer->GetCodepointLength(iCode); }
		virtual int						GetMaxCodepointLength() const { return m_pTokenizer->GetMaxCodepointLength(); }

		virtual const char* GetTokenStart() const { return m_pTokenizer->GetTokenStart(); }
		virtual const char* GetTokenEnd() const { return m_pTokenizer->GetTokenEnd(); }
		virtual const char* GetBufferPtr() const { return m_pTokenizer->GetBufferPtr(); }
		virtual const char* GetBufferEnd() const { return m_pTokenizer->GetBufferEnd(); }
		virtual void					SetBufferPtr(const char* sNewPtr) { m_pTokenizer->SetBufferPtr(sNewPtr); }
		virtual uint64_t				GetSettingsFNV() const { return m_pTokenizer->GetSettingsFNV(); }

		virtual void					SetBuffer(const BYTE* sBuffer, int iLength) { m_pTokenizer->SetBuffer(sBuffer, iLength); }
		virtual BYTE* GetToken() { return m_pTokenizer->GetToken(); }

		virtual bool					WasTokenMultiformDestination(bool& bHead, int& iDestCount) const { return m_pTokenizer->WasTokenMultiformDestination(bHead, iDestCount); }
	};


	/// token filter for multiforms support
	class CSphMultiformTokenizer : public CSphTokenFilter
	{
	public:
		CSphMultiformTokenizer(ISphTokenizer* pTokenizer, const CSphMultiformContainer* pContainer);
		~CSphMultiformTokenizer();

		virtual bool					SetCaseFolding(const char* sConfig, CSphString& sError) { return m_pTokenizer->SetCaseFolding(sConfig, sError); }
		virtual void					AddPlainChar(char c) { m_pTokenizer->AddPlainChar(c); }
		virtual void					AddSpecials(const char* sSpecials) { m_pTokenizer->AddSpecials(sSpecials); }
		virtual bool					SetIgnoreChars(const char* sIgnored, CSphString& sError) { return m_pTokenizer->SetIgnoreChars(sIgnored, sError); }
		virtual bool					SetNgramChars(const char* sConfig, CSphString& sError) { return m_pTokenizer->SetNgramChars(sConfig, sError); }
		virtual void					SetNgramLen(int iLen) { m_pTokenizer->SetNgramLen(iLen); }
		virtual bool					LoadSynonyms(const char* sFilename, const CSphEmbeddedFiles* pFiles, CSphString& sError) { return m_pTokenizer->LoadSynonyms(sFilename, pFiles, sError); }
		virtual bool					SetBoundary(const char* sConfig, CSphString& sError) { return m_pTokenizer->SetBoundary(sConfig, sError); }
		virtual void					Setup(const CSphTokenizerSettings& tSettings) { m_pTokenizer->Setup(tSettings); }
		virtual const CSphTokenizerSettings& GetSettings() const { return m_pTokenizer->GetSettings(); }
		virtual const CSphSavedFile& GetSynFileInfo() const { return m_pTokenizer->GetSynFileInfo(); }
		virtual bool					EnableSentenceIndexing(CSphString& sError) { return m_pTokenizer->EnableSentenceIndexing(sError); }
		virtual bool					EnableZoneIndexing(CSphString& sError) { return m_pTokenizer->EnableZoneIndexing(sError); }

	public:
		virtual void					SetBuffer(const BYTE* sBuffer, int iLength);
		virtual BYTE* GetToken();
		virtual void					EnableTokenizedMultiformTracking() { m_bBuildMultiform = true; }
		virtual int						GetLastTokenLen() const { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_iTokenLen : m_pTokenizer->GetLastTokenLen(); }
		virtual bool					GetBoundary() { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_bBoundary : m_pTokenizer->GetBoundary(); }
		virtual bool					WasTokenSpecial() { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_bSpecial : m_pTokenizer->WasTokenSpecial(); }
		virtual int						GetOvershortCount() { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_iOvershortCount : m_pTokenizer->GetOvershortCount(); }
		virtual BYTE* GetTokenizedMultiform() { return m_sTokenizedMultiform[0] ? m_sTokenizedMultiform : NULL; }
		virtual bool					TokenIsBlended() const { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_bBlended : m_pTokenizer->TokenIsBlended(); }
		virtual bool					TokenIsBlendedPart() const { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_bBlendedPart : m_pTokenizer->TokenIsBlendedPart(); }
		virtual int						SkipBlended();

	public:
		virtual ISphTokenizer* Clone(ESphTokenizerClone eMode) const;
		virtual const char* GetTokenStart() const { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_szTokenStart : m_pTokenizer->GetTokenStart(); }
		virtual const char* GetTokenEnd() const { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_szTokenEnd : m_pTokenizer->GetTokenEnd(); }
		virtual const char* GetBufferPtr() const { return m_iStart < m_dStoredTokens.GetLength() ? m_dStoredTokens[m_iStart].m_pBufferPtr : m_pTokenizer->GetBufferPtr(); }
		virtual void					SetBufferPtr(const char* sNewPtr);
		virtual uint64_t				GetSettingsFNV() const;
		virtual bool					WasTokenMultiformDestination(bool& bHead, int& iDestCount) const;

	private:
		const CSphMultiformContainer* m_pMultiWordforms;
		int								m_iStart;
		int								m_iOutputPending;
		const CSphMultiform* m_pCurrentForm;

		bool				m_bBuildMultiform;
		BYTE				m_sTokenizedMultiform[3 * SPH_MAX_WORD_LEN + 4];

		CSphVector<StoredToken_t>		m_dStoredTokens;
	};


	/// token filter for bigram indexing
	///
	/// passes tokens through until an eligible pair is found
	/// then buffers and returns that pair as a blended token
	/// then returns the first token as a regular one
	/// then pops the first one and cycles again
	///
	/// pair (aka bigram) eligibility depends on bigram_index value
	/// "all" means that all token pairs gets indexed
	/// "first_freq" means that 1st token must be from bigram_freq_words
	/// "both_freq" means that both tokens must be from bigram_freq_words
	class CSphBigramTokenizer : public CSphTokenFilter
	{
	protected:
		enum
		{
			BIGRAM_CLEAN,	//clean slate, nothing accumulated
			BIGRAM_PAIR,	//just returned a pair from m_sBuf, and m_iFirst/m_pSecond are correct
			BIGRAM_FIRST	//just returned a first token from m_sBuf, so m_iFirst/m_pSecond are still good
		}		m_eState;
		BYTE	m_sBuf[MAX_KEYWORD_BYTES];	//pair buffer
		BYTE* m_pSecond;						//second token pointer
		int		m_iFirst;						//first token length, bytes

		ESphBigram			m_eMode;			//bigram indexing mode
		int					m_iMaxLen;			//max bigram_freq_words length
		int					m_dWordsHash[256];	//offsets into m_dWords hashed by 1st byte
		CSphVector<BYTE>	m_dWords;			//case-folded, sorted bigram_freq_words

	public:
		CSphBigramTokenizer(ISphTokenizer* pTok, ESphBigram eMode, CSphVector<CSphString>& dWords)
			: CSphTokenFilter(pTok)
		{
			assert(pTok);
			assert(eMode != SPH_BIGRAM_NONE);
			assert(eMode == SPH_BIGRAM_ALL || dWords.GetLength());

			m_sBuf[0] = 0;
			m_pSecond = NULL;
			m_eState = BIGRAM_CLEAN;
			memset(m_dWordsHash, 0, sizeof(m_dWordsHash));

			m_eMode = eMode;
			m_iMaxLen = 0;

			// only keep unique, real, short enough words
			dWords.Uniq();
			ARRAY_FOREACH(i, dWords)
			{
				int iLen =(int) Min(dWords[i].Length(), 255);
				if (!iLen)
					continue;
				m_iMaxLen = Max(m_iMaxLen, iLen);

				// hash word blocks by the first letter
				BYTE uFirst = *(BYTE*)(dWords[i].cstr());
				if (!m_dWordsHash[uFirst])
				{
					m_dWords.Add(0); // end marker for the previous block
					m_dWordsHash[uFirst] =(int) m_dWords.GetLength(); // hash new block
				}

				// store that word
				auto iPos = m_dWords.GetLength();
				m_dWords.Resize(iPos + (size_t)iLen + 1);

				m_dWords[iPos] = (BYTE)iLen;
				memcpy(&m_dWords[iPos + 1], dWords[i].cstr(), iLen);
			}
			m_dWords.Add(0);
		}

		CSphBigramTokenizer(ISphTokenizer* pTok, const CSphBigramTokenizer* pBase)
			: CSphTokenFilter(pTok)
		{
			m_sBuf[0] = 0;
			m_pSecond = NULL;
			m_eState = BIGRAM_CLEAN;
			m_eMode = pBase->m_eMode;
			m_iMaxLen = pBase->m_iMaxLen;
			memcpy(m_dWordsHash, pBase->m_dWordsHash, sizeof(m_dWordsHash));
			m_dWords = pBase->m_dWords;
		}

		ISphTokenizer* Clone(ESphTokenizerClone eMode) const
		{
			ISphTokenizer* pTok = m_pTokenizer->Clone(eMode);
			return new CSphBigramTokenizer(pTok, this);
		}

		void SetBuffer(const BYTE* sBuffer, int iLength)
		{
			m_pTokenizer->SetBuffer(sBuffer, iLength);
		}

		bool TokenIsBlended() const
		{
			if (m_eState == BIGRAM_PAIR)
				return true;
			if (m_eState == BIGRAM_FIRST)
				return false;
			return m_pTokenizer->TokenIsBlended();
		}

		bool IsFreq(int iLen, BYTE* sWord)
		{
			// early check
			if (iLen > m_iMaxLen)
				return false;

			// hash lookup, then linear scan
			int iPos = m_dWordsHash[*sWord];
			if (!iPos)
				return false;
			while (m_dWords[iPos])
			{
				if (m_dWords[iPos] == iLen && !memcmp(sWord, &m_dWords[iPos + 1], iLen))
					break;
				iPos += 1 + m_dWords[iPos];
			}
			return m_dWords[iPos] != 0;
		}

		BYTE* GetToken()
		{
			if (m_eState == BIGRAM_FIRST || m_eState == BIGRAM_CLEAN)
			{
				BYTE* pFirst;
				if (m_eState == BIGRAM_FIRST)
				{
					// first out, clean slate again, actually
					// and second will now become our next first
					assert(m_pSecond);
					m_eState = BIGRAM_CLEAN;
					pFirst = m_pSecond;
					m_pSecond = NULL;
				}
				else
				{
					// just clean slate
					// assure we're, well, clean
					assert(!m_pSecond);
					pFirst = m_pTokenizer->GetToken();
				}

				// clean slate
				// get first non-blended token
				if (!pFirst)
					return NULL;

				// pass through blended
				// could handle them as first too, but.. cumbersome
				if (m_pTokenizer->TokenIsBlended())
					return pFirst;

				// check pair
				// in first_freq and both_freq modes, 1st token must be listed
				m_iFirst =(int) strlen((const char*)pFirst);
				if (m_eMode != SPH_BIGRAM_ALL && !IsFreq(m_iFirst, pFirst))
					return pFirst;

				// copy it
				// subsequent calls can and will override token accumulator
				memcpy(m_sBuf, pFirst, m_iFirst + 1);

				// grow a pair!
				// get a second one (lookahead, in a sense)
				BYTE* pSecond = m_pTokenizer->GetToken();

				// eof? oi
				if (!pSecond)
					return m_sBuf;

				// got a pair!
				// check combined length
				m_pSecond = pSecond;
				int iSecond =(int) strlen((const char*)pSecond);
				if (m_iFirst + iSecond + 1 > SPH_MAX_WORD_LEN)
				{
					// too long pair
					// return first token as is
					m_eState = BIGRAM_FIRST;
					return m_sBuf;
				}

				// check pair
				// in freq2 mode, both tokens must be listed
				if (m_eMode == SPH_BIGRAM_BOTHFREQ && !IsFreq(iSecond, m_pSecond))
				{
					m_eState = BIGRAM_FIRST;
					return m_sBuf;
				}

				// ok, this is a eligible pair
				// begin with returning first+second pair (as blended)
				m_eState = BIGRAM_PAIR;
				m_sBuf[m_iFirst] = MAGIC_WORD_BIGRAM;
				assert(m_iFirst + strlen((const char*)pSecond) < sizeof(m_sBuf));
				strcpy((char*)m_sBuf + m_iFirst + 1, (const char*)pSecond); //NOLINT
				return m_sBuf;

			}
			else if (m_eState == BIGRAM_PAIR)
			{
				// pair (aka bigram) out, return first token as a regular token
				m_eState = BIGRAM_FIRST;
				m_sBuf[m_iFirst] = 0;
				return m_sBuf;
			}

			assert(0 && "unhandled bigram tokenizer internal state");
			return NULL;
		}

		uint64_t GetSettingsFNV() const
		{
			uint64_t uHash = CSphTokenFilter::GetSettingsFNV();
			uHash = sphFNV64(m_dWords.Begin(), m_dWords.GetLength(), uHash);
			return uHash;
		}
	};

	DWORD sphParseMorphAot(const char*);

	//fwd dec
	struct CSphTokenizerSettings;
	struct CSphFieldFilterSettings;
	class CSphIndex;


	struct CSphReconfigureSettings
	{
		CSphTokenizerSettings	m_tTokenizer;
		CSphDictSettings		m_tDict;
		CSphIndexSettings		m_tIndex;
		CSphFieldFilterSettings m_tFieldFilter;
	};

	struct CSphReconfigureSetup
	{
		ISphTokenizer* m_pTokenizer;
		CSphDict* m_pDict;
		CSphIndexSettings	m_tIndex;
		ISphFieldFilter* m_pFieldFilter;

		CSphReconfigureSetup();
		~CSphReconfigureSetup();
	};

	uint64_t sphGetSettingsFNV(const CSphIndexSettings& tSettings);

	void FillStoredTokenInfo(StoredToken_t& tToken, const BYTE* sToken, ISphTokenizer* pTokenizer);

	void			SaveTokenizerSettings(CSphWriter& tWriter, ISphTokenizer* pTokenizer, int iEmbeddedLimit);
	bool			LoadTokenizerSettings(CSphReader& tReader, CSphTokenizerSettings& tSettings, CSphEmbeddedFiles& tEmbeddedFiles, DWORD uVersion, CSphString& sWarning);

}