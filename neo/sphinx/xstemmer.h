#pragma once

//#include "neo/sphinx.h"
#include "neo/tokenizer/tokenizer.h"


	/// initialize English stemmar
	void	stem_en_init();

	/// initialize Russian stemmar
	void	stem_ru_init();

	/// stem lowercase English word
	void	stem_en(BYTE* pWord, int iLen);

	/// stem lowercase Russian word in Windows-1251 encoding
	void	stem_ru_cp1251(BYTE* pWord);

	/// stem lowercase Russian word in UTF-8 encoding
	void	stem_ru_utf8(WORD* pWord);

	/// initialize Czech stemmer
	void	stem_cz_init();

	/// stem lowercase Czech word
	void	stem_cz(BYTE* pWord);

	/// stem Arabic word in UTF-8 encoding
	void	stem_ar_utf8(BYTE* word);

	/// calculate soundex in-place if the word is lowercase English letters only;
	/// do nothing if it's not
	void	stem_soundex(BYTE* pWord);

	namespace NEO {

	/// double metaphone stemmer
	void	stem_dmetaphone(BYTE* pWord);

		//fwd dec
		class CSphDict;

	/// pre-init AOT setup, cache size (in bytes)
	void	sphAotSetCacheSize(int iCacheSize);

	/// init AOT lemmatizer
	bool	sphAotInit(const CSphString& sDictFile, CSphString& sError, int iLang);

	// functions below by design used in indexing time
	/// lemmatize (or guess a normal form) a Russian word in Windows-1251 encoding
	void	sphAotLemmatizeRu1251(BYTE* pWord);

	/// lemmatize (or guess a normal form) a Russian word in UTF-8 encoding, return a single "best" lemma
	void	sphAotLemmatizeRuUTF8(BYTE* pWord);

	/// lemmatize (or guess a normal form) a German word in Windows-1252 encoding
	void	sphAotLemmatizeDe1252(BYTE* pWord);

	/// lemmatize (or guess a normal form) a German word in UTF-8 encoding, return a single "best" lemma
	void	sphAotLemmatizeDeUTF8(BYTE* pWord);

	/// lemmatize (or guess a normal form) a word in single-byte ASCII encoding, return a single "best" lemma
	void	sphAotLemmatize(BYTE* pWord, int iLang);

	// functions below by design used in search time
	/// lemmatize (or guess a normal form) a Russian word, return all lemmas
	void	sphAotLemmatizeRu(CSphVector<CSphString>& dLemmas, const BYTE* pWord);
	void	sphAotLemmatizeDe(CSphVector<CSphString>& dLemmas, const BYTE* pWord);
	void	sphAotLemmatize(CSphVector<CSphString>& dLemmas, const BYTE* pWord, int iLang);

	/// get lemmatizer dictionary info (file name, crc)
	const CSphNamedInt& sphAotDictinfo(int iLang);

	/// create token filter that returns all morphological hypotheses
	/// NOTE, takes over wordforms from pDict, in AOT case they must be handled by the fitler
	class CSphTokenFilter;
	CSphTokenFilter* sphAotCreateFilter(ISphTokenizer* pTokenizer, CSphDict* pDict, bool bIndexExact, DWORD uLangMask);

	/// free lemmatizers on shutdown
	void	sphAotShutdown();

}
