#pragma once

#include "neo/int/types.h"
#include "neo/int/throttle_state.h"
#include "neo/dict/dict_settings.h"
#include "neo/dict/dict_entry.h"
#include "neo/dict/dict_header.h"
#include "neo/dict/word_forms.h"
#include "neo/io/autofile.h"
#include "neo/io/file.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/core/word_hit.h"
#include "neo/tokenizer/tokenizer.h"

namespace NEO {


	/// stored normal form
	struct CSphStoredNF
	{
		CSphString					m_sWord;
		bool						m_bAfterMorphology;
	};



	//forward declarations
	//struct CSphWordHit;
	//class CSphAutofile;
	//struct DictHeader_t;
	//struct ThrottleState_t;
	//class ISphTokenizer;

	class CSphDict
	{
	public:
		static const int	ST_OK = 0;
		static const int	ST_ERROR = 1;
		static const int	ST_WARNING = 2;

	public:
		/// virtualizing dtor
		virtual				~CSphDict() {}

		/// Get word ID by word, "text" version
		/// may apply stemming and modify word inplace
		/// modified word may become bigger than the original one, so make sure you have enough space in buffer which is pointer by pWord
		/// a general practice is to use char[3*SPH_MAX_WORD_LEN+4] as a buffer
		/// returns 0 for stopwords
		virtual SphWordID_t	GetWordID(BYTE* pWord) = 0;

		/// get word ID by word, "text" version
		/// may apply stemming and modify word inplace
		/// accepts words with already prepended MAGIC_WORD_HEAD
		/// appends MAGIC_WORD_TAIL
		/// returns 0 for stopwords
		virtual SphWordID_t	GetWordIDWithMarkers(BYTE* pWord) { return GetWordID(pWord); }

		/// get word ID by word, "text" version
		/// does NOT apply stemming
		/// accepts words with already prepended MAGIC_WORD_HEAD_NONSTEMMED
		/// returns 0 for stopwords
		virtual SphWordID_t	GetWordIDNonStemmed(BYTE* pWord) { return GetWordID(pWord); }

		/// get word ID by word, "binary" version
		/// only used with prefix/infix indexing
		/// must not apply stemming and modify anything
		/// filters stopwords on request
		virtual SphWordID_t	GetWordID(const BYTE* pWord, int iLen, bool bFilterStops) = 0;

		/// apply stemmers to the given word
		virtual void		ApplyStemmers(BYTE*) const {}

		/// load stopwords from given files
		virtual void		LoadStopwords(const char* sFiles, const ISphTokenizer* pTokenizer) = 0;

		/// load stopwords from an array
		virtual void		LoadStopwords(const CSphVector<SphWordID_t>& dStopwords) = 0;

		/// write stopwords to a file
		virtual void		WriteStopwords( CSphWriter& tWriter) = 0;

		/// load wordforms from a given list of files
		virtual bool		LoadWordforms(const CSphVector<CSphString>&, const CSphEmbeddedFiles* pEmbedded, const ISphTokenizer* pTokenizer, const char* sIndex) = 0;

		/// write wordforms to a file
		virtual void		WriteWordforms(CSphWriter& tWriter) = 0;

		/// get wordforms
		virtual const CSphWordforms* GetWordforms() { return NULL; }

		/// disable wordforms processing
		virtual void		DisableWordforms() {}

		/// set morphology
		/// returns 0 on success, 1 on hard error, 2 on a warning (see ST_xxx constants)
		virtual int			SetMorphology(const char* szMorph, CSphString& sMessage) = 0;

		/// are there any morphological processors?
		virtual bool		HasMorphology() const { return false; }

		/// morphological data fingerprint (lemmatizer filenames and crc32s)
		virtual const CSphString& GetMorphDataFingerprint() const { return m_sMorphFingerprint; }

		/// setup dictionary using settings
		virtual void		Setup(const CSphDictSettings& tSettings) = 0;

		/// get dictionary settings
		virtual const CSphDictSettings& GetSettings() const = 0;

		/// stopwords file infos
		virtual const CSphVector <CSphSavedFile>& GetStopwordsFileInfos() = 0;

		/// wordforms file infos
		virtual const CSphVector <CSphSavedFile>& GetWordformsFileInfos() = 0;

		/// get multiwordforms
		virtual const CSphMultiformContainer* GetMultiWordforms() const = 0;

		/// check what given word is stopword
		virtual bool IsStopWord(const BYTE* pWord) const = 0;

	public:
		/// enable actually collecting keywords (needed for stopwords/wordforms loading)
		virtual void			HitblockBegin() {}

		/// callback to let dictionary do hit block post-processing
		virtual void			HitblockPatch(CSphWordHit*, int) const {}

		/// resolve temporary hit block wide wordid (!) back to keyword
		virtual const char* HitblockGetKeyword(SphWordID_t) { return NULL; }

		/// check current memory usage
		virtual int				HitblockGetMemUse() { return 0; }

		/// hit block dismissed
		virtual void			HitblockReset() {}

	public:
		/// begin creating dictionary file, setup any needed internal structures
		virtual void			DictBegin(CSphAutofile& tTempDict, CSphAutofile& tDict, int iDictLimit, ThrottleState_t* pThrottle);

		/// add next keyword entry to final dict
		virtual void			DictEntry(const CSphDictEntry& tEntry);

		/// flush last entry
		virtual void			DictEndEntries(SphOffset_t iDoclistOffset);

		/// end indexing, store dictionary and checkpoints
		virtual bool			DictEnd(DictHeader_t* pHeader, int iMemLimit, CSphString& sError, ThrottleState_t* pThrottle);

		/// check whether there were any errors during indexing
		virtual bool			DictIsError() const;

	public:
		/// check whether this dict is stateful (when it comes to lookups)
		virtual bool			HasState() const { return false; }

		/// make a clone
		virtual CSphDict* Clone() const { return NULL; }

		/// get settings hash
		virtual uint64_t		GetSettingsFNV() const = 0;

	protected:
		CSphString				m_sMorphFingerprint;
	};


	/// clear wordform cache
	void sphShutdownWordforms();

	/// update/clear global IDF cache
	bool sphPrereadGlobalIDF(const CSphString& sPath, CSphString& sError);
	void sphUpdateGlobalIDFs(const CSphVector<CSphString>& dFiles);
	void sphInitGlobalIDFs();
	void sphShutdownGlobalIDFs();


	void SaveDictionarySettings(CSphWriter& tWriter, CSphDict* pDict, bool bForceWordDict, int iEmbeddedLimit);
	void LoadDictionarySettings(CSphReader& tReader, CSphDictSettings& tSettings, CSphEmbeddedFiles& tEmbeddedFiles, DWORD uVersion, CSphString& sWarning);

	CSphDict* SetupDictionary(CSphDict* pDict, const CSphDictSettings& tSettings,
		const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex,
		CSphString& sError);


}