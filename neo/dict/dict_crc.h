#pragma once
#include "neo/int/types.h"
#include "neo/dict/dict.h"
#include "neo/tokenizer/form.h"
#include "neo/dict/word_forms.h"
#include "neo/core/word_list.h"
#include "neo/io/writer.h"
#include "neo/io/autofile.h"

namespace NEO {

	
	//fwd dec
	class ISphTokenizer;

	struct CSphTemplateDictTraits : CSphDict
	{
		CSphTemplateDictTraits();
		virtual				~CSphTemplateDictTraits();

		virtual void		LoadStopwords(const char* sFiles, const ISphTokenizer* pTokenizer);
		virtual void		LoadStopwords(const CSphVector<SphWordID_t>& dStopwords);
		virtual void		WriteStopwords(CSphWriter& tWriter);
		virtual bool		LoadWordforms(const CSphVector<CSphString>& dFiles, const CSphEmbeddedFiles* pEmbedded, const ISphTokenizer* pTokenizer, const char* sIndex);
		virtual void		WriteWordforms(CSphWriter& tWriter);
		virtual const CSphWordforms* GetWordforms() { return m_pWordforms; }
		virtual void		DisableWordforms() { m_bDisableWordforms = true; }
		virtual int			SetMorphology(const char* szMorph, CSphString& sMessage);
		virtual bool		HasMorphology() const;
		virtual void		ApplyStemmers(BYTE* pWord) const;

		virtual void		Setup(const CSphDictSettings& tSettings) { m_tSettings = tSettings; }
		virtual const CSphDictSettings& GetSettings() const { return m_tSettings; }
		virtual const CSphVector <CSphSavedFile>& GetStopwordsFileInfos() { return m_dSWFileInfos; }
		virtual const CSphVector <CSphSavedFile>& GetWordformsFileInfos() { return m_dWFFileInfos; }
		virtual const CSphMultiformContainer* GetMultiWordforms() const;
		virtual uint64_t	GetSettingsFNV() const;
		static void			SweepWordformContainers(const CSphVector<CSphSavedFile>& dFiles);

	protected:
		CSphVector < int >	m_dMorph;
#if USE_LIBSTEMMER
		CSphVector < sb_stemmer* >	m_dStemmers;
		CSphVector<CSphString> m_dDescStemmers;
#endif

		int					m_iStopwords;	//stopwords count
		SphWordID_t* m_pStopwords;	//stopwords ID list
		CSphFixedVector<SphWordID_t> m_dStopwordContainer;

	protected:
		int					ParseMorphology(const char* szMorph, CSphString& sError);
		SphWordID_t			FilterStopword(SphWordID_t uID) const;	//filter ID against stopwords list
		CSphDict* CloneBase(CSphTemplateDictTraits* pDict) const;
		virtual bool		HasState() const;

		bool				m_bDisableWordforms;

	private:
		CSphWordforms* m_pWordforms;
		CSphVector<CSphSavedFile>	m_dSWFileInfos;
		CSphVector<CSphSavedFile>	m_dWFFileInfos;
		CSphDictSettings			m_tSettings;

		static CSphVector<CSphWordforms*>		m_dWordformContainers;

		CSphWordforms* GetWordformContainer(const CSphVector<CSphSavedFile>& dFileInfos, const CSphVector<CSphString>* pEmbeddedWordforms, const ISphTokenizer* pTokenizer, const char* sIndex);
		CSphWordforms* LoadWordformContainer(const CSphVector<CSphSavedFile>& dFileInfos, const CSphVector<CSphString>* pEmbeddedWordforms, const ISphTokenizer* pTokenizer, const char* sIndex);

		int					InitMorph(const char* szMorph, int iLength, CSphString& sError);
		int					AddMorph(int iMorph); //helper that always returns ST_OK
		bool				StemById(BYTE* pWord, int iStemmer) const;
		void				AddWordform(CSphWordforms* pContainer, char* sBuffer, int iLen, ISphTokenizer* pTokenizer, const char* szFile, const CSphVector<int>& dBlended, int iFileId);
	};



	/// common CRC32/64 dictionary stuff
	struct CSphDiskDictTraits : CSphTemplateDictTraits
	{
		CSphDiskDictTraits()
			: m_iEntries(0)
			, m_iLastDoclistPos(0)
			, m_iLastWordID(0)
		{}
		virtual				~CSphDiskDictTraits() {}

		virtual void DictBegin(CSphAutofile& tTempDict, CSphAutofile& tDict, int iDictLimit, ThrottleState_t* pThrottle);
		virtual void DictEntry(const CSphDictEntry& tEntry);
		virtual void DictEndEntries(SphOffset_t iDoclistOffset);
		virtual bool DictEnd(DictHeader_t* pHeader, int iMemLimit, CSphString& sError, ThrottleState_t*);
		virtual bool DictIsError() const { return m_wrDict.IsError(); }

	protected:

		CSphTightVector<CSphWordlistCheckpoint>	m_dCheckpoints;		//checkpoint offsets
		CSphWriter			m_wrDict;			//final dict file writer
		CSphString			m_sWriterError;		//writer error message storage
		int					m_iEntries;			//dictionary entries stored
		SphOffset_t			m_iLastDoclistPos;
		SphWordID_t			m_iLastWordID;
	};


	template < bool CRCALGO >
	struct CCRCEngine
	{
		inline static SphWordID_t		DoCrc(const BYTE* pWord);
		inline static SphWordID_t		DoCrc(const BYTE* pWord, int iLen);
	};

	/// specialized CRC32/64 implementations
	template < bool CRC32DICT >
	struct CSphDictCRC : public CSphDiskDictTraits, CCRCEngine<CRC32DICT>
	{
		typedef CCRCEngine<CRC32DICT> tHASH;
		virtual SphWordID_t		GetWordID(BYTE* pWord);
		virtual SphWordID_t		GetWordID(const BYTE* pWord, int iLen, bool bFilterStops);
		virtual SphWordID_t		GetWordIDWithMarkers(BYTE* pWord);
		virtual SphWordID_t		GetWordIDNonStemmed(BYTE* pWord);
		virtual bool			IsStopWord(const BYTE* pWord) const;

		virtual CSphDict* Clone() const { return CloneBase(new CSphDictCRC<CRC32DICT>()); }
	};

	struct CSphDictTemplate : public CSphTemplateDictTraits, CCRCEngine<false> // based on flv64
	{
		virtual SphWordID_t		GetWordID(BYTE* pWord);
		virtual SphWordID_t		GetWordID(const BYTE* pWord, int iLen, bool bFilterStops);
		virtual SphWordID_t		GetWordIDWithMarkers(BYTE* pWord);
		virtual SphWordID_t		GetWordIDNonStemmed(BYTE* pWord);
		virtual bool			IsStopWord(const BYTE* pWord) const;

		virtual CSphDict* Clone() const { return CloneBase(new CSphDictTemplate()); }
	};


	/// CRC32/FNV64 dictionary factory
	CSphDict* sphCreateDictionaryCRC(const CSphDictSettings& tSettings, const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex, CSphString& sError);


	/// traits dictionary factory (no storage, only tokenizing, lemmatizing, etc.)
	CSphDict* sphCreateDictionaryTemplate(const CSphDictSettings& tSettings, const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex, CSphString& sError);



}