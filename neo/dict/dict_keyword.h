#pragma once
#include "neo/core/globals.h"
#include "neo/int/types.h"
#include "neo/dict/dict_entry.h"
#include "neo/dict/dict_crc.h"

namespace NEO {
	/// dict=keywords block reader
	class KeywordsBlockReader_c : public CSphDictEntry
	{
	private:
		const BYTE* m_pBuf;
		BYTE			m_sWord[ MAX_KEYWORD_BYTES ];
		int				m_iLen;
		BYTE			m_uHint;
		bool			m_bHaveSkips;

	public:
		explicit		KeywordsBlockReader_c(const BYTE* pBuf, bool bHaveSkiplists);
		void			Reset(const BYTE* pBuf);
		bool			UnpackWord();

		const char* GetWord() const { return (const char*)m_sWord; }
		int				GetWordLen() const { return m_iLen; }
	};


	class CSphDictKeywords : public CSphDictCRC<true>
	{
	private:
		static const int				SLOTS = 65536;
		static const int				ENTRY_CHUNK = 65536;
		static const int				KEYWORD_CHUNK = 1048576;
		static const int				DICT_CHUNK = 65536;

	public:
		// OPTIMIZE? change pointers to 8:24 locators to save RAM on x64 gear?
		struct HitblockKeyword_t
		{
			SphWordID_t					m_uWordid;			// locally unique word id (crc value, adjusted in case of collsion)
			HitblockKeyword_t* m_pNextHash;		// next hashed entry
			char* m_pKeyword;			// keyword
		};

		struct HitblockException_t
		{
			HitblockKeyword_t* m_pEntry;			// hash entry
			SphWordID_t					m_uCRC;				// original unadjusted crc

			bool operator < (const HitblockException_t& rhs) const
			{
				return m_pEntry->m_uWordid < rhs.m_pEntry->m_uWordid;
			}
		};

		struct DictKeyword_t
		{
			char* m_sKeyword;
			SphOffset_t					m_uOff;
			int							m_iDocs;
			int							m_iHits;
			BYTE						m_uHint;
			int							m_iSkiplistPos;		//position in .spe file; not exactly likely to hit 2B
		};

		struct DictBlock_t
		{
			SphOffset_t					m_iPos;
			int							m_iLen;
		};

	private:
		HitblockKeyword_t* m_dHash[SLOTS];	//hash by wordid (!)
		CSphVector<HitblockException_t>	m_dExceptions;

		bool							m_bHitblock;		//should we store words on GetWordID or not
		int								m_iMemUse;			//current memory use by all the chunks
		int								m_iDictLimit;		//allowed memory limit for dict block collection

		CSphVector<HitblockKeyword_t*>	m_dEntryChunks;		//hash chunks, only used when indexing hitblocks
		HitblockKeyword_t* m_pEntryChunk;
		int								m_iEntryChunkFree;

		CSphVector<BYTE*>				m_dKeywordChunks;	//keyword storage
		BYTE* m_pKeywordChunk;
		int								m_iKeywordChunkFree;

		CSphVector<DictKeyword_t*>		m_dDictChunks;		//dict entry chunks, only used when sorting final dict
		DictKeyword_t* m_pDictChunk;
		int								m_iDictChunkFree;

		int								m_iTmpFD;			//temp dict file descriptor
		CSphWriter						m_wrTmpDict;		//temp dict writer
		CSphVector<DictBlock_t>			m_dDictBlocks;		//on-disk locations of dict entry blocks

		char							m_sClippedWord[ MAX_KEYWORD_BYTES ]; //keyword storage for cliiped word

	private:
		SphWordID_t						HitblockGetID(const char* pWord, int iLen, SphWordID_t uCRC);
		HitblockKeyword_t* HitblockAddKeyword(DWORD uHash, const char* pWord, int iLen, SphWordID_t uID);

	public:
		explicit				CSphDictKeywords();
		virtual					~CSphDictKeywords();

		virtual void			HitblockBegin() { m_bHitblock = true; }
		virtual void			HitblockPatch(CSphWordHit* pHits, int iHits) const;
		virtual const char* HitblockGetKeyword(SphWordID_t uWordID);
		virtual int				HitblockGetMemUse() { return m_iMemUse; }
		virtual void			HitblockReset();

		virtual void			DictBegin(CSphAutofile& tTempDict, CSphAutofile& tDict, int iDictLimit, ThrottleState_t* pThrottle);
		virtual void			DictEntry(const CSphDictEntry& tEntry);
		virtual void			DictEndEntries(SphOffset_t) {}
		virtual bool			DictEnd(DictHeader_t* pHeader, int iMemLimit, CSphString& sError, ThrottleState_t* pThrottle);

		virtual SphWordID_t		GetWordID(BYTE* pWord);
		virtual SphWordID_t		GetWordIDWithMarkers(BYTE* pWord);
		virtual SphWordID_t		GetWordIDNonStemmed(BYTE* pWord);
		virtual SphWordID_t		GetWordID(const BYTE* pWord, int iLen, bool bFilterStops);
		virtual CSphDict* Clone() const { return CloneBase(new CSphDictKeywords()); }

	private:
		void					DictFlush();
	};

	//fwd dec
	class CSphDict;
	class ISphtokenizer;
	/// keyword-storing dictionary factory
	CSphDict* sphCreateDictionaryKeywords(const CSphDictSettings& tSettings, const CSphEmbeddedFiles* pFiles, const ISphTokenizer* pTokenizer, const char* sIndex, CSphString& sError);


}