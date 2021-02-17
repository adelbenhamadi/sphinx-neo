#pragma once
#include "neo/int/types.h"
#include "neo/dict/dict.h"
#include "neo/io/crc32.h"



namespace NEO {
	struct Word_t
	{
		const char* m_sWord;
		int				m_iCount;
	};

	inline bool operator < (const Word_t& a, const Word_t& b)
	{
		return a.m_iCount < b.m_iCount;
	}
	
	/// ///////////////////////////////////

	template < typename T > struct CSphMTFHashEntry
	{
		CSphString				m_sKey;
		CSphMTFHashEntry<T>* m_pNext;
		int						m_iSlot;
		T						m_tValue;
	};


	template < typename T, int SIZE, class HASHFUNC > 
	class CSphMTFHash
	{
	public:
		/// ctor
		CSphMTFHash()
		{
			m_pData = new CSphMTFHashEntry<T> *[SIZE];
			for (int i = 0; i < SIZE; i++)
				m_pData[i] = NULL;
		}

		/// dtor
		~CSphMTFHash()
		{
			for (int i = 0; i < SIZE; i++)
			{
				CSphMTFHashEntry<T>* pHead = m_pData[i];
				while (pHead)
				{
					CSphMTFHashEntry<T>* pNext = pHead->m_pNext;
					SafeDelete(pHead);
					pHead = pNext;
				}
			}
		}

		/// add record to hash
		/// OPTIMIZE: should pass T not by reference for simple types
		T& Add(const char* sKey, int iKeyLen, T& tValue)
		{
			DWORD uHash = HASHFUNC::Hash(sKey) % SIZE;

			// find matching entry
			CSphMTFHashEntry<T>* pEntry = m_pData[uHash];
			CSphMTFHashEntry<T>* pPrev = NULL;
			while (pEntry && strcmp(sKey, pEntry->m_sKey.cstr()))
			{
				pPrev = pEntry;
				pEntry = pEntry->m_pNext;
			}

			if (!pEntry)
			{
				// not found, add it, but don't MTF
				pEntry = new CSphMTFHashEntry<T>;
				if (iKeyLen)
					pEntry->m_sKey.SetBinary(sKey, iKeyLen);
				else
					pEntry->m_sKey = sKey;
				pEntry->m_pNext = NULL;
				pEntry->m_iSlot = (int)uHash;
				pEntry->m_tValue = tValue;
				if (!pPrev)
					m_pData[uHash] = pEntry;
				else
					pPrev->m_pNext = pEntry;
			}
			else
			{
				// MTF on access
				if (pPrev)
				{
					pPrev->m_pNext = pEntry->m_pNext;
					pEntry->m_pNext = m_pData[uHash];
					m_pData[uHash] = pEntry;
				}
			}

			return pEntry->m_tValue;
		}

		/// find first non-empty entry
		const CSphMTFHashEntry<T>* FindFirst()
		{
			for (int i = 0; i < SIZE; i++)
				if (m_pData[i])
					return m_pData[i];
			return NULL;
		}

		/// find next non-empty entry
		const CSphMTFHashEntry<T>* FindNext(const CSphMTFHashEntry<T>* pEntry)
		{
			assert(pEntry);
			if (pEntry->m_pNext)
				return pEntry->m_pNext;

			for (int i = 1 + pEntry->m_iSlot; i < SIZE; i++)
				if (m_pData[i])
					return m_pData[i];
			return NULL;
		}

	protected:
		CSphMTFHashEntry<T>** m_pData;
	};


	/////////////////////////////////////////////////////////////////////////////

	class CSphStopwordBuilderDict : public CSphDict
	{
	public:
		CSphStopwordBuilderDict() {}
		void				Save(const char* sOutput, int iTop, bool bFreqs);

	public:
		virtual SphWordID_t	GetWordID(BYTE* pWord);
		virtual SphWordID_t	GetWordID(const BYTE* pWord, int iLen, bool);

		virtual void		LoadStopwords(const char*, const ISphTokenizer*) {}
		virtual void		LoadStopwords(const CSphVector<SphWordID_t>&) {}
		virtual void		WriteStopwords(CSphWriter&) {}
		virtual bool		LoadWordforms(const CSphVector<CSphString>&, const CSphEmbeddedFiles*, const ISphTokenizer*, const char*) { return true; }
		virtual void		WriteWordforms(CSphWriter&) {}
		virtual int			SetMorphology(const char*, CSphString&) { return ST_OK; }

		virtual void		Setup(const CSphDictSettings& tSettings) { m_tSettings = tSettings; }
		virtual const CSphDictSettings& GetSettings() const { return m_tSettings; }
		virtual const CSphVector <CSphSavedFile>& GetStopwordsFileInfos() { return m_dSWFileInfos; }
		virtual const CSphVector <CSphSavedFile>& GetWordformsFileInfos() { return m_dWFFileInfos; }
		virtual const CSphMultiformContainer* GetMultiWordforms() const { return NULL; }
		virtual uint64_t		GetSettingsFNV() const { return 0; }

		virtual bool IsStopWord(const BYTE*) const { return false; }

	protected:
		struct HashFunc_t
		{
			static inline DWORD Hash(const char* sKey)
			{
				return sphCRC32(sKey);
			}
		};

	protected:
		CSphMTFHash < int, 1048576, HashFunc_t >	m_hWords;

		// fake setttings
		CSphDictSettings			m_tSettings;
		CSphVector <CSphSavedFile>	m_dSWFileInfos;
		CSphVector <CSphSavedFile>	m_dWFFileInfos;
	};

}