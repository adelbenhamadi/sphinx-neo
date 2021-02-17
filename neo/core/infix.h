#pragma once
#include "neo/int/types.h"
#include "neo/io/writer.h"
#include "neo/core/globals.h"
#include "neo/core/generic.h"
#include "neo/io/unzip.h"
#include "neo/tools/utf8_tools.h"

namespace NEO {

	template < int SIZE >
	struct Infix_t
	{
		DWORD m_Data[SIZE];

#ifndef NDEBUG
		BYTE m_TrailingZero;

		Infix_t()
			: m_TrailingZero(0)
		{}
#endif

		void Reset()
		{
			for (int i = 0; i < SIZE; i++)
				m_Data[i] = 0;
		}

		bool operator == (const Infix_t<SIZE>& rhs) const;
	};




	struct InfixBlock_t
	{
		union
		{
			const char* m_sInfix;
			DWORD			m_iInfixOffset;
		};
		DWORD				m_iOffset;


	};




	struct InfixIntvec_t
	{
	public:
		// do not change the order of fields in this union - it matters a lot
		union
		{
			DWORD			m_dData[4];
			struct
			{
				int				m_iDynLen;
				int				m_iDynLimit;
				DWORD* m_pDynData;
			};
		};

	public:
		InfixIntvec_t()
		{
			m_dData[0] = 0;
			m_dData[1] = 0;
			m_dData[2] = 0;
			m_dData[3] = 0;
		}

		~InfixIntvec_t()
		{
			if (IsDynamic())
				SafeDeleteArray(m_pDynData);
		}

		bool IsDynamic() const
		{
			return (m_dData[0] & 0x80000000UL) != 0;
		}

		void Add(DWORD uVal)
		{
			if (!m_dData[0])
			{
				// empty
				m_dData[0] = uVal | (1UL << 24);

			}
			else if (!IsDynamic())
			{
				// 1..4 static entries
				int iLen = m_dData[0] >> 24;
				DWORD uLast = m_dData[iLen - 1] & 0xffffffUL;

				// redundant
				if (uVal == uLast)
					return;

				// grow static part
				if (iLen < 4)
				{
					m_dData[iLen] = uVal;
					m_dData[0] = (m_dData[0] & 0xffffffUL) | (++iLen << 24);
					return;
				}

				// dynamize
				DWORD* pDyn = new DWORD[16];
				pDyn[0] = m_dData[0] & 0xffffffUL;
				pDyn[1] = m_dData[1];
				pDyn[2] = m_dData[2];
				pDyn[3] = m_dData[3];
				pDyn[4] = uVal;
				m_iDynLen = 0x80000005UL; // dynamic flag, len=5
				m_iDynLimit = 16; // limit=16
				m_pDynData = pDyn;

			}
			else
			{
				// N dynamic entries
				int iLen = m_iDynLen & 0xffffffUL;
				if (uVal == m_pDynData[iLen - 1])
					return;
				if (iLen >= m_iDynLimit)
				{
					m_iDynLimit *= 2;
					DWORD* pNew = new DWORD[m_iDynLimit];
					for (int i = 0; i < iLen; i++)
						pNew[i] = m_pDynData[i];
					SafeDeleteArray(m_pDynData);
					m_pDynData = pNew;
				}

				m_pDynData[iLen] = uVal;
				m_iDynLen++;
			}
		}

		bool operator == (const InfixIntvec_t& rhs) const
		{
			// check dynflag, length, maybe first element
			if (m_dData[0] != rhs.m_dData[0])
				return false;

			// check static data
			if (!IsDynamic())
			{
				for (int i = 1; i < (int)(m_dData[0] >> 24); i++)
					if (m_dData[i] != rhs.m_dData[i])
						return false;
				return true;
			}

			// check dynamic data
			const DWORD* a = m_pDynData;
			const DWORD* b = rhs.m_pDynData;
			const DWORD* m = a + (m_iDynLen & 0xffffffUL);
			while (a < m)
				if (*a++ != *b++)
					return false;
			return true;
		}

	public:
		int GetLength() const
		{
			if (!IsDynamic())
				return m_dData[0] >> 24;
			return m_iDynLen & 0xffffffUL;
		}

		DWORD operator[] (int iIndex)const
		{
			if (!IsDynamic())
				return m_dData[iIndex] & 0xffffffUL;
			return m_pDynData[iIndex];
		}
	};




	/// infix hash builder
	class ISphInfixBuilder
	{
	public:
		explicit		ISphInfixBuilder() {}
		virtual			~ISphInfixBuilder() {}
		virtual void	AddWord(const BYTE* pWord, int iWordLength, int iCheckpoint, bool bHasMorphology) = 0;
		virtual void	SaveEntries(CSphWriter& wrDict) = 0;
		virtual int64_t	SaveEntryBlocks(CSphWriter& wrDict) = 0;
		virtual int		GetBlocksWordsSize() const = 0;
	};

	template < int SIZE >
	struct InfixHashEntry_t
	{
		Infix_t<SIZE>	m_tKey;		//key, owned by the hash
		InfixIntvec_t	m_tValue;	//data, owned by the hash
		int				m_iNext;	//next entry in hash arena
	};



	template < int SIZE >
	struct InfixHashCmp_fn
	{
		InfixHashEntry_t<SIZE>* m_pBase;

		explicit InfixHashCmp_fn(InfixHashEntry_t<SIZE>* pBase)
			: m_pBase(pBase)
		{}

		bool IsLess(int a, int b) const
		{
			return strncmp((const char*)m_pBase[a].m_tKey.m_Data, (const char*)m_pBase[b].m_tKey.m_Data, sizeof(DWORD) * SIZE) < 0;
		}
	};


	template < int SIZE >
	class InfixBuilder_c : public ISphInfixBuilder
	{
	protected:
		static const int							LENGTH = 1048576;

	protected:
		int											m_dHash[LENGTH];		//all the hash entries
		CSphSwapVector < InfixHashEntry_t<SIZE> >	m_dArena;
		CSphVector<InfixBlock_t>					m_dBlocks;
		CSphTightVector<BYTE>						m_dBlocksWords;

	public:
		InfixBuilder_c();
		virtual void	AddWord(const BYTE* pWord, int iWordLength, int iCheckpoint, bool bHasMorphology);
		virtual void	SaveEntries(CSphWriter& wrDict);
		virtual int64_t	SaveEntryBlocks(CSphWriter& wrDict);
		virtual int		GetBlocksWordsSize() const { return m_dBlocksWords.GetLength(); }

	protected:
		/// add new entry
		void AddEntry(const Infix_t<SIZE>& tKey, DWORD uHash, int iCheckpoint)
		{
			uHash &= (LENGTH - 1);

			int iEntry = m_dArena.GetLength();
			InfixHashEntry_t<SIZE>& tNew = m_dArena.Add();
			tNew.m_tKey = tKey;
			tNew.m_tValue.m_dData[0] = 0x1000000UL | iCheckpoint; // len=1, data=iCheckpoint
			tNew.m_iNext = m_dHash[uHash];
			m_dHash[uHash] = iEntry;
		}

		/// get value pointer by key
		InfixIntvec_t* LookupEntry(const Infix_t<SIZE>& tKey, DWORD uHash)
		{
			uHash &= (LENGTH - 1);
			int iEntry = m_dHash[uHash];
			int iiEntry = 0;

			while (iEntry)
			{
				if (m_dArena[iEntry].m_tKey == tKey)
				{
					// mtf it, if needed
					if (iiEntry)
					{
						m_dArena[iiEntry].m_iNext = m_dArena[iEntry].m_iNext;
						m_dArena[iEntry].m_iNext = m_dHash[uHash];
						m_dHash[uHash] = iEntry;
					}
					return &m_dArena[iEntry].m_tValue;
				}
				iiEntry = iEntry;
				iEntry = m_dArena[iEntry].m_iNext;
			}
			return NULL;
		}
	};



	ISphInfixBuilder* sphCreateInfixBuilder(int iCodepointBytes, CSphString* pError);

	bool sphLookupInfixCheckpoints(const char* sInfix, int iBytes, const BYTE* pInfixes, const CSphVector<InfixBlock_t>& dInfixBlocks, int iInfixCodepointBytes, CSphVector<DWORD>& dCheckpoints);


	// calculate length, upto iInfixCodepointBytes chars from infix start
	int sphGetInfixLength(const char* sInfix, int iBytes, int iInfixCodepointBytes);


}