#pragma once
#include "neo/core/globals.h"
#include "neo/io/writer.h"
#include "neo/int/types.h"

#include "neo/sphinx/xutility.h"

#include <cassert>

namespace NEO {

	/// exceptions trie, stored in a tidy simple blob
	/// we serialize each trie node as follows:
	///
	/// int result_offset, 0 if no output mapping
	/// BYTE num_bytes, 0 if no further valid bytes can be accepted
	/// BYTE values[num_bytes], known accepted byte values
	/// BYTE offsets[num_bytes], and the respective next node offsets
	///
	/// output mappings themselves are serialized just after the nodes,
	/// as plain old ASCIIZ strings
	class ExceptionsTrie_c
	{
		friend class		ExceptionsTrieGen_c;

	protected:
		size_t					m_dFirst[256];	//table to speedup 1st byte lookup
		CSphVector<BYTE>	m_dData;		//data blob
		size_t					m_iCount;		//number of exceptions
		size_t					m_iMappings;	//offset where the nodes end, and output mappings start

	public:
		const BYTE* GetMapping(size_t i) const;

		size_t GetFirst(BYTE v) const
		{
			return m_dFirst[v];
		}

		size_t GetNext(size_t i, BYTE v) const;

	public:
		void Export(CSphWriter& w) const;

	protected:
		void Export(CSphWriter& w, CSphVector<BYTE>& dPrefix, size_t iNode, size_t* pCount) const
		{
			assert(iNode >= 0 && iNode < m_iMappings);
			const BYTE* p = &m_dData[iNode];

			size_t iTo = *(size_t*)p;
			if (iTo > 0)
			{
				CSphString s;
				const char* sTo = (char*)&m_dData[iTo];
				s.SetBinary((char*)dPrefix.Begin(), dPrefix.GetLength());
				s.SetSprintf("%s => %s\n", s.cstr(), sTo);
				w.PutString(s.cstr());
				(*pCount)++;
			}

			auto n = p[4];
			if (n == 0)
				return;

			p += 5;
			for (size_t i = 0; i < n; i++)
			{
				dPrefix.Add(p[i]);
				Export(w, dPrefix, *(size_t*)&p[n + 4 * i], pCount);
				dPrefix.Pop();
			}
		}
	};


	/// intermediate exceptions trie node
	/// only used by ExceptionsTrieGen_c, while building a blob
	class ExceptionsTrieNode_c
	{
		friend class						ExceptionsTrieGen_c;

	protected:
		struct Entry_t
		{
			BYTE					m_uValue;
			ExceptionsTrieNode_c* m_pKid;
		};

		CSphString					m_sTo;		//output mapping for current prefix, if any
		CSphVector<Entry_t>			m_dKids;	//known and accepted incoming byte values

	public:
		~ExceptionsTrieNode_c()
		{
			ARRAY_FOREACH(i, m_dKids)
				SafeDelete(m_dKids[i].m_pKid);
		}

		/// returns false on a duplicate "from" part, or true on success
		bool AddMapping(const BYTE* sFrom, const BYTE* sTo)
		{
			// no more bytes to consume? this is our output mapping then
			if (!*sFrom)
			{
				if (!m_sTo.IsEmpty())
					return false;
				m_sTo = (const char*)sTo;
				return true;
			}

			size_t i;
			for (i = 0; i < m_dKids.GetLength(); i++)
				if (m_dKids[i].m_uValue == *sFrom)
					break;
			if (i == m_dKids.GetLength())
			{
				Entry_t& t = m_dKids.Add();
				t.m_uValue = *sFrom;
				t.m_pKid = new ExceptionsTrieNode_c();
			}
			return m_dKids[i].m_pKid->AddMapping(sFrom + 1, sTo);
		}
	};


	/// exceptions trie builder
	/// plain old text mappings in, nice useful trie out
	class ExceptionsTrieGen_c
	{
	protected:
		ExceptionsTrieNode_c* m_pRoot;
		int						m_iCount;

	public:
		ExceptionsTrieGen_c()
		{
			m_pRoot = new ExceptionsTrieNode_c();
			m_iCount = 0;
		}

		~ExceptionsTrieGen_c()
		{
			SafeDelete(m_pRoot);
		}

		/// trims left/right whitespace, folds inner whitespace
		void FoldSpace(char* s) const
		{
			// skip leading spaces
			char* d = s;
			while (*s && sphIsSpace(*s))
				s++;

			// handle degenerate (empty string) case
			if (!*s)
			{
				*d = '\0';
				return;
			}

			while (*s)
			{
				// copy another token, add exactly 1 space after it, and skip whitespace
				while (*s && !sphIsSpace(*s))
					*d++ = *s++;
				*d++ = ' ';
				while (sphIsSpace(*s))
					s++;
			}

			// replace that last space that we added
			d[-1] = '\0';
		}

		bool ParseLine(char* sBuffer, CSphString& sError)
		{
#define LOC_ERR(_arg) { sError = _arg; return false; }
			assert(m_pRoot);

			// extract map-from and map-to parts
			char* sSplit = strstr(sBuffer, "=>");
			if (!sSplit)
				LOC_ERR("mapping token (=>) not found");

			char* sFrom = sBuffer;
			char* sTo = sSplit + 2; // skip "=>"
			*sSplit = '\0';

			// trim map-from, map-to
			FoldSpace(sFrom);
			FoldSpace(sTo);
			if (!*sFrom)
				LOC_ERR("empty map-from part");
			if (!*sTo)
				LOC_ERR("empty map-to part");
			if (strlen(sFrom) > MAX_KEYWORD_BYTES)
				LOC_ERR("map-from part too long");
			if (strlen(sTo) > MAX_KEYWORD_BYTES)
				LOC_ERR("map-from part too long");

			// all parsed ok; add it!
			if (m_pRoot->AddMapping((BYTE*)sFrom, (BYTE*)sTo))
				m_iCount++;
			else
				LOC_ERR("duplicate map-from part");

			return true;
#undef LOC_ERR
		}

		ExceptionsTrie_c* Build()
		{
			if (!m_pRoot || !m_pRoot->m_sTo.IsEmpty() || m_pRoot->m_dKids.GetLength() == 0)
				return NULL;

			ExceptionsTrie_c* pRes = new ExceptionsTrie_c();
			pRes->m_iCount = m_iCount;

			// save the nodes themselves
			CSphVector<BYTE> dMappings;
			SaveNode(pRes, m_pRoot, dMappings);

			// append and fixup output mappings
			CSphVector<BYTE>& d = pRes->m_dData;
			pRes->m_iMappings = d.GetLength();
			memcpy(d.AddN(dMappings.GetLength()), dMappings.Begin(), dMappings.GetLength());

			BYTE* p = d.Begin();
			BYTE* pMax = p + pRes->m_iMappings;
			while (p < pMax)
			{
				// fixup offset in the current node, if needed
				size_t* pOff = (size_t*)p; // FIXME? unaligned
				if ((*pOff) < 0)
					*pOff = 0; // convert -1 to 0 for non-outputs
				else
					(*pOff) += pRes->m_iMappings; // fixup offsets for outputs

				// proceed to the next node
				auto n = p[4];
				p += 5 + 5 * n;
			}
			assert(p == pMax);

			// build the speedup table for the very 1st byte
			for (size_t i = 0; i < 256; i++)
				pRes->m_dFirst[i] = -1;
			size_t n =(size_t) d[4];
			for (size_t i = 0; i < n; i++)
				pRes->m_dFirst[d[5 + i]] = *(size_t*)&pRes->m_dData[5 + n + 4 * i];

			SafeDelete(m_pRoot);
			m_pRoot = new ExceptionsTrieNode_c();
			m_iCount = 0;
			return pRes;
		}

	protected:
		void SaveInt(CSphVector<BYTE>& v, size_t p, size_t x)
		{
#if USE_LITTLE_ENDIAN
			v[p] = x & 0xff;
			v[p + 1] = (x >> 8) & 0xff;
			v[p + 2] = (x >> 16) & 0xff;
			v[p + 3] = (x >> 24) & 0xff;
#else
			v[p] = (x >> 24) & 0xff;
			v[p + 1] = (x >> 16) & 0xff;
			v[p + 2] = (x >> 8) & 0xff;
			v[p + 3] = x & 0xff;
#endif
		}

		size_t SaveNode(ExceptionsTrie_c* pRes, ExceptionsTrieNode_c* pNode, CSphVector<BYTE>& dMappings)
		{
			CSphVector<BYTE>& d = pRes->m_dData; // shortcut

			// remember the start node offset
			auto iRes = d.GetLength();
			auto n = pNode->m_dKids.GetLength();
			assert(!(pNode->m_sTo.IsEmpty() && n == 0));

			// save offset into dMappings, or temporary (!) save -1 if there is no output mapping
			// note that we will fixup those -1's to 0's afterwards
			size_t iOff = -1;
			if (!pNode->m_sTo.IsEmpty())
			{
				iOff = dMappings.GetLength();
				auto iLen = pNode->m_sTo.Length();
				memcpy(dMappings.AddN(iLen + 1), pNode->m_sTo.cstr(), iLen + 1);
			}
			d.AddN(4);
			SaveInt(d, d.GetLength() - 4, iOff);

			// sort children nodes by value
			pNode->m_dKids.Sort(bind(&ExceptionsTrieNode_c::Entry_t::m_uValue));

			// save num_values, and values[]
			d.Add((BYTE)n);
			ARRAY_FOREACH(i, pNode->m_dKids)
				d.Add(pNode->m_dKids[i].m_uValue);

			// save offsets[], and the respective child nodes
			auto p = d.GetLength();
			d.AddN(4 * n);
			for (size_t i = 0; i < n; i++, p += 4)
				SaveInt(d, p, SaveNode(pRes, pNode->m_dKids[i].m_pKid, dMappings));
			assert(p == iRes + 5 + 5 * n);

			// done!
			return iRes;
		}
	};


}