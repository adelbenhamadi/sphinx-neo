#pragma once
#include "neo/int/types.h"
#include "neo/query/enums.h"
#include "neo/core/iwordlist.h"
#include "neo/dict/dict_entry.h"
#include "neo/dict/dict_keyword.h"
#include "neo/query/field_mask.h"
#include "neo/query/iqword.h"
#include "neo/core/match.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/core/generic.h"
#include "neo/index/index_settings.h"


namespace NEO {

	struct Slice64_t
	{
		uint64_t	m_uOff;
		int			m_iLen;
	};


	struct ISphSubstringPayload
	{
		ISphSubstringPayload() {}
		virtual ~ISphSubstringPayload() {}
	};


	struct DiskSubstringPayload_t : public ISphSubstringPayload
	{
		explicit DiskSubstringPayload_t(int iDoclists)
			: m_dDoclist(iDoclists)
			, m_iTotalDocs(0)
			, m_iTotalHits(0)
		{}
		CSphFixedVector<Slice64_t>	m_dDoclist;
		int							m_iTotalDocs;
		int							m_iTotalHits;
	};


	class CSphIndex_VLN;
	class ISphQwordSetup;
	class CSphAutofile;
	class CSphQueryProfile;



	/// everything required to setup search term
	class DiskIndexQwordSetup_c : public ISphQwordSetup
	{
	public:
		const CSphAutofile& m_tDoclist;
		const CSphAutofile& m_tHitlist;
		bool					m_bSetupReaders;
		const BYTE* m_pSkips;
		CSphQueryProfile* m_pProfile;

	public:
		DiskIndexQwordSetup_c(const CSphAutofile& tDoclist, const CSphAutofile& tHitlist, const BYTE* pSkips, CSphQueryProfile* pProfile)
			: m_tDoclist(tDoclist)
			, m_tHitlist(tHitlist)
			, m_bSetupReaders(false)
			, m_pSkips(pSkips)
			, m_pProfile(pProfile)
		{
		}

		virtual ISphQword* QwordSpawn(const XQKeyword_t& tWord) const;
		virtual bool						QwordSetup(ISphQword*) const;

		bool								Setup(ISphQword*) const;
	};


	/// query word from the searcher's point of view
	class DiskIndexQwordTraits_c : public ISphQword
	{
		static const int	MINIBUFFER_LEN = 1024;

	public:
		/// tricky bit
		/// m_uHitPosition is always a current position in the .spp file
		/// base ISphQword::m_iHitlistPos carries the inlined hit data when m_iDocs==1
		/// but this one is always a real position, used for delta coding
		SphOffset_t		m_uHitPosition;
		Hitpos_t		m_uInlinedHit;
		DWORD			m_uHitState;

		CSphMatch		m_tDoc;			//current match (partial)
		Hitpos_t		m_iHitPos;		//current hit postition, from hitlist

		BYTE			m_dDoclistBuf[MINIBUFFER_LEN];
		BYTE			m_dHitlistBuf[MINIBUFFER_LEN];
		CSphReader		m_rdDoclist;	//my doclist reader
		CSphReader		m_rdHitlist;	//my hitlist reader

		SphDocID_t		m_iMinID;		//min ID to fixup
		int				m_iInlineAttrs;	//inline attributes count

		const CSphRowitem* m_pInlineFixup;	//inline attributes fixup (POINTER TO EXTERNAL DATA, NOT MANAGED BY THIS CLASS!)

#ifndef NDEBUG
		bool			m_bHitlistOver;
#endif

	public:
		explicit DiskIndexQwordTraits_c(bool bUseMini, bool bExcluded)
			: m_uHitPosition(0)
			, m_uHitState(0)
			, m_iHitPos()
			, m_rdDoclist(bUseMini ? m_dDoclistBuf : NULL, bUseMini ? MINIBUFFER_LEN : 0)
			, m_rdHitlist(bUseMini ? m_dHitlistBuf : NULL, bUseMini ? MINIBUFFER_LEN : 0)
			, m_iMinID(0)
			, m_iInlineAttrs(0)
			, m_pInlineFixup(NULL)
#ifndef NDEBUG
			, m_bHitlistOver(true)
#endif
		{
			m_iHitPos = EMPTY_HIT;
			m_bExcluded = bExcluded;
		}

		void ResetDecoderState()
		{
			ISphQword::Reset();
			m_uHitPosition = 0;
			m_uInlinedHit = 0;
			m_uHitState = 0;
			m_tDoc.m_uDocID = m_iMinID;
			m_iHitPos = EMPTY_HIT;
		}

		virtual bool Setup(const DiskIndexQwordSetup_c* pSetup) = 0;
	};


	/// query word from the searcher's point of view
	template < bool INLINE_HITS, bool INLINE_DOCINFO, bool DISABLE_HITLIST_SEEK >
	class DiskIndexQword_c : public DiskIndexQwordTraits_c
	{
	public:
		DiskIndexQword_c(bool bUseMinibuffer, bool bExcluded)
			: DiskIndexQwordTraits_c(bUseMinibuffer, bExcluded)
		{}

		virtual void Reset()
		{
			m_rdDoclist.Reset();
			m_rdDoclist.Reset();
			m_iInlineAttrs = 0;
			ResetDecoderState();
		}

		void GetHitlistEntry()
		{
			assert(!m_bHitlistOver);
			DWORD iDelta = m_rdHitlist.UnzipInt();
			if (iDelta)
			{
				m_iHitPos += iDelta;
			}
			else
			{
				m_iHitPos = EMPTY_HIT;
#ifndef NDEBUG
				m_bHitlistOver = true;
#endif
			}
		}

		virtual void HintDocid(SphDocID_t uMinID)
		{
			// tricky bit
			// FindSpan() will match a block where BaseDocid is >= RefValue
			// meaning that the subsequent ids decoded will be strictly > RefValue
			// meaning that if previous (!) blocks end with uMinID exactly,
			// and we use uMinID itself as RefValue, that document gets lost!
			// OPTIMIZE? keep last matched block index maybe?
			int iBlock = FindSpan(m_dSkiplist, uMinID - m_iMinID - 1);
			if (iBlock < 0)
				return;
			const SkiplistEntry_t& t = m_dSkiplist[iBlock];
			if (t.m_iOffset <= m_rdDoclist.GetPos())
				return;
			m_rdDoclist.SeekTo(t.m_iOffset, -1);
			m_tDoc.m_uDocID = t.m_iBaseDocid + m_iMinID;
			m_uHitPosition = m_iHitlistPos = t.m_iBaseHitlistPos;
		}

		virtual const CSphMatch& GetNextDoc(DWORD* pDocinfo)
		{
			SphDocID_t uDelta = m_rdDoclist.UnzipDocid();
			if (uDelta)
			{
				m_bAllFieldsKnown = false;
				m_tDoc.m_uDocID += uDelta;
				if_const(INLINE_DOCINFO)
				{
					assert(pDocinfo);
					for (int i = 0; i < m_iInlineAttrs; i++)
						pDocinfo[i] = m_rdDoclist.UnzipInt() + m_pInlineFixup[i];
				}

				if_const(INLINE_HITS)
				{
					m_uMatchHits = m_rdDoclist.UnzipInt();
					const DWORD uFirst = m_rdDoclist.UnzipInt();
					if (m_uMatchHits == 1 && m_bHasHitlist)
					{
						DWORD uField = m_rdDoclist.UnzipInt(); // field and end marker
						m_iHitlistPos = uFirst | (uField << 23) | (U64C(1) << 63);
						m_dQwordFields.UnsetAll();
						// want to make sure bad field data not cause crash
						m_dQwordFields.Set((uField >> 1) & ((DWORD)SPH_MAX_FIELDS - 1));
						m_bAllFieldsKnown = true;
					}
					else
					{
						m_dQwordFields.Assign32(uFirst);
						m_uHitPosition += m_rdDoclist.UnzipOffset();
						m_iHitlistPos = m_uHitPosition;
					}
				}
			else
			{
				SphOffset_t iDeltaPos = m_rdDoclist.UnzipOffset();
				assert(iDeltaPos >= 0);

				m_iHitlistPos += iDeltaPos;

				m_dQwordFields.Assign32(m_rdDoclist.UnzipInt());
				m_uMatchHits = m_rdDoclist.UnzipInt();
			}
			}
			else
			{
				m_tDoc.m_uDocID = 0;
			}
			return m_tDoc;
		}

		virtual void SeekHitlist(SphOffset_t uOff)
		{
			if (uOff >> 63)
			{
				m_uHitState = 1;
				m_uInlinedHit = (DWORD)uOff; // truncate high dword
			}
			else
			{
				m_uHitState = 0;
				m_iHitPos = EMPTY_HIT;
				if_const(DISABLE_HITLIST_SEEK)
					assert(m_rdHitlist.GetPos() == uOff); // make sure we're where caller thinks we are.
			else
			m_rdHitlist.SeekTo(uOff, READ_NO_SIZE_HINT);
			}
#ifndef NDEBUG
			m_bHitlistOver = false;
#endif
		}

		virtual Hitpos_t GetNextHit()
		{
			assert(m_bHasHitlist);
			switch (m_uHitState)
			{
			case 0: // read hit from hitlist
				GetHitlistEntry();
				return m_iHitPos;

			case 1: // return inlined hit
				m_uHitState = 2;
				return m_uInlinedHit;

			case 2: // return end-of-hitlist marker after inlined hit
#ifndef NDEBUG
				m_bHitlistOver = true;
#endif
				m_uHitState = 0;
				return EMPTY_HIT;
			}
			sphDie("INTERNAL ERROR: impossible hit emitter state");
			return EMPTY_HIT;
		}

		bool Setup(const DiskIndexQwordSetup_c* pSetup)
		{
			return pSetup->Setup(this);
		}
	};



	template < bool INLINE_HITS >
	class DiskPayloadQword_c : public DiskIndexQword_c<INLINE_HITS, false, false>
	{
		typedef DiskIndexQword_c<INLINE_HITS, false, false> BASE;

	public:
		explicit DiskPayloadQword_c(const DiskSubstringPayload_t* pPayload, bool bExcluded,
			const CSphAutofile& tDoclist, const CSphAutofile& tHitlist, CSphQueryProfile* pProfile)
			: BASE(true, bExcluded)
		{
			m_pPayload = pPayload;
			this->m_iDocs = m_pPayload->m_iTotalDocs;
			this->m_iHits = m_pPayload->m_iTotalHits;
			m_iDoclist = 0;

			this->m_rdDoclist.SetFile(tDoclist);
			this->m_rdDoclist.SetBuffers(g_iReadBuffer, g_iReadUnhinted);
			this->m_rdDoclist.m_pProfile = pProfile;
			this->m_rdDoclist.m_eProfileState = SPH_QSTATE_READ_DOCS;

			this->m_rdHitlist.SetFile(tHitlist);
			this->m_rdHitlist.SetBuffers(g_iReadBuffer, g_iReadUnhinted);
			this->m_rdHitlist.m_pProfile = pProfile;
			this->m_rdHitlist.m_eProfileState = SPH_QSTATE_READ_HITS;
		}

		virtual const CSphMatch& GetNextDoc(DWORD* pDocinfo)
		{
			const CSphMatch& tMatch = BASE::GetNextDoc(pDocinfo);
			assert(&tMatch == &this->m_tDoc);
			if (!tMatch.m_uDocID && m_iDoclist < m_pPayload->m_dDoclist.GetLength())
			{
				BASE::ResetDecoderState();
				SetupReader();
				BASE::GetNextDoc(pDocinfo);
				assert(this->m_tDoc.m_uDocID);
			}

			return this->m_tDoc;
		}

		bool Setup(const DiskIndexQwordSetup_c*)
		{
			if (m_iDoclist >= m_pPayload->m_dDoclist.GetLength())
				return false;

			SetupReader();
			return true;
		}

	private:
		void SetupReader()
		{
			uint64_t uDocOff = m_pPayload->m_dDoclist[m_iDoclist].m_uOff;
			int iHint = m_pPayload->m_dDoclist[m_iDoclist].m_iLen;
			m_iDoclist++;

			this->m_rdDoclist.SeekTo(uDocOff, iHint);
		}

		const DiskSubstringPayload_t* m_pPayload;
		int								m_iDoclist;
	};


	struct DiskExpandedEntry_t
	{
		int		m_iNameOff;
		int		m_iDocs;
		int		m_iHits;
	};

	struct DiskExpandedPayload_t
	{
		int			m_iDocs;
		int			m_iHits;
		uint64_t	m_uDoclistOff;
		int			m_iDoclistHint;
	};


	inline int sphGetExpansionMagic(int iDocs, int iHits)
	{
		return (iHits <= 256 ? 1 : iDocs + 1); // magic threshold; mb make this configurable?
	}
	inline bool sphIsExpandedPayload(int iDocs, int iHits)
	{
		return (iHits <= 256 || iDocs < 32); // magic threshold; mb make this configurable?
	}

	struct DictEntryDiskPayload_t
	{
		explicit DictEntryDiskPayload_t(bool bPayload, ESphHitless eHitless)
		{
			m_bPayload = bPayload;
			m_eHitless = eHitless;
			if (bPayload)
				m_dWordPayload.Reserve(1000);

			m_dWordExpand.Reserve(1000);
			m_dWordBuf.Reserve(8096);
		}

		void Add(const CSphDictEntry& tWord, int iWordLen)
		{
			if (!m_bPayload || !sphIsExpandedPayload(tWord.m_iDocs, tWord.m_iHits) ||
				m_eHitless == SPH_HITLESS_ALL || (m_eHitless == SPH_HITLESS_SOME && (tWord.m_iDocs & HITLESS_DOC_FLAG) != 0)) // FIXME!!! do we need hitless=some as payloads?
			{
				DiskExpandedEntry_t& tExpand = m_dWordExpand.Add();

				int iOff = m_dWordBuf.GetLength();
				tExpand.m_iNameOff = iOff;
				tExpand.m_iDocs = tWord.m_iDocs;
				tExpand.m_iHits = tWord.m_iHits;
				m_dWordBuf.Resize(iOff + iWordLen + 1);
				memcpy(m_dWordBuf.Begin() + iOff + 1, tWord.m_sKeyword, iWordLen);
				m_dWordBuf[iOff] = (BYTE)iWordLen;

			}
			else
			{
				DiskExpandedPayload_t& tExpand = m_dWordPayload.Add();
				tExpand.m_iDocs = tWord.m_iDocs;
				tExpand.m_iHits = tWord.m_iHits;
				tExpand.m_uDoclistOff = tWord.m_iDoclistOffset;
				tExpand.m_iDoclistHint = tWord.m_iDoclistHint;
			}
		}

		void Convert(ISphWordlist::Args_t& tArgs)
		{
			if (!m_dWordExpand.GetLength() && !m_dWordPayload.GetLength())
				return;

			int iTotalDocs = 0;
			int iTotalHits = 0;
			if (m_dWordExpand.GetLength())
			{
				LimitExpanded(tArgs.m_iExpansionLimit, m_dWordExpand);

				const BYTE* sBase = m_dWordBuf.Begin();
				ARRAY_FOREACH(i, m_dWordExpand)
				{
					const DiskExpandedEntry_t& tCur = m_dWordExpand[i];
					int iDocs = tCur.m_iDocs;

					if (m_eHitless == SPH_HITLESS_SOME)
						iDocs = (tCur.m_iDocs & HITLESS_DOC_MASK);

					tArgs.AddExpanded(sBase + tCur.m_iNameOff + 1, sBase[tCur.m_iNameOff], iDocs, tCur.m_iHits);

					iTotalDocs += iDocs;
					iTotalHits += tCur.m_iHits;
				}
			}

			if (m_dWordPayload.GetLength())
			{
				LimitExpanded(tArgs.m_iExpansionLimit, m_dWordPayload);

				DiskSubstringPayload_t* pPayload = new DiskSubstringPayload_t(m_dWordPayload.GetLength());
				// sorting by ascending doc-list offset gives some (15%) speed-up too
				sphSort(m_dWordPayload.Begin(), m_dWordPayload.GetLength(), bind(&DiskExpandedPayload_t::m_uDoclistOff));

				ARRAY_FOREACH(i, m_dWordPayload)
				{
					const DiskExpandedPayload_t& tCur = m_dWordPayload[i];
					assert(m_eHitless == SPH_HITLESS_NONE || (m_eHitless == SPH_HITLESS_SOME && (tCur.m_iDocs & HITLESS_DOC_FLAG) == 0));

					iTotalDocs += tCur.m_iDocs;
					iTotalHits += tCur.m_iHits;
					pPayload->m_dDoclist[i].m_uOff = tCur.m_uDoclistOff;
					pPayload->m_dDoclist[i].m_iLen = tCur.m_iDoclistHint;
				}

				pPayload->m_iTotalDocs = iTotalDocs;
				pPayload->m_iTotalHits = iTotalHits;
				tArgs.m_pPayload = pPayload;
			}
			tArgs.m_iTotalDocs = iTotalDocs;
			tArgs.m_iTotalHits = iTotalHits;
		}

		// sort expansions by frequency desc
		// clip the less frequent ones if needed, as they are likely misspellings
		template < typename T >
		void LimitExpanded(int iExpansionLimit, CSphVector<T>& dVec) const
		{
			if (!iExpansionLimit || dVec.GetLength() <= iExpansionLimit)
				return;

			sphSort(dVec.Begin(), dVec.GetLength(), ExpandedOrderDesc_T<T>());
			dVec.Resize(iExpansionLimit);
		}

		bool								m_bPayload;
		ESphHitless							m_eHitless;
		CSphVector<DiskExpandedEntry_t>		m_dWordExpand;
		CSphVector<DiskExpandedPayload_t>	m_dWordPayload;
		CSphVector<BYTE>					m_dWordBuf;
	};


}