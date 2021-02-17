#pragma once
#include "neo/int/types.h"

namespace NEO {

	const BYTE* SkipQuoted(const BYTE* p);

	class CSphHTMLStripper
	{
	public:
		explicit					CSphHTMLStripper(bool bDefaultTags);
		bool						SetIndexedAttrs(const char* sConfig, CSphString& sError);
		bool						SetRemovedElements(const char* sConfig, CSphString& sError);
		bool						SetZones(const char* sZones, CSphString& sError);
		void						EnableParagraphs();
		void						Strip(BYTE* sData) const;

	public:

		struct StripperTag_t
		{
			CSphString				m_sTag;			///< tag name
			int						m_iTagLen;		///< tag name length
			bool					m_bInline;		///< whether this tag is inline
			bool					m_bIndexAttrs;	///< whether to index attrs
			bool					m_bRemove;		///< whether to remove contents
			bool					m_bPara;		///< whether to mark a paragraph boundary
			bool					m_bZone;		///< whether to mark a zone boundary
			bool					m_bZonePrefix;	///< whether the zone name is a full name or a prefix
			CSphVector<CSphString>	m_dAttrs;		///< attr names to index

			StripperTag_t()
				: m_iTagLen(0)
				, m_bInline(false)
				, m_bIndexAttrs(false)
				, m_bRemove(false)
				, m_bPara(false)
				, m_bZone(false)
				, m_bZonePrefix(false)
			{}

			inline bool operator < (const StripperTag_t& rhs) const
			{
				return strcmp(m_sTag.cstr(), rhs.m_sTag.cstr()) < 0;
			}
		};

		/// finds appropriate tag and zone name ( tags zone name could be prefix only )
		/// advances source to the end of the tag
		const BYTE* FindTag(const BYTE* sSrc, const StripperTag_t** ppTag, const BYTE** ppZoneName, int* pZoneNameLen) const;
		bool						IsValidTagStart(int iCh) const;

	protected:
		static const int			MAX_CHAR_INDEX = 28;		///< max valid char index (a-z, underscore, colon)

		CSphVector<StripperTag_t>	m_dTags;					///< known tags to index attrs and/or to remove contents
		int							m_dStart[MAX_CHAR_INDEX];	///< maps index of the first tag name char to start offset in m_dTags
		int							m_dEnd[MAX_CHAR_INDEX];		///< maps index of the first tag name char to end offset in m_dTags

	protected:
		int							GetCharIndex(int iCh) const;	///< calcs index by raw char
		void						UpdateTags();				///< sorts tags, updates internal helpers
	};

}