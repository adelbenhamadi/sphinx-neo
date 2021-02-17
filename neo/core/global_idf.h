#pragma once
#include "neo/int/types.h"
#include "neo/io/buffer.h"

namespace NEO {

	/// global IDF
	class CSphGlobalIDF
	{
	public:
		CSphGlobalIDF()
			: m_iTotalDocuments(0)
			, m_iTotalWords(0)
		{}

		bool			Touch(const CSphString& sFilename);
		bool			Preread(const CSphString& sFilename, CSphString& sError);
		DWORD			GetDocs(const CSphString& sWord) const;
		float			GetIDF(const CSphString& sWord, int64_t iDocsLocal, bool bPlainIDF);

	protected:
#pragma pack(push,4)
		struct IDFWord_t
		{
			uint64_t				m_uWordID;
			DWORD					m_iDocs;
		};
#pragma pack(pop)
		STATIC_SIZE_ASSERT(IDFWord_t, 12);

		static const int			HASH_BITS = 16;
		int64_t						m_iTotalDocuments;
		int64_t						m_iTotalWords;
		SphOffset_t					m_uMTime;
		CSphLargeBuffer<IDFWord_t>	m_pWords;
		CSphLargeBuffer<int64_t>	m_pHash;
	};

}