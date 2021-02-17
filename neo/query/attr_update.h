#pragma once

namespace NEO {


	/////////////////////////////////////////////////////////////////////////////
	// ATTRIBUTE UPDATE QUERY
	/////////////////////////////////////////////////////////////////////////////

	struct CSphAttrUpdate
	{
		CSphVector<char*>				m_dAttrs;		///< update schema, attr names to update
		CSphVector<ESphAttr>			m_dTypes;		///< update schema, attr types to update
		CSphVector<DWORD>				m_dPool;		///< update values pool
		CSphVector<SphDocID_t>			m_dDocids;		///< document IDs vector
		CSphVector<const CSphRowitem*>	m_dRows;		///< document attribute's vector, used instead of m_dDocids.
		CSphVector<int>					m_dRowOffset;	///< document row offsets in the pool (1 per doc, i.e. the length is the same as of m_dDocids)
		bool							m_bIgnoreNonexistent;	///< whether to warn about non-existen attrs, or just silently ignore them
		bool							m_bStrict;		///< whether to check for incompatible types first, or just ignore them

		CSphAttrUpdate()
			: m_bIgnoreNonexistent(false)
			, m_bStrict(false)
		{}

		~CSphAttrUpdate()
		{
			ARRAY_FOREACH(i, m_dAttrs)
				SafeDeleteArray(m_dAttrs[i]);
		}
	};

	// update attributes with index pointer attached
	struct CSphAttrUpdateEx
	{
		const CSphAttrUpdate* m_pUpdate;		///< the unchangeable update pool
		CSphIndex* m_pIndex;		///< the index on which the update should happen
		CSphString* m_pError;		///< the error, if any
		CSphString* m_pWarning;		///< the warning, if any
		int						m_iAffected;	///< num of updated rows.
		CSphAttrUpdateEx()
			: m_pUpdate(NULL)
			, m_pIndex(NULL)
			, m_pError(NULL)
			, m_pWarning(NULL)
			, m_iAffected(0)
		{}
	};

}