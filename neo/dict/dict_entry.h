#pragma once

namespace NEO {

	/// dictionary entry
	/// some of the fields might be unused depending on specific dictionary type
	struct CSphDictEntry
	{
		SphWordID_t		m_uWordID;			///< keyword id (for dict=crc)
		const BYTE* m_sKeyword;			///< keyword text (for dict=keywords)
		int				m_iDocs;			///< number of matching documents
		int				m_iHits;			///< number of occurrences
		SphOffset_t		m_iDoclistOffset;	///< absolute document list offset (into .spd)
		SphOffset_t		m_iDoclistLength;	///< document list length in bytes
		SphOffset_t		m_iSkiplistOffset;	///< absolute skiplist offset (into .spe)
		int				m_iDoclistHint;		///< raw document list length hint value (0..255 range, 1 byte)
	};


}