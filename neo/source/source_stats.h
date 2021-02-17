#pragma once

#include <cstdint>

namespace NEO {

	/// source statistics
	struct CSphSourceStats
	{
		int64_t			m_iTotalDocuments;	///< how much documents
		int64_t			m_iTotalBytes;		///< how much bytes

		/// ctor
		CSphSourceStats()
		{
			Reset();
		}

		/// reset
		void Reset()
		{
			m_iTotalDocuments = 0;
			m_iTotalBytes = 0;
		}
	};

}
