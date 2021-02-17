#pragma once
#if USE_ODBC
#include "neo/source/source_odbc.h"

namespace NEO {

	/// MS SQL source implementation
	struct CSphSource_MSSQL : public CSphSource_ODBC
	{
		explicit				CSphSource_MSSQL(const char* sName) : CSphSource_ODBC(sName) { m_bUnicode = true; }
		virtual void			OdbcPostConnect();
	};

}
#endif