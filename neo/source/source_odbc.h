#pragma once
#if USE_ODBC

#include <sql.h>


#include "neo/source/source_sql.h"
#include "neo/utility/hash.h"


#if DL_UNIXODBC
#define ODBC_F(name) F_DL(name)
#else
#define ODBC_F( name ) F_DR(name)
#endif

ODBC_F(SQLFreeHandle);
ODBC_F(SQLDisconnect);
ODBC_F(SQLCloseCursor);
ODBC_F(SQLGetDiagRec);
ODBC_F(SQLSetEnvAttr);
ODBC_F(SQLAllocHandle);
ODBC_F(SQLFetch);
ODBC_F(SQLExecDirect);
ODBC_F(SQLNumResultCols);
ODBC_F(SQLDescribeCol);
ODBC_F(SQLBindCol);
ODBC_F(SQLDrivers);
ODBC_F(SQLDriverConnect);

#if DL_UNIXODBC
#ifndef UNIXODBC_LIB
#define UNIXODBC_LIB "libodbc.so"
#endif

#define ODBC_NUM_FUNCS (13)


bool InitDynamicOdbc()
{
	const char* sFuncs[] = { "SQLFreeHandle", "SQLDisconnect",
			"SQLCloseCursor", "SQLGetDiagRec", "SQLSetEnvAttr", "SQLAllocHandle",
			"SQLFetch", "SQLExecDirect", "SQLNumResultCols", "SQLDescribeCol",
			"SQLBindCol", "SQLDrivers", "SQLDriverConnect" };

	void** pFuncs[] = { (void**)&sph_SQLFreeHandle, (void**)&sph_SQLDisconnect,
			(void**)&sph_SQLCloseCursor, (void**)&sph_SQLGetDiagRec, (void**)&sph_SQLSetEnvAttr,
			(void**)&sph_SQLAllocHandle, (void**)&sph_SQLFetch, (void**)&sph_SQLExecDirect,
			(void**)&sph_SQLNumResultCols, (void**)&sph_SQLDescribeCol, (void**)&sph_SQLBindCol,
			(void**)&sph_SQLDrivers, (void**)&sph_SQLDriverConnect };

	static CSphDynamicLibrary dLib(UNIXODBC_LIB);
	if (!dLib.LoadSymbols(sFuncs, pFuncs, ODBC_NUM_FUNCS))
		return false;
	return true;
}

#else
#define InitDynamicOdbc() (true)
#endif


namespace NEO {


	struct CSphSourceParams_ODBC : CSphSourceParams_SQL
	{
		CSphString	m_sOdbcDSN;			///< ODBC DSN
		CSphString	m_sColBuffers;		///< column buffer sizes (eg "col1=2M, col2=4M")
		bool		m_bWinAuth;			///< auth type (MS SQL only)

		CSphSourceParams_ODBC();
	};

	/// ODBC source implementation
	struct CSphSource_ODBC : CSphSource_SQL
	{
		explicit				CSphSource_ODBC(const char* sName);
		bool					Setup(const CSphSourceParams_ODBC& tParams);

	protected:
		virtual void			SqlDismissResult();
		virtual bool			SqlQuery(const char* sQuery);
		virtual bool			SqlIsError();
		virtual const char* SqlError();
		virtual bool			SqlConnect();
		virtual void			SqlDisconnect();
		virtual int				SqlNumFields();
		virtual bool			SqlFetchRow();
		virtual const char* SqlColumn(int iIndex);
		virtual const char* SqlFieldName(int iIndex);
		virtual DWORD			SqlColumnLength(int iIndex);

		virtual void			OdbcPostConnect() {}

	protected:
		CSphString				m_sOdbcDSN;
		bool					m_bWinAuth;
		bool					m_bUnicode;

		SQLHENV					m_hEnv;
		SQLHDBC					m_hDBC;
		SQLHANDLE				m_hStmt;
		int						m_nResultCols;
		CSphString				m_sError;

		struct QueryColumn_t
		{
			CSphVector<char>	m_dContents;
			CSphVector<char>	m_dRaw;
			CSphString			m_sName;
			SQLLEN				m_iInd;
			int					m_iBytes;		///< size of actual data in m_dContents, in bytes
			int					m_iBufferSize;	///< size of m_dContents and m_dRaw buffers, in bytes
			bool				m_bUCS2;		///< whether this column needs UCS-2 to UTF-8 translation
			bool				m_bTruncated;	///< whether data was truncated when fetching rows
		};

		static const int		DEFAULT_COL_SIZE = 1024;			///< default column buffer size
		static const int		VARCHAR_COL_SIZE = 1048576;		///< default column buffer size for VARCHAR columns
		static const int		MAX_COL_SIZE = 8 * 1048576;	///< hard limit on column buffer size
		static const int		WARN_ROW_SIZE = 32 * 1048576;	///< warning thresh (NOT a hard limit) on row buffer size

		CSphVector<QueryColumn_t>	m_dColumns;
		SmallStringHash_T<int>		m_hColBuffers;

		void					GetSqlError(SQLSMALLINT iHandleType, SQLHANDLE hHandle);
	};


	

}
#endif