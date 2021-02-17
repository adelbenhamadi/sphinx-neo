#if USE_ODBC
#include "neo/source/source_odbc.h"

namespace NEO {
	CSphSourceParams_ODBC::CSphSourceParams_ODBC()
		: m_bWinAuth(false)
	{
	}


	CSphSource_ODBC::CSphSource_ODBC(const char* sName)
		: CSphSource_SQL(sName)
		, m_bWinAuth(false)
		, m_bUnicode(false)
		, m_hEnv(NULL)
		, m_hDBC(NULL)
		, m_hStmt(NULL)
		, m_nResultCols(0)
	{
	}


	void CSphSource_ODBC::SqlDismissResult()
	{
		if (m_hStmt)
		{
			sph_SQLCloseCursor(m_hStmt);
			sph_SQLFreeHandle(SQL_HANDLE_STMT, m_hStmt);
			m_hStmt = NULL;
		}
	}


#define MS_SQL_BUFFER_GAP 16


	bool CSphSource_ODBC::SqlQuery(const char* sQuery)
	{
		if (sph_SQLAllocHandle(SQL_HANDLE_STMT, m_hDBC, &m_hStmt) == SQL_ERROR)
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-QUERY: %s: FAIL (SQLAllocHandle failed)\n", sQuery);
			return false;
		}

		if (sph_SQLExecDirect(m_hStmt, (SQLCHAR*)sQuery, SQL_NTS) == SQL_ERROR)
		{
			GetSqlError(SQL_HANDLE_STMT, m_hStmt);
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-QUERY: %s: FAIL\n", sQuery);
			return false;
		}
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-QUERY: %s: ok\n", sQuery);

		SQLSMALLINT nCols = 0;
		m_nResultCols = 0;
		if (sph_SQLNumResultCols(m_hStmt, &nCols) == SQL_ERROR)
			return false;

		m_nResultCols = nCols;

		const int MAX_NAME_LEN = 512;
		char szColumnName[MAX_NAME_LEN];

		m_dColumns.Resize(m_nResultCols);
		int iTotalBuffer = 0;
		ARRAY_FOREACH(i, m_dColumns)
		{
			QueryColumn_t& tCol = m_dColumns[i];

			SQLULEN uColSize = 0;
			SQLSMALLINT iNameLen = 0;
			SQLSMALLINT iDataType = 0;
			if (sph_SQLDescribeCol(m_hStmt, (SQLUSMALLINT)(i + 1), (SQLCHAR*)szColumnName,
				MAX_NAME_LEN, &iNameLen, &iDataType, &uColSize, NULL, NULL) == SQL_ERROR)
				return false;

			tCol.m_sName = szColumnName;
			tCol.m_sName.ToLower();

			// deduce buffer size
			// use a small buffer by default, and a bigger one for varchars
			int iBuffLen = DEFAULT_COL_SIZE;
			if (iDataType == SQL_WCHAR || iDataType == SQL_WVARCHAR || iDataType == SQL_WLONGVARCHAR || iDataType == SQL_VARCHAR)
				iBuffLen = VARCHAR_COL_SIZE;

			if (m_hColBuffers(tCol.m_sName))
				iBuffLen = m_hColBuffers[tCol.m_sName]; // got explicit user override
			else if (uColSize)
				iBuffLen = Min(uColSize + 1, (SQLULEN)MAX_COL_SIZE); // got data from driver

			tCol.m_dContents.Resize(iBuffLen + MS_SQL_BUFFER_GAP);
			tCol.m_dRaw.Resize(iBuffLen + MS_SQL_BUFFER_GAP);
			tCol.m_iInd = 0;
			tCol.m_iBytes = 0;
			tCol.m_iBufferSize = iBuffLen;
			tCol.m_bUCS2 = m_bUnicode && (iDataType == SQL_WCHAR || iDataType == SQL_WVARCHAR || iDataType == SQL_WLONGVARCHAR);
			tCol.m_bTruncated = false;
			iTotalBuffer += iBuffLen;

			if (sph_SQLBindCol(m_hStmt, (SQLUSMALLINT)(i + 1),
				tCol.m_bUCS2 ? SQL_UNICODE : SQL_C_CHAR,
				tCol.m_bUCS2 ? tCol.m_dRaw.Begin() : tCol.m_dContents.Begin(),
				iBuffLen, &(tCol.m_iInd)) == SQL_ERROR)
				return false;
		}

		if (iTotalBuffer > WARN_ROW_SIZE)
			sphWarn("row buffer is over %d bytes; consider revising sql_column_buffers", iTotalBuffer);

		return true;
	}


	bool CSphSource_ODBC::SqlIsError()
	{
		return !m_sError.IsEmpty();
	}


	const char* CSphSource_ODBC::SqlError()
	{
		return m_sError.cstr();
	}


	bool CSphSource_ODBC::SqlConnect()
	{
		if_const(!InitDynamicOdbc())
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL (NO ODBC CLIENT LIB)\n");
			return false;
		}

		if (sph_SQLAllocHandle(SQL_HANDLE_ENV, NULL, &m_hEnv) == SQL_ERROR)
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL\n");
			return false;
		}

		sph_SQLSetEnvAttr(m_hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, SQL_IS_INTEGER);

		if (sph_SQLAllocHandle(SQL_HANDLE_DBC, m_hEnv, &m_hDBC) == SQL_ERROR)
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL\n");
			return false;
		}

		OdbcPostConnect();

		char szOutConn[2048];
		SQLSMALLINT iOutConn = 0;
		if (sph_SQLDriverConnect(m_hDBC, NULL, (SQLTCHAR*)m_sOdbcDSN.cstr(), SQL_NTS,
			(SQLCHAR*)szOutConn, sizeof(szOutConn), &iOutConn, SQL_DRIVER_NOPROMPT) == SQL_ERROR)
		{
			GetSqlError(SQL_HANDLE_DBC, m_hDBC);
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL\n");
			return false;
		}

		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-CONNECT: ok\n");
		return true;
	}


	void CSphSource_ODBC::SqlDisconnect()
	{
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-DISCONNECT\n");

		if (m_hStmt != NULL)
			sph_SQLFreeHandle(SQL_HANDLE_STMT, m_hStmt);

		if (m_hDBC)
		{
			sph_SQLDisconnect(m_hDBC);
			sph_SQLFreeHandle(SQL_HANDLE_DBC, m_hDBC);
		}

		if (m_hEnv)
			sph_SQLFreeHandle(SQL_HANDLE_ENV, m_hEnv);
	}


	int CSphSource_ODBC::SqlNumFields()
	{
		if (!m_hStmt)
			return -1;

		return m_nResultCols;
	}


	bool CSphSource_ODBC::SqlFetchRow()
	{
		if (!m_hStmt)
			return false;

		SQLRETURN iRet = sph_SQLFetch(m_hStmt);
		if (iRet == SQL_ERROR || iRet == SQL_INVALID_HANDLE || iRet == SQL_NO_DATA)
		{
			GetSqlError(SQL_HANDLE_STMT, m_hStmt);
			return false;
		}

		ARRAY_FOREACH(i, m_dColumns)
		{
			QueryColumn_t& tCol = m_dColumns[i];
			switch (tCol.m_iInd)
			{
			case SQL_NULL_DATA:
				tCol.m_dContents[0] = '\0';
				tCol.m_iBytes = 0;
				break;

			default:
#if USE_WINDOWS // FIXME! support UCS-2 columns on Unix too
				if (tCol.m_bUCS2)
				{
					// WideCharToMultiByte should get NULL terminated string
					memset(tCol.m_dRaw.Begin() + tCol.m_iBufferSize, 0, MS_SQL_BUFFER_GAP);

					int iConv = WideCharToMultiByte(CP_UTF8, 0, LPCWSTR(tCol.m_dRaw.Begin()), tCol.m_iInd / sizeof(WCHAR),
						LPSTR(tCol.m_dContents.Begin()), tCol.m_iBufferSize - 1, NULL, NULL);

					if (iConv == 0)
						if (GetLastError() == ERROR_INSUFFICIENT_BUFFER)
							iConv = tCol.m_iBufferSize - 1;

					tCol.m_dContents[iConv] = '\0';
					tCol.m_iBytes = iConv;

				}
				else
#endif
				{
					if (tCol.m_iInd >= 0 && tCol.m_iInd < tCol.m_iBufferSize)
					{
						// data fetched ok; add trailing zero
						tCol.m_dContents[tCol.m_iInd] = '\0';
						tCol.m_iBytes = tCol.m_iInd;

					}
					else if (tCol.m_iInd >= tCol.m_iBufferSize && !tCol.m_bTruncated)
					{
						// out of buffer; warn about that (once)
						tCol.m_bTruncated = true;
						sphWarn("'%s' column truncated (buffer=%d, got=%d); consider revising sql_column_buffers",
							tCol.m_sName.cstr(), tCol.m_iBufferSize - 1, (int)tCol.m_iInd);
						tCol.m_iBytes = tCol.m_iBufferSize;
					}
				}
				break;
			}
		}

		return iRet != SQL_NO_DATA;
	}


	const char* CSphSource_ODBC::SqlColumn(int iIndex)
	{
		if (!m_hStmt)
			return NULL;

		return &(m_dColumns[iIndex].m_dContents[0]);
	}


	const char* CSphSource_ODBC::SqlFieldName(int iIndex)
	{
		return m_dColumns[iIndex].m_sName.cstr();
	}


	DWORD CSphSource_ODBC::SqlColumnLength(int iIndex)
	{
		return m_dColumns[iIndex].m_iBytes;
	}


	bool CSphSource_ODBC::Setup(const CSphSourceParams_ODBC& tParams)
	{
		if (!CSphSource_SQL::Setup(tParams))
			return false;

		// parse column buffers spec, if any
		if (!tParams.m_sColBuffers.IsEmpty())
		{
			const char* p = tParams.m_sColBuffers.cstr();
			while (*p)
			{
				// skip space
				while (sphIsSpace(*p))
					p++;

				// expect eof or ident
				if (!*p)
					break;
				if (!sphIsAlpha(*p))
				{
					m_sError.SetSprintf("identifier expected in sql_column_buffers near '%s'", p);
					return false;
				}

				// get ident
				CSphString sCol;
				const char* pIdent = p;
				while (sphIsAlpha(*p))
					p++;
				sCol.SetBinary(pIdent, p - pIdent);

				// skip space
				while (sphIsSpace(*p))
					p++;

				// expect assignment
				if (*p != '=')
				{
					m_sError.SetSprintf("'=' expected in sql_column_buffers near '%s'", p);
					return false;
				}
				p++;

				// skip space
				while (sphIsSpace(*p))
					p++;

				// expect number
				if (!(*p >= '0' && *p <= '9'))
				{
					m_sError.SetSprintf("number expected in sql_column_buffers near '%s'", p);
					return false;
				}

				// get value
				int iSize = 0;
				while (*p >= '0' && *p <= '9')
				{
					iSize = 10 * iSize + (*p - '0');
					p++;
				}
				if (*p == 'K')
				{
					iSize *= 1024;
					p++;
				}
				else if (*p == 'M')
				{
					iSize *= 1048576;
					p++;
				}

				// hash value
				sCol.ToLower();
				m_hColBuffers.Add(iSize, sCol);

				// skip space
				while (sphIsSpace(*p))
					p++;

				// expect eof or comma
				if (!*p)
					break;
				if (*p != ',')
				{
					m_sError.SetSprintf("comma expected in sql_column_buffers near '%s'", p);
					return false;
				}
				p++;
			}
		}

		// ODBC specific params
		m_sOdbcDSN = tParams.m_sOdbcDSN;
		m_bWinAuth = tParams.m_bWinAuth;

		// build and store DSN for error reporting
		char sBuf[1024];
		snprintf(sBuf, sizeof(sBuf), "odbc%s", m_sSqlDSN.cstr() + 3);
		m_sSqlDSN = sBuf;

		return true;
	}


	void CSphSource_ODBC::GetSqlError(SQLSMALLINT iHandleType, SQLHANDLE hHandle)
	{
		if (!hHandle)
		{
			m_sError.SetSprintf("invalid handle");
			return;
		}

		char szState[16] = "";
		char szMessageText[1024] = "";
		SQLINTEGER iError;
		SQLSMALLINT iLen;
		sph_SQLGetDiagRec(iHandleType, hHandle, 1, (SQLCHAR*)szState, &iError, (SQLCHAR*)szMessageText, 1024, &iLen);
		m_sError = szMessageText;
	}

}

#endif