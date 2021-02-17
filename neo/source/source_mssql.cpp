#include "neo/source/source_mssql.h"

#if USE_ODBC
void NEO::CSphSource_MSSQL::OdbcPostConnect()
{
	if (!m_sOdbcDSN.IsEmpty())
		return;

	const int MAX_LEN = 1024;
	char szDriver[MAX_LEN];
	char szDriverAttrs[MAX_LEN];
	SQLSMALLINT iDescLen = 0;
	SQLSMALLINT iAttrLen = 0;
	SQLSMALLINT iDir = SQL_FETCH_FIRST;

	CSphString sDriver;
	for (;; )
	{
		SQLRETURN iRet = sph_SQLDrivers(m_hEnv, iDir, (SQLCHAR*)szDriver, MAX_LEN, &iDescLen, (SQLCHAR*)szDriverAttrs, MAX_LEN, &iAttrLen);
		if (iRet == SQL_NO_DATA)
			break;

		iDir = SQL_FETCH_NEXT;
		if (!strcmp(szDriver, "SQL Native Client")
			|| !strncmp(szDriver, "SQL Server Native Client", strlen("SQL Server Native Client")))
		{
			sDriver = szDriver;
			break;
		}
	}

	if (sDriver.IsEmpty())
		sDriver = "SQL Server";

	if (m_bWinAuth && m_tParams.m_sUser.IsEmpty())
	{
		m_sOdbcDSN.SetSprintf("DRIVER={%s};SERVER={%s};Database={%s};Trusted_Connection=yes",
			sDriver.cstr(), m_tParams.m_sHost.cstr(), m_tParams.m_sDB.cstr());

	}
	else if (m_bWinAuth)
	{
		m_sOdbcDSN.SetSprintf("DRIVER={%s};SERVER={%s};UID={%s};PWD={%s};Database={%s};Trusted_Connection=yes",
			sDriver.cstr(), m_tParams.m_sHost.cstr(), m_tParams.m_sUser.cstr(), m_tParams.m_sPass.cstr(), m_tParams.m_sDB.cstr());
	}
	else
	{
		m_sOdbcDSN.SetSprintf("DRIVER={%s};SERVER={%s};UID={%s};PWD={%s};Database={%s}",
			sDriver.cstr(), m_tParams.m_sHost.cstr(), m_tParams.m_sUser.cstr(), m_tParams.m_sPass.cstr(), m_tParams.m_sDB.cstr());
	}
}

#endif