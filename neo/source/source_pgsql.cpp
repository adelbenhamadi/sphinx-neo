#if USE_PGSQL
#include "neo/source/source_pgsql.h"

namespace NEO {
	CSphSourceParams_PgSQL::CSphSourceParams_PgSQL()
	{
		m_iRangeStep = 1024;
		m_iPort = 5432;
	}


	CSphSource_PgSQL::CSphSource_PgSQL(const char* sName)
		: CSphSource_SQL(sName)
		, m_pPgResult(NULL)
		, m_iPgRows(0)
		, m_iPgRow(0)
	{
	}


	bool CSphSource_PgSQL::SqlIsError()
	{
		return (m_iPgRow < m_iPgRows); // if we're over, it's just last row
	}


	const char* CSphSource_PgSQL::SqlError()
	{
		return sph_PQerrorMessage(m_tPgDriver);
	}


	bool CSphSource_PgSQL::Setup(const CSphSourceParams_PgSQL& tParams)
	{
		// checks
		CSphSource_SQL::Setup(tParams);

		m_sPgClientEncoding = tParams.m_sClientEncoding;
		if (!m_sPgClientEncoding.cstr())
			m_sPgClientEncoding = "";

		// build and store DSN for error reporting
		char sBuf[1024];
		snprintf(sBuf, sizeof(sBuf), "pgsql%s", m_sSqlDSN.cstr() + 3);
		m_sSqlDSN = sBuf;

		return true;
	}


	bool CSphSource_PgSQL::IterateStart(CSphString& sError)
	{
		bool bResult = CSphSource_SQL::IterateStart(sError);
		if (!bResult)
			return false;

		int iMaxIndex = 0;
		for (int i = 0; i < m_tSchema.GetAttrsCount(); i++)
			iMaxIndex = Max(iMaxIndex, m_tSchema.GetAttr(i).m_iIndex);

		ARRAY_FOREACH(i, m_tSchema.m_dFields)
			iMaxIndex = Max(iMaxIndex, m_tSchema.m_dFields[i].m_iIndex);

		m_dIsColumnBool.Resize(iMaxIndex + 1);
		ARRAY_FOREACH(i, m_dIsColumnBool)
			m_dIsColumnBool[i] = false;

		for (int i = 0; i < m_tSchema.GetAttrsCount(); i++)
			m_dIsColumnBool[m_tSchema.GetAttr(i).m_iIndex] = (m_tSchema.GetAttr(i).m_eAttrType == ESphAttr::SPH_ATTR_BOOL);

		return true;
	}


	bool CSphSource_PgSQL::SqlConnect()
	{
		if (!InitDynamicPosgresql())
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL (NO POSGRES CLIENT LIB)\n");
			return false;
		}

		char sPort[64];
		snprintf(sPort, sizeof(sPort), "%d", m_tParams.m_iPort);
		m_tPgDriver = sph_PQsetdbLogin(m_tParams.m_sHost.cstr(), sPort, NULL, NULL,
			m_tParams.m_sDB.cstr(), m_tParams.m_sUser.cstr(), m_tParams.m_sPass.cstr());

		if (sph_PQstatus(m_tPgDriver) == CONNECTION_BAD)
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL\n");
			return false;
		}

		// set client encoding
		if (!m_sPgClientEncoding.IsEmpty())
			if (-1 == sph_PQsetClientEncoding(m_tPgDriver, m_sPgClientEncoding.cstr()))
			{
				SqlDisconnect();
				if (m_tParams.m_bPrintQueries)
					fprintf(stdout, "SQL-CONNECT: FAIL\n");
				return false;
			}

		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-CONNECT: ok\n");
		return true;
	}


	void CSphSource_PgSQL::SqlDisconnect()
	{
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-DISCONNECT\n");

		sph_PQfinish(m_tPgDriver);
	}


	bool CSphSource_PgSQL::SqlQuery(const char* sQuery)
	{
		m_iPgRow = -1;
		m_iPgRows = 0;

		m_pPgResult = sph_PQexec(m_tPgDriver, sQuery);

		ExecStatusType eRes = sph_PQresultStatus(m_pPgResult);
		if ((eRes != PGRES_COMMAND_OK) && (eRes != PGRES_TUPLES_OK))
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-QUERY: %s: FAIL\n", sQuery);
			return false;
		}
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-QUERY: %s: ok\n", sQuery);

		m_iPgRows = sph_PQntuples(m_pPgResult);
		return true;
	}


	void CSphSource_PgSQL::SqlDismissResult()
	{
		if (!m_pPgResult)
			return;

		sph_PQclear(m_pPgResult);
		m_pPgResult = NULL;
	}


	int CSphSource_PgSQL::SqlNumFields()
	{
		if (!m_pPgResult)
			return -1;

		return sph_PQnfields(m_pPgResult);
	}


	const char* CSphSource_PgSQL::SqlColumn(int iIndex)
	{
		if (!m_pPgResult)
			return NULL;

		const char* szValue = sph_PQgetvalue(m_pPgResult, m_iPgRow, iIndex);
		if (m_dIsColumnBool.GetLength() && m_dIsColumnBool[iIndex] && szValue[0] == 't' && !szValue[1])
			return "1";

		return szValue;
	}


	const char* CSphSource_PgSQL::SqlFieldName(int iIndex)
	{
		if (!m_pPgResult)
			return NULL;

		return sph_PQfname(m_pPgResult, iIndex);
	}


	bool CSphSource_PgSQL::SqlFetchRow()
	{
		if (!m_pPgResult)
			return false;
		return (++m_iPgRow < m_iPgRows);
	}


	DWORD CSphSource_PgSQL::SqlColumnLength(int iIndex)
	{
		return sph_PQgetlength(m_pPgResult, m_iPgRow, iIndex);
	}

}

#endif