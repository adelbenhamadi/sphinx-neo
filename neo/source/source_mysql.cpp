
#include "neo/source/source_mysql.h"
#include "neo/utility/log.h"
#include "neo/source/def.h"



#if DL_MYSQL
#define MYSQL_F(name) F_DL(name)
#else
#define MYSQL_F(name) F_DR(name)
#endif

MYSQL_F(mysql_free_result);
MYSQL_F(mysql_next_result);
MYSQL_F(mysql_use_result);
MYSQL_F(mysql_num_rows);
MYSQL_F(mysql_query);
MYSQL_F(mysql_errno);
MYSQL_F(mysql_error);
MYSQL_F(mysql_init);
MYSQL_F(mysql_ssl_set);
MYSQL_F(mysql_real_connect);
MYSQL_F(mysql_close);
MYSQL_F(mysql_num_fields);
MYSQL_F(mysql_fetch_row);
MYSQL_F(mysql_fetch_fields);
MYSQL_F(mysql_fetch_lengths);

#if DL_MYSQL
#ifndef MYSQL_LIB
#define MYSQL_LIB "libmysqlclient.so"
#endif

#define MYSQL_NUM_FUNCS (15)

bool InitDynamicMysql()
{
	const char* sFuncs[] = { "mysql_free_result", "mysql_next_result", "mysql_use_result"
		, "mysql_num_rows", "mysql_query", "mysql_errno", "mysql_error"
		, "mysql_init", "mysql_ssl_set", "mysql_real_connect", "mysql_close"
		, "mysql_num_fields", "mysql_fetch_row", "mysql_fetch_fields"
		, "mysql_fetch_lengths" };

	void** pFuncs[] = { (void**)&sph_mysql_free_result, (void**)&sph_mysql_next_result
		, (void**)&sph_mysql_use_result, (void**)&sph_mysql_num_rows, (void**)&sph_mysql_query
		, (void**)&sph_mysql_errno, (void**)&sph_mysql_error, (void**)&sph_mysql_init
		, (void**)&sph_mysql_ssl_set, (void**)&sph_mysql_real_connect, (void**)&sph_mysql_close
		, (void**)&sph_mysql_num_fields, (void**)&sph_mysql_fetch_row
		, (void**)&sph_mysql_fetch_fields, (void**)&sph_mysql_fetch_lengths };

	static CSphDynamicLibrary dLib(MYSQL_LIB);
	if (!dLib.LoadSymbols(sFuncs, pFuncs, MYSQL_NUM_FUNCS))
		return false;
	return true;
}
#else
#define InitDynamicMysql() (true)

#endif

namespace NEO {

	CSphSourceParams_MySQL::CSphSourceParams_MySQL()
		: m_iFlags(0)
	{
		m_iPort = 3306;
	}


	CSphSource_MySQL::CSphSource_MySQL(const char* sName)
		: CSphSource_SQL(sName)
		, m_pMysqlResult(NULL)
		, m_pMysqlFields(NULL)
		, m_tMysqlRow(NULL)
		, m_pMysqlLengths(NULL)
	{
		m_bCanUnpack = true;
	}


	void CSphSource_MySQL::SqlDismissResult()
	{
		if (!m_pMysqlResult)
			return;

		while (m_pMysqlResult)
		{
			sph_mysql_free_result(m_pMysqlResult);
			m_pMysqlResult = NULL;

			// stored procedures might return multiple result sets
			// FIXME? we might want to index all of them
			// but for now, let's simply dismiss additional result sets
			if (sph_mysql_next_result(&m_tMysqlDriver) == 0)
			{
				m_pMysqlResult = sph_mysql_use_result(&m_tMysqlDriver);

				static bool bOnce = false;
				if (!bOnce && m_pMysqlResult && sph_mysql_num_rows(m_pMysqlResult))
				{
					sphWarn("indexing of multiple result sets is not supported yet; some results sets were dismissed!");
					bOnce = true;
				}
			}
		}

		m_pMysqlFields = NULL;
		m_pMysqlLengths = NULL;
	}


	bool CSphSource_MySQL::SqlQuery(const char* sQuery)
	{
		if (sph_mysql_query(&m_tMysqlDriver, sQuery))
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-QUERY: %s: FAIL\n", sQuery);
			return false;
		}
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-QUERY: %s: ok\n", sQuery);

		m_pMysqlResult = sph_mysql_use_result(&m_tMysqlDriver);
		m_pMysqlFields = NULL;
		return true;
	}


	bool CSphSource_MySQL::SqlIsError()
	{
		return sph_mysql_errno(&m_tMysqlDriver) != 0;
	}


	const char* CSphSource_MySQL::SqlError()
	{
		return sph_mysql_error(&m_tMysqlDriver);
	}


	bool CSphSource_MySQL::SqlConnect()
	{
		if_const(!InitDynamicMysql())
		{
			if (m_tParams.m_bPrintQueries)
				fprintf(stdout, "SQL-CONNECT: FAIL (NO MYSQL CLIENT LIB)\n");
			return false;
		}

		sph_mysql_init(&m_tMysqlDriver);
		if (!m_sSslKey.IsEmpty() || !m_sSslCert.IsEmpty() || !m_sSslCA.IsEmpty())
			sph_mysql_ssl_set(&m_tMysqlDriver, m_sSslKey.cstr(), m_sSslCert.cstr(), m_sSslCA.cstr(), NULL, NULL);

		m_iMysqlConnectFlags |= CLIENT_MULTI_RESULTS; // we now know how to handle this
		bool bRes = (NULL != sph_mysql_real_connect(&m_tMysqlDriver,
			m_tParams.m_sHost.cstr(), m_tParams.m_sUser.cstr(), m_tParams.m_sPass.cstr(),
			m_tParams.m_sDB.cstr(), m_tParams.m_iPort, m_sMysqlUsock.cstr(), m_iMysqlConnectFlags));
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, bRes ? "SQL-CONNECT: ok\n" : "SQL-CONNECT: FAIL\n");
		return bRes;
	}


	void CSphSource_MySQL::SqlDisconnect()
	{
		if (m_tParams.m_bPrintQueries)
			fprintf(stdout, "SQL-DISCONNECT\n");

		sph_mysql_close(&m_tMysqlDriver);
	}


	int CSphSource_MySQL::SqlNumFields()
	{
		if (!m_pMysqlResult)
			return -1;

		return sph_mysql_num_fields(m_pMysqlResult);
	}


	bool CSphSource_MySQL::SqlFetchRow()
	{
		if (!m_pMysqlResult)
			return false;

		m_tMysqlRow = sph_mysql_fetch_row(m_pMysqlResult);
		return m_tMysqlRow != NULL;
	}


	const char* CSphSource_MySQL::SqlColumn(int iIndex)
	{
		if (!m_pMysqlResult)
			return NULL;

		return m_tMysqlRow[iIndex];
	}


	const char* CSphSource_MySQL::SqlFieldName(int iIndex)
	{
		if (!m_pMysqlResult)
			return NULL;

		if (!m_pMysqlFields)
			m_pMysqlFields = sph_mysql_fetch_fields(m_pMysqlResult);

		return m_pMysqlFields[iIndex].name;
	}


	DWORD CSphSource_MySQL::SqlColumnLength(int iIndex)
	{
		if (!m_pMysqlResult)
			return 0;

		if (!m_pMysqlLengths)
			m_pMysqlLengths = sph_mysql_fetch_lengths(m_pMysqlResult);

		return m_pMysqlLengths[iIndex];
	}


	bool CSphSource_MySQL::Setup(const CSphSourceParams_MySQL& tParams)
	{
		if (!CSphSource_SQL::Setup(tParams))
			return false;

		m_sMysqlUsock = tParams.m_sUsock;
		m_iMysqlConnectFlags = tParams.m_iFlags;
		m_sSslKey = tParams.m_sSslKey;
		m_sSslCert = tParams.m_sSslCert;
		m_sSslCA = tParams.m_sSslCA;

		// build and store DSN for error reporting
		char sBuf[1024];
		snprintf(sBuf, sizeof(sBuf), "mysql%s", m_sSqlDSN.cstr() + 3);
		m_sSqlDSN = sBuf;

		return true;
	}


}