#pragma once
#if USE_PGSQL
#include "neo/source/source_sql.h"
#include "neo/source/def.h"

#include <libpq-fe.h>

#if DL_PGSQL
#define PGSQL_F(name) F_DL(name)
#else
#define PGSQL_F(name) F_DR(name)
#endif


PGSQL_F(PQgetvalue);
PGSQL_F(PQgetlength);
PGSQL_F(PQclear);
PGSQL_F(PQsetdbLogin);
PGSQL_F(PQstatus);
PGSQL_F(PQsetClientEncoding);
PGSQL_F(PQexec);
PGSQL_F(PQresultStatus);
PGSQL_F(PQntuples);
PGSQL_F(PQfname);
PGSQL_F(PQnfields);
PGSQL_F(PQfinish);
PGSQL_F(PQerrorMessage);

#if DL_PGSQL
#ifndef PGSQL_LIB
#define PGSQL_LIB "libpq.so"
#endif

#define POSTRESQL_NUM_FUNCS (13)

bool InitDynamicPosgresql()
{
	const char* sFuncs[] = { "PQgetvalue", "PQgetlength", "PQclear",
			"PQsetdbLogin", "PQstatus", "PQsetClientEncoding", "PQexec",
			"PQresultStatus", "PQntuples", "PQfname", "PQnfields",
			"PQfinish", "PQerrorMessage" };

	void** pFuncs[] = { (void**)&sph_Qgetvalue, (void**)&sph_PQgetlength, (void**)&sph_PQclear,
			(void**)&sph_PQsetdbLogin, (void**)&sph_PQstatus, (void**)&sph_PQsetClientEncoding,
			(void**)&sph_PQexec, (void**)&sph_PQresultStatus, (void**)&sph_PQntuples,
			(void**)&sph_PQfname, (void**)&sph_PQnfields, (void**)&sph_PQfinish,
			(void**)&sph_PQerrorMessage };

	static CSphDynamicLibrary dLib(PGSQL_LIB);
	if (!dLib.LoadSymbols(sFuncs, pFuncs, POSTRESQL_NUM_FUNCS))
		return false;
	return true;
}

#else
#define InitDynamicPosgresql() (true)
#endif

namespace NEO {
/// PgSQL specific source params
struct CSphSourceParams_PgSQL : CSphSourceParams_SQL
{
	CSphString		m_sClientEncoding;
					CSphSourceParams_PgSQL ();
};


/// PgSQL source implementation
/// multi-field plain-text documents fetched from given query
struct CSphSource_PgSQL : CSphSource_SQL
{
	explicit				CSphSource_PgSQL ( const char * sName );
	bool					Setup ( const CSphSourceParams_PgSQL & pParams );
	virtual bool			IterateStart ( CSphString & sError );

protected:
	PGresult * 				m_pPgResult;	///< postgresql execution restult context
	PGconn *				m_tPgDriver;	///< postgresql connection context

	int						m_iPgRows;		///< how much rows last step returned
	int						m_iPgRow;		///< current row (0 based, as in PQgetvalue)

	CSphString				m_sPgClientEncoding;
	CSphVector<bool>		m_dIsColumnBool;

protected:
	virtual void			SqlDismissResult ();
	virtual bool			SqlQuery ( const char * sQuery );
	virtual bool			SqlIsError ();
	virtual const char *	SqlError ();
	virtual bool			SqlConnect ();
	virtual void			SqlDisconnect ();
	virtual int				SqlNumFields();
	virtual bool			SqlFetchRow();
	virtual DWORD	SqlColumnLength ( int iIndex );
	virtual const char *	SqlColumn ( int iIndex );
	virtual const char *	SqlFieldName ( int iIndex );
};

}

#endif