#pragma once

#include "neo/source/base.h"
#include "neo/source/source_sql.h"
#include "mysql.h"

namespace NEO {


	/// MySQL source params
	struct CSphSourceParams_MySQL : CSphSourceParams_SQL
	{
		CSphString	m_sUsock;					///< UNIX socket
		int			m_iFlags;					///< connection flags
		CSphString	m_sSslKey;
		CSphString	m_sSslCert;
		CSphString	m_sSslCA;

		CSphSourceParams_MySQL();	///< ctor. sets defaults
	};


	/// MySQL source implementation
	/// multi-field plain-text documents fetched from given query
	struct CSphSource_MySQL : CSphSource_SQL
	{
		explicit				CSphSource_MySQL(const char* sName);
		bool					Setup(const CSphSourceParams_MySQL& tParams);

	protected:
		MYSQL_RES* m_pMysqlResult;
		MYSQL_FIELD* m_pMysqlFields;
		MYSQL_ROW				m_tMysqlRow;
		MYSQL					m_tMysqlDriver;
		unsigned long* m_pMysqlLengths;

		CSphString				m_sMysqlUsock;
		int						m_iMysqlConnectFlags;
		CSphString				m_sSslKey;
		CSphString				m_sSslCert;
		CSphString				m_sSslCA;

	protected:
		virtual void			SqlDismissResult();
		virtual bool			SqlQuery(const char* sQuery);
		virtual bool			SqlIsError();
		virtual const char* SqlError();
		virtual bool			SqlConnect();
		virtual void			SqlDisconnect();
		virtual int				SqlNumFields();
		virtual bool			SqlFetchRow();
		virtual DWORD			SqlColumnLength(int iIndex);
		virtual const char* SqlColumn(int iIndex);
		virtual const char* SqlFieldName(int iIndex);
	};

}