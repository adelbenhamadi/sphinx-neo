#pragma once
#include "neo/source/enums.h"
#include "neo/source/document.h"
#include "neo/source/column_info.h"


namespace NEO {
	/// generic SQL source params
	struct CSphSourceParams_SQL
	{
		// query params
		CSphString						m_sQuery;
		CSphString						m_sQueryRange;
		CSphString						m_sQueryKilllist;
		int64_t							m_iRangeStep;
		int64_t							m_iRefRangeStep;
		bool							m_bPrintQueries;

		CSphVector<CSphString>			m_dQueryPre;
		CSphVector<CSphString>			m_dQueryPost;
		CSphVector<CSphString>			m_dQueryPostIndex;
		CSphVector<CSphColumnInfo>		m_dAttrs;
		CSphVector<CSphString>			m_dFileFields;

		int								m_iRangedThrottle;
		int								m_iMaxFileBufferSize;
		ESphOnFileFieldError			m_eOnFileFieldError;

		CSphVector<CSphUnpackInfo>		m_dUnpack;
		DWORD							m_uUnpackMemoryLimit;

		CSphVector<CSphJoinedField>		m_dJoinedFields;

		// connection params
		CSphString						m_sHost;
		CSphString						m_sUser;
		CSphString						m_sPass;
		CSphString						m_sDB;
		int								m_iPort;

		// hooks
		CSphString						m_sHookConnect;
		CSphString						m_sHookQueryRange;
		CSphString						m_sHookPostIndex;

		CSphSourceParams_SQL();
	};


	/// generic SQL source
	/// multi-field plain-text documents fetched from given query
	struct CSphSource_SQL : CSphSource_Document
	{
		explicit			CSphSource_SQL(const char* sName);
		virtual				~CSphSource_SQL() {}

		bool				Setup(const CSphSourceParams_SQL& pParams);
		virtual bool		Connect(CSphString& sError);
		virtual void		Disconnect();

		virtual bool		IterateStart(CSphString& sError);
		virtual BYTE** NextDocument(CSphString& sError);
		virtual const int* GetFieldLengths() const;
		virtual void		PostIndex();

		virtual bool		HasAttrsConfigured() { return m_tParams.m_dAttrs.GetLength() != 0; }

		virtual ISphHits* IterateJoinedHits(CSphString& sError);

		virtual bool		IterateMultivaluedStart(int iAttr, CSphString& sError);
		virtual bool		IterateMultivaluedNext();

		virtual bool		IterateKillListStart(CSphString& sError);
		virtual bool		IterateKillListNext(SphDocID_t& tDocId);

	private:
		bool				m_bSqlConnected;	///< am i connected?

	protected:
		CSphString			m_sSqlDSN;

		BYTE* m_dFields[SPH_MAX_FIELDS];
		int					m_dFieldLengths[SPH_MAX_FIELDS];
		ESphUnpackFormat	m_dUnpack[SPH_MAX_FIELDS];

		SphDocID_t			m_uMinID;			///< grand min ID
		SphDocID_t			m_uMaxID;			///< grand max ID
		SphDocID_t			m_uCurrentID;		///< current min ID
		SphDocID_t			m_uMaxFetchedID;	///< max actually fetched ID
		int					m_iMultiAttr;		///< multi-valued attr being currently fetched
		int					m_iSqlFields;		///< field count (for row dumper)

		CSphSourceParams_SQL		m_tParams;

		bool				m_bCanUnpack;
		bool				m_bUnpackFailed;
		bool				m_bUnpackOverflow;
		CSphVector<char>	m_dUnpackBuffers[SPH_MAX_FIELDS];

		int					m_iJoinedHitField;	///< currently pulling joined hits from this field (index into schema; -1 if not pulling)
		SphDocID_t			m_iJoinedHitID;		///< last document id
		int					m_iJoinedHitPos;	///< last hit position

		static const int			MACRO_COUNT = 2;
		static const char* const	MACRO_VALUES[MACRO_COUNT];

	protected:
		/// by what reason the internal SetupRanges called
		enum ERangesReason
		{
			SRE_DOCS,
			SRE_MVA,
			SRE_JOINEDHITS
		};

	protected:
		bool					SetupRanges(const char* sRangeQuery, const char* sQuery, const char* sPrefix, CSphString& sError, ERangesReason iReason);
		bool					RunQueryStep(const char* sQuery, CSphString& sError);

	protected:
		virtual void			SqlDismissResult() = 0;
		virtual bool			SqlQuery(const char* sQuery) = 0;
		virtual bool			SqlIsError() = 0;
		virtual const char* SqlError() = 0;
		virtual bool			SqlConnect() = 0;
		virtual void			SqlDisconnect() = 0;
		virtual int				SqlNumFields() = 0;
		virtual bool			SqlFetchRow() = 0;
		virtual DWORD			SqlColumnLength(int iIndex) = 0;
		virtual const char* SqlColumn(int iIndex) = 0;
		virtual const char* SqlFieldName(int iIndex) = 0;

		const char* SqlUnpackColumn(int iIndex, DWORD& uUnpackedLen, ESphUnpackFormat eFormat);
		void			ReportUnpackError(int iIndex, int iError);
	};


	void SqlAttrsConfigure(CSphSourceParams_SQL& tParams, const CSphVariant* pHead,
		ESphAttr eAttrType, const char* sSourceName, bool bIndexedAttr = false);
}