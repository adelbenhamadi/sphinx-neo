#pragma once
#include "neo/source/document.h"
#include "neo/tools/config_parser.h"
#include "neo/source/schema_configurator.h"

namespace NEO {

	struct RemapXSV_t
	{
		int m_iAttr;
		int m_iField;
	};


	class CSphSource_BaseSV : public CSphSource_Document, public CSphSchemaConfigurator<CSphSource_BaseSV>
	{
	public:
		explicit		CSphSource_BaseSV(const char* sName);
		virtual			~CSphSource_BaseSV();

		virtual bool	Connect(CSphString& sError);				//run the command and open the pipe
		virtual void	Disconnect();									//close the pipe
		const char* DecorateMessage(const char* sTemplate, ...) const __attribute__((format(printf, 2, 3)));

		virtual bool	IterateStart(CSphString&);					//Connect() starts getting documents automatically, so this one is empty
		virtual BYTE** NextDocument(CSphString&);					//parse incoming chunk and emit some hits
		virtual const int* GetFieldLengths() const { return m_dFieldLengths.Begin(); }

		virtual bool	HasAttrsConfigured() { return (m_tSchema.GetAttrsCount() > 0); }
		virtual bool	IterateMultivaluedStart(int, CSphString&) { return false; }
		virtual bool	IterateMultivaluedNext() { return false; }
		virtual bool	IterateKillListStart(CSphString&) { return false; }
		virtual bool	IterateKillListNext(SphDocID_t&) { return false; }

		bool			Setup(const CSphConfigSection& hSource, FILE* pPipe, CSphString& sError);

	protected:
		enum ESphParseResult
		{
			PARSING_FAILED,
			GOT_DOCUMENT,
			DATA_OVER
		};

		BYTE** ReportDocumentError();
		virtual bool			SetupSchema(const CSphConfigSection& hSource, bool bWordDict, CSphString& sError) = 0;
		virtual ESphParseResult	SplitColumns(CSphString&) = 0;

		CSphVector<BYTE>			m_dBuf;
		CSphFixedVector<char>		m_dError;
		CSphFixedVector<int>		m_dColumnsLen;
		CSphFixedVector<RemapXSV_t>	m_dRemap;

		// output
		CSphFixedVector<BYTE*>		m_dFields;
		CSphFixedVector<int>		m_dFieldLengths;

		FILE* m_pFP;
		int							m_iDataStart;		//where the next line to parse starts in m_dBuf
		int							m_iDocStart;		//where the last parsed document stats in m_dBuf
		int							m_iBufUsed;			//bytes [0,m_iBufUsed) are actually currently used; the rest of m_dBuf is free
		int							m_iLine;
		int							m_iAutoCount;
	};


	class CSphSource_TSV : public CSphSource_BaseSV
	{
	public:
		explicit				CSphSource_TSV(const char* sName) : CSphSource_BaseSV(sName) {}
		virtual ESphParseResult	SplitColumns(CSphString& sError);					//parse incoming chunk and emit some hits
		virtual bool			SetupSchema(const CSphConfigSection& hSource, bool bWordDict, CSphString& sError);
	};


	class CSphSource_CSV : public CSphSource_BaseSV
	{
	public:
		explicit				CSphSource_CSV(const char* sName);
		virtual ESphParseResult	SplitColumns(CSphString& sError);					//parse incoming chunk and emit some hits
		virtual bool			SetupSchema(const CSphConfigSection& hSource, bool bWordDict, CSphString& sError);
		void					SetDelimiter(const char* sDelimiter);

	private:
		BYTE			m_iDelimiter;
	};

	CSphSource* sphCreateSourceTSVpipe(const CSphConfigSection* pSource, FILE* pPipe, const char* sSourceName, bool bProxy);
	CSphSource* sphCreateSourceCSVpipe(const CSphConfigSection* pSource, FILE* pPipe, const char* sSourceName, bool bProxy);


}