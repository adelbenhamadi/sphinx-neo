#pragma once
#if USE_LIBEXPAT

#include "neo/source/document.h"
#include "neo/source/schema.h"
#include "neo/tools/config_parser.h"
#include "neo/source/schema_configurator.h"

#if DL_EXPAT
#define EXPAT_F(name) F_DL(name)
#else
#define EXPAT_F(name) F_DR(name)
#endif


EXPAT_F(XML_ParserFree);
EXPAT_F(XML_Parse);
EXPAT_F(XML_GetCurrentColumnNumber);
EXPAT_F(XML_GetCurrentLineNumber);
EXPAT_F(XML_GetErrorCode);
EXPAT_F(XML_ErrorString);
EXPAT_F(XML_ParserCreate);
EXPAT_F(XML_SetUserData);
EXPAT_F(XML_SetElementHandler);
EXPAT_F(XML_SetCharacterDataHandler);
EXPAT_F(XML_SetUnknownEncodingHandler);

#if DL_EXPAT
#ifndef EXPAT_LIB
#define EXPAT_LIB "libexpat.so"
#endif

#define EXPAT_NUM_FUNCS (11)

bool InitDynamicExpat()
{
	const char* sFuncs[] = { "XML_ParserFree", "XML_Parse",
			"XML_GetCurrentColumnNumber", "XML_GetCurrentLineNumber", "XML_GetErrorCode", "XML_ErrorString",
			"XML_ParserCreate", "XML_SetUserData", "XML_SetElementHandler", "XML_SetCharacterDataHandler",
			"XML_SetUnknownEncodingHandler" };

	void** pFuncs[] = { (void**)&sph_XML_ParserFree, (void**)&sph_XML_Parse,
			(void**)&sph_XML_GetCurrentColumnNumber, (void**)&sph_XML_GetCurrentLineNumber,
			(void**)&sph_XML_GetErrorCode, (void**)&sph_XML_ErrorString,
			(void**)&sph_XML_ParserCreate, (void**)&sph_XML_SetUserData,
			(void**)&sph_XML_SetElementHandler, (void**)&sph_XML_SetCharacterDataHandler,
			(void**)&sph_XML_SetUnknownEncodingHandler };

	static CSphDynamicLibrary dLib(EXPAT_LIB);
	if (!dLib.LoadSymbols(sFuncs, pFuncs, EXPAT_NUM_FUNCS))
		return false;
	return true;
}

#else
#define InitDynamicExpat() (true)
#endif

namespace NEO {

	class CSphSource_XMLPipe2 : public CSphSource_Document, public CSphSchemaConfigurator<CSphSource_XMLPipe2>
	{
	public:
		explicit			CSphSource_XMLPipe2(const char* sName);
		~CSphSource_XMLPipe2();

		bool			Setup(int iFieldBufferMax, bool bFixupUTF8, FILE* pPipe, const CSphConfigSection& hSource, CSphString& sError);			//memorize the command
		virtual bool	Connect(CSphString& sError);			//run the command and open the pipe
		virtual void	Disconnect();								//close the pipe

		virtual bool	IterateStart(CSphString&) { m_iPlainFieldsLength = m_tSchema.m_dFields.GetLength(); return true; }	//Connect() starts getting documents automatically, so this one is empty
		virtual BYTE** NextDocument(CSphString& sError);			//parse incoming chunk and emit some hits
		virtual const int* GetFieldLengths() const { return m_dFieldLengths.Begin(); }

		virtual bool	HasAttrsConfigured() { return true; }	//xmlpipe always has some attrs for now
		virtual bool	IterateMultivaluedStart(int, CSphString&) { return false; }
		virtual bool	IterateMultivaluedNext() { return false; }
		virtual bool	IterateKillListStart(CSphString&);
		virtual bool	IterateKillListNext(SphDocID_t& uDocId);

		void			StartElement(const char* szName, const char** pAttrs);
		void			EndElement(const char* pName);
		void			Characters(const char* pCharacters, int iLen);

		void			Error(const char* sTemplate, ...) __attribute__((format(printf, 2, 3)));
		const char* DecorateMessage(const char* sTemplate, ...) const __attribute__((format(printf, 2, 3)));
		const char* DecorateMessageVA(const char* sTemplate, va_list ap) const;

	private:
		struct Document_t
		{
			SphDocID_t					m_uDocID;
			CSphVector < CSphVector<BYTE> >	m_dFields;
			CSphVector<CSphString>		m_dAttrs;
		};

		Document_t* m_pCurDocument;
		CSphVector<Document_t*>	m_dParsedDocuments;

		FILE* m_pPipe;			//incoming stream
		CSphString		m_sError;
		CSphVector<CSphString> m_dDefaultAttrs;
		CSphVector<CSphString> m_dInvalid;
		CSphVector<CSphString> m_dWarned;
		int				m_iElementDepth;

		BYTE* m_pBuffer;
		int				m_iBufferSize;

		CSphVector<BYTE*>m_dFieldPtrs;
		CSphVector<int>	m_dFieldLengths;
		bool			m_bRemoveParsed;

		bool			m_bInDocset;
		bool			m_bInSchema;
		bool			m_bInDocument;
		bool			m_bInKillList;
		bool			m_bInId;
		bool			m_bInIgnoredTag;
		bool			m_bFirstTagAfterDocset;

		int				m_iKillListIterator;
		CSphVector < SphDocID_t > m_dKillList;

		int				m_iMVA;
		int				m_iMVAIterator;
		CSphVector < CSphVector <DWORD> > m_dFieldMVAs;
		CSphVector < int > m_dAttrToMVA;

		int				m_iCurField;
		int				m_iCurAttr;

		XML_Parser		m_pParser;

		int				m_iFieldBufferMax;
		BYTE* m_pFieldBuffer;
		int				m_iFieldBufferLen;

		bool			m_bFixupUTF8;		//whether to replace invalid utf-8 codepoints with spaces
		int				m_iReparseStart;	//utf-8 fixerupper might need to postpone a few bytes, starting at this offset
		int				m_iReparseLen;		//and this much bytes (under 4)

		void			UnexpectedCharaters(const char* pCharacters, int iLen, const char* szComment);

		bool			ParseNextChunk(int iBufferLen, CSphString& sError);

		void DocumentError(const char* sWhere)
		{
			Error("malformed source, <sphinx:document> found inside %s", sWhere);

			// Ideally I'd like to display a notice on the next line that
			// would say where exactly it's allowed. E.g.:
			//
			// <sphinx:document> must be contained in <sphinx:docset>
		}
	};

	// callbacks
	static void XMLCALL xmlStartElement(void* user_data, const XML_Char* name, const XML_Char** attrs)
	{
		CSphSource_XMLPipe2* pSource = (CSphSource_XMLPipe2*)user_data;
		pSource->StartElement(name, attrs);
	}


	static void XMLCALL xmlEndElement(void* user_data, const XML_Char* name)
	{
		CSphSource_XMLPipe2* pSource = (CSphSource_XMLPipe2*)user_data;
		pSource->EndElement(name);
	}


	static void XMLCALL xmlCharacters(void* user_data, const XML_Char* ch, int len)
	{
		CSphSource_XMLPipe2* pSource = (CSphSource_XMLPipe2*)user_data;
		pSource->Characters(ch, len);
	}

#if USE_LIBICONV
	static int XMLCALL xmlUnknownEncoding(void*, const XML_Char* name, XML_Encoding* info)
	{
		iconv_t pDesc = iconv_open("UTF-16", name);
		if (!pDesc)
			return XML_STATUS_ERROR;

		for (size_t i = 0; i < 256; i++)
		{
			char cIn = (char)i;
			char dOut[4];
			memset(dOut, 0, sizeof(dOut));
#if ICONV_INBUF_CONST
			const char* pInbuf = &cIn;
#else
			char* pInbuf = &cIn;
#endif
			char* pOutbuf = dOut;
			size_t iInBytesLeft = 1;
			size_t iOutBytesLeft = 4;

			if (iconv(pDesc, &pInbuf, &iInBytesLeft, &pOutbuf, &iOutBytesLeft) != size_t(-1))
				info->map[i] = int(BYTE(dOut[0])) << 8 | int(BYTE(dOut[1]));
			else
				info->map[i] = 0;
		}

		iconv_close(pDesc);

		return XML_STATUS_OK;
	}
#endif

}
#endif