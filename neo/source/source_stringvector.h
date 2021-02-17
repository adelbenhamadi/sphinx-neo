#pragma once
#include "neo/source/document.h"
#include "neo/source/enums.h"

namespace NEO {

	class CSphSource_StringVector :public  CSphSource_Document
	{
	public:
		explicit			CSphSource_StringVector(int iFields, const char** ppFields, const CSphSchema& tSchema);
		virtual				~CSphSource_StringVector() {}

		virtual bool		Connect(CSphString&);
		virtual void		Disconnect();

		virtual bool		HasAttrsConfigured() { return false; }
		virtual bool		IterateStart(CSphString&) { m_iPlainFieldsLength = (int)m_tSchema.m_dFields.GetLength(); return true; }

		virtual bool		IterateMultivaluedStart(int, CSphString&) { return false; }
		virtual bool		IterateMultivaluedNext() { return false; }

		virtual bool		IterateFieldMVAStart(int, CSphString&) { return false; }
		virtual bool		IterateFieldMVANext() { return false; }

		virtual bool		IterateKillListStart(CSphString&) { return false; }
		virtual bool		IterateKillListNext(SphDocID_t&) { return false; }

		virtual BYTE** NextDocument(CSphString&) { return m_dFields.Begin(); }
		virtual const int* GetFieldLengths() const { return m_dFieldLengths.Begin(); }

	protected:
		CSphVector<BYTE*>			m_dFields;
		CSphVector<int>				m_dFieldLengths;
	};


}
