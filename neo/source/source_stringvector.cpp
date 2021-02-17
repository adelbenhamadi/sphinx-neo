#include "neo/source/source_stringvector.h"
#include "neo/source/schema.h"


NEO::CSphSource_StringVector::CSphSource_StringVector(int iFields, const char** ppFields, const CSphSchema& tSchema)
	: CSphSource_Document("$stringvector")
{
	m_tSchema = tSchema;

	m_dFields.Resize(1 + iFields);
	m_dFieldLengths.Resize(iFields);
	for (int i = 0; i < iFields; i++)
	{
		m_dFields[i] = (BYTE*)ppFields[i];
		m_dFieldLengths[i] = strlen(ppFields[i]);
		assert(m_dFields[i]);
	}
	m_dFields[iFields] = NULL;

	m_iMaxHits = 0; // force all hits build
}

bool NEO::CSphSource_StringVector::Connect(CSphString&)
{
	// no AddAutoAttrs() here; they should already be in the schema
	m_tHits.m_dData.Reserve(1024);
	return true;
}

void NEO::CSphSource_StringVector::Disconnect()
{
	m_tHits.m_dData.Reset();
}
