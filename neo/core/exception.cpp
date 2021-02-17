#include "neo/core/exception.h"


const BYTE* NEO::ExceptionsTrie_c::GetMapping(size_t i) const
{
	assert(i >= 0 && i < m_iMappings);
	auto p = *(size_t*)&m_dData[i];
	if (!p)
		return NULL;
	assert(p >= m_iMappings && p < m_dData.GetLength());
	return &m_dData[p];
}

size_t NEO::ExceptionsTrie_c::GetNext(size_t i, BYTE v) const
{
	assert(i >= 0 && i < m_iMappings);
	if (i == 0)
		return m_dFirst[v];
	const BYTE* p = &m_dData[i];
	int n = p[4];
	p += 5;
	for (i = 0; i < n; i++)
		if (p[i] == v)
			return *(int*)&p[n + 4 * i]; // FIXME? unaligned
	return -1;
}
void NEO::ExceptionsTrie_c::Export(CSphWriter& w) const
{
	CSphVector<BYTE> dPrefix;
	size_t iCount = 0;

	w.PutDword((DWORD)m_iCount);
	Export(w, dPrefix, 0, &iCount);
	assert(iCount == m_iCount);
}