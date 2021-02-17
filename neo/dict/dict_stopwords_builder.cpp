#include "neo/dict/dict_stopwords_builder.h"
#include "neo/int/vector.h"

void NEO::CSphStopwordBuilderDict::Save(const char* sOutput, int iTop, bool bFreqs)
{
	FILE* fp = fopen(sOutput, "w+");
	if (!fp)
		return;

	CSphVector<Word_t> dTop;
	dTop.Reserve(1024);

	const CSphMTFHashEntry<int>* it;
#define HASH_FOREACH(_it,_hash) \
	for ( _it=_hash.FindFirst(); _it; _it=_hash.FindNext(_it) )
	HASH_FOREACH(it, m_hWords)
	{
		Word_t t;
		t.m_sWord = it->m_sKey.cstr();
		t.m_iCount = it->m_tValue;
		dTop.Add(t);
	}
#undef HASH_FOREACH
	dTop.RSort();

	ARRAY_FOREACH(i, dTop)
	{
		if (i >= iTop)
			break;
		if (bFreqs)
			fprintf(fp, "%s %d\n", dTop[i].m_sWord, dTop[i].m_iCount);
		else
			fprintf(fp, "%s\n", dTop[i].m_sWord);
	}

	fclose(fp);
}


SphWordID_t NEO::CSphStopwordBuilderDict::GetWordID(BYTE* pWord)
{
	int iZero = 0;
	m_hWords.Add((const char*)pWord, 0, iZero)++;
	return 1;
}


SphWordID_t NEO::CSphStopwordBuilderDict::GetWordID(const BYTE* pWord, int iLen, bool)
{
	int iZero = 0;
	m_hWords.Add((const char*)pWord, iLen, iZero)++;
	return 1;
}
