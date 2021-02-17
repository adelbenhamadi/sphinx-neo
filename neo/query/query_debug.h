#pragma once


/// query debugging printouts
#define QDEBUG 0
// #define XQ_DUMP_TRANSFORMED_TREE 1
// #define XQ_DUMP_NODE_ADDR 1



#if QDEBUG
#define QDEBUGARG(_arg) _arg
#else
#define QDEBUGARG(_arg)
#endif

namespace NEO {

	static void PrintDocsChunk(int QDEBUGARG(iCount), int QDEBUGARG(iAtomPos), const ExtDoc_t* QDEBUGARG(pDocs), const char* QDEBUGARG(sNode), void* QDEBUGARG(pNode))
	{
#if QDEBUG
		CSphStringBuilder tRes;
		tRes.Appendf("node %s 0x%x:%p getdocs (%d) = [", sNode ? sNode : "???", iAtomPos, pNode, iCount);
		for (int i = 0; i < iCount; i++)
			tRes.Appendf(i ? ", 0x%x" : "0x%x", DWORD(pDocs[i].m_uDocid));
		tRes.Appendf("]");
		printf("%s", tRes.cstr());
#endif
	}

	static void PrintHitsChunk(int QDEBUGARG(iCount), int QDEBUGARG(iAtomPos), const ExtHit_t* QDEBUGARG(pHits), const char* QDEBUGARG(sNode), void* QDEBUGARG(pNode))
	{
#if QDEBUG
		CSphStringBuilder tRes;
		tRes.Appendf("node %s 0x%x:%p gethits (%d) = [", sNode ? sNode : "???", iAtomPos, pNode, iCount);
		for (int i = 0; i < iCount; i++)
			tRes.Appendf(i ? ", 0x%x:0x%x" : "0x%x:0x%x", DWORD(pHits[i].m_uDocid), DWORD(pHits[i].m_uHitpos));
		tRes.Appendf("]");
		printf("%s", tRes.cstr());
#endif
	}


}