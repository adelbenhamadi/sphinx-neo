#include "neo/core/kill_list.h"
#include "neo/int/types.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"
#include "neo/io/io.h"

namespace NEO {

	// is already id32<>id64 safe
	void CSphKilllist::LoadFromFile(const char* sFilename)
	{
		Reset(NULL, 0);

		CSphString sName, sError;
		sName.SetSprintf("%s.kill", sFilename);
		if (!sphIsReadable(sName.cstr(), &sError))
			return;

		CSphAutoreader tKlistReader;
		if (!tKlistReader.Open(sName, sError))
			return;

		// FIXME!!! got rid of locks here
		m_tLock.WriteLock();
		m_dLargeKlist.Resize(tKlistReader.GetDword());
		SphDocID_t uLastDocID = 0;
		ARRAY_FOREACH(i, m_dLargeKlist)
		{
			uLastDocID += (SphDocID_t)tKlistReader.UnzipOffset();
			m_dLargeKlist[i] = uLastDocID;
		};
		m_tLock.Unlock();
	}

	void CSphKilllist::SaveToFile(const char* sFilename)
	{
		// FIXME!!! got rid of locks here
		m_tLock.WriteLock();
		NakedFlush(NULL, 0);

		CSphWriter tKlistWriter;
		CSphString sName, sError;
		sName.SetSprintf("%s.kill", sFilename);
		tKlistWriter.OpenFile(sName.cstr(), sError);

		tKlistWriter.PutDword(m_dLargeKlist.GetLength());
		SphDocID_t uLastDocID = 0;
		ARRAY_FOREACH(i, m_dLargeKlist)
		{
			tKlistWriter.ZipOffset(m_dLargeKlist[i] - uLastDocID);
			uLastDocID = (SphDocID_t)m_dLargeKlist[i];
		};
		m_tLock.Unlock();
		tKlistWriter.CloseFile();
	}

}