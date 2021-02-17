#include "neo/core/version.h"
#include "neo/io/reader.h"
#include "neo/utility/inline_misc.h"

namespace NEO {


	DWORD ReadVersion(const char* sPath, CSphString& sError)
	{
		BYTE dBuffer[8];
		CSphAutoreader rdHeader(dBuffer, sizeof(dBuffer));
		if (!rdHeader.Open(sPath, sError))
			return 0;

		// check magic header
		const char* sMsg = CheckFmtMagic(rdHeader.GetDword());
		if (sMsg)
		{
			sError.SetSprintf(sMsg, sPath);
			return 0;
		}

		// get version
		DWORD uVersion = rdHeader.GetDword();
		if (uVersion == 0 || uVersion > INDEX_FORMAT_VERSION)
		{
			sError.SetSprintf("%s is v.%d, binary is v.%d", sPath, uVersion, INDEX_FORMAT_VERSION);
			return 0;
		}

		return uVersion;
	}

	//indexe files extensions stuff
	static const char* g_dNewExts17[] = { ".new.sph", ".new.spa", ".new.spi", ".new.spd", ".new.spp", ".new.spm", ".new.spk", ".new.sps" };
	static const char* g_dOldExts17[] = { ".old.sph", ".old.spa", ".old.spi", ".old.spd", ".old.spp", ".old.spm", ".old.spk", ".old.sps", ".old.mvp" };
	static const char* g_dCurExts17[] = { ".sph", ".spa", ".spi", ".spd", ".spp", ".spm", ".spk", ".sps", ".mvp" };
	static const char* g_dLocExts17[] = { ".sph", ".spa", ".spi", ".spd", ".spp", ".spm", ".spk", ".sps", ".spl" };

	static const char* g_dNewExts31[] = { ".new.sph", ".new.spa", ".new.spi", ".new.spd", ".new.spp", ".new.spm", ".new.spk", ".new.sps", ".new.spe" };
	static const char* g_dOldExts31[] = { ".old.sph", ".old.spa", ".old.spi", ".old.spd", ".old.spp", ".old.spm", ".old.spk", ".old.sps", ".old.spe", ".old.mvp" };
	static const char* g_dCurExts31[] = { ".sph", ".spa", ".spi", ".spd", ".spp", ".spm", ".spk", ".sps", ".spe", ".mvp" };
	static const char* g_dLocExts31[] = { ".sph", ".spa", ".spi", ".spd", ".spp", ".spm", ".spk", ".sps", ".spe", ".spl" };

	static const char** g_pppAllExts[] = { g_dCurExts31, g_dNewExts31, g_dOldExts31, g_dLocExts31 };


	const char** sphGetExts(ESphExtType eType, DWORD uVersion)
	{
		if (uVersion < 31)
		{
			switch (eType)
			{
			case SPH_EXT_TYPE_NEW: return g_dNewExts17;
			case SPH_EXT_TYPE_OLD: return g_dOldExts17;
			case SPH_EXT_TYPE_CUR: return g_dCurExts17;
			case SPH_EXT_TYPE_LOC: return g_dLocExts17;
			}

		}
		else
		{
			switch (eType)
			{
			case SPH_EXT_TYPE_NEW: return g_dNewExts31;
			case SPH_EXT_TYPE_OLD: return g_dOldExts31;
			case SPH_EXT_TYPE_CUR: return g_dCurExts31;
			case SPH_EXT_TYPE_LOC: return g_dLocExts31;
			}
		}

		assert(0 && "Unknown extension type");
		return NULL;
	}

	int sphGetExtCount(DWORD uVersion)
	{
		if (uVersion < 31)
			return 8;
		else
			return 9;
	}

	const char* sphGetExt(ESphExtType eType, ESphExt eExt)
	{
		if (eExt == SPH_EXT_MVP)
		{
			assert(eType == SPH_EXT_TYPE_CUR || eType == SPH_EXT_TYPE_OLD);
			return g_pppAllExts[eType][eExt];
		}

		assert(eExt >= 0 && eExt <= (int)sizeof(g_pppAllExts[0]) / (int)sizeof(g_pppAllExts[0][0]));

		return g_pppAllExts[eType][eExt];
	}



}