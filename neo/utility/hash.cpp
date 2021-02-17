#include "neo/utility/hash.h"
#include "neo/int/types.h"
#include "neo/int/string.h"
#include "neo/io/crc32.h"

namespace NEO {

	int CSphStrHashFunc::Hash(const CSphString& sKey)
	{
		return sKey.IsEmpty() ? 0 : sphCRC32(sKey.cstr());
	}


}