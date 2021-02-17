#include "neo/utility/inline_misc.h"

namespace NEO {
#if USE_WINDOWS
	void localtime_r(const time_t* clock, struct tm* res)
	{
		*res = *localtime(clock);
	}

	void gmtime_r(const time_t* clock, struct tm* res)
	{
		*res = *gmtime(clock);
	}
#endif


	/*inline*/ const char* sphTypeName(ESphAttr eType)
	{
		switch (eType)
		{
		case ESphAttr::SPH_ATTR_NONE:			return "none";
		case ESphAttr::SPH_ATTR_INTEGER:		return "uint";
		case ESphAttr::SPH_ATTR_TIMESTAMP:	return "timestamp";
		case ESphAttr::SPH_ATTR_BOOL:			return "bool";
		case ESphAttr::SPH_ATTR_FLOAT:		return "float";
		case ESphAttr::SPH_ATTR_BIGINT:		return "bigint";
		case ESphAttr::SPH_ATTR_STRING:		return "string";
		case ESphAttr::SPH_ATTR_STRINGPTR:	return "stringptr";
		case ESphAttr::SPH_ATTR_TOKENCOUNT:	return "tokencount";
		case ESphAttr::SPH_ATTR_JSON:			return "json";

		case ESphAttr::SPH_ATTR_UINT32SET:	return "mva";
		case ESphAttr::SPH_ATTR_INT64SET:		return "mva64";
		default:					return "unknown";
		}
	}

	/*inline*/ const char* sphTypeDirective(ESphAttr eType)
	{
		switch (eType)
		{
		case ESphAttr::SPH_ATTR_NONE:			return "???";
		case ESphAttr::SPH_ATTR_INTEGER:		return "sql_attr_uint";
		case ESphAttr::SPH_ATTR_TIMESTAMP:	return "sql_attr_timestamp";
		case ESphAttr::SPH_ATTR_BOOL:			return "sql_attr_bool";
		case ESphAttr::SPH_ATTR_FLOAT:		return "sql_attr_float";
		case ESphAttr::SPH_ATTR_BIGINT:		return "sql_attr_bigint";
		case ESphAttr::SPH_ATTR_STRING:		return "sql_attr_string";
		case ESphAttr::SPH_ATTR_STRINGPTR:	return "sql_attr_string";
		case ESphAttr::SPH_ATTR_TOKENCOUNT:	return "_autogenerated_tokencount";
		case ESphAttr::SPH_ATTR_JSON:			return "sql_attr_json";

		case ESphAttr::SPH_ATTR_UINT32SET:	return "sql_attr_multi";
		case ESphAttr::SPH_ATTR_INT64SET:		return "sql_attr_multi bigint";
		default:					return "???";
		}
	}

	/*inline*/ void SqlUnescape(CSphString& sRes, const char* sEscaped, int iLen)
	{
		assert(iLen >= 2);
		assert(
			(sEscaped[0] == '\'' && sEscaped[iLen - 1] == '\'') ||
			(sEscaped[0] == '"' && sEscaped[iLen - 1] == '"'));

		// skip heading and trailing quotes
		const char* s = sEscaped + 1;
		const char* sMax = s + iLen - 2;

		sRes.Reserve(iLen);
		char* d = (char*)sRes.cstr();

		while (s < sMax)
		{
			if (s[0] == '\\')
			{
				switch (s[1])
				{
				case 'b': *d++ = '\b'; break;
				case 'n': *d++ = '\n'; break;
				case 'r': *d++ = '\r'; break;
				case 't': *d++ = '\t'; break;
				case '0': *d++ = ' '; break;
				default:
					*d++ = s[1];
				}
				s += 2;
			}
			else
				*d++ = *s++;
		}

		*d++ = '\0';
	}

	/*inline*/ void StripPath(CSphString& sPath)
	{
		if (sPath.IsEmpty())
			return;

		const char* s = sPath.cstr();
		if (*s != '/')
			return;

		const char* sLastSlash = s;
		for (; *s; s++)
			if (*s == '/')
				sLastSlash = s;

		int iPos = (int)(sLastSlash - sPath.cstr() + 1);
		int iLen = (int)(s - sPath.cstr());
		sPath = sPath.SubString(iPos, iLen - iPos);
	}

	/*static inline*/ size_t sphAddMva64(CSphVector<DWORD>& dStorage, int64_t iVal)
	{
		auto uOff = dStorage.GetLength();
		dStorage.Resize(uOff + 2);
		dStorage[uOff] = MVA_DOWNSIZE(iVal);
		dStorage[uOff + 1] = MVA_DOWNSIZE((iVal >> 32) & 0xffffffff);
		return uOff;
	}

}