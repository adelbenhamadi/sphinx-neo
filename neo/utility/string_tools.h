#pragma once


/// my own isalpha (let's build our own theme park!)
inline int sphIsAlpha(int c)
{
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '-' || c == '_';
}


/// my own isspace
inline bool sphIsSpace(int iCode)
{
	return iCode == ' ' || iCode == '\t' || iCode == '\n' || iCode == '\r';
}


/// check for keyword modifiers
inline bool sphIsModifier(int iSymbol)
{
	return iSymbol == '^' || iSymbol == '$' || iSymbol == '=' || iSymbol == '*';
}


/// all wildcards
template < typename T >
inline bool sphIsWild(T c)
{
	return c == '*' || c == '?' || c == '%';
}


static char* ltrim(char* sLine)
{
	while (*sLine && sphIsSpace(*sLine))
		sLine++;
	return sLine;
}


static char* rtrim(char* sLine)
{
	char* p = sLine + strlen(sLine) - 1;
	while (p >= sLine && sphIsSpace(*p))
		p--;
	p[1] = '\0';
	return sLine;
}


static char* trim(char* sLine)
{
	return ltrim(rtrim(sLine));
}

inline int sphBitCount(DWORD n)
{
	// MIT HACKMEM count
	// works for 32-bit numbers only
	// fix last line for 64-bit numbers
	register DWORD tmp;
	tmp = n - ((n >> 1) & 033333333333) - ((n >> 2) & 011111111111);
	return (int)((tmp + (tmp >> 3)) & 030707070707) % 63;
}


/// how much bits do we need for given int
inline int sphLog2(uint64_t uValue)
{
#if USE_WINDOWS
	DWORD uRes;
	if (BitScanReverse(&uRes, (DWORD)(uValue >> 32)))
		return 33 + uRes;
	BitScanReverse(&uRes, DWORD(uValue));
	return 1 + (int)uRes;
#elif __GNUC__ || __clang__
	if (!uValue)
		return 0;
	return 64 - __builtin_clzl(uValue);
#else
	int iBits = 0;
	while (uValue)
	{
		uValue >>= 1;
		iBits++;
	}
	return iBits;
#endif
}
