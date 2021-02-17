#pragma once
#include "neo/platform/compat.h"
#include "neo/core/generic.h"
#include "neo/core/macros.h"

#include <utility>
#include <cassert>

namespace NEO {

	/// immutable C string proxy
	struct CSphString
	{
	protected:
		char* m_sValue;
		// Empty ("") string optimization.
		static char EMPTY[];

	private:
		/// safety gap after the string end; for instance, UTF-8 Russian stemmer
		/// which treats strings as 16-bit word sequences needs this in some cases.
		/// note that this zero-filled gap does NOT include trailing C-string zero,
		/// and does NOT affect strlen() as well.
		static const size_t	SAFETY_GAP = 4;

	public:
		CSphString()
			: m_sValue(nullptr)
		{
		}

		// take a note this is not an explicit constructor
		// so a lot of silent constructing and deleting of strings is possible
		// Example:
		// SmallStringHash_T<int> hHash;
		// ...
		// hHash.Exists ( "asdf" ); // implicit CSphString construction and deletion here
		CSphString(const CSphString& rhs)
			: m_sValue(nullptr)
		{
			*this = rhs;
		}

		CSphString(CSphString&& rhs)
			: m_sValue(std::move(rhs.m_sValue))
		{
			rhs.m_sValue = nullptr;
		}

		~CSphString()
		{
			if (m_sValue != EMPTY)
				SafeDeleteArray(m_sValue);
		}

		const char* cstr() const
		{
			return m_sValue;
		}

		const char* scstr() const
		{
			return m_sValue ? m_sValue : EMPTY;
		}

		inline bool operator == (const char* t) const
		{
			if (!t || !m_sValue)
				return ((!t && !m_sValue) || (!t && m_sValue && !*m_sValue) || (!m_sValue && t && !*t));
			return strcmp(m_sValue, t) == 0;
		}

		inline bool operator == (const CSphString& t) const
		{
			return operator==(t.cstr());
		}

		inline bool operator != (const CSphString& t) const
		{
			return !operator==(t);
		}

		bool operator != (const char* t) const
		{
			return !operator==(t);
		}

		CSphString(const char* sString) // NOLINT
		{
			if (sString)
			{
				if (sString[0] == '\0')
				{
					m_sValue = EMPTY;
				}
				else
				{
					auto iLen = 1 + strlen(sString);
					m_sValue = new char[iLen + SAFETY_GAP];

					strcpy(m_sValue, sString); // NOLINT
					memset(m_sValue + iLen, 0, SAFETY_GAP);
				}
			}
			else
			{
				m_sValue = NULL;
			}
		}

		CSphString(const char* sValue, size_t iLen)
			: m_sValue(NULL)
		{
			SetBinary(sValue, iLen);
		}

		const CSphString& operator = (const CSphString& rhs)
		{
			if (m_sValue == rhs.m_sValue)
				return *this;
			if (m_sValue != EMPTY)
				SafeDeleteArray(m_sValue);
			if (rhs.m_sValue)
			{
				if (rhs.m_sValue[0] == '\0')
				{
					m_sValue = EMPTY;
				}
				else
				{
					auto iLen = 1 + strlen(rhs.m_sValue);
					m_sValue = new char[iLen + SAFETY_GAP];

					strcpy(m_sValue, rhs.m_sValue); // NOLINT
					memset(m_sValue + iLen, 0, SAFETY_GAP);
				}
			}
			return *this;
		}

		CSphString& operator = (CSphString&& rhs)
		{
			if (m_sValue == rhs.m_sValue)
				return *this;
			if (m_sValue != EMPTY)
				SafeDeleteArray(m_sValue);

			if (rhs.m_sValue)
			{
				if (rhs.m_sValue[0] == '\0')
				{
					m_sValue = EMPTY;
				}
				else
				{
					m_sValue = std::move(rhs.m_sValue);
					rhs.m_sValue = nullptr;
				}
			}
			return *this;
		}

		CSphString SubString(size_t iStart, size_t iCount) const
		{
#ifndef NDEBUG
			size_t iLen = strlen(m_sValue);
#endif
			assert(iStart >= 0 && iStart < iLen);
			assert(iCount > 0);
			assert((iStart + iCount) >= 0 && (iStart + iCount) <= iLen);

			CSphString sRes;
			sRes.m_sValue = new char[1 + SAFETY_GAP + iCount];
			strncpy(sRes.m_sValue, m_sValue + iStart, iCount);
			memset(sRes.m_sValue + iCount, 0, 1 + SAFETY_GAP);
			return sRes;
		}

		// tries to reuse memory buffer, but calls Length() every time
		// hope this won't kill performance on a huge strings
		void SetBinary(const char* sValue, size_t iLen)
		{
			if (Length() < iLen)
			{
				if (m_sValue != EMPTY)
					SafeDeleteArray(m_sValue);
				m_sValue = new char[1 + SAFETY_GAP + iLen];
				memcpy(m_sValue, sValue, iLen);
				memset(m_sValue + iLen, 0, 1 + SAFETY_GAP);
				return;
			}

			if (sValue[0] == '\0' || iLen == 0)
			{
				m_sValue = EMPTY;
			}
			else
			{
				memcpy(m_sValue, sValue, iLen);
				m_sValue[iLen] = '\0';
			}
		}

		void Reserve(int iLen)
		{
			if (m_sValue != EMPTY)
				SafeDeleteArray(m_sValue);
			m_sValue = new char[1 + SAFETY_GAP + iLen];
			memset(m_sValue, 0, 1 + SAFETY_GAP + iLen);
		}

		const CSphString& SetSprintf(const char* sTemplate, ...) __attribute__((format(printf, 2, 3)))
		{
			char sBuf[1024];
			va_list ap;

			va_start(ap, sTemplate);
			vsnprintf(sBuf, sizeof(sBuf), sTemplate, ap);
			va_end(ap);

			(*this) = sBuf;
			return (*this);
		}

		const CSphString& SetSprintfVa(const char* sTemplate, va_list ap)
		{
			char sBuf[1024];
			vsnprintf(sBuf, sizeof(sBuf), sTemplate, ap);

			(*this) = sBuf;
			return (*this);
		}

		bool IsEmpty() const
		{
			if (!m_sValue)
				return true;
			return ((*m_sValue) == '\0');
		}

		CSphString& ToLower()
		{
			if (m_sValue)
				for (char* s = m_sValue; *s; s++)
					*s = (char)tolower(*s);
			return *this;
		}

		CSphString& ToUpper()
		{
			if (m_sValue)
				for (char* s = m_sValue; *s; s++)
					*s = (char)toupper(*s);
			return *this;
		}

		void SwapWith(CSphString& rhs)
		{
			Swap(m_sValue, rhs.m_sValue);
		}

		bool Begins(const char* sPrefix) const
		{
			if (!m_sValue || !sPrefix)
				return false;
			return strncmp(m_sValue, sPrefix, strlen(sPrefix)) == 0;
		}

		bool Ends(const char* sSuffix) const
		{
			if (!m_sValue || !sSuffix)
				return false;

			auto iVal = strlen(m_sValue);
			auto iSuffix = strlen(sSuffix);
			if (iVal < iSuffix)
				return false;
			return strncmp(m_sValue + iVal - iSuffix, sSuffix, iSuffix) == 0;
		}

		void Trim()
		{
			if (m_sValue)
			{
				const char* sStart = m_sValue;
				const char* sEnd = m_sValue + strlen(m_sValue) - 1;
				while (sStart <= sEnd && isspace((unsigned char)*sStart)) sStart++;
				while (sStart <= sEnd && isspace((unsigned char)*sEnd)) sEnd--;
				memmove(m_sValue, sStart, sEnd - sStart + 1);
				m_sValue[sEnd - sStart + 1] = '\0';
			}
		}

		size_t Length() const
		{
			return m_sValue ? strlen(m_sValue) : 0;
		}

		char* Leak()
		{
			if (m_sValue == EMPTY)
			{
				m_sValue = nullptr;
				char* pBuf = new char[1];
				pBuf[0] = '\0';
				return pBuf;
			}
			char* pBuf = m_sValue;
			m_sValue = nullptr;
			return pBuf;
		}

		// opposite to Leak()
		void Adopt(char** sValue)
		{
			if (m_sValue != EMPTY)
				SafeDeleteArray(m_sValue);
			m_sValue = *sValue;
			*sValue = nullptr;
		}

		bool operator < (const CSphString& b) const
		{
			if (!m_sValue && !b.m_sValue)
				return false;
			if (!m_sValue || !b.m_sValue)
				return !m_sValue;
			return strcmp(m_sValue, b.m_sValue) < 0;
		}

		void Unquote()
		{
			auto l = Length();
			if (l && m_sValue[0] == '\'' && m_sValue[l - 1] == '\'')
			{
				memmove(m_sValue, m_sValue + 1, l - 2);
				m_sValue[l - 2] = '\0';
			}
		}
	};

	/// string swapper
	inline void Swap(CSphString& v1, CSphString& v2)
	{
		v1.SwapWith(v2);
	}


	/// string builder
	/// somewhat quicker than a series of SetSprintf()s
	/// lets you build strings bigger than 1024 bytes, too
	template <typename T>
	class SphStringBuilder_T
	{
	protected:
		char* m_sBuffer;
		size_t		m_iSize;
		size_t		m_iUsed;

	public:
		SphStringBuilder_T()
		{
			Reset();
		}

		~SphStringBuilder_T()
		{
			SafeDeleteArray(m_sBuffer);
		}

		void Reset()
		{
			m_iSize = 256;
			m_sBuffer = new char[m_iSize];
			Clear();
		}

		void Clear()
		{
			m_sBuffer[0] = '\0';
			m_iUsed = 0;
		}

		SphStringBuilder_T<T>& Appendf(const char* sTemplate, ...) __attribute__((format(printf, 2, 3)))
		{
			assert(m_sBuffer);
			assert(m_iUsed < m_iSize);

			for (;; )
			{
				int iLeft = m_iSize - m_iUsed;

				// try to append
				va_list ap;
				va_start(ap, sTemplate);
				int iPrinted = vsnprintf(m_sBuffer + m_iUsed, iLeft, sTemplate, ap);
				va_end(ap);

				// success? bail
				// note that we check for strictly less, not less or equal
				// that is because vsnprintf does *not* count the trailing zero
				// meaning that if we had N bytes left, and N bytes w/o the zero were printed,
				// we do not have a trailing zero anymore, but vsnprintf succeeds anyway
				if (iPrinted >= 0 && iPrinted < iLeft)
				{
					m_iUsed += iPrinted;
					break;
				}

				// we need more chars!
				// either 256 (happens on Windows; lets assume we need 256 more chars)
				// or get all the needed chars and 64 more for future calls
				Grow(iPrinted < 0 ? 256 : iPrinted - iLeft + 64);
			}
			return *this;
		}

		const char* cstr() const
		{
			return m_sBuffer;
		}

		int Length()
		{
			return m_iUsed;
		}

		const SphStringBuilder_T<T>& operator += (const char* sText)
		{
			if (!sText || *sText == '\0')
				return *this;

			int iLen = strlen(sText);
			int iLeft = m_iSize - m_iUsed;
			if (iLen >= iLeft)
				Grow(iLen - iLeft + 64);

			memcpy(m_sBuffer + m_iUsed, sText, iLen + 1);
			m_iUsed += iLen;
			return *this;
		}

		const SphStringBuilder_T<T>& operator = (const SphStringBuilder_T<T>& rhs)
		{
			if (this != &rhs)
			{
				m_iUsed = rhs.m_iUsed;
				m_iSize = rhs.m_iSize;
				SafeDeleteArray(m_sBuffer);
				m_sBuffer = new char[m_iSize];
				memcpy(m_sBuffer, rhs.m_sBuffer, m_iUsed + 1);
			}
			return *this;
		}

		// FIXME? move escaping to another place
		void AppendEscaped(const char* sText, bool bEscape = true, bool bFixupSpace = true)
		{
			if (!sText || !*sText)
				return;

			const char* pBuf = sText;
			int iEsc = 0;
			for (; *pBuf; )
			{
				char s = *pBuf++;
				iEsc = (bEscape && T::IsEscapeChar(s) ? (iEsc + 1) : iEsc);
			}

			int iLen = pBuf - sText + iEsc;
			int iLeft = m_iSize - m_iUsed;
			if (iLen >= iLeft)
				Grow(iLen - iLeft + 64);

			pBuf = sText;
			char* pCur = m_sBuffer + m_iUsed;
			for (; *pBuf; )
			{
				char s = *pBuf++;
				if (bEscape && T::IsEscapeChar(s))
				{
					*pCur++ = '\\';
					*pCur++ = T::GetEscapedChar(s);
				}
				else if (bFixupSpace && (s == ' ' || s == '\t' || s == '\n' || s == '\r'))
				{
					*pCur++ = ' ';
				}
				else
				{
					*pCur++ = s;
				}
			}
			*pCur = '\0';
			m_iUsed = pCur - m_sBuffer;
		}

	private:
		void Grow(int iLen)
		{
			m_iSize += iLen;
			char* pNew = new char[m_iSize];
			memcpy(pNew, m_sBuffer, m_iUsed + 1);
			Swap(pNew, m_sBuffer);
			SafeDeleteArray(pNew);
		}
	};


	struct EscapeQuotation_t
	{
		static bool IsEscapeChar(char c)
		{
			return (c == '\\' || c == '\'');
		}

		static char GetEscapedChar(char c)
		{
			return c;
		}
	};


	typedef SphStringBuilder_T<EscapeQuotation_t> CSphStringBuilder;


}