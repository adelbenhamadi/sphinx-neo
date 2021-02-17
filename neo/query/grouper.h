#pragma once
#include "neo/index/enums.h"
#include "neo/int/types.h"
#include "neo/io/fnv64.h"
#include "neo/source/attrib_locator.h"
#include "neo/core/match.h"
#include "neo/utility/inline_misc.h"

#include "neo/sphinxexpr.h"



namespace NEO {


	static bool IsCount(const CSphString& s)
	{
		return s == "@count" || s == "count(*)";
	}

	static bool IsGroupby(const CSphString& s)
	{
		return s == "@groupby" || s == "@distinct" || s == "groupby()" || s == "@groupbystr";
	}

	static bool IsGroupbyMagic(const CSphString& s)
	{
		return IsGroupby(s) || IsCount(s);
	}



	/// groupby key type
	typedef int64_t				SphGroupKey_t;


	/// base grouper (class that computes groupby key)
	class CSphGrouper
	{
	public:
		virtual					~CSphGrouper() {}
		virtual SphGroupKey_t	KeyFromValue(SphAttr_t uValue) const = 0;
		virtual SphGroupKey_t	KeyFromMatch(const CSphMatch& tMatch) const = 0;
		virtual void			GetLocator(CSphAttrLocator& tOut) const = 0;
		virtual ESphAttr		GetResultType() const = 0;
		virtual void			SetStringPool(const BYTE*) {}
		virtual bool			CanMulti() const { return true; }
	};


	/// groupers
#define GROUPER_BEGIN(_name) \
	class _name : public CSphGrouper \
	{ \
	protected: \
		CSphAttrLocator m_tLocator; \
	public: \
		explicit _name ( const CSphAttrLocator & tLoc ) : m_tLocator ( tLoc ) {} \
		virtual void GetLocator ( CSphAttrLocator & tOut ) const { tOut = m_tLocator; } \
		virtual ESphAttr GetResultType () const { return m_tLocator.m_iBitCount>8*(int)sizeof(DWORD) ? ESphAttr::SPH_ATTR_BIGINT : ESphAttr::SPH_ATTR_INTEGER; } \
		virtual SphGroupKey_t KeyFromMatch ( const CSphMatch & tMatch ) const { return KeyFromValue ( tMatch.GetAttr ( m_tLocator ) ); } \
		virtual SphGroupKey_t KeyFromValue ( SphAttr_t uValue ) const \
		{
// NOLINT

#define GROUPER_END \
		} \
	};


#define GROUPER_BEGIN_SPLIT(_name) \
	GROUPER_BEGIN(_name) \
	time_t tStamp = (time_t)uValue; \
	struct tm tSplit; \
	localtime_r ( &tStamp, &tSplit );


	GROUPER_BEGIN(CSphGrouperAttr)
		return uValue;
	GROUPER_END


		GROUPER_BEGIN_SPLIT(CSphGrouperDay)
		return (tSplit.tm_year + 1900) * 10000 + (1 + tSplit.tm_mon) * 100 + tSplit.tm_mday;
	GROUPER_END


		GROUPER_BEGIN_SPLIT(CSphGrouperWeek)
		int iPrevSunday = (1 + tSplit.tm_yday) - tSplit.tm_wday; // prev Sunday day of year, base 1
	int iYear = tSplit.tm_year + 1900;
	if (iPrevSunday <= 0) // check if we crossed year boundary
	{
		// adjust day and year
		iPrevSunday += 365;
		iYear--;

		// adjust for leap years
		if (iYear % 4 == 0 && (iYear % 100 != 0 || iYear % 400 == 0))
			iPrevSunday++;
	}
	return iYear * 1000 + iPrevSunday;
	GROUPER_END


		GROUPER_BEGIN_SPLIT(CSphGrouperMonth)
		return (tSplit.tm_year + 1900) * 100 + (1 + tSplit.tm_mon);
	GROUPER_END


		GROUPER_BEGIN_SPLIT(CSphGrouperYear)
		return (tSplit.tm_year + 1900);
	GROUPER_END

		template <class PRED>
	class CSphGrouperString : public CSphGrouperAttr, public PRED
	{
	protected:
		const BYTE* m_pStringBase;

	public:

		explicit CSphGrouperString(const CSphAttrLocator& tLoc)
			: CSphGrouperAttr(tLoc)
			, m_pStringBase(NULL)
		{
		}

		virtual ESphAttr GetResultType() const
		{
			return ESphAttr::SPH_ATTR_BIGINT;
		}

		virtual SphGroupKey_t KeyFromValue(SphAttr_t uValue) const
		{
			if (!m_pStringBase || !uValue)
				return 0;

			const BYTE* pStr = NULL;
			int iLen = sphUnpackStr(m_pStringBase + uValue, &pStr);

			if (!pStr || !iLen)
				return 0;

			return PRED::Hash(pStr, iLen);
		}

		virtual void SetStringPool(const BYTE* pStrings)
		{
			m_pStringBase = pStrings;
		}

		virtual bool CanMulti() const { return false; }
	};


	class BinaryHash_fn
	{
	public:
		uint64_t Hash(const BYTE* pStr, int iLen, uint64_t uPrev = SPH_FNV64_SEED) const
		{
			assert(pStr && iLen);
			return sphFNV64(pStr, iLen, uPrev);
		}
	};


	template < typename T >
	inline static char* FormatInt(char sBuf[32], T v)
	{
		if_const(sizeof(T) == 4 && v == INT_MIN)
			return strncpy(sBuf, "-2147483648", 32);
		if_const(sizeof(T) == 8 && v == LLONG_MIN)
			return strncpy(sBuf, "-9223372036854775808", 32);

		bool s = (v < 0);
		if (s)
			v = -v;

		char* p = sBuf + 31;
		*p = 0;
		do
		{
			*--p = '0' + char(v % 10);
			v /= 10;
		} while (v);
		if (s)
			*--p = '-';
		return p;
	}


	/// lookup JSON key, group by looked up value (used in CSphKBufferJsonGroupSorter)
	class CSphGrouperJsonField : public CSphGrouper
	{
	protected:
		CSphAttrLocator m_tLocator;

	public:
		ISphExpr* m_pExpr;
		const BYTE* m_pStrings;

		explicit CSphGrouperJsonField(const CSphAttrLocator& tLoc, ISphExpr* pExpr)
			: m_tLocator(tLoc)
			, m_pExpr(pExpr)
			, m_pStrings(NULL)
		{}

		virtual ~CSphGrouperJsonField()
		{
			SafeRelease(m_pExpr);
		}

		virtual void SetStringPool(const BYTE* pStrings)
		{
			m_pStrings = pStrings;
			if (m_pExpr)
				m_pExpr->Command(SPH_EXPR_SET_STRING_POOL, (void*)pStrings);
		}

		virtual void GetLocator(CSphAttrLocator& tOut) const
		{
			tOut = m_tLocator;
		}

		virtual ESphAttr GetResultType() const
		{
			return ESphAttr::SPH_ATTR_BIGINT;
		}

		virtual SphGroupKey_t KeyFromMatch(const CSphMatch& tMatch) const
		{
			if (!m_pExpr)
				return SphGroupKey_t();
			return m_pExpr->Int64Eval(tMatch);
		}

		virtual SphGroupKey_t KeyFromValue(SphAttr_t) const { assert(0); return SphGroupKey_t(); }
	};


	template <class PRED>
	class CSphGrouperMulti : public CSphGrouper, public PRED
	{
	public:
		CSphGrouperMulti(const CSphVector<CSphAttrLocator>& dLocators, const CSphVector<ESphAttr>& dAttrTypes, const CSphVector<ISphExpr*>& dJsonKeys)
			: m_dLocators(dLocators)
			, m_dAttrTypes(dAttrTypes)
			, m_dJsonKeys(dJsonKeys)
		{
			assert(m_dLocators.GetLength() > 1);
			assert(m_dLocators.GetLength() == m_dAttrTypes.GetLength() && m_dLocators.GetLength() == dJsonKeys.GetLength());
		}

		virtual ~CSphGrouperMulti()
		{
			ARRAY_FOREACH(i, m_dJsonKeys)
				SafeDelete(m_dJsonKeys[i]);
		}

		virtual SphGroupKey_t KeyFromMatch(const CSphMatch& tMatch) const
		{
			SphGroupKey_t tKey = SPH_FNV64_SEED;

			for (int i = 0; i < m_dLocators.GetLength(); i++)
			{
				SphAttr_t tAttr = tMatch.GetAttr(m_dLocators[i]);
				if (m_dAttrTypes[i] == ESphAttr::SPH_ATTR_STRING)
				{
					assert(m_pStringBase);

					const BYTE* pStr = NULL;
					int iLen = sphUnpackStr(m_pStringBase + tAttr, &pStr);

					if (!pStr || !iLen)
						continue;

					tKey = PRED::Hash(pStr, iLen, tKey);

				}
				else if (m_dAttrTypes[i] == ESphAttr::SPH_ATTR_JSON)
				{
					assert(m_pStringBase);

					const BYTE* pStr = NULL;
					int iLen = sphUnpackStr(m_pStringBase + tAttr, &pStr);

					if (!pStr || !iLen)
						continue;

					uint64_t uValue = m_dJsonKeys[i]->Int64Eval(tMatch);
					const BYTE* pValue = m_pStringBase + (uValue & 0xffffffff);
					ESphJsonType eRes = (ESphJsonType)(uValue >> 32);

					int i32Val;
					int64_t i64Val;
					double fVal;
					switch (eRes)
					{
					case JSON_STRING:
						iLen = sphJsonUnpackInt(&pValue);
						tKey = sphFNV64(pValue, iLen, tKey);
						break;
					case JSON_INT32:
						i32Val = sphJsonLoadInt(&pValue);
						tKey = sphFNV64(&i32Val, sizeof(i32Val), tKey);
						break;
					case JSON_INT64:
						i64Val = sphJsonLoadBigint(&pValue);
						tKey = sphFNV64(&i64Val, sizeof(i64Val), tKey);
						break;
					case JSON_DOUBLE:
						fVal = sphQW2D(sphJsonLoadBigint(&pValue));
						tKey = sphFNV64(&fVal, sizeof(fVal), tKey);
						break;
					default:
						break;
					}

				}
				else
					tKey = sphFNV64(&tAttr, sizeof(SphAttr_t), tKey);
			}

			return tKey;
		}

		virtual void SetStringPool(const BYTE* pStrings)
		{
			m_pStringBase = pStrings;

			ARRAY_FOREACH(i, m_dJsonKeys)
			{
				if (m_dJsonKeys[i])
					m_dJsonKeys[i]->Command(SPH_EXPR_SET_STRING_POOL, (void*)pStrings);
			}
		}

		virtual SphGroupKey_t KeyFromValue(SphAttr_t) const { assert(0); return SphGroupKey_t(); }
		virtual void GetLocator(CSphAttrLocator&) const { assert(0); }
		virtual ESphAttr GetResultType() const { return ESphAttr::SPH_ATTR_BIGINT; }
		virtual bool CanMulti() const { return false; }

	private:
		CSphVector<CSphAttrLocator>	m_dLocators;
		CSphVector<ESphAttr>		m_dAttrTypes;
		const BYTE* m_pStringBase;
		CSphVector<ISphExpr*>		m_dJsonKeys;
	};

}