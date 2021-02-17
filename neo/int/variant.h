#pragma once
#include "neo/platform/compat.h"
#include "neo/core/generic.h"
#include "neo/int/string.h"

#include <cassert>

namespace NEO {

	/// immutable string/int/float variant list proxy
	/// used in config parsing
	struct CSphVariant
	{
	protected:
		CSphString		m_sValue;
		int				m_iValue;
		int64_t			m_i64Value;
		float			m_fValue;

	public:
		CSphVariant* m_pNext;
		// tags are used for handling multiple same keys
		bool			m_bTag; // 'true' means override - no multi-valued; 'false' means multi-valued - chain them
		int				m_iTag; // stores order like in config file

	public:
		/// default ctor
		CSphVariant()
			: m_iValue(0)
			, m_i64Value(0)
			, m_fValue(0.0f)
			, m_pNext(NULL)
			, m_bTag(false)
			, m_iTag(0)
		{
		}

		/// ctor from C string
		CSphVariant(const char* sString, int iTag)
			: m_sValue(sString)
			, m_iValue(sString ? atoi(sString) : 0)
			, m_i64Value(sString ? (int64_t)strtoull(sString, NULL, 10) : 0)
			, m_fValue(sString ? (float)atof(sString) : 0.0f)
			, m_pNext(NULL)
			, m_bTag(false)
			, m_iTag(iTag)
		{
		}

		/// copy ctor
		CSphVariant(const CSphVariant& rhs)
		{
			m_pNext = NULL;
			*this = rhs;
		}

		/// default dtor
		/// WARNING: automatically frees linked items!
		~CSphVariant()
		{
			SafeDelete(m_pNext);
		}

		const char* cstr() const { return m_sValue.cstr(); }

		const CSphString& strval() const { return m_sValue; }
		int intval() const { return m_iValue; }
		int64_t int64val() const { return m_i64Value; }
		float floatval() const { return m_fValue; }

		/// default copy operator
		const CSphVariant& operator = (const CSphVariant& rhs)
		{
			assert(!m_pNext);
			if (rhs.m_pNext)
				m_pNext = new CSphVariant(*rhs.m_pNext);

			m_sValue = rhs.m_sValue;
			m_iValue = rhs.m_iValue;
			m_i64Value = rhs.m_i64Value;
			m_fValue = rhs.m_fValue;
			m_bTag = rhs.m_bTag;
			m_iTag = rhs.m_iTag;

			return *this;
		}

		bool operator== (const char* s) const { return m_sValue == s; }
		bool operator!= (const char* s) const { return m_sValue != s; }
	};

	/// named int/string variant
	/// used for named expression function arguments block
	/// ie. {..} part in, for example, BM25F(1.2, 0.8, {title=3}) call
	struct CSphNamedVariant
	{
		CSphString		m_sKey;		///< key
		CSphString		m_sValue;	///< value for strings, empty for ints
		int				m_iValue;	///< value for ints
	};

}