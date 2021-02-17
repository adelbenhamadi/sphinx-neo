#pragma once
#include "neo/int/types.h"
#include "neo/utility/hash.h"

namespace NEO {
	bool TryToExec(char* pBuffer, const char* szFilename, CSphVector<char>& dResult, char* sError, int iErrorLen);


	/// config section (hash of variant values)
	class CSphConfigSection : public SmallStringHash_T < CSphVariant >
	{
	public:
		CSphConfigSection()
			: m_iTag(0)
		{}

		/// get integer option value by key and default value
		int GetInt(const char* sKey, int iDefault = 0) const
		{
			CSphVariant* pEntry = (*this)(sKey);
			return pEntry ? pEntry->intval() : iDefault;
		}

		/// get float option value by key and default value
		float GetFloat(const char* sKey, float fDefault = 0.0f) const
		{
			CSphVariant* pEntry = (*this)(sKey);
			return pEntry ? pEntry->floatval() : fDefault;
		}

		/// get string option value by key and default value
		const char* GetStr(const char* sKey, const char* sDefault = "") const
		{
			CSphVariant* pEntry = (*this)(sKey);
			return pEntry ? pEntry->strval().cstr() : sDefault;
		}

		/// get size option (plain int, or with K/M prefix) value by key and default value
		int		GetSize(const char* sKey, int iDefault) const;
		int64_t GetSize64(const char* sKey, int64_t iDefault) const;

		int m_iTag;
	};

	/// config section type (hash of sections)
	typedef SmallStringHash_T < CSphConfigSection >	CSphConfigType;

	/// config (hash of section types)
	typedef SmallStringHash_T < CSphConfigType >	CSphConfig;

	/// simple config file
	class CSphConfigParser
	{
	public:
		CSphConfig		m_tConf;

	public:
		CSphConfigParser();
		bool			Parse(const char* sFileName, const char* pBuffer = NULL);

		// fail-save loading new config over existing.
		bool			ReParse(const char* sFileName, const char* pBuffer = NULL);

	protected:
		CSphString		m_sFileName;
		int				m_iLine;
		CSphString		m_sSectionType;
		CSphString		m_sSectionName;
		char			m_sError[1024];

		int					m_iWarnings;
		static const int	WARNS_THRESH = 5;

	protected:
		bool			IsPlainSection(const char* sKey);
		bool			IsNamedSection(const char* sKey);
		bool			AddSection(const char* sType, const char* sSection);
		void			AddKey(const char* sKey, char* sValue);
		bool			ValidateKey(const char* sKey);
		char* GetBufferString(char* szDest, int iMax, const char*& szSource);
	};


}