#pragma once

namespace NEO {

	/// name+int pair
	struct CSphNamedInt
	{
		CSphString	m_sName;
		int			m_iValue;

		CSphNamedInt() : m_iValue(0) {}
	};

	inline void Swap(CSphNamedInt& a, CSphNamedInt& b)
	{
		a.m_sName.SwapWith(b.m_sName);
		Swap(a.m_iValue, b.m_iValue);
	}

}