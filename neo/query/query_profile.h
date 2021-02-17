#pragma once
#include "neo/int/types.h"
#include "neo/platform/thread.h"
#include "neo/query/query_state.h"

namespace NEO {
	/// search query profile
	class CSphQueryProfile
	{
	public:
		ESphQueryState	m_eState;							///< current state
		int64_t			m_tmStamp;							///< timestamp when we entered the current state

		int				m_dSwitches[SPH_QSTATE_TOTAL + 1];	///< number of switches to given state
		int64_t			m_tmTotal[SPH_QSTATE_TOTAL + 1];	///< total time spent per state

		CSphStringBuilder	m_sTransformedTree;					///< transformed query tree

	public:
		/// create empty and stopped profile
		CSphQueryProfile()
		{
			Start(SPH_QSTATE_TOTAL);
		}

		/// switch to a new query state, and record a timestamp
		/// returns previous state, to simplify Push/Pop like scenarios
		ESphQueryState Switch(ESphQueryState eNew)
		{
			int64_t tmNow = sphMicroTimer();
			ESphQueryState eOld = m_eState;
			m_dSwitches[eOld]++;
			m_tmTotal[eOld] += tmNow - m_tmStamp;
			m_eState = eNew;
			m_tmStamp = tmNow;
			return eOld;
		}

		/// reset everything and start profiling from a given state
		void Start(ESphQueryState eNew)
		{
			memset(m_dSwitches, 0, sizeof(m_dSwitches));
			memset(m_tmTotal, 0, sizeof(m_tmTotal));
			m_eState = eNew;
			m_tmStamp = sphMicroTimer();
		}

		/// stop profiling
		void Stop()
		{
			Switch(SPH_QSTATE_TOTAL);
		}
	};

	class CSphScopedProfile
	{
	private:
		CSphQueryProfile* m_pProfile;
		ESphQueryState		m_eOldState;

	public:
		explicit CSphScopedProfile(CSphQueryProfile* pProfile, ESphQueryState eNewState)
		{
			m_pProfile = pProfile;
			m_eOldState = SPH_QSTATE_UNKNOWN;
			if (m_pProfile)
				m_eOldState = m_pProfile->Switch(eNewState);
		}

		~CSphScopedProfile()
		{
			if (m_pProfile)
				m_pProfile->Switch(m_eOldState);
		}
	};

}