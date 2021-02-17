# pragma once


#define MEM_CATEGORIES \
	MEM_CATEGORY(MEM_CORE), \
	MEM_CATEGORY(MEM_INDEX_DISK), \
	MEM_CATEGORY(MEM_INDEX_RT), \
	MEM_CATEGORY(MEM_API_HANDLE ), \
	MEM_CATEGORY(MEM_API_SEARCH ), \
	MEM_CATEGORY(MEM_API_QUERY ), \
	MEM_CATEGORY(MEM_RT_ACCUM), \
	MEM_CATEGORY(MEM_MMAPED), \
	MEM_CATEGORY(MEM_BINLOG), \
	MEM_CATEGORY(MEM_SQL_HANDLE), \
	MEM_CATEGORY(MEM_SQL_INSERT), \
	MEM_CATEGORY(MEM_SQL_SELECT), \
	MEM_CATEGORY(MEM_SQL_DELETE), \
	MEM_CATEGORY(MEM_SQL_SET), \
	MEM_CATEGORY(MEM_SQL_BEGIN), \
	MEM_CATEGORY(MEM_SQL_COMMIT), \
	MEM_CATEGORY(MEM_SQL_ALTER), \
	MEM_CATEGORY(MEM_DISK_QUERY), \
	MEM_CATEGORY(MEM_DISK_QUERYEX), \
	MEM_CATEGORY(MEM_RT_QUERY), \
	MEM_CATEGORY(MEM_RT_RES_MATCHES), \
	MEM_CATEGORY(MEM_RT_RES_STRINGS)

#define MEM_CATEGORY(_arg) _arg

namespace NEO {

	enum MemCategory_e
	{
		MEM_CATEGORIES,
		MEM_TOTAL
	};
#undef MEM_CATEGORY

#if SPH_ALLOCS_PROFILER

	void sphMemStatPush(MemCategory_e eCategory);
	void sphMemStatPop(MemCategory_e eCategory);

	// memory tracker
	struct MemTracker_c : ISphNoncopyable
	{
		const MemCategory_e m_eCategory; ///< category

		/// ctor
		explicit MemTracker_c(MemCategory_e eCategory)
			: m_eCategory(eCategory)
		{
			sphMemStatPush(m_eCategory);
		}

		/// dtor
		~MemTracker_c()
		{
			sphMemStatPop(m_eCategory);
		}
	};

#define MEMORY(name) MemTracker_c tracker_##__LINE__##name(name);

#else // SPH_ALLOCS_PROFILER 0

#define MEMORY(name)

#endif // if SPH_ALLOCS_PROFILER

}