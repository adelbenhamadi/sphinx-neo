#pragma once
#include "neo/int/types.h"
#include "neo/io/buffer.h"
#include "neo/platform/mutex.h"

namespace NEO {

	extern DWORD* g_pMvaArena;

	/// startup mva updates arena
	const char* sphArenaInit(int iMaxBytes);



	class tTester : public ISphNoncopyable
	{
	public:
		virtual void Reset() = 0;
		virtual void TestData(int iData) = 0;
		virtual ~tTester() {}
	};


	/// shared-memory arena allocator
	/// manages small tagged dword strings, upto 4096 bytes in size
	class CSphArena
	{
	public:
		CSphArena();
		~CSphArena();

		DWORD* ReInit(int uMaxBytes);
		const char* GetError() const { return m_sError.cstr(); }

		int						TaggedAlloc(int iTag, int iBytes);
		void					TaggedFreeIndex(int iTag, int iIndex);
		void					TaggedFreeTag(int iTag);

		void					ExamineTag(tTester* pTest, int iTag);

	protected:
		static const int		MIN_BITS = 4;
		static const int		MAX_BITS = 12;
		static const int		NUM_SIZES = MAX_BITS - MIN_BITS + 2;	//one for 0 (empty pages), and one for each size from min to max

		static const int		PAGE_SIZE = 1 << MAX_BITS;
		static const int		PAGE_ALLOCS = 1 << (MAX_BITS - MIN_BITS);
		static const int		PAGE_BITMAP = (PAGE_ALLOCS + 8 * sizeof(DWORD) - 1) / (8 * sizeof(DWORD));

		static const int		MAX_TAGS = 1024;
		static const int		MAX_LOGENTRIES = 29;

		//page descriptor
		struct PageDesc_t
		{
			int					m_iSizeBits;			//alloc size
			int					m_iPrev;				//prev free page of this size
			int					m_iNext;				//next free page of this size
			int					m_iUsed;				//usage count
			DWORD				m_uBitmap[PAGE_BITMAP];	//usage bitmap
		};

		//tag descriptor
		struct TagDesc_t
		{
			int					m_iTag;					//tag value
			int					m_iAllocs;				//active allocs
			int					m_iLogHead;				//pointer to head allocs log entry
		};

		//allocs log entry
		struct AllocsLogEntry_t
		{
			int					m_iUsed;
			int					m_iNext;
			int					m_dEntries[MAX_LOGENTRIES];
		};
		STATIC_SIZE_ASSERT(AllocsLogEntry_t, 124);

	protected:
		DWORD* Init(int uMaxBytes);
		int						RawAlloc(int iBytes);
		void					RawFree(int iIndex);
		void					RemoveTag(TagDesc_t* pTag);

	protected:
		CSphMutex				m_tThdMutex;

		int						m_iPages;			//max pages count
		CSphLargeBuffer<DWORD>	m_pArena;			//arena that stores everything (all other pointers point here)

		PageDesc_t* m_pPages;			//page descriptors
		int* m_pFreelistHeads;	//free-list heads
		int* m_pTagCount;
		TagDesc_t* m_pTags;

		DWORD* m_pBasePtr;			//base data storage pointer
		CSphString				m_sError;

#if ARENADEBUG
	protected:
		int* m_pTotalAllocs;
		int* m_pTotalBytes;

	public:
		void					CheckFreelists();
#else
		inline void				CheckFreelists() {}
#endif // ARENADEBUG
	};



	extern CSphArena g_tMvaArena;

}