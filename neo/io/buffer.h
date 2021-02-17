#pragma once
#include "neo/utility/log.h"

#include <cassert>

namespace NEO {

	/// buffer trait that neither own buffer nor clean-up it on destroy
	template < typename T >
	class CSphBufferTrait : public ISphNoncopyable
	{
	public:
		/// ctor
		CSphBufferTrait()
			: m_pData(NULL)
			, m_iCount(0)
			, m_bMemLocked(false)
		{ }

		/// dtor
		virtual ~CSphBufferTrait()
		{
			assert(!m_bMemLocked && !m_pData);
		}

		virtual void Reset() = 0;

		/// accessor
		inline const T& operator [] (int64_t iIndex) const
		{
			assert(iIndex >= 0 && iIndex < m_iCount);
			return m_pData[iIndex];
		}

		/// get write address
		T* GetWritePtr() const
		{
			return m_pData;
		}

		/// check if i'm empty
		bool IsEmpty() const
		{
			return (m_pData == NULL);
		}

		/// get length in bytes
		size_t GetLengthBytes() const
		{
			return sizeof(T) * (size_t)m_iCount;
		}

		/// get length in entries
		int64_t GetNumEntries() const
		{
			return m_iCount;
		}

		void Set(T* pData, int64_t iCount)
		{
			m_pData = pData;
			m_iCount = iCount;
		}

		bool MemLock(CSphString& sWarning)
		{
#if USE_WINDOWS
			m_bMemLocked = (VirtualLock(m_pData, GetLengthBytes()) != 0);
			if (!m_bMemLocked)
				sWarning.SetSprintf("mlock() failed: errno %d", GetLastError());

#else
			m_bMemLocked = (mlock(m_pData, GetLengthBytes()) == 0);
			if (!m_bMemLocked)
				sWarning.SetSprintf("mlock() failed: %s", strerror(errno));
#endif

			return m_bMemLocked;
		}

	protected:

		T* m_pData;
		int64_t		m_iCount;
		bool		m_bMemLocked;

		void MemUnlock()
		{
			if (!m_bMemLocked)
				return;

			m_bMemLocked = false;
#if USE_WINDOWS
			bool bOk = (VirtualUnlock(m_pData, GetLengthBytes()) != 0);
			if (!bOk)
				sphWarn("munlock() failed: errno %d", GetLastError());

#else
			bool bOk = (munlock(m_pData, GetLengthBytes()) == 0);
			if (!bOk)
				sphWarn("munlock() failed: %s", strerror(errno));
#endif
		}
	};


	//////////////////////////////////////////////////////////////////////////

#if !USE_WINDOWS
#ifndef MADV_DONTFORK
#define MADV_DONTFORK MADV_NORMAL
#endif
#endif

	/////////////////////////////////////////////////////////////////////////

/// in-memory buffer shared between processes
	template < typename T, bool SHARED = false >
	class CSphLargeBuffer : public CSphBufferTrait < T >
	{
	public:
		/// ctor
		CSphLargeBuffer() {}

		/// dtor
		virtual ~CSphLargeBuffer()
		{
			this->Reset();
		}

	public:
		/// allocate storage
#if USE_WINDOWS
		bool Alloc(int64_t iEntries, CSphString& sError)
#else
		bool Alloc(int64_t iEntries, CSphString& sError)
#endif
		{
			assert(!this->GetWritePtr());

			int64_t uCheck = sizeof(T);
			uCheck *= iEntries;

			int64_t iLength = (size_t)uCheck;
			if (uCheck != iLength)
			{
				sError.SetSprintf("impossible to mmap() over 4 GB on 32-bit system");
				return false;
			}

#if USE_WINDOWS
			T* pData = new T[(size_t)iEntries];
#else
			int iFlags = MAP_ANON | MAP_PRIVATE;
			if (SHARED)
				iFlags = MAP_ANON | MAP_SHARED;

			T* pData = (T*)mmap(NULL, iLength, PROT_READ | PROT_WRITE, iFlags, -1, 0);
			if (pData == MAP_FAILED)
			{
				if (iLength > (int64_t)0x7fffffffUL)
					sError.SetSprintf("mmap() failed: %s (length=" INT64_FMT " is over 2GB, impossible on some 32-bit systems)",
						strerror(errno), iLength);
				else
					sError.SetSprintf("mmap() failed: %s (length=" INT64_FMT ")", strerror(errno), iLength);
				return false;
			}

			if (!SHARED)
				madvise(pData, iLength, MADV_DONTFORK);

#if SPH_ALLOCS_PROFILER
			sphMemStatMMapAdd(iLength);
#endif

#endif // USE_WINDOWS

			assert(pData);
			this->Set(pData, iEntries);
			return true;
		}


		/// deallocate storage
		virtual void Reset()
		{
			this->MemUnlock();

			if (!this->GetWritePtr())
				return;

#if USE_WINDOWS
			delete[] this->GetWritePtr();
#else
			int iRes = munmap(this->GetWritePtr(), this->GetLengthBytes());
			if (iRes)
				sphWarn("munmap() failed: %s", strerror(errno));

#if SPH_ALLOCS_PROFILER
			sphMemStatMMapDel(this->GetLengthBytes());
#endif

#endif // USE_WINDOWS

			this->Set(NULL, 0);
		}
	};


	//////////////////////////////////////////////////////////////////////////

	template < typename T >
	class CSphMappedBuffer : public CSphBufferTrait < T >
	{
	public:
		/// ctor
		CSphMappedBuffer()
		{
#if USE_WINDOWS
			m_iFD = INVALID_HANDLE_VALUE;
			m_iMap = INVALID_HANDLE_VALUE;
#else
			m_iFD = -1;
#endif
		}

		/// dtor
		virtual ~CSphMappedBuffer()
		{
			this->Reset();
		}

		bool Setup(const char* sFile, CSphString& sError, bool bWrite)
		{
#if USE_WINDOWS
			assert(m_iFD == INVALID_HANDLE_VALUE);
#else
			assert(m_iFD == -1);
#endif
			assert(!this->GetWritePtr() && !this->GetNumEntries());

			T* pData = NULL;
			int64_t iCount = 0;

#if USE_WINDOWS
			int iAccessMode = GENERIC_READ;
			if (bWrite)
				iAccessMode |= GENERIC_WRITE;

			DWORD uShare = FILE_SHARE_READ | FILE_SHARE_DELETE;
			if (bWrite)
				uShare |= FILE_SHARE_WRITE; // wo this flag indexer and indextool unable to open attribute file that was opened by daemon

			HANDLE iFD = CreateFile(sFile, iAccessMode, uShare, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
			if (iFD == INVALID_HANDLE_VALUE)
			{
				sError.SetSprintf("failed to open file '%s' (errno %d)", sFile, ::GetLastError());
				return false;
			}
			m_iFD = iFD;

			LARGE_INTEGER tLen;
			if (GetFileSizeEx(iFD, &tLen) == 0)
			{
				sError.SetSprintf("failed to fstat file '%s' (errno %d)", sFile, ::GetLastError());
				Reset();
				return false;
			}

			// FIXME!!! report abount tail, ie m_iLen*sizeof(T)!=tLen.QuadPart
			iCount = tLen.QuadPart / sizeof(T);

			// mmap fails to map zero-size file
			if (tLen.QuadPart > 0)
			{
				int iProtectMode = PAGE_READONLY;
				if (bWrite)
					iProtectMode = PAGE_READWRITE;
				m_iMap = ::CreateFileMapping(iFD, NULL, iProtectMode, 0, 0, NULL);
				int iAccessMode = FILE_MAP_READ;
				if (bWrite)
					iAccessMode |= FILE_MAP_WRITE;
				pData = (T*)::MapViewOfFile(m_iMap, iAccessMode, 0, 0, 0);
				if (!pData)
				{
					sError.SetSprintf("failed to map file '%s': (errno %d, length=" INT64_FMT ")", sFile, ::GetLastError(), (int64_t)tLen.QuadPart);
					Reset();
					return false;
				}
			}
#else

			int iFD = sphOpenFile(sFile, sError, bWrite);
			if (iFD < 0)
				return false;
			m_iFD = iFD;

			int64_t iFileSize = sphGetFileSize(iFD, sError);
			if (iFileSize < 0)
				return false;

			// FIXME!!! report abount tail, ie m_iLen*sizeof(T)!=st.st_size
			iCount = iFileSize / sizeof(T);

			// mmap fails to map zero-size file
			if (iFileSize > 0)
			{
				int iProt = PROT_READ;
				int iFlags = MAP_PRIVATE;

				if (bWrite)
					iProt |= PROT_WRITE;

				pData = (T*)mmap(NULL, iFileSize, iProt, iFlags, iFD, 0);
				if (pData == MAP_FAILED)
				{
					sError.SetSprintf("failed to mmap file '%s': %s (length=" INT64_FMT ")", sFile, strerror(errno), iFileSize);
					Reset();
					return false;
				}

				madvise(pData, iFileSize, MADV_DONTFORK);
			}
#endif

			this->Set(pData, iCount);
			return true;
		}

		virtual void Reset()
		{
			this->MemUnlock();

#if USE_WINDOWS
			if (this->GetWritePtr())
				::UnmapViewOfFile(this->GetWritePtr());

			if (m_iMap != INVALID_HANDLE_VALUE)
				::CloseHandle(m_iMap);
			m_iMap = INVALID_HANDLE_VALUE;

			if (m_iFD != INVALID_HANDLE_VALUE)
				::CloseHandle(m_iFD);
			m_iFD = INVALID_HANDLE_VALUE;
#else
			if (this->GetWritePtr())
				::munmap(this->GetWritePtr(), this->GetLengthBytes());

			SafeClose(m_iFD);
#endif

			this->Set(NULL, 0);
		}

	private:
#if USE_WINDOWS
		HANDLE		m_iFD;
		HANDLE		m_iMap;
#else
		int			m_iFD;
#endif
	};
}