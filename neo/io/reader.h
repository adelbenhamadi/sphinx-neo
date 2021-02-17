#pragma once
#include "neo/int/types.h"
#include "neo/io/autofile.h"
#include "neo/query/query_profile.h"
#include "neo/query/query_state.h"
#include "neo/int/throttle_state.h"



namespace NEO {

	/// file reader with read buffering and int decoder
	class CSphReader
	{
	public:
		CSphQueryProfile* m_pProfile;
		ESphQueryState		m_eProfileState;

	public:
		CSphReader(BYTE* pBuf = NULL, int iSize = 0);
		virtual		~CSphReader();

		void		SetBuffers(int iReadBuffer, int iReadUnhinted);
		void		SetFile(int iFD, const char* sFilename);
		void		SetFile(const CSphAutofile& tFile);
		void		Reset();
		void		SeekTo(SphOffset_t iPos, int iSizeHint);

		void		SkipBytes(int iCount);
		SphOffset_t	GetPos() const { return m_iPos + m_iBuffPos; }

		void		GetBytes(void* pData, int iSize);
		int			GetBytesZerocopy(const BYTE** ppData, int iMax); ///< zerocopy method; returns actual length present in buffer (upto iMax)

		int			GetByte();
		DWORD		GetDword();
		SphOffset_t	GetOffset();
		CSphString	GetString();
		int			GetLine(char* sBuffer, int iMaxLen);
		bool		Tag(const char* sTag);

		DWORD		UnzipInt();
		uint64_t	UnzipOffset();

		bool					GetErrorFlag() const { return m_bError; }
		const CSphString& GetErrorMessage() const { return m_sError; }
		const CSphString& GetFilename() const { return m_sFilename; }
		void					ResetError();

#if USE_64BIT
		SphDocID_t	GetDocid() { return GetOffset(); }
		SphDocID_t	UnzipDocid() { return UnzipOffset(); }
		SphWordID_t	UnzipWordid() { return UnzipOffset(); }
#else
		SphDocID_t	GetDocid() { return GetDword(); }
		SphDocID_t	UnzipDocid() { return UnzipInt(); }
		SphWordID_t	UnzipWordid() { return UnzipInt(); }
#endif

		const CSphReader& operator = (const CSphReader& rhs);
		void		SetThrottle(ThrottleState_t* pState) { m_pThrottle = pState; }

	protected:

		int			m_iFD;
		SphOffset_t	m_iPos;

		int			m_iBuffPos;
		int			m_iBuffUsed;
		BYTE* m_pBuff;
		int			m_iSizeHint;	///< how much do we expect to read

		int			m_iBufSize;
		bool		m_bBufOwned;
		int			m_iReadUnhinted;

		bool		m_bError;
		CSphString	m_sError;
		CSphString	m_sFilename;
		ThrottleState_t* m_pThrottle;

	protected:
		virtual void		UpdateCache();
	};
}

namespace NEO {

		/// scoped reader
		class CSphAutoreader : public CSphReader
		{
		public:
			CSphAutoreader(BYTE* pBuf = NULL, int iSize = 0) : CSphReader(pBuf, iSize) {}
			~CSphAutoreader();

			bool		Open(const CSphString& sFilename, CSphString& sError);
			void		Close();
			SphOffset_t	GetFilesize();

		public:
			// added for DebugCheck()
			int			GetFD() { return m_iFD; }
		};
	
}