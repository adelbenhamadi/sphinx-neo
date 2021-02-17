#pragma once
#include "neo/int/types.h"
#include "neo/io/autofile.h"
#include "neo/int/throttle_state.h"


namespace NEO {


		/// file writer with write buffering and int encoder
		class CSphWriter : ISphNoncopyable
		{
		public:
			CSphWriter();
			virtual			~CSphWriter();

			void			SetBufferSize(int iBufferSize);	///< tune write cache size; must be called before OpenFile() or SetFile()

			bool			OpenFile(const CSphString& sName, CSphString& sError);
			void			SetFile(CSphAutofile& tAuto, SphOffset_t* pSharedOffset, CSphString& sError);
			void			CloseFile(bool bTruncate = false);	///< note: calls Flush(), ie. IsError() might get true after this call
			void			UnlinkFile(); /// some shit happened (outside) and the file is no more actual.

			void			PutByte(int uValue);
			void			PutBytes(const void* pData, int64_t iSize);
			void			PutDword(DWORD uValue) { PutBytes(&uValue, sizeof(DWORD)); }
			void			PutOffset(SphOffset_t uValue) { PutBytes(&uValue, sizeof(SphOffset_t)); }
			void			PutString(const char* szString);
			void			PutString(const CSphString& sString);
			void			Tag(const char* sTag);

			void			SeekTo(SphOffset_t pos); ///< seeking inside the buffer will truncate it

#if USE_64BIT
			void			PutDocid(SphDocID_t uValue) { PutOffset(uValue); }
#else
			void			PutDocid(SphDocID_t uValue) { PutDword(uValue); }
#endif

			void			ZipInt(DWORD uValue);
			void			ZipOffset(uint64_t uValue);
			void			ZipOffsets(CSphVector<SphOffset_t>* pData);

			bool			IsError() const { return m_bError; }
			SphOffset_t		GetPos() const { return m_iPos; }
			void			SetThrottle(ThrottleState_t* pState) { m_pThrottle = pState; }

		protected:
			CSphString		m_sName;
			SphOffset_t		m_iPos;
			SphOffset_t		m_iWritten;

			int				m_iFD;
			int				m_iPoolUsed;
			BYTE* m_pBuffer;
			BYTE* m_pPool;
			bool			m_bOwnFile;
			SphOffset_t* m_pSharedOffset;
			int				m_iBufferSize;

			bool			m_bError;
			CSphString* m_pError;
			ThrottleState_t* m_pThrottle;

			virtual void	Flush();
		};
	
}
