#pragma once
#include "neo/index/progress.h"
#include "neo/int/types.h"

namespace NEO {

	/// file which closes automatically when going out of scope
	class CSphAutofile : ISphNoncopyable
	{
	protected:
		int			m_iFD;			///< my file descriptor
		CSphString	m_sFilename;	///< my file name
		bool		m_bTemporary;	///< whether to unlink this file on Close()
		bool		m_bWouldTemporary; ///< backup of the m_bTemporary

		CSphIndexProgress* m_pStat;

	public:
		CSphAutofile();
		CSphAutofile(const CSphString& sName, int iMode, CSphString& sError, bool bTemp = false);
		~CSphAutofile();

		int				Open(const CSphString& sName, int iMode, CSphString& sError, bool bTemp = false);
		void			Close();
		void			SetTemporary(); ///< would be set if a shit happened and the file is not actual.

	public:
		int				GetFD() const { return m_iFD; }
		const char* GetFilename() const;
		SphOffset_t		GetSize(SphOffset_t iMinSize, bool bCheckSizeT, CSphString& sError);
		SphOffset_t		GetSize();

		bool			Read(void* pBuf, int64_t iCount, CSphString& sError);
		void			SetProgressCallback( CSphIndexProgress* pStat);
	};


}
