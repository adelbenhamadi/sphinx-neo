#pragma once


namespace NEO {

	class CSphKeywordDeltaWriter
	{
	private:
		BYTE m_sLastKeyword[SPH_MAX_WORD_LEN * 3 + 4];
		int m_iLastLen;

	public:
		CSphKeywordDeltaWriter()
		{
			Reset();
		}

		void Reset()
		{
			m_iLastLen = 0;
		}

		template <typename F>
		void PutDelta(F& WRITER, const BYTE* pWord, int iLen)
		{
			assert(pWord && iLen);

			// how many bytes of a previous keyword can we reuse?
			BYTE iMatch = 0;
			int iMinLen = Min(m_iLastLen, iLen);
			assert(iMinLen < (int)sizeof(m_sLastKeyword));
			while (iMatch < iMinLen && m_sLastKeyword[iMatch] == pWord[iMatch])
			{
				iMatch++;
			}

			BYTE iDelta = (BYTE)(iLen - iMatch);
			assert(iDelta > 0);

			assert(iLen < (int)sizeof(m_sLastKeyword));
			memcpy(m_sLastKeyword, pWord, iLen);
			m_iLastLen = iLen;

			// match and delta are usually tiny, pack them together in 1 byte
			// tricky bit, this byte leads the entry so it must never be 0 (aka eof mark)!
			if (iDelta <= 8 && iMatch <= 15)
			{
				BYTE uPacked = (0x80 + ((iDelta - 1) << 4) + iMatch);
				WRITER.PutBytes(&uPacked, 1);
			}
			else
			{
				WRITER.PutBytes(&iDelta, 1); // always greater than 0
				WRITER.PutBytes(&iMatch, 1);
			}

			WRITER.PutBytes(pWord + iMatch, iDelta);
		}
	};

}