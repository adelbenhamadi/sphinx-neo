#pragma once

namespace NEO {
	/*static*/ const int FIELD_BITS = 8;


	/// hit processing tools
	/// Hitpos_t consists of three things:
	/// 1) high bits store field number
	/// 2) middle bit - field end marker
	/// 3) lower bits store hit position in field
	template < int FIELD_BITS >
	class Hitman_c
	{
	protected:
		enum
		{
			POS_BITS = 31 - FIELD_BITS,
			FIELD_OFF = 32 - FIELD_BITS,
			FIELDEND_OFF = 31 - FIELD_BITS,
			FIELDEND_MASK = (1UL << POS_BITS),
			POS_MASK = (1UL << POS_BITS) - 1
		};

	public:
		static Hitpos_t Create(int iField, int iPos)
		{
			return (iField << FIELD_OFF) + (iPos & POS_MASK);
		}

		static Hitpos_t Create(int iField, int iPos, bool bEnd)
		{
			return (iField << FIELD_OFF) + (((int)bEnd) << FIELDEND_OFF) + (iPos & POS_MASK);
		}

		static inline int GetField(Hitpos_t uHitpos)
		{
			return uHitpos >> FIELD_OFF;
		}

		static inline int GetPos(Hitpos_t uHitpos)
		{
			return uHitpos & POS_MASK;
		}

		static inline bool IsEnd(Hitpos_t uHitpos)
		{
			return (uHitpos & FIELDEND_MASK) != 0;
		}

		static inline DWORD GetPosWithField(Hitpos_t uHitpos)
		{
			return uHitpos & ~FIELDEND_MASK;
		}

		static void AddPos(Hitpos_t* pHitpos, int iAdd)
		{
			// FIXME! add range checks (eg. so that 0:0-1 does not overflow)
			*pHitpos += iAdd;
		}

		static Hitpos_t CreateSum(Hitpos_t uHitpos, int iAdd)
		{
			// FIXME! add range checks (eg. so that 0:0-1 does not overflow)
			return (uHitpos + iAdd) & ~FIELDEND_MASK;
		}

		static void SetEndMarker(Hitpos_t* pHitpos)
		{
			*pHitpos |= FIELDEND_MASK;
		}
	};


	typedef Hitman_c<FIELD_BITS> HITMAN;

}