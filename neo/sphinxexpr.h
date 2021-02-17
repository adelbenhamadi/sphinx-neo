//
// $Id$
//

//
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

#ifndef _sphinxexpr_
#define _sphinxexpr_


#include "neo/index/enums.h"
#include "neo/int/types.h"
#include "neo/int/ref_counted.h"

//#include <cassert>



namespace NEO {

	/// forward decls
	class CSphMatch;
	class ISphSchema;
	class CSphSchema;
	struct CSphColumnInfo;




	/// expression evaluator
	/// can always be evaluated in floats using Eval()
	/// can sometimes be evaluated in integers using IntEval(), depending on type as returned from sphExprParse()
	struct ISphExpr : public ISphRefcounted
	{
	public:
		/// evaluate this expression for that match
		virtual float Eval(const CSphMatch& tMatch) const = 0;

		/// evaluate this expression for that match, using int math
		virtual int IntEval(const CSphMatch& tMatch) const { assert(0); return (int)Eval(tMatch); }

		/// evaluate this expression for that match, using int64 math
		virtual int64_t Int64Eval(const CSphMatch& tMatch) const { assert(0); return (int64_t)Eval(tMatch); }

		/// Evaluate string attr.
		/// Note, that sometimes this method returns pointer to a static buffer
		/// and sometimes it allocates a new buffer, so aware of memory leaks.
		/// IsStringPtr() returns true if this method allocates a new buffer and false otherwise.
		virtual int StringEval(const CSphMatch&, const BYTE** ppStr) const { *ppStr = NULL; return 0; }

		/// evaluate MVA attr
		virtual const DWORD* MvaEval(const CSphMatch&) const { assert(0); return NULL; }

		/// evaluate Packed factors
		virtual const DWORD* FactorEval(const CSphMatch&) const { assert(0); return NULL; }

		/// check for arglist subtype
		/// FIXME? replace with a single GetType() call?
		virtual bool IsArglist() const { return false; }

		/// check for stringptr subtype
		/// FIXME? replace with a single GetType() call?
		virtual bool IsStringPtr() const { return false; }

		/// get Nth arg of an arglist
		virtual ISphExpr* GetArg(int) const { return NULL; }

		/// get the number of args in an arglist
		virtual int GetNumArgs() const { return 0; }

		/// run a tree wide action (1st arg is an action, 2nd is its parameter)
		/// usually sets something into ISphExpr like string pool or gets something from it like dependent columns
		virtual void Command(ESphExprCommand, void*) {}

		/// check for const type
		virtual bool IsConst() const { return false; }

		/// get expression hash (for query cache)
		virtual uint64_t GetHash(const ISphSchema& tSorterSchema, uint64_t uPrevHash, bool& bDisable) = 0;
	};

	/// string expression traits
	/// can never be evaluated in floats or integers, only StringEval() is allowed
	struct ISphStringExpr : public ISphExpr
	{
		virtual float Eval(const CSphMatch&) const { assert(0 && "one just does not simply evaluate a string as float"); return 0; }
		virtual int IntEval(const CSphMatch&) const { assert(0 && "one just does not simply evaluate a string as int"); return 0; }
		virtual int64_t Int64Eval(const CSphMatch&) const { assert(0 && "one just does not simply evaluate a string as bigint"); return 0; }
	};

	/// hook to extend expressions
	/// lets one to add her own identifier and function handlers
	struct ISphExprHook
	{
		virtual ~ISphExprHook() {}
		/// checks for an identifier known to the hook
		/// returns -1 on failure, a non-negative OID on success
		virtual int IsKnownIdent(const char* sIdent) = 0;

		/// checks for a valid function call
		/// returns -1 on failure, a non-negative OID on success (possibly adjusted)
		virtual int IsKnownFunc(const char* sFunc) = 0;

		/// create node by OID
		/// pEvalStage is an optional out-parameter
		/// hook may fill it, but that is *not* required
		virtual ISphExpr* CreateNode(int iID, ISphExpr* pLeft, ESphEvalStage* pEvalStage, CSphString& sError) = 0;

		/// get identifier return type by OID
		virtual ESphAttr GetIdentType(int iID) = 0;

		/// get function return type by OID and argument types vector
		/// must return ESphAttr::SPH_ATTR_NONE and fill the message on failure
		virtual ESphAttr GetReturnType(int iID, const CSphVector<ESphAttr>& dArgs, bool bAllConst, CSphString& sError) = 0;

		/// recursive scope check
		virtual void CheckEnter(int iID) = 0;

		/// recursive scope check
		virtual void CheckExit(int iID) = 0;
	};



	/// a container used to pass maps of constants/variables around the evaluation tree
	struct Expr_MapArg_c : public ISphExpr
	{
		CSphVector<CSphNamedVariant> m_dValues;

		explicit Expr_MapArg_c(CSphVector<CSphNamedVariant>& dValues)
		{
			m_dValues.SwapData(dValues);
		}

		virtual float Eval(const CSphMatch&) const
		{
			assert(0 && "one just does not simply evaluate a const hash");
			return 0.0f;
		}

		virtual uint64_t GetHash(const ISphSchema&, uint64_t, bool&)
		{
			assert(0 && "calling GetHash from a const hash");
			return 0;
		}
	};




	/// parses given expression, builds evaluator
	/// returns NULL and fills sError on failure
	/// returns pointer to evaluator on success
	/// fills pAttrType with result type (for now, can be ESphAttr::SPH_ATTR_SINT or ESphAttr::SPH_ATTR_FLOAT)
	/// fills pUsesWeight with a flag whether match relevance is referenced in expression AST
	/// fills pEvalStage with a required (!) evaluation stage
	class CSphQueryProfile;
	ISphExpr* sphExprParse(const char* sExpr, const ISphSchema& tSchema, ESphAttr* pAttrType, bool* pUsesWeight,
		CSphString& sError, CSphQueryProfile* pProfiler, ESphCollation eCollation = SPH_COLLATION_DEFAULT, ISphExprHook* pHook = NULL,
		bool* pZonespanlist = NULL, DWORD* pPackedFactorsFlags = NULL, ESphEvalStage* pEvalStage = NULL);

	ISphExpr* sphJsonFieldConv(ISphExpr* pExpr);

	//////////////////////////////////////////////////////////////////////////

	/// init tables used by our geodistance functions
	void GeodistInit();

	/// haversine sphere distance, radians
	float GeodistSphereRad(float lat1, float lon1, float lat2, float lon2);

	/// haversine sphere distance, degrees
	float GeodistSphereDeg(float lat1, float lon1, float lat2, float lon2);

	/// flat ellipsoid distance, degrees
	float GeodistFlatDeg(float fLat1, float fLon1, float fLat2, float fLon2);

	/// adaptive flat/haversine distance, degrees
	float GeodistAdaptiveDeg(float lat1, float lon1, float lat2, float lon2);

	/// adaptive flat/haversine distance, radians
	float GeodistAdaptiveRad(float lat1, float lon1, float lat2, float lon2);


}
#endif // _sphinxexpr_

//
// $Id$
//
