#pragma once
#include "neo/int/types.h"
#include "neo/source/column_info.h"

namespace NEO {

	class CSphMatch;


	/// barebones schema interface
	/// everything that is needed from every implementation of a schema
	class ISphSchema
	{
	protected:
		CSphVector<CSphNamedInt>		m_dPtrAttrs;		///< names and rowitems of STRINGPTR and other ptrs to copy and delete
		CSphVector<CSphNamedInt>		m_dFactorAttrs;		///< names and rowitems of ESphAttr::SPH_ATTR_FACTORS attributes

	public:
		/// get row size (static+dynamic combined)
		virtual int						GetRowSize() const = 0;

		/// get static row part size
		virtual int						GetStaticSize() const = 0;

		/// get dynamic row part size
		virtual int						GetDynamicSize() const = 0;

		/// get attrs count
		virtual int						GetAttrsCount() const = 0;

		/// get attribute index by name, returns -1 if not found
		virtual int						GetAttrIndex(const char* sName) const = 0;

		/// get attr by index
		virtual const CSphColumnInfo& GetAttr(int iIndex) const = 0;

		/// get attr by name
		virtual const CSphColumnInfo* GetAttr(const char* sName) const = 0;

		/// assign current schema to rset schema (kind of a visitor operator)
		virtual void					AssignTo(class CSphRsetSchema& lhs) const = 0;

		/// get the first one of field length attributes. return -1 if none exist
		virtual int						GetAttrId_FirstFieldLen() const = 0;

		/// get the last one of field length attributes. return -1 if none exist
		virtual int						GetAttrId_LastFieldLen() const = 0;

	public:
		/// full copy, for purely dynamic matches
		void							CloneWholeMatch(CSphMatch* pDst, const CSphMatch& rhs) const;

		/// free the linked strings and/or just initialize the pointers with NULL
		void							FreeStringPtrs(CSphMatch* pMatch) const;

		/// ???
		void							CopyPtrs(CSphMatch* pDst, const CSphMatch& rhs) const;

	protected:
		/// generic InsertAttr() implementation that tracks STRINGPTR, FACTORS attributes
		void							InsertAttr(CSphVector<CSphColumnInfo>& dAttrs, CSphVector<int>& dUsed, int iPos, const CSphColumnInfo& tCol, bool dDynamic);

		/// reset my trackers
		void							Reset();

		/// dtor
		virtual ~ISphSchema() {}
	};


}