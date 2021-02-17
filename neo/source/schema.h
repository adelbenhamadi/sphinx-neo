#pragma once
#include "neo/int/types.h"
#include "neo/core/globals.h"
#include "neo/core/match.h"
#include "neo/int/named_int.h"
#include "neo/int/ref_counted.h"
#include "neo/source/enums.h"
#include "neo/source/schema_int.h"
#include "neo/source/hitman.h"
#include "neo/source/attrib_locator.h"
#include "neo/source/column_info.h"
#include "neo/io/reader.h"
#include "neo/io/writer.h"

namespace NEO {

	/// plain good old schema
	/// container that actually holds and owns all the fields, columns, etc
	///
	/// NOTE that while this one can be used everywhere where we need a schema
	/// it might be huge (say 1000+ attributes) and expensive to copy, modify, etc!
	/// so for most of the online query work, consider CSphRsetSchema
	class CSphSchema : public ISphSchema
	{
		friend class CSphRsetSchema;

	protected:
		static const int			HASH_THRESH = 32;
		static const int			BUCKET_COUNT = 256;

	public:
		CSphString					m_sName;		///< my human-readable name
		CSphVector<CSphColumnInfo>	m_dFields;		///< my fulltext-searchable fields


		CSphVector<CSphColumnInfo>	m_dAttrs;		///< all my attributes
		CSphVector<int>				m_dStaticUsed;	///< static row part map (amount of used bits in each rowitem)
		CSphVector<int>				m_dDynamicUsed;	///< dynamic row part map
		int							m_iStaticSize;	///< static row size (can be different from m_dStaticUsed.GetLength() because of gaps)

	protected:
		WORD						m_dBuckets[BUCKET_COUNT];	///< uses indexes in m_dAttrs as ptrs; 0xffff is like NULL in this hash

		int							m_iFirstFieldLenAttr;///< position of the first field length attribute (cached on insert/delete)
		int							m_iLastFieldLenAttr; ///< position of the last field length attribute (cached on insert/delete)

	public:

		/// ctor
		explicit				CSphSchema(const char* sName = "(nameless)");

		/// get field index by name
		/// returns -1 if not found
		int						GetFieldIndex(const char* sName) const;

		/// get attribute index by name
		/// returns -1 if not found
		int						GetAttrIndex(const char* sName) const;

		/// checks if two schemas fully match (ie. fields names, attr names, types and locators are the same)
		/// describe mismatch (if any) to sError
		bool					CompareTo(const CSphSchema& rhs, CSphString& sError, bool bFullComparison = true) const;

		/// reset fields and attrs
		void					Reset();

		/// get row size (static+dynamic combined)
		int						GetRowSize() const { return  m_iStaticSize + (int) m_dDynamicUsed.GetLength(); }

		/// get static row part size
		int						GetStaticSize() const { return m_iStaticSize; }

		/// get dynamic row part size
		int						GetDynamicSize() const { return (int) m_dDynamicUsed.GetLength(); }

		/// get attrs count
		int						GetAttrsCount() const { return (int) m_dAttrs.GetLength(); }

		/// get attr by index
		const CSphColumnInfo& GetAttr(int iIndex) const { return m_dAttrs[iIndex]; }

		/// get attr by name
		const CSphColumnInfo* GetAttr(const char* sName) const;

		/// insert attr
		void					InsertAttr(int iPos, const CSphColumnInfo& tAggr, bool bDynamic);

		/// add attr
		void					AddAttr(const CSphColumnInfo& tAttr, bool bDynamic);

		/// remove attr
		void					RemoveAttr(const char* szAttr, bool bDynamic);

		/// get the first one of field length attributes. return -1 if none exist
		virtual int				GetAttrId_FirstFieldLen() const;

		/// get the last one of field length attributes. return -1 if none exist
		virtual int				GetAttrId_LastFieldLen() const;


		static bool				IsReserved(const char* szToken);

	protected:
		/// returns 0xffff if bucket list is empty and position otherwise
		WORD& GetBucketPos(const char* sName);

		/// reset hash and re-add all attributes
		void					RebuildHash();

		/// add iAddVal to all indexes strictly greater than iStartIdx in hash structures
		void					UpdateHash(int iStartIdx, int iAddVal);

		/// visitor-style uber-virtual assignment implementation
		void					AssignTo(CSphRsetSchema& lhs) const;
	};


	/// lightweight schema to be used in sorters, result sets, etc
	/// avoids copying of static attributes part by keeping a pointer
	/// manages the additional dynamic attributes on its own
	///
	/// NOTE that for that reason CSphRsetSchema needs the originating index to exist
	/// (in case it keeps and uses a pointer to original schema in that index)
	class CSphRsetSchema : public ISphSchema
	{
	protected:
		const CSphSchema* m_pIndexSchema;		///< original index schema, for the static part
		CSphVector<CSphColumnInfo>	m_dExtraAttrs;		///< additional dynamic attributes, for the dynamic one
		CSphVector<int>				m_dDynamicUsed;		///< dynamic row part map
		CSphVector<int>				m_dRemoved;			///< original indexes that are suppressed from the index schema by RemoveStaticAttr()

	public:
		CSphVector<CSphColumnInfo>	m_dFields;			///< standalone case (agent result set), fields container

	public:
		CSphRsetSchema();
		CSphRsetSchema& operator = (const ISphSchema& rhs);
		CSphRsetSchema& operator = (const CSphSchema& rhs);
		virtual void				AssignTo(CSphRsetSchema& lhs) const { lhs = *this; }

	public:
		int							GetRowSize() const;
		int							GetStaticSize() const;
		int							GetDynamicSize() const;
		int							GetAttrsCount() const;
		int							GetAttrIndex(const char* sName) const;
		const CSphColumnInfo& GetAttr(int iIndex) const;
		const CSphColumnInfo* GetAttr(const char* sName) const;

		virtual int					GetAttrId_FirstFieldLen() const;
		virtual int					GetAttrId_LastFieldLen() const;

	public:
		void						AddDynamicAttr(const CSphColumnInfo& tCol);
		void						RemoveStaticAttr(int iAttr);
		void						Reset();

	public:
		/// simple copy; clones either the entire dynamic part, or a part thereof
		void CloneMatch(CSphMatch* pDst, const CSphMatch& rhs) const;

		/// swap in a subset of current attributes, with not necessarily (!) unique names
		/// used to create a network result set (ie. rset to be sent and then discarded)
		/// WARNING, DO NOT USE THIS UNLESS ABSOLUTELY SURE!
		void SwapAttrs(CSphVector<CSphColumnInfo>& dAttrs);
	};

	void			WriteSchema(CSphWriter& fdInfo, const CSphSchema& tSchema);
	void			ReadSchema(CSphReader& rdInfo, CSphSchema& m_tSchema, DWORD uVersion, bool bDynamic);


}