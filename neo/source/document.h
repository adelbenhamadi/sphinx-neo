#pragma once
#include "neo/source/enums.h"
#include "neo/source/base.h"
#include "neo/source/schema.h"



namespace NEO {

	bool			AddFieldLens(CSphSchema& tSchema, bool bDynamic, CSphString& sError);
	/// generic document source
	/// provides multi-field support and generic tokenizer
	class CSphSource_Document : public CSphSource
	{
	public:
		/// ctor
		explicit				CSphSource_Document(const char* sName);

		/// dtor
		virtual					~CSphSource_Document() { SafeDeleteArray(m_pReadFileBuffer); }

		/// my generic tokenizer
		virtual bool			IterateDocument(CSphString& sError);
		virtual ISphHits* IterateHits(CSphString& sError);
		void					BuildHits(CSphString& sError, bool bSkipEndMarker);

		/// field data getter
		/// to be implemented by descendants
		virtual BYTE** NextDocument(CSphString& sError) = 0;
		virtual const int* GetFieldLengths() const = 0;

		virtual void			SetDumpRows(FILE* fpDumpRows) { m_fpDumpRows = fpDumpRows; }

		virtual SphRange_t		IterateFieldMVAStart(int iAttr);
		virtual bool			IterateFieldMVAStart(int, CSphString&) { assert(0 && "not implemented"); return false; }
		virtual bool			HasJoinedFields() { return m_iPlainFieldsLength != m_tSchema.m_dFields.GetLength(); }

	protected:
		int						ParseFieldMVA(CSphVector < DWORD >& dMva, const char* szValue, bool bMva64) const;
		bool					CheckFileField(const BYTE* sField);
		int						LoadFileField(BYTE** ppField, CSphString& sError);

		bool					BuildZoneHits(SphDocID_t uDocid, BYTE* sWord);
		void					BuildSubstringHits(SphDocID_t uDocid, bool bPayload, ESphWordpart eWordpart, bool bSkipEndMarker);
		void					BuildRegularHits(SphDocID_t uDocid, bool bPayload, bool bSkipEndMarker);

		/// register autocomputed attributes such as field lengths (see index_field_lengths)
		bool					AddAutoAttrs(CSphString& sError);

		/// allocate m_tDocInfo storage, do post-alloc magic (compute pointer to field lengths, etc)
		void					AllocDocinfo();

	protected:
		ISphHits				m_tHits;				///< my hitvector

	protected:
		char* m_pReadFileBuffer;
		int						m_iReadFileBufferSize;	///< size of read buffer for the 'sql_file_field' fields
		int						m_iMaxFileBufferSize;	///< max size of read buffer for the 'sql_file_field' fields
		ESphOnFileFieldError	m_eOnFileFieldError;
		FILE* m_fpDumpRows;
		int						m_iPlainFieldsLength;
		DWORD* m_pFieldLengthAttrs;	///< pointer into the part of m_tDocInfo where field lengths are stored

		CSphVector<SphDocID_t>	m_dAllIds;				///< used for joined fields FIXME! unlimited RAM use
		bool					m_bIdsSorted;			///< we sort array to use binary search

	protected:
		struct CSphBuildHitsState_t
		{
			bool m_bProcessingHits;
			bool m_bDocumentDone;

			BYTE** m_dFields;
			CSphVector<int> m_dFieldLengths;

			CSphVector<BYTE*> m_dTmpFieldStorage;
			CSphVector<BYTE*> m_dTmpFieldPtrs;
			CSphVector<BYTE> m_dFiltered;

			int m_iStartPos;
			Hitpos_t m_iHitPos;
			int m_iField;
			int m_iStartField;
			int m_iEndField;

			int m_iBuildLastStep;

			CSphBuildHitsState_t();
			~CSphBuildHitsState_t();

			void Reset();
		};

		CSphBuildHitsState_t	m_tState;
		int						m_iMaxHits;
	};

	struct CSphUnpackInfo
	{
		ESphUnpackFormat	m_eFormat;
		CSphString			m_sName;
	};

	struct CSphJoinedField
	{
		CSphString			m_sName;
		CSphString			m_sQuery;
		CSphString			m_sRanged;
		bool				m_bPayload;
	};
	

}

