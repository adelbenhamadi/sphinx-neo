#pragma once
#include "neo/int/types.h"
#include "neo/source/enums.h"
#include "neo/source/source_stats.h"
#include "neo/source/schema.h"
#include "neo/core/word_hit.h"
#include "neo/core/match.h"
#include "neo/tools/html_stripper.h"


#include <cstdio>

namespace NEO {

	/// indexing-related source settings
	/// NOTE, newly added fields should be synced with CSphSource::Setup()
	struct CSphSourceSettings
	{
		int		m_iMinPrefixLen;	///< min indexable prefix (0 means don't index prefixes)
		int		m_iMinInfixLen;		///< min indexable infix length (0 means don't index infixes)
		int		m_iMaxSubstringLen;	///< max indexable infix and prefix (0 means don't limit infixes and prefixes)
		int		m_iBoundaryStep;	///< additional boundary word position increment
		bool	m_bIndexExactWords;	///< exact (non-stemmed) word indexing flag
		int		m_iOvershortStep;	///< position step on overshort token (default is 1)
		int		m_iStopwordStep;	///< position step on stopword token (default is 1)
		bool	m_bIndexSP;			///< whether to index sentence and paragraph delimiters
		bool	m_bIndexFieldLens;	///< whether to index field lengths

		CSphVector<CSphString>	m_dPrefixFields;	///< list of prefix fields
		CSphVector<CSphString>	m_dInfixFields;		///< list of infix fields

		explicit				CSphSourceSettings();
		ESphWordpart			GetWordpart(const char* sField, bool bWordDict);
	};



	/// hit vector interface
	/// because specific position type might vary (dword, qword, etc)
	/// but we don't want to template and instantiate everything because of that
	class ISphHits
	{
	public:
		int Length() const
		{
			return (int)m_dData.GetLength();
		}

		const CSphWordHit* First() const
		{
			return m_dData.Begin();
		}

		const CSphWordHit* Last() const
		{
			return &m_dData.Last();
		}

		void AddHit(SphDocID_t uDocid, SphWordID_t uWordid, Hitpos_t uPos)
		{
			if (uWordid)
			{
				CSphWordHit& tHit = m_dData.Add();
				tHit.m_uDocID = uDocid;
				tHit.m_uWordID = uWordid;
				tHit.m_uWordPos = uPos;
			}
		}

	public:
		CSphVector<CSphWordHit> m_dData;
	};


	struct SphRange_t
	{
		int m_iStart;
		int m_iLength;
	};

	struct CSphFieldFilterSettings
	{
		CSphVector<CSphString>	m_dRegexps;
	};

	/// field filter
	class ISphFieldFilter
	{
	public:
		ISphFieldFilter();
		virtual						~ISphFieldFilter();

		virtual	int					Apply(const BYTE* sField, int iLength, CSphVector<BYTE>& dStorage, bool bQuery) = 0;
		virtual	void				GetSettings(CSphFieldFilterSettings& tSettings) const = 0;
		virtual ISphFieldFilter* Clone() = 0;

		void						SetParent(ISphFieldFilter* pParent);

	protected:
		ISphFieldFilter* m_pParent;
	};

	/// create a regexp field filter
	ISphFieldFilter* sphCreateRegexpFilter(const CSphFieldFilterSettings& tFilterSettings, CSphString& sError);

	/// create a RLP field filter
	ISphFieldFilter* sphCreateRLPFilter(ISphFieldFilter* pParent, const char* szRLPRoot, const char* szRLPEnv, const char* szRLPCtx, const char* szBlendChars, CSphString& sError);

	void			LoadFieldFilterSettings(CSphReader& tReader, CSphFieldFilterSettings& tFieldFilterSettings);
	void			SaveFieldFilterSettings(CSphWriter& tWriter, ISphFieldFilter* pFieldFilter);

	//fwd dec
	class ISphTokenizer;
	class CSphDict;
	class CSphMatch;

	/// generic data source
	class CSphSource : public CSphSourceSettings
	{
	public:
		CSphMatch							m_tDocInfo;		///< current document info
		CSphVector<CSphString>				m_dStrAttrs;	///< current document string attrs
		CSphVector<DWORD>					m_dMva;			///< MVA storage for mva64

	public:
		/// ctor
		explicit							CSphSource(const char* sName);

		/// dtor
		virtual								~CSphSource();

		/// set dictionary
		void								SetDict(CSphDict* dict);

		/// set HTML stripping mode
		///
		/// sExtractAttrs defines what attributes to store. format is "img=alt; a=alt,title".
		/// empty string means to strip all tags; NULL means to disable stripping.
		///
		/// sRemoveElements defines what elements to cleanup. format is "style, script"
		///
		/// on failure, returns false and fills sError
		bool								SetStripHTML(const char* sExtractAttrs, const char* sRemoveElements, bool bDetectParagraphs, const char* sZones, CSphString& sError);

		/// set field filter
		virtual void						SetFieldFilter(ISphFieldFilter* pFilter);

		/// set tokenizer
		void								SetTokenizer(ISphTokenizer* pTokenizer);

		/// set rows dump file
		virtual void						SetDumpRows(FILE*) {}

		/// get stats
		virtual const CSphSourceStats& GetStats();

		/// updates schema fields and attributes
		/// updates pInfo if it's empty; checks for match if it's not
		/// must be called after IterateStart(); will always fail otherwise
		virtual bool						UpdateSchema(CSphSchema* pInfo, CSphString& sError);

		/// setup misc indexing settings (prefix/infix/exact-word indexing, position steps)
		void								Setup(const CSphSourceSettings& tSettings);

	public:
		/// connect to the source (eg. to the database)
		/// connection settings are specific for each source type and as such
		/// are implemented in specific descendants
		virtual bool						Connect(CSphString& sError) = 0;

		/// disconnect from the source
		virtual void						Disconnect() = 0;

		/// check if there are any attributes configured
		/// note that there might be NO actual attributes in the case if configured
		/// ones do not match those actually returned by the source
		virtual bool						HasAttrsConfigured() = 0;

		/// check if there are any joined fields
		virtual bool						HasJoinedFields() { return false; }

		/// begin indexing this source
		/// to be implemented by descendants
		virtual bool						IterateStart(CSphString& sError) = 0;

		/// get next document
		/// to be implemented by descendants
		/// returns false on error
		/// returns true and fills m_tDocInfo on success
		/// returns true and sets m_tDocInfo.m_uDocID to 0 on eof
		virtual bool						IterateDocument(CSphString& sError) = 0;

		/// get next hits chunk for current document
		/// to be implemented by descendants
		/// returns NULL when there are no more hits
		/// returns pointer to hit vector (with at most MAX_SOURCE_HITS) on success
		/// fills out-string with error message on failure
		virtual ISphHits* IterateHits(CSphString& sError) = 0;

		/// get joined hits from joined fields (w/o attached docinfos)
		/// returns false and fills out-string with error message on failure
		/// returns true and sets m_tDocInfo.m_uDocID to 0 on eof
		/// returns true and sets m_tDocInfo.m_uDocID to non-0 on success
		virtual ISphHits* IterateJoinedHits(CSphString& sError);

		/// begin iterating values of out-of-document multi-valued attribute iAttr
		/// will fail if iAttr is out of range, or is not multi-valued
		/// can also fail if configured settings are invalid (eg. SQL query can not be executed)
		virtual bool						IterateMultivaluedStart(int iAttr, CSphString& sError) = 0;

		/// get next multi-valued (id,attr-value) or (id, offset) for mva64 tuple to m_tDocInfo
		virtual bool						IterateMultivaluedNext() = 0;

		/// begin iterating values of multi-valued attribute iAttr stored in a field
		/// will fail if iAttr is out of range, or is not multi-valued
		virtual SphRange_t					IterateFieldMVAStart(int iAttr) = 0;

		/// begin iterating kill list
		virtual bool						IterateKillListStart(CSphString& sError) = 0;

		/// get next kill list doc id
		virtual bool						IterateKillListNext(SphDocID_t& uDocId) = 0;

		/// post-index callback
		/// gets called when the indexing is succesfully (!) over
		virtual void						PostIndex() {}

	protected:
		ISphTokenizer* m_pTokenizer;	///< my tokenizer
		CSphDict* m_pDict;		///< my dict
		ISphFieldFilter* m_pFieldFilter;	///< my field filter

		CSphSourceStats						m_tStats;		///< my stats
		CSphSchema							m_tSchema;		///< my schema

		CSphHTMLStripper* m_pStripper;	///< my HTML stripper

		int			m_iNullIds;
		int			m_iMaxIds;

		SphDocID_t	VerifyID(SphDocID_t uID);
	};


	static bool SourceCheckSchema(const CSphSchema& tSchema, CSphString& sError)
	{
		SmallStringHash_T<int> hAttrs;
		for (int i = 0; i < tSchema.GetAttrsCount(); i++)
		{
			const CSphColumnInfo& tAttr = tSchema.GetAttr(i);
			bool bUniq = hAttrs.Add(1, tAttr.m_sName);

			if (!bUniq)
			{
				sError.SetSprintf("attribute %s declared multiple times", tAttr.m_sName.cstr());
				return false;
			}
		}

		return true;
	}
}