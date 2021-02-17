#include "neo/tools/config_parser.h"
#include "neo/utility/log.h"
#include "neo/utility/string_tools.h"

namespace NEO {

	/// key flags
	enum
	{
		KEY_DEPRECATED = 1UL << 0,
		KEY_LIST = 1UL << 1,
		KEY_HIDDEN = 1UL << 2,
		KEY_REMOVED = 1UL << 3
	};

	/// key descriptor for validation purposes
	struct KeyDesc_t
	{
		const char* m_sKey;		///< key name
		int					m_iFlags;	///< flags
		const char* m_sExtra;	///< extra stuff (deprecated name, for now)
	};



	/// allowed keys for source section
	static KeyDesc_t g_dKeysSource[] =
	{
		{ "type",					0, NULL },
		{ "sql_host",				0, NULL },
		{ "sql_user",				0, NULL },
		{ "sql_pass",				0, NULL },
		{ "sql_db",					0, NULL },
		{ "sql_port",				0, NULL },
		{ "sql_sock",				0, NULL },
		{ "mysql_connect_flags",	0, NULL },
		{ "mysql_ssl_key",			0, NULL }, // check.pl mysql_ssl
		{ "mysql_ssl_cert",			0, NULL }, // check.pl mysql_ssl
		{ "mysql_ssl_ca",			0, NULL }, // check.pl mysql_ssl
		{ "mssql_winauth",			0, NULL },
		{ "mssql_unicode",			KEY_REMOVED, NULL },
		{ "sql_query_pre",			KEY_LIST, NULL },
		{ "sql_query",				0, NULL },
		{ "sql_query_range",		0, NULL },
		{ "sql_range_step",			0, NULL },
		{ "sql_query_killlist",		0, NULL },
		{ "sql_attr_uint",			KEY_LIST, NULL },
		{ "sql_attr_bool",			KEY_LIST, NULL },
		{ "sql_attr_timestamp",		KEY_LIST, NULL },
		{ "sql_attr_str2ordinal",	KEY_REMOVED | KEY_LIST, NULL },
		{ "sql_attr_float",			KEY_LIST, NULL },
		{ "sql_attr_bigint",		KEY_LIST, NULL },
		{ "sql_attr_multi",			KEY_LIST, NULL },
		{ "sql_query_post",			KEY_LIST, NULL },
		{ "sql_query_post_index",	KEY_LIST, NULL },
		{ "sql_ranged_throttle",	0, NULL },
		{ "sql_query_info",			KEY_REMOVED, NULL },
		{ "xmlpipe_command",		0, NULL },
		{ "xmlpipe_field",			KEY_LIST, NULL },
		{ "xmlpipe_attr_uint",		KEY_LIST, NULL },
		{ "xmlpipe_attr_timestamp",	KEY_LIST, NULL },
		{ "xmlpipe_attr_str2ordinal", KEY_REMOVED | KEY_LIST, NULL },
		{ "xmlpipe_attr_bool",		KEY_LIST, NULL },
		{ "xmlpipe_attr_float",		KEY_LIST, NULL },
		{ "xmlpipe_attr_bigint",	KEY_LIST, NULL },
		{ "xmlpipe_attr_multi",		KEY_LIST, NULL },
		{ "xmlpipe_attr_multi_64",	KEY_LIST, NULL },
		{ "xmlpipe_attr_string",	KEY_LIST, NULL },
		{ "xmlpipe_attr_wordcount", KEY_REMOVED | KEY_LIST, NULL },
		{ "xmlpipe_attr_json",		KEY_LIST, NULL },
		{ "xmlpipe_field_string",	KEY_LIST, NULL },
		{ "xmlpipe_field_wordcount", KEY_REMOVED | KEY_LIST, NULL },
		{ "xmlpipe_fixup_utf8",		0, NULL },
		{ "sql_str2ordinal_column", KEY_LIST | KEY_REMOVED, NULL },
		{ "unpack_zlib",			KEY_LIST, NULL },
		{ "unpack_mysqlcompress",	KEY_LIST, NULL },
		{ "unpack_mysqlcompress_maxsize", 0, NULL },
		{ "odbc_dsn",				0, NULL },
		{ "sql_joined_field",		KEY_LIST, NULL },
		{ "sql_attr_string",		KEY_LIST, NULL },
		{ "sql_attr_str2wordcount", KEY_REMOVED | KEY_LIST, NULL },
		{ "sql_field_string",		KEY_LIST, NULL },
		{ "sql_field_str2wordcount", KEY_REMOVED | KEY_LIST, NULL },
		{ "sql_file_field",			KEY_LIST, NULL },
		{ "sql_column_buffers",		0, NULL },
		{ "sql_attr_json",			KEY_LIST, NULL },
		{ "hook_connect",			KEY_HIDDEN, NULL },
		{ "hook_query_range",		KEY_HIDDEN, NULL },
		{ "hook_post_index",		KEY_HIDDEN, NULL },
		{ "tsvpipe_command",		0, NULL },
		{ "tsvpipe_field",			KEY_LIST, NULL },
		{ "tsvpipe_attr_uint",		KEY_LIST, NULL },
		{ "tsvpipe_attr_timestamp",	KEY_LIST, NULL },
		{ "tsvpipe_attr_bool",		KEY_LIST, NULL },
		{ "tsvpipe_attr_float",		KEY_LIST, NULL },
		{ "tsvpipe_attr_bigint",	KEY_LIST, NULL },
		{ "tsvpipe_attr_multi",		KEY_LIST, NULL },
		{ "tsvpipe_attr_multi_64",	KEY_LIST, NULL },
		{ "tsvpipe_attr_string",	KEY_LIST, NULL },
		{ "tsvpipe_attr_json",		KEY_LIST, NULL },
		{ "tsvpipe_field_string",	KEY_LIST, NULL },
		{ "csvpipe_command",		0, NULL },
		{ "csvpipe_field",			KEY_LIST, NULL },
		{ "csvpipe_attr_uint",		KEY_LIST, NULL },
		{ "csvpipe_attr_timestamp",	KEY_LIST, NULL },
		{ "csvpipe_attr_bool",		KEY_LIST, NULL },
		{ "csvpipe_attr_float",		KEY_LIST, NULL },
		{ "csvpipe_attr_bigint",	KEY_LIST, NULL },
		{ "csvpipe_attr_multi",		KEY_LIST, NULL },
		{ "csvpipe_attr_multi_64",	KEY_LIST, NULL },
		{ "csvpipe_attr_string",	KEY_LIST, NULL },
		{ "csvpipe_attr_json",		KEY_LIST, NULL },
		{ "csvpipe_field_string",	KEY_LIST, NULL },
		{ "csvpipe_delimiter",		0, NULL },
		{ NULL,						0, NULL }
	};

	/// allowed keys for index section
	static KeyDesc_t g_dKeysIndex[] =
	{
		{ "source",					KEY_LIST, NULL },
		{ "path",					0, NULL },
		{ "docinfo",				0, NULL },
		{ "mlock",					0, NULL },
		{ "morphology",				0, NULL },
		{ "stopwords",				0, NULL },
		{ "exceptions",				0, NULL },
		{ "wordforms",				KEY_LIST, NULL },
		{ "embedded_limit",			0, NULL },
		{ "min_word_len",			0, NULL },
		{ "charset_type",			KEY_REMOVED, NULL },
		{ "charset_table",			0, NULL },
		{ "ignore_chars",			0, NULL },
		{ "min_prefix_len",			0, NULL },
		{ "min_infix_len",			0, NULL },
		{ "max_substring_len",		KEY_DEPRECATED, "dict=keywords" },
		{ "prefix_fields",			0, NULL },
		{ "infix_fields",			0, NULL },
		{ "enable_star",			KEY_REMOVED, NULL },
		{ "ngram_len",				0, NULL },
		{ "ngram_chars",			0, NULL },
		{ "phrase_boundary",		0, NULL },
		{ "phrase_boundary_step",	0, NULL },
		{ "ondisk_dict",			KEY_REMOVED, NULL },
		{ "type",					0, NULL },
		{ "local",					KEY_LIST, NULL },
		{ "agent",					KEY_LIST, NULL },
		{ "agent_blackhole",		KEY_LIST, NULL },
		{ "agent_persistent",		KEY_LIST, NULL },
		{ "agent_connect_timeout",	0, NULL },
		{ "ha_strategy",			0, NULL	},
		{ "agent_query_timeout",	0, NULL },
		{ "html_strip",				0, NULL },
		{ "html_index_attrs",		0, NULL },
		{ "html_remove_elements",	0, NULL },
		{ "preopen",				0, NULL },
		{ "inplace_enable",			0, NULL },
		{ "inplace_hit_gap",		0, NULL },
		{ "inplace_docinfo_gap",	0, NULL },
		{ "inplace_reloc_factor",	0, NULL },
		{ "inplace_write_factor",	0, NULL },
		{ "index_exact_words",		0, NULL },
		{ "min_stemming_len",		0, NULL },
		{ "overshort_step",			0, NULL },
		{ "stopword_step",			0, NULL },
		{ "blend_chars",			0, NULL },
		{ "expand_keywords",		0, NULL },
		{ "hitless_words",			0, NULL },
		{ "hit_format",				KEY_HIDDEN | KEY_DEPRECATED, "default value" },
		{ "rt_field",				KEY_LIST, NULL },
		{ "rt_attr_uint",			KEY_LIST, NULL },
		{ "rt_attr_bigint",			KEY_LIST, NULL },
		{ "rt_attr_float",			KEY_LIST, NULL },
		{ "rt_attr_timestamp",		KEY_LIST, NULL },
		{ "rt_attr_string",			KEY_LIST, NULL },
		{ "rt_attr_multi",			KEY_LIST, NULL },
		{ "rt_attr_multi_64",		KEY_LIST, NULL },
		{ "rt_attr_json",			KEY_LIST, NULL },
		{ "rt_attr_bool",			KEY_LIST, NULL },
		{ "rt_mem_limit",			0, NULL },
		{ "dict",					0, NULL },
		{ "index_sp",				0, NULL },
		{ "index_zones",			0, NULL },
		{ "blend_mode",				0, NULL },
		{ "regexp_filter",			KEY_LIST, NULL },
		{ "bigram_freq_words",		0, NULL },
		{ "bigram_index",			0, NULL },
		{ "index_field_lengths",	0, NULL },
		{ "divide_remote_ranges",	KEY_HIDDEN, NULL },
		{ "stopwords_unstemmed",	0, NULL },
		{ "global_idf",				0, NULL },
		{ "rlp_context",			0, NULL },
		{ "ondisk_attrs",			0, NULL },
		{ "index_token_filter",		0, NULL },
		{ NULL,						0, NULL }
	};

	/// allowed keys for indexer section
	static KeyDesc_t g_dKeysIndexer[] =
	{
		{ "mem_limit",				0, NULL },
		{ "max_iops",				0, NULL },
		{ "max_iosize",				0, NULL },
		{ "max_xmlpipe2_field",		0, NULL },
		{ "max_file_field_buffer",	0, NULL },
		{ "write_buffer",			0, NULL },
		{ "on_file_field_error",	0, NULL },
		{ "on_json_attr_error",		KEY_DEPRECATED, "on_json_attr_error in common{..} section" },
		{ "json_autoconv_numbers",	KEY_DEPRECATED, "json_autoconv_numbers in common{..} section" },
		{ "json_autoconv_keynames",	KEY_DEPRECATED, "json_autoconv_keynames in common{..} section" },
		{ "lemmatizer_cache",		0, NULL },
		{ NULL,						0, NULL }
	};

	/// allowed keys for searchd section
	static KeyDesc_t g_dKeysSearchd[] =
	{
		{ "address",				KEY_REMOVED, NULL },
		{ "port",					KEY_REMOVED, NULL },
		{ "listen",					KEY_LIST, NULL },
		{ "log",					0, NULL },
		{ "query_log",				0, NULL },
		{ "read_timeout",			0, NULL },
		{ "client_timeout",			0, NULL },
		{ "max_children",			0, NULL },
		{ "pid_file",				0, NULL },
		{ "max_matches",			KEY_REMOVED, NULL },
		{ "seamless_rotate",		0, NULL },
		{ "preopen_indexes",		0, NULL },
		{ "unlink_old",				0, NULL },
		{ "ondisk_dict_default",	KEY_REMOVED, NULL },
		{ "attr_flush_period",		0, NULL },
		{ "max_packet_size",		0, NULL },
		{ "mva_updates_pool",		0, NULL },
		{ "max_filters",			0, NULL },
		{ "max_filter_values",		0, NULL },
		{ "listen_backlog",			0, NULL },
		{ "read_buffer",			0, NULL },
		{ "read_unhinted",			0, NULL },
		{ "max_batch_queries",		0, NULL },
		{ "subtree_docs_cache",		0, NULL },
		{ "subtree_hits_cache",		0, NULL },
		{ "workers",				0, NULL },
		{ "prefork",				KEY_HIDDEN, NULL },
		{ "dist_threads",			0, NULL },
		{ "binlog_flush",			0, NULL },
		{ "binlog_path",			0, NULL },
		{ "binlog_max_log_size",	0, NULL },
		{ "thread_stack",			0, NULL },
		{ "expansion_limit",		0, NULL },
		{ "rt_flush_period",		0, NULL },
		{ "query_log_format",		0, NULL },
		{ "mysql_version_string",	0, NULL },
		{ "plugin_dir",				KEY_DEPRECATED, "plugin_dir in common{..} section" },
		{ "collation_server",		0, NULL },
		{ "collation_libc_locale",	0, NULL },
		{ "watchdog",				0, NULL },
		{ "prefork_rotation_throttle", KEY_REMOVED, NULL },
		{ "snippets_file_prefix",	0, NULL },
		{ "sphinxql_state",			0, NULL },
		{ "rt_merge_iops",			0, NULL },
		{ "rt_merge_maxiosize",		0, NULL },
		{ "ha_ping_interval",		0, NULL },
		{ "ha_period_karma",		0, NULL },
		{ "predicted_time_costs",	0, NULL },
		{ "persistent_connections_limit",	0, NULL },
		{ "ondisk_attrs_default",	0, NULL },
		{ "shutdown_timeout",		0, NULL },
		{ "query_log_min_msec",		0, NULL },
		{ "agent_connect_timeout",	0, NULL },
		{ "agent_query_timeout",	0, NULL },
		{ "agent_retry_delay",		0, NULL },
		{ "agent_retry_count",		0, NULL },
		{ "net_wait_tm",			0, NULL },
		{ "net_throttle_action",	0, NULL },
		{ "net_throttle_accept",	0, NULL },
		{ "net_send_job",			0, NULL },
		{ "net_workers",			0, NULL },
		{ "queue_max_length",		0, NULL },
		{ "qcache_ttl_sec",			0, NULL },
		{ "qcache_max_bytes",		0, NULL },
		{ "qcache_thresh_msec",		0, NULL },
		{ "sphinxql_timeout",		0, NULL },
		{ "hostname_lookup",		0, NULL },
		{ NULL,						0, NULL }
	};

	/// allowed keys for common section
	static KeyDesc_t g_dKeysCommon[] =
	{
		{ "lemmatizer_base",		0, NULL },
		{ "on_json_attr_error",		0, NULL },
		{ "json_autoconv_numbers",	0, NULL },
		{ "json_autoconv_keynames",	0, NULL },
		{ "rlp_root",				0, NULL },
		{ "rlp_environment",		0, NULL },
		{ "rlp_max_batch_size",		0, NULL },
		{ "rlp_max_batch_docs",		0, NULL },
		{ "plugin_dir",				0, NULL },
		{ "progressive_merge",		0, NULL },
		{ NULL,						0, NULL }
	};


	//////////////////////


	struct KeySection_t
	{
		const char* m_sKey;		///< key name
		KeyDesc_t* m_pSection; ///< section to refer
		bool				m_bNamed; ///< true if section is named. false if plain
	};

	static KeySection_t g_dConfigSections[] =
	{
		{ "source",		g_dKeysSource,	true },
		{ "index",		g_dKeysIndex,	true },
		{ "indexer",	g_dKeysIndexer,	false },
		{ "searchd",	g_dKeysSearchd,	false },
		{ "common",		g_dKeysCommon,	false },
		{ NULL,			NULL,			false }
	};



#if !USE_WINDOWS

	bool TryToExec(char* pBuffer, const char* szFilename, CSphVector<char>& dResult, char* sError, int iErrorLen)
	{
		int dPipe[2] = { -1, -1 };

		if (pipe(dPipe))
		{
			snprintf(sError, iErrorLen, "pipe() failed (error=%s)", strerror(errno));
			return false;
		}

		pBuffer = trim(pBuffer);

		int iRead = dPipe[0];
		int iWrite = dPipe[1];

		int iChild = fork();

		if (iChild == 0)
		{
			close(iRead);
			close(STDOUT_FILENO);
			dup2(iWrite, STDOUT_FILENO);

			char* pPtr = pBuffer;
			char* pArgs = NULL;
			while (*pPtr)
			{
				if (sphIsSpace(*pPtr))
				{
					*pPtr = '\0';
					pArgs = trim(pPtr + 1);
					break;
				}

				pPtr++;
			}

			if (pArgs)
				execl(pBuffer, pBuffer, pArgs, szFilename, (char*)NULL);
			else
				execl(pBuffer, pBuffer, szFilename, (char*)NULL);

			exit(1);

		}
		else if (iChild == -1)
		{
			snprintf(sError, iErrorLen, "fork failed: [%d] %s", errno, strerror(errno));
			return false;
		}

		close(iWrite);

		int iBytesRead, iTotalRead = 0;
		const int BUFFER_SIZE = 65536;

		dResult.Reset();

		do
		{
			dResult.Resize(iTotalRead + BUFFER_SIZE);
			for (;; )
			{
				iBytesRead = (int)read(iRead, (void*)&(dResult[iTotalRead]), BUFFER_SIZE);
				if (iBytesRead == -1 && errno == EINTR) // we can get SIGCHLD just before eof
					continue;
				break;
			}
			iTotalRead += iBytesRead;
		} while (iBytesRead > 0);
		close(iRead);

		int iStatus, iResult;
		do
		{
			// can be interrupted by pretty much anything (e.g. SIGCHLD from other searchd children)
			iResult = waitpid(iChild, &iStatus, 0);

			// they say this can happen if child exited and SIGCHLD was ignored
			// a cleaner one would be to temporary handle it here, but can we be bothered
			if (iResult == -1 && errno == ECHILD)
			{
				iResult = iChild;
				iStatus = 0;
			}

			if (iResult == -1 && errno != EINTR)
			{
				snprintf(sError, iErrorLen, "waitpid() failed: [%d] %s", errno, strerror(errno));
				return false;
			}
		} while (iResult != iChild);

		if (WIFEXITED(iStatus) && WEXITSTATUS(iStatus))
		{
			// FIXME? read stderr and log that too
			snprintf(sError, iErrorLen, "error executing '%s' status = %d", pBuffer, WEXITSTATUS(iStatus));
			return false;
		}

		if (WIFSIGNALED(iStatus))
		{
			snprintf(sError, iErrorLen, "error executing '%s', killed by signal %d", pBuffer, WTERMSIG(iStatus));
			return false;
		}

		if (iBytesRead < 0)
		{
			snprintf(sError, iErrorLen, "pipe read error: [%d] %s", errno, strerror(errno));
			return false;
		}

		dResult.Resize(iTotalRead + 1);
		dResult[iTotalRead] = '\0';

		return true;
	}

#endif


	int64_t CSphConfigSection::GetSize64(const char* sKey, int64_t iDefault) const
	{
		CSphVariant* pEntry = (*this)(sKey);
		if (!pEntry)
			return iDefault;

		char sMemLimit[256];
		strncpy(sMemLimit, pEntry->cstr(), sizeof(sMemLimit));
		sMemLimit[sizeof(sMemLimit) - 1] = '\0';

		size_t iLen = strlen(sMemLimit);
		if (!iLen)
			return iDefault;

		iLen--;
		int iScale = 1;
		if (toupper(sMemLimit[iLen]) == 'K')
		{
			iScale = 1024;
			sMemLimit[iLen] = '\0';

		}
		else if (toupper(sMemLimit[iLen]) == 'M')
		{
			iScale = 1048576;
			sMemLimit[iLen] = '\0';
		}

		char* sErr;
		int64_t iRes = strtoll(sMemLimit, &sErr, 10);

		if (!*sErr)
		{
			iRes *= iScale;
		}
		else
		{
			sphWarning("'%s = %s' parse error '%s'", sKey, pEntry->cstr(), sErr);
			iRes = iDefault;
		}

		return iRes;
	}

	int CSphConfigSection::GetSize(const char* sKey, int iDefault) const
	{
		int64_t iSize = GetSize64(sKey, iDefault);
		if (iSize > INT_MAX)
		{
			iSize = INT_MAX;
			sphWarning("'%s = " INT64_FMT "' clamped to %d(INT_MAX)", sKey, iSize, INT_MAX);
		}
		return (int)iSize;
	}

	CSphConfigParser::CSphConfigParser()
		: m_sFileName("")
		, m_iLine(-1)
	{
	}

	bool CSphConfigParser::IsPlainSection(const char* sKey)
	{
		assert(sKey);
		const KeySection_t* pSection = g_dConfigSections;
		while (pSection->m_sKey && strcasecmp(sKey, pSection->m_sKey))
			++pSection;
		return pSection->m_sKey && !pSection->m_bNamed;
	}

	bool CSphConfigParser::IsNamedSection(const char* sKey)
	{
		assert(sKey);
		const KeySection_t* pSection = g_dConfigSections;
		while (pSection->m_sKey && strcasecmp(sKey, pSection->m_sKey))
			++pSection;
		return pSection->m_sKey && pSection->m_bNamed;
	}

	bool CSphConfigParser::AddSection(const char* sType, const char* sName)
	{
		m_sSectionType = sType;
		m_sSectionName = sName;

		if (!m_tConf.Exists(m_sSectionType))
			m_tConf.Add(CSphConfigType(), m_sSectionType); // FIXME! be paranoid, verify that it returned true

		if (m_tConf[m_sSectionType].Exists(m_sSectionName))
		{
			snprintf(m_sError, sizeof(m_sError), "section '%s' (type='%s') already exists", sName, sType);
			return false;
		}
		m_tConf[m_sSectionType].Add(CSphConfigSection(), m_sSectionName); // FIXME! be paranoid, verify that it returned true

		return true;
	}


	void CSphConfigParser::AddKey(const char* sKey, char* sValue)
	{
		assert(m_tConf.Exists(m_sSectionType));
		assert(m_tConf[m_sSectionType].Exists(m_sSectionName));

		sValue = trim(sValue);
		CSphConfigSection& tSec = m_tConf[m_sSectionType][m_sSectionName];
		int iTag = tSec.m_iTag;
		tSec.m_iTag++;
		if (tSec(sKey))
		{
			if (tSec[sKey].m_bTag)
			{
				// override value or list with a new value
				SafeDelete(tSec[sKey].m_pNext); // only leave the first array element
				tSec[sKey] = CSphVariant(sValue, iTag); // update its value
				tSec[sKey].m_bTag = false; // mark it as overridden

			}
			else
			{
				// chain to tail, to keep the order
				CSphVariant* pTail = &tSec[sKey];
				while (pTail->m_pNext)
					pTail = pTail->m_pNext;
				pTail->m_pNext = new CSphVariant(sValue, iTag);
			}

		}
		else
		{
			// just add
			tSec.Add(CSphVariant(sValue, iTag), sKey); // FIXME! be paranoid, verify that it returned true
		}
	}


	bool CSphConfigParser::ValidateKey(const char* sKey)
	{
		// get proper descriptor table
		// OPTIMIZE! move lookup to AddSection
		const KeySection_t* pSection = g_dConfigSections;
		const KeyDesc_t* pDesc = NULL;
		while (pSection->m_sKey && m_sSectionType != pSection->m_sKey)
			++pSection;
		if (pSection->m_sKey)
			pDesc = pSection->m_pSection;

		if (!pDesc)
		{
			snprintf(m_sError, sizeof(m_sError), "unknown section type '%s'", m_sSectionType.cstr());
			return false;
		}

		// check if the key is known
		while (pDesc->m_sKey && strcasecmp(pDesc->m_sKey, sKey))
			pDesc++;
		if (!pDesc->m_sKey)
		{
			snprintf(m_sError, sizeof(m_sError), "unknown key name '%s'", sKey);
			return false;
		}

		// warn about deprecate keys
		if (pDesc->m_iFlags & KEY_DEPRECATED)
			if (++m_iWarnings <= WARNS_THRESH)
				fprintf(stdout, "WARNING: key '%s' is deprecated in %s line %d; use '%s' instead.\n",
					sKey, m_sFileName.cstr(), m_iLine, pDesc->m_sExtra);

		// warn about list/non-list keys
		if (!(pDesc->m_iFlags & KEY_LIST))
		{
			CSphConfigSection& tSec = m_tConf[m_sSectionType][m_sSectionName];
			if (tSec(sKey) && !tSec[sKey].m_bTag)
				if (++m_iWarnings <= WARNS_THRESH)
					fprintf(stdout, "WARNING: key '%s' is not multi-value; value in %s line %d will be ignored.\n", sKey, m_sFileName.cstr(), m_iLine);
		}

		if (pDesc->m_iFlags & KEY_REMOVED)
			if (++m_iWarnings <= WARNS_THRESH)
				fprintf(stdout, "WARNING: key '%s' was permanently removed from Sphinx configuration. Refer to documentation for details.\n", sKey);

		return true;
	}


	char* CSphConfigParser::GetBufferString(char* szDest, int iMax, const char*& szSource)
	{
		int nCopied = 0;

		while (nCopied < iMax - 1 && szSource[nCopied] && (nCopied == 0 || szSource[nCopied - 1] != '\n'))
		{
			szDest[nCopied] = szSource[nCopied];
			nCopied++;
		}

		if (!nCopied)
			return NULL;

		szSource += nCopied;
		szDest[nCopied] = '\0';

		return szDest;
	}

	bool CSphConfigParser::ReParse(const char* sFileName, const char* pBuffer)
	{
		CSphConfig tOldConfig = m_tConf;
		m_tConf.Reset();
		if (Parse(sFileName, pBuffer))
			return true;
		m_tConf = tOldConfig;
		return false;
	}

	bool CSphConfigParser::Parse(const char* sFileName, const char* pBuffer)
	{
		const int L_STEPBACK = 16;
		const int L_TOKEN = 64;
		const int L_BUFFER = 8192;

		FILE* fp = NULL;
		if (!pBuffer)
		{
			// open file
			fp = fopen(sFileName, "rb");
			if (!fp)
				return false;
		}

		// init parser
		m_sFileName = sFileName;
		m_iLine = 0;
		m_iWarnings = 0;

		char* p = NULL;
		char* pEnd = NULL;

		char sBuf[L_BUFFER] = { 0 };

		char sToken[L_TOKEN] = { 0 };
		int iToken = 0;
		int iCh = -1;

		enum { S_TOP, S_SKIP2NL, S_TOK, S_TYPE, S_SEC, S_CHR, S_VALUE, S_SECNAME, S_SECBASE, S_KEY } eState = S_TOP, eStack[8];
		int iStack = 0;

		int iValue = 0, iValueMax = 65535;
		char* sValue = new char[iValueMax + 1];

#define LOC_ERROR(_msg) { strncpy ( m_sError, _msg, sizeof(m_sError) ); break; }
#define LOC_ERROR2(_msg,_a) { snprintf ( m_sError, sizeof(m_sError), _msg, _a ); break; }
#define LOC_ERROR3(_msg,_a,_b) { snprintf ( m_sError, sizeof(m_sError), _msg, _a, _b ); break; }
#define LOC_ERROR4(_msg,_a,_b,_c) { snprintf ( m_sError, sizeof(m_sError), _msg, _a, _b, _c ); break; }

#define LOC_PUSH(_new) { assert ( iStack<int(sizeof(eStack)/sizeof(eState)) ); eStack[iStack++] = eState; eState = _new; }
#define LOC_POP() { assert ( iStack>0 ); eState = eStack[--iStack]; }
#define LOC_BACK() { p--; }

		m_sError[0] = '\0';

		for (; ; p++)
		{
			// if this line is over, load next line
			if (p >= pEnd)
			{
				char* szResult = pBuffer ? GetBufferString(sBuf, L_BUFFER, pBuffer) : fgets(sBuf, L_BUFFER, fp);
				if (!szResult)
					break; // FIXME! check for read error

				m_iLine++;
				size_t iLen = strlen(sBuf);
				if (iLen <= 0)
					LOC_ERROR("internal error; fgets() returned empty string");

				p = sBuf;
				pEnd = sBuf + iLen;
				if (pEnd[-1] != '\n')
				{
					if (iLen == L_BUFFER - 1)
						LOC_ERROR("line too long");
				}
			}

			// handle S_TOP state
			if (eState == S_TOP)
			{
				if (isspace(*p))				continue;

				if (*p == '#')
				{
#if !USE_WINDOWS
					if (!pBuffer && m_iLine == 1 && p == sBuf && p[1] == '!')
					{
						CSphVector<char> dResult;
						if (TryToExec(p + 2, sFileName, dResult, m_sError, sizeof(m_sError)))
							Parse(sFileName, &dResult[0]);
						break;
					}
					else
#endif
					{
						LOC_PUSH(S_SKIP2NL);
						continue;
					}
				}

				if (!sphIsAlpha(*p))			LOC_ERROR("invalid token");
				iToken = 0; LOC_PUSH(S_TYPE); LOC_PUSH(S_TOK); LOC_BACK(); continue;
			}

			// handle S_SKIP2NL state
			if (eState == S_SKIP2NL)
			{
				LOC_POP();
				p = pEnd;
				continue;
			}

			// handle S_TOK state
			if (eState == S_TOK)
			{
				if (!iToken && !sphIsAlpha(*p))LOC_ERROR("internal error (non-alpha in S_TOK pos 0)");
				if (iToken == sizeof(sToken))	LOC_ERROR("token too long");
				if (!sphIsAlpha(*p)) { LOC_POP(); sToken[iToken] = '\0'; iToken = 0; LOC_BACK(); continue; }
				if (!iToken) { sToken[0] = '\0'; }
				sToken[iToken++] = *p; continue;
			}

			// handle S_TYPE state
			if (eState == S_TYPE)
			{
				if (isspace(*p))				continue;
				if (*p == '#') { LOC_PUSH(S_SKIP2NL); continue; }
				if (!sToken[0]) { LOC_ERROR("internal error (empty token in S_TYPE)"); }
				if (IsPlainSection(sToken))
				{
					if (!AddSection(sToken, sToken))
						break;
					sToken[0] = '\0';
					LOC_POP();
					LOC_PUSH(S_SEC);
					LOC_PUSH(S_CHR);
					iCh = '{';
					LOC_BACK();
					continue;
				}
				if (IsNamedSection(sToken)) { m_sSectionType = sToken; sToken[0] = '\0'; LOC_POP(); LOC_PUSH(S_SECNAME); LOC_BACK(); continue; }
				LOC_ERROR2("invalid section type '%s'", sToken);
			}

			// handle S_CHR state
			if (eState == S_CHR)
			{
				if (isspace(*p))				continue;
				if (*p == '#') { LOC_PUSH(S_SKIP2NL); continue; }
				if (*p != iCh)					LOC_ERROR3("expected '%c', got '%c'", iCh, *p);
				LOC_POP(); continue;
			}

			// handle S_SEC state
			if (eState == S_SEC)
			{
				if (isspace(*p))				continue;
				if (*p == '#') { LOC_PUSH(S_SKIP2NL); continue; }
				if (*p == '}') { LOC_POP(); continue; }
				if (sphIsAlpha(*p)) { LOC_PUSH(S_KEY); LOC_PUSH(S_TOK); LOC_BACK(); iValue = 0; sValue[0] = '\0'; continue; }
				LOC_ERROR2("section contents: expected token, got '%c'", *p);
			}

			// handle S_KEY state
			if (eState == S_KEY)
			{
				// validate the key
				if (!ValidateKey(sToken))
					break;

				// an assignment operator and a value must follow
				LOC_POP(); LOC_PUSH(S_VALUE); LOC_PUSH(S_CHR); iCh = '=';
				LOC_BACK(); // because we did not work the char at all
				continue;
			}

			// handle S_VALUE state
			if (eState == S_VALUE)
			{
				if (*p == '\n') { AddKey(sToken, sValue); iValue = 0; LOC_POP(); continue; }
				if (*p == '#') { AddKey(sToken, sValue); iValue = 0; LOC_POP(); LOC_PUSH(S_SKIP2NL); continue; }
				if (*p == '\\')
				{
					// backslash at the line end: continuation operator; let the newline be unhanlded
					if (p[1] == '\r' || p[1] == '\n') { LOC_PUSH(S_SKIP2NL); continue; }

					// backslash before number sign: comment start char escaping; advance and pass it
					if (p[1] == '#') { p++; }

					// otherwise: just a char, pass it
				}
				if (iValue < iValueMax) { sValue[iValue++] = *p; sValue[iValue] = '\0'; }
				continue;
			}

			// handle S_SECNAME state
			if (eState == S_SECNAME)
			{
				if (isspace(*p)) { continue; }
				if (!sToken[0] && !sphIsAlpha(*p)) { LOC_ERROR2("named section: expected name, got '%c'", *p); }

				if (!sToken[0]) { LOC_PUSH(S_TOK); LOC_BACK(); continue; }

				if (!AddSection(m_sSectionType.cstr(), sToken))
					break;
				sToken[0] = '\0';

				if (*p == ':') { eState = S_SECBASE; continue; }
				if (*p == '{') { eState = S_SEC; continue; }
				LOC_ERROR2("named section: expected ':' or '{', got '%c'", *p);
			}

			// handle S_SECBASE state
			if (eState == S_SECBASE)
			{
				if (isspace(*p)) { continue; }
				if (!sToken[0] && !sphIsAlpha(*p)) { LOC_ERROR2("named section: expected parent name, got '%c'", *p); }
				if (!sToken[0]) { LOC_PUSH(S_TOK); LOC_BACK(); continue; }

				// copy the section
				assert(m_tConf.Exists(m_sSectionType));

				if (!m_tConf[m_sSectionType].Exists(sToken))
					LOC_ERROR4("inherited section '%s': parent doesn't exist (parent name='%s', type='%s')",
						m_sSectionName.cstr(), sToken, m_sSectionType.cstr());

				CSphConfigSection& tDest = m_tConf[m_sSectionType][m_sSectionName];
				tDest = m_tConf[m_sSectionType][sToken];

				// mark all values in the target section as "to be overridden"
				tDest.IterateStart();
				while (tDest.IterateNext())
					tDest.IterateGet().m_bTag = true;

				LOC_BACK();
				eState = S_SEC;
				LOC_PUSH(S_CHR);
				iCh = '{';
				continue;
			}

			LOC_ERROR2("internal error (unhandled state %d)", eState);
		}

#undef LOC_POP
#undef LOC_PUSH
#undef LOC_ERROR

		if (!pBuffer)
			fclose(fp);

		SafeDeleteArray(sValue);

		if (m_iWarnings > WARNS_THRESH)
			fprintf(stdout, "WARNING: %d more warnings skipped.\n", m_iWarnings - WARNS_THRESH);

		if (strlen(m_sError))
		{
			int iCol = (int)(p - sBuf + 1);

			int iCtx = Min(L_STEPBACK, iCol); // error context is upto L_STEPBACK chars back, but never going to prev line
			const char* sCtx = p - iCtx + 1;
			if (sCtx < sBuf)
				sCtx = sBuf;

			char sStepback[L_STEPBACK + 1];
			memcpy(sStepback, sCtx, size_t(iCtx));
			sStepback[iCtx] = '\0';

			fprintf(stdout, "ERROR: %s in %s line %d col %d.\n", m_sError, m_sFileName.cstr(), m_iLine, iCol);
			return false;
		}

		return true;
	}


}