#pragma once

namespace NEO {
	/// possible bin states
	enum ESphBinState
	{
		BIN_ERR_READ = -2,	//bin read error
		BIN_ERR_END = -1,	//bin end
		BIN_POS = 0,	//bin is in "expects pos delta" state
		BIN_DOC = 1,	//bin is in "expects doc delta" state
		BIN_WORD = 2		//bin is in "expects word delta" state
	};


	enum ESphBinRead
	{
		BIN_READ_OK,			//bin read ok
		BIN_READ_EOF,			//bin end
		BIN_READ_ERROR,			//bin read error
		BIN_PRECACHE_OK,		//precache ok
		BIN_PRECACHE_ERROR		//precache failed
	};


}