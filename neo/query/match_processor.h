#pragma once

namespace NEO {

	//fwd class 
	class CSphMatch;

	/// match processor interface
	struct ISphMatchProcessor
	{
		virtual ~ISphMatchProcessor() {}
		virtual void Process(CSphMatch* pMatch) = 0;
	};

}