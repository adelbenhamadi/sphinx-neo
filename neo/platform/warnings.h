#pragma once 

//////////////////////////////////////
//disable warnings!!!
///////////////////////////////////////

// workaround to suppress C4511/C4512 warnings (copy ctor and assignment operator) in VS 2003
#if _MSC_VER>=1300 && _MSC_VER<1400
#pragma warning(disable:4511)
#pragma warning(disable:4512)
#endif

// suppress C4201 (nameless struct/union is a nonstandard extension) because even min-spec gcc 3.4.6 works ok
#if defined(_MSC_VER)
#pragma warning(disable:4201)
#endif

//////////////////////////////////////