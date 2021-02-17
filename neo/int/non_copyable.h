#pragma once

namespace NEO {
	/// prevent copy
	class ISphNoncopyable
	{
	public:
		ISphNoncopyable() {}

	private:
		ISphNoncopyable(const ISphNoncopyable&) = delete;
		const ISphNoncopyable& operator = (const ISphNoncopyable&) = delete;
	};

}