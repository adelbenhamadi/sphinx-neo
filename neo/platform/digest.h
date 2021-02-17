#pragma once
#include "neo/platform/random.h"
#include "neo/int/types.h"



class TDigest_i
{
public:
	virtual				~TDigest_i() {}

	virtual void		Add(double fValue, int64_t iWeight = 1) = 0;
	virtual double		Percentile(int iPercent) const = 0;
};



#if USE_WINDOWS
#pragma warning(push,1)
#pragma warning(disable:4530)
#endif

#include <map>

#if USE_WINDOWS
#pragma warning(pop)
#endif

class TDigest_c : public TDigest_i
{
public:
	TDigest_c()
	{
		Reset();
	}

	virtual void Add(double fValue, int64_t iWeight = 1)
	{
		if (m_dMap.empty())
		{
			m_dMap.insert(std::pair<double, int64_t>(fValue, iWeight));
			m_iCount = iWeight;
			return;
		}

		auto tStart = m_dMap.lower_bound(fValue);
		if (tStart == m_dMap.end())
			tStart = m_dMap.begin();
		else
		{
			while (tStart != m_dMap.begin() && tStart->first == fValue)
				tStart--;
		}

		double fMinDist = DBL_MAX;
		auto tLastNeighbor = m_dMap.end();
		for (auto i = tStart; i != m_dMap.end(); ++i)
		{
			double fDist = fabs(i->first - fValue);
			if (fDist < fMinDist)
			{
				tStart = i;
				fMinDist = fDist;
			}
			else if (fDist > fMinDist)
			{
				// we've passed the nearest nearest neighbor
				tLastNeighbor = i;
				break;
			}
		}

		auto tClosest = m_dMap.end();
		int64_t iSum = 0;
		for (auto i = m_dMap.begin(); i != tStart; ++i)
			iSum += i->second;

		int64_t iN = 0;
		const double COMPRESSION = 200.0;
		for (auto i = tStart; i != tLastNeighbor; ++i)
		{
			double fQuantile = m_iCount == 1 ? 0.5 : (iSum + (i->second - 1) / 2.0) / (m_iCount - 1);
			double fThresh = 4.0 * m_iCount * fQuantile * (1 - fQuantile) / COMPRESSION;

			if (i->second + iWeight <= fThresh)
			{
				iN++;
				if ((double(sphRand()) / UINT_MAX) < 1.0 / iN)
					tClosest = i;
			}

			iSum += i->second;
		}

		if (tClosest == m_dMap.end())
			m_dMap.insert(std::pair<double, int64_t>(fValue, iWeight));
		else
		{
			double fNewMean = WeightedAvg(tClosest->first, tClosest->second, fValue, iWeight);
			int64_t iNewCount = tClosest->second + iWeight;
			m_dMap.erase(tClosest);
			m_dMap.insert(std::pair<double, int64_t>(fNewMean, iNewCount));
		}

		m_iCount += iWeight;

		const DWORD K = 20;
		if (m_dMap.size() > K * COMPRESSION)
			Compress();
	}


	virtual double Percentile(int iPercent) const
	{
		assert(iPercent >= 0 && iPercent <= 100);

		if (m_dMap.empty())
			return 0.0;

		int64_t iTotalCount = 0;
		double fPercent = double(iPercent) / 100.0;
		fPercent *= m_iCount;

		auto iMapFirst = m_dMap.begin();
		auto iMapLast = m_dMap.end();
		--iMapLast;

		for (auto i = iMapFirst; i != m_dMap.end(); ++i)
		{
			if (fPercent < iTotalCount + i->second)
			{
				if (i == iMapFirst || i == iMapLast)
					return i->first;
				else
				{
					// get mean from previous iterator; get mean from next iterator; calc delta
					auto iPrev = i;
					auto iNext = i;
					iPrev--;
					iNext++;

					double fDelta = (iNext->first - iPrev->first) / 2.0;
					return i->first + ((fPercent - iTotalCount) / i->second - 0.5) * fDelta;
				}
			}

			iTotalCount += i->second;
		}

		return iMapLast->first;
	}

private:
	typedef std::multimap<double, int64_t> BalancedTree_c;
	BalancedTree_c		m_dMap;
	int64_t				m_iCount;

	double WeightedAvg(double fX1, int64_t iW1, double fX2, int64_t iW2)
	{
		return (fX1 * iW1 + fX2 * iW2) / (iW1 + iW2);
	}

	void Reset()
	{
		m_dMap.clear();
		m_iCount = 0;
	}

	void Compress()
	{
		struct Centroid_t
		{
			double	m_fMean;
			int64_t	m_iCount;
		};

		CSphTightVector<Centroid_t> dValues;
		dValues.Reserve(m_dMap.size());
		for (auto i : m_dMap)
		{
			Centroid_t& tCentroid = dValues.Add();
			tCentroid.m_fMean = i.first;
			tCentroid.m_iCount = i.second;
		}

		Reset();

		while (dValues.GetLength())
		{
			int iValue = sphRand() % dValues.GetLength();
			Add(dValues[iValue].m_fMean, dValues[iValue].m_iCount);
			dValues.RemoveFast(iValue);
		}
	}
};


TDigest_i* sphCreateTDigest()
{
	return new TDigest_c;
}

