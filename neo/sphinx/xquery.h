//
// $Id$
//

//
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

#ifndef _sphinxquery_
#define _sphinxquery_

#include "neo/int/types.h"
#include "neo/int/non_copyable.h"
#include "neo/query/extra.h"
#include "neo/index/keyword_stat.h"
#include "neo/query/node_cache.h"
#include "neo/tokenizer/tokenizer.h"


namespace NEO {

	//fwd dec
	class CSphQuery;
	struct CSphIndexSettings;
	class CSphIndex;
	class CSphFilterSettings;
	//struct XQKeyword_t;
	//struct XQLimitSpec_t;
	//struct XQNode_t;

	/// extended query
	struct XQQuery_t : public ISphNoncopyable
	{
		CSphString				m_sParseError;
		CSphString				m_sParseWarning;

		CSphVector<CSphString>	m_dZones;
		XQNode_t* m_pRoot;
		bool					m_bNeedSZlist;
		bool					m_bSingleWord;

		/// ctor
		XQQuery_t()
		{
			m_pRoot = NULL;
			m_bNeedSZlist = false;
			m_bSingleWord = false;
		}

		/// dtor
		~XQQuery_t()
		{
			SafeDelete(m_pRoot);
		}
	};

	//////////////////////////////////////////////////////////////////////////////


	//////////////////////////////////////////////////////////////////////////////

	/// setup tokenizer for query parsing (ie. add all specials and whatnot)
	void	sphSetupQueryTokenizer(ISphTokenizer* pTokenizer, bool bWildcards, bool bExact);

	/// parses the query and returns the resulting tree
	/// return false and fills tQuery.m_sParseError on error
	/// WARNING, parsed tree might be NULL (eg. if query was empty)
	/// lots of arguments here instead of simply the index pointer, because
	/// a) we do not always have an actual real index class, and
	/// b) might need to tweak stuff even we do
	/// FIXME! remove either pQuery or sQuery
	bool	sphParseExtendedQuery(XQQuery_t& tQuery, const char* sQuery, const CSphQuery* pQuery, const ISphTokenizer* pTokenizer, const CSphSchema* pSchema, CSphDict* pDict, const CSphIndexSettings& tSettings);

	// perform boolean optimization on tree
	void	sphOptimizeBoolean(XQNode_t** pXQ, const ISphKeywordsStat* pKeywords);

	/// analyze vector of trees and tag common parts of them (to cache them later)
	int		sphMarkCommonSubtrees(int iXQ, const XQQuery_t* pXQ);


	//////////////////////////////////////////////////////////////////////////
	// COMMON SUBTREES DETECTION
	//////////////////////////////////////////////////////////////////////////

	/// Decides if given pTree is appropriate for caching or not. Currently we don't cache
	/// the end values (leafs).
	static bool IsAppropriate(XQNode_t* pTree)
	{
		if (!pTree) return false;

		// skip nodes that actually are leaves (eg. "AND smth" node instead of merely "smth")
		return !(pTree->m_dWords.GetLength() == 1 && pTree->GetOp() != SPH_QUERY_NOT);
	}

	typedef CSphOrderedHash < DWORD, uint64_t, IdentityHash_fn, 128 > CDwordHash;

	// stores the pair of a tree, and the bitmask of common nodes
	// which contains the tree.
	class BitMask_t
	{
		XQNode_t* m_pTree;
		uint64_t		m_uMask;

	public:
		BitMask_t()
			: m_pTree(NULL)
			, m_uMask(0ull)
		{}

		void Init(XQNode_t* pTree, uint64_t uMask)
		{
			m_pTree = pTree;
			m_uMask = uMask;
		}

		inline uint64_t GetMask() const { return m_uMask; }
		inline XQNode_t* GetTree() const { return m_pTree; }
	};

	// a list of unique values.
	class Associations_t : public CDwordHash
	{
	public:

		// returns true when add the second member.
		// The reason is that only one is not interesting for us,
		// but more than two will flood the caller.
		bool Associate2nd(uint64_t uTree)
		{
			if (Exists(uTree))
				return false;
			Add(0, uTree);
			return GetLength() == 2;
		}

		// merge with another similar
		void Merge(const Associations_t& parents)
		{
			parents.IterateStart();
			while (parents.IterateNext())
				Associate2nd(parents.IterateGetKey());
		}
	};

	// associate set of nodes, common bitmask for these nodes,
	// and gives the < to compare different pairs
	class BitAssociation_t
	{
	private:
		const Associations_t* m_pAssociations;
		mutable int				m_iBits;

		// The key method of subtree selection.
		// Most 'heavy' subtrees will be extracted first.
		inline int GetWeight() const
		{
			assert(m_pAssociations);
			int iNodes = m_pAssociations->GetLength();
			if (m_iBits == 0 && m_uMask != 0)
			{
				for (uint64_t dMask = m_uMask; dMask; dMask >>= 1)
					m_iBits += (int)(dMask & 1);
			}

			// current working formula is num_nodes^2 * num_hits
			return iNodes * iNodes * m_iBits;
		}

	public:
		uint64_t			m_uMask;

		BitAssociation_t()
			: m_pAssociations(NULL)
			, m_iBits(0)
			, m_uMask(0)
		{}

		void Init(uint64_t uMask, const Associations_t* dNodes)
		{
			m_uMask = uMask;
			m_pAssociations = dNodes;
			m_iBits = 0;
		}

		bool operator< (const BitAssociation_t& second) const
		{
			return GetWeight() < second.GetWeight();
		}
	};

	// for pairs of values builds and stores the association "key -> list of values"
	class CAssociations_t
		: public CSphOrderedHash < Associations_t, uint64_t, IdentityHash_fn, 128 >
	{
		int		m_iBits;			// number of non-unique associations
	public:

		CAssociations_t() : m_iBits(0) {}

		// Add the given pTree into the list of pTrees, associated with given uHash
		int Associate(XQNode_t* pTree, uint64_t uHash)
		{
			if (!Exists(uHash))
				Add(Associations_t(), uHash);
			if (operator[](uHash).Associate2nd(pTree->GetHash()))
				m_iBits++;
			return m_iBits;
		}

		// merge the existing association of uHash with given chain
		void MergeAssociations(const Associations_t& chain, uint64_t uHash)
		{
			if (!Exists(uHash))
				Add(chain, uHash);
			else
				operator[](uHash).Merge(chain);
		}

		inline int GetBits() const { return m_iBits; }
	};

	// The main class for working with common subtrees
	class RevealCommon_t : ISphNoncopyable
	{
	private:
		static const int			MAX_MULTINODES = 64;
		CSphVector<BitMask_t>		m_dBitmasks;		// all bitmasks for all the nodes
		CSphVector<uint64_t>		m_dSubQueries;		// final vector with roadmap for tree division.
		CAssociations_t				m_hNodes;			// initial accumulator for nodes
		CAssociations_t				m_hInterSections;	// initial accumulator for nodes
		CDwordHash					m_hBitOrders;		// order numbers for found common subnodes
		XQOperator_e				m_eOp;				// my operator which I process

	private:

		// returns the order for given uHash (if any).
		inline int GetBitOrder(uint64_t uHash) const
		{
			if (!m_hBitOrders.Exists(uHash))
				return -1;
			return m_hBitOrders[uHash];
		}

		// recursively scans the whole tree and builds the maps
		// where a list of parents associated with every "leaf" nodes (i.e. with children)
		bool BuildAssociations(XQNode_t* pTree)
		{
			if (IsAppropriate(pTree))
			{
				ARRAY_FOREACH(i, pTree->m_dChildren)
					if ((!BuildAssociations(pTree->m_dChildren[i]))
						|| ((m_eOp == pTree->GetOp())
							&& (m_hNodes.Associate(pTree, pTree->m_dChildren[i]->GetHash()) >= MAX_MULTINODES)))
					{
						return false;
					}
			}
			return true;
		}

		// Find all leafs, non-unique across the tree,
		// and associate the order number with every of them
		bool CalcCommonNodes()
		{
			if (!m_hNodes.GetBits())
				return false; // there is totally no non-unique leaves
			int iBit = 0;
			m_hNodes.IterateStart();
			while (m_hNodes.IterateNext())
				if (m_hNodes.IterateGet().GetLength() > 1)
					m_hBitOrders.Add(iBit++, m_hNodes.IterateGetKey());
			assert(m_hNodes.GetBits() == m_hBitOrders.GetLength());
			m_hNodes.Reset(); ///< since from now we don't need this data anymore
			return true;
		}

		// recursively builds for every node the bitmaks
		// of common nodes it has as children
		void BuildBitmasks(XQNode_t* pTree)
		{
			if (!IsAppropriate(pTree))
				return;

			if (m_eOp == pTree->GetOp())
			{
				// calculate the bitmask
				int iOrder;
				uint64_t dMask = 0;
				ARRAY_FOREACH(i, pTree->m_dChildren)
				{
					iOrder = GetBitOrder(pTree->m_dChildren[i]->GetHash());
					if (iOrder >= 0)
						dMask |= 1ull << iOrder;
				}

				// add the bitmask into the array
				if (dMask)
					m_dBitmasks.Add().Init(pTree, dMask);
			}

			// recursively process all the children
			ARRAY_FOREACH(i, pTree->m_dChildren)
				BuildBitmasks(pTree->m_dChildren[i]);
		}

		// Collect all possible intersections of Bitmasks.
		// For every non-zero intersection we collect the list of trees which contain it.
		void CalcIntersections()
		{
			// Round 1. Intersect all content of bitmasks one-by-one.
			ARRAY_FOREACH(i, m_dBitmasks)
				for (int j = i + 1; j < m_dBitmasks.GetLength(); j++)
				{
					// intersect one-by-one and group (grouping is done by nature of a hash)
					uint64_t uMask = m_dBitmasks[i].GetMask() & m_dBitmasks[j].GetMask();
					if (uMask)
					{
						m_hInterSections.Associate(m_dBitmasks[i].GetTree(), uMask);
						m_hInterSections.Associate(m_dBitmasks[j].GetTree(), uMask);
					}
				}

			// Round 2. Intersect again all collected intersection one-by-one - until zero.
			void* p1 = NULL, * p2;
			uint64_t uMask1, uMask2;
			while (m_hInterSections.IterateNext(&p1))
			{
				p2 = p1;
				while (m_hInterSections.IterateNext(&p2))
				{
					uMask1 = CAssociations_t::IterateGetKey(&p1);
					uMask2 = CAssociations_t::IterateGetKey(&p2);
					assert(uMask1 != uMask2);
					uMask1 &= uMask2;
					if (uMask1)
					{
						m_hInterSections.MergeAssociations(CAssociations_t::IterateGet(&p1), uMask1);
						m_hInterSections.MergeAssociations(CAssociations_t::IterateGet(&p2), uMask1);
					}
				}
			}
		}

		// create the final kit of common-subsets
		// which we will actually reveal (extract) from original trees
		void MakeQueries()
		{
			CSphVector<BitAssociation_t> dSubnodes; // masks for our selected subnodes
			dSubnodes.Reserve(m_hInterSections.GetLength());
			m_hInterSections.IterateStart();
			while (m_hInterSections.IterateNext())
				dSubnodes.Add().Init(m_hInterSections.IterateGetKey(), &m_hInterSections.IterateGet());

			// sort by weight descending (weight sorting is hold by operator <)
			dSubnodes.RSort();
			m_dSubQueries.Reset();

			// make the final subtrees vector: get one-by-one from the beginning,
			// intresect with all the next and throw out zeros.
			// The final subqueries will not be intersected between each other.
			int j;
			uint64_t uMask;
			ARRAY_FOREACH(i, dSubnodes)
			{
				uMask = dSubnodes[i].m_uMask;
				m_dSubQueries.Add(uMask);
				j = i + 1;
				while (j < dSubnodes.GetLength())
				{
					if (!(dSubnodes[j].m_uMask &= ~uMask))
						dSubnodes.Remove(j);
					else
						j++;
				}
			}
		}

		// Now we finally extract the common subtrees from original tree
		// and (recursively) from it's children
		void Reorganize(XQNode_t* pTree)
		{
			if (!IsAppropriate(pTree))
				return;

			if (m_eOp == pTree->GetOp())
			{
				// pBranch is for common subset of children, pOtherChildren is for the rest.
				CSphOrderedHash < XQNode_t*, int, IdentityHash_fn, 64 > hBranches;
				XQNode_t* pOtherChildren = NULL;
				int iBit;
				int iOptimizations = 0;
				ARRAY_FOREACH(i, pTree->m_dChildren)
				{
					iBit = GetBitOrder(pTree->m_dChildren[i]->GetHash());

					// works only with children which are actually common with somebody else
					if (iBit >= 0)
					{
						// since subqueries doesn't intersected between each other,
						// the first hit we found in this loop is exactly what we searched.
						ARRAY_FOREACH(j, m_dSubQueries)
							if ((1ull << iBit) & m_dSubQueries[j])
							{
								XQNode_t* pNode;
								if (!hBranches.Exists((int)j))
								{
									pNode = new XQNode_t(pTree->m_dSpec);
									pNode->SetOp(m_eOp, pTree->m_dChildren[i]);
									hBranches.Add(pNode,(int) j);
								}
								else
								{
									pNode = hBranches[(int)j];
									pNode->m_dChildren.Add(pTree->m_dChildren[i]);

									// Count essential subtrees (with at least 2 children)
									if (pNode->m_dChildren.GetLength() == 2)
										iOptimizations++;
								}
								break;
							}
						// another nodes add to the set of "other" children
					}
					else
					{
						if (!pOtherChildren)
						{
							pOtherChildren = new XQNode_t(pTree->m_dSpec);
							pOtherChildren->SetOp(m_eOp, pTree->m_dChildren[i]);
						}
						else
							pOtherChildren->m_dChildren.Add(pTree->m_dChildren[i]);
					}
				}

				// we don't reorganize explicit simple case - as no "others" and only one common.
				// Also reject optimization if there is nothing to optimize.
				if ((iOptimizations == 0)
					| (!pOtherChildren && (hBranches.GetLength() == 1)))
				{
					if (pOtherChildren)
						pOtherChildren->m_dChildren.Reset();
					hBranches.IterateStart();
					while (hBranches.IterateNext())
					{
						assert(hBranches.IterateGet());
						hBranches.IterateGet()->m_dChildren.Reset();
						SafeDelete(hBranches.IterateGet());
					}
				}
				else
				{
					// reorganize the tree: replace the common subset to explicit node with
					// only common members inside. This will give the the possibility
					// to cache the node.
					pTree->m_dChildren.Reset();
					if (pOtherChildren)
						pTree->m_dChildren.SwapData(pOtherChildren->m_dChildren);

					hBranches.IterateStart();
					while (hBranches.IterateNext())
					{
						if (hBranches.IterateGet()->m_dChildren.GetLength() == 1)
						{
							pTree->m_dChildren.Add(hBranches.IterateGet()->m_dChildren[0]);
							hBranches.IterateGet()->m_dChildren.Reset();
							SafeDelete(hBranches.IterateGet());
						}
						else
							pTree->m_dChildren.Add(hBranches.IterateGet());
					}
				}
				SafeDelete(pOtherChildren);
			}

			// recursively process all the children
			ARRAY_FOREACH(i, pTree->m_dChildren)
				Reorganize(pTree->m_dChildren[i]);
		}

	public:
		explicit RevealCommon_t(XQOperator_e eOp)
			: m_eOp(eOp)
		{}

		// actual method for processing tree and reveal (extract) common subtrees
		void Transform(int iXQ, const XQQuery_t* pXQ)
		{
			// collect all non-unique nodes
			for (int i = 0; i < iXQ; i++)
				if (!BuildAssociations(pXQ[i].m_pRoot))
					return;

			// count and order all non-unique nodes
			if (!CalcCommonNodes())
				return;

			// create and collect bitmask for every node
			for (int i = 0; i < iXQ; i++)
				BuildBitmasks(pXQ[i].m_pRoot);

			// intersect all bitmasks one-by-one, and also intersect all intersections
			CalcIntersections();

			// the die-hard: actually select the set of subtrees which we'll process
			MakeQueries();

			// ... and finally - process all our trees.
			for (int i = 0; i < iXQ; i++)
				Reorganize(pXQ[i].m_pRoot);
		}
	};


	struct MarkedNode_t
	{
		int			m_iCounter;
		XQNode_t* m_pTree;
		bool		m_bMarked;
		int			m_iOrder;

		explicit MarkedNode_t(XQNode_t* pTree = NULL)
			: m_iCounter(1)
			, m_pTree(pTree)
			, m_bMarked(false)
			, m_iOrder(0)
		{}

		void MarkIt(bool bMark)
		{
			// mark
			if (bMark)
			{
				m_iCounter++;
				m_bMarked = true;
				return;
			}

			// unmark
			if (m_bMarked && m_iCounter > 1)
				m_iCounter--;
			if (m_iCounter < 2)
				m_bMarked = false;
		}
	};

	typedef CSphOrderedHash < MarkedNode_t, uint64_t, IdentityHash_fn, 128 > CSubtreeHash;

	struct XqTreeComparator_t
	{
		CSphVector<const XQKeyword_t*>		m_dTerms1;
		CSphVector<const XQKeyword_t*>		m_dTerms2;

		bool IsEqual(const XQNode_t* pNode1, const XQNode_t* pNode2);
		bool CheckCollectTerms(const XQNode_t* pNode1, const XQNode_t* pNode2);
	};

	//////////////////////////////



	class CSphTransformation : public ISphNoncopyable
	{
	public:
		CSphTransformation(XQNode_t** ppRoot, const ISphKeywordsStat* pKeywords);
		void Transform();
		inline void Dump(const XQNode_t* pNode, const char* sHeader = "");

	private:

		typedef CSphOrderedHash < CSphVector<XQNode_t*>, uint64_t, IdentityHash_fn, 32> HashSimilar_t;
		CSphOrderedHash < HashSimilar_t, uint64_t, IdentityHash_fn, 256 >	m_hSimilar;
		CSphVector<XQNode_t*>		m_dRelatedNodes;
		const ISphKeywordsStat* m_pKeywords;
		XQNode_t** m_ppRoot;
		typedef bool (*Checker_fn) (const XQNode_t*);

	private:

		void		Dump();
		void		SetCosts(XQNode_t* pNode, const CSphVector<XQNode_t*>& dNodes);
		int			GetWeakestIndex(const CSphVector<XQNode_t*>& dNodes);

		template < typename Group, typename SubGroup >
		inline void TreeCollectInfo(XQNode_t* pParent, Checker_fn pfnChecker);

		template < typename Group, typename SubGroup >
		inline bool CollectInfo(XQNode_t* pParent, Checker_fn pfnChecker);

		template < typename Excluder, typename Parenter >
		inline bool	CollectRelatedNodes(const CSphVector<XQNode_t*>& dSimilarNodes);

		// ((A !N) | (B !N)) -> ((A|B) !N)
		static bool CheckCommonNot(const XQNode_t* pNode);
		bool		TransformCommonNot();
		bool		MakeTransformCommonNot(CSphVector<XQNode_t*>& dSimilarNodes);

		// ((A !(N AA)) | (B !(N BB))) -> (((A|B) !N) | (A !AA) | (B !BB)) [ if cost(N) > cost(A) + cost(B) ]
		static bool	CheckCommonCompoundNot(const XQNode_t* pNode);
		bool		TransformCommonCompoundNot();
		bool		MakeTransformCommonCompoundNot(CSphVector<XQNode_t*>& dSimilarNodes);

		// ((A (X | AA)) | (B (X | BB))) -> (((A|B) X) | (A AA) | (B BB)) [ if cost(X) > cost(A) + cost(B) ]
		static bool	CheckCommonSubTerm(const XQNode_t* pNode);
		bool		TransformCommonSubTerm();
		void		MakeTransformCommonSubTerm(CSphVector<XQNode_t*>& dX);

		// (A | "A B"~N) -> A ; ("A B" | "A B C") -> "A B" ; ("A B"~N | "A B C"~N) -> ("A B"~N)
		static bool CheckCommonKeywords(const XQNode_t* pNode);
		bool		TransformCommonKeywords();

		// ("X A B" | "Y A B") -> (("X|Y") "A B")
		// ("A B X" | "A B Y") -> (("X|Y") "A B")
		static bool CheckCommonPhrase(const XQNode_t* pNode);
		bool 		TransformCommonPhrase();
		void		MakeTransformCommonPhrase(CSphVector<XQNode_t*>& dCommonNodes, int iCommonLen, bool bHeadIsCommon);

		// ((A !X) | (A !Y) | (A !Z)) -> (A !(X Y Z))
		static bool CheckCommonAndNotFactor(const XQNode_t* pNode);
		bool		TransformCommonAndNotFactor();
		bool		MakeTransformCommonAndNotFactor(CSphVector<XQNode_t*>& dSimilarNodes);

		// ((A !(N | N1)) | (B !(N | N2))) -> (( (A !N1) | (B !N2) ) !N)
		static bool CheckCommonOrNot(const XQNode_t* pNode);
		bool 		TransformCommonOrNot();
		bool		MakeTransformCommonOrNot(CSphVector<XQNode_t*>& dSimilarNodes);

		// The main goal of transformations below is tree clarification and
		// further applying of standard transformations above.

		// "hung" operand ( AND(OR) node with only 1 child ) appears after an internal transformation
		static bool CheckHungOperand(const XQNode_t* pNode);
		bool		TransformHungOperand();

		// ((A | B) | C) -> ( A | B | C )
		// ((A B) C) -> ( A B C )
		static bool CheckExcessBrackets(const XQNode_t* pNode);
		bool 		TransformExcessBrackets();

		// ((A !N1) !N2) -> (A !(N1 | N2))
		static bool CheckExcessAndNot(const XQNode_t* pNode);
		bool		TransformExcessAndNot();

	private:
		static const uint64_t CONST_GROUP_FACTOR;

		struct NullNode
		{
			static inline uint64_t By(XQNode_t*) { return CONST_GROUP_FACTOR; } // NOLINT
			static inline const XQNode_t* From(const XQNode_t*) { return NULL; } // NOLINT
		};

		struct CurrentNode
		{
			static inline uint64_t By(XQNode_t* p) { return p->GetFuzzyHash(); }
			static inline const XQNode_t* From(const XQNode_t* p) { return p; }
		};

		struct ParentNode
		{
			static inline uint64_t By(XQNode_t* p) { return p->m_pParent->GetFuzzyHash(); }
			static inline const XQNode_t* From(const XQNode_t* p) { return p->m_pParent; }
		};

		struct GrandNode
		{
			static inline uint64_t By(XQNode_t* p) { return p->m_pParent->m_pParent->GetFuzzyHash(); }
			static inline const XQNode_t* From(const XQNode_t* p) { return p->m_pParent->m_pParent; }
		};

		struct Grand2Node {
			static inline uint64_t By(XQNode_t* p) { return p->m_pParent->m_pParent->m_pParent->GetFuzzyHash(); }
			static inline const XQNode_t* From(const XQNode_t* p) { return p->m_pParent->m_pParent->m_pParent; }
		};

		struct Grand3Node
		{
			static inline uint64_t By(XQNode_t* p) { return p->m_pParent->m_pParent->m_pParent->m_pParent->GetFuzzyHash(); }
			static inline const XQNode_t* From(const XQNode_t* p) { return p->m_pParent->m_pParent->m_pParent->m_pParent; }
		};
	};


}

#endif