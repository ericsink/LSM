/*
	Copyright 2014 Zumero, LLC

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.IO;

using Zumero.LSM;

namespace lsm_tests
{
	public class trivialMemoryPageManager : IPages
	{
		int pageSize;

		public trivialMemoryPageManager(int _pageSize)
		{
			pageSize = _pageSize;
		}

		int IPages.PageSize
		{
			get {
				return pageSize;
			}
		}

		IPendingSegment IPages.Begin()
		{
			return null;
		}

		Guid IPages.End(IPendingSegment token, int lastPage)
		{
			return Guid.NewGuid();
		}

		PageBlock IPages.GetRange(IPendingSegment token)
		{
			return new PageBlock(1, -1);
		}

	}

	public class SimplePageManager : IPages
	{
		private class PendingSegment : IPendingSegment
		{
			private List<PageBlock> blockList = new List<PageBlock>();

			public void Add(PageBlock t)
			{
				blockList.Add (t);
			}

			public Tuple<Guid,List<PageBlock>> End(int lastPage)
			{
				var lastBlock = blockList[blockList.Count-1];
				// assert lastPage >= lastBlock.Item1;
				if (lastPage < lastBlock.lastPage) {
					// this segment did not use all the pages we gave it
					blockList.Remove (lastBlock);
					blockList.Add (new PageBlock (lastBlock.firstPage, lastPage));
				}
				return new Tuple<Guid,List<PageBlock>> (Guid.NewGuid (), blockList);
			}
		}

		int cur = 1;
		private readonly Dictionary<Guid,List<PageBlock>> segments;
		int pageSize;

		// TODO could be a param
		const int PAGES_PER_BLOCK = 10; // TODO very low, for testing purposes

		// surprisingly enough, the test suite passes with only ONE page per block.
		// this is still absurd and should probably be disallowed.

		const int WASTE_PAGES_AFTER_EACH_BLOCK = 3; // obviously, for testing purposes only

		public SimplePageManager(int _pageSize)
		{
			pageSize = _pageSize;
			segments = new Dictionary<Guid, List<PageBlock>> ();
		}

        int IPages.PageSize
        {
            get {
				return pageSize;
            }
        }

		IPendingSegment IPages.Begin()
		{
			return new PendingSegment ();
		}
			
		Guid IPages.End(IPendingSegment token, int lastPage)
		{
			var ps = (token as PendingSegment);
			var end = ps.End (lastPage);
			lock (this) {
				segments [end.Item1] = end.Item2;
			}
			return end.Item1;
		}

		private PageBlock GetRange(int num)
        {
			lock (this) {
				var t = new PageBlock (cur, cur + num - 1);
				cur = cur + num + WASTE_PAGES_AFTER_EACH_BLOCK;
				return t;
			}
        }

		PageBlock IPages.GetRange(IPendingSegment token)
		{
			var ps = (token as PendingSegment);
            var t = GetRange(PAGES_PER_BLOCK);
			ps.Add (t);

            return t;
		}

	}

}

