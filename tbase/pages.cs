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
#if not

    // header is 4K, regardless of page size.  if page size
    // is >4K, the rest of page 1 is unused.
    //
    // header contains the segment list.  but it's just guids
    // and page numbers.
    //
    // and maybe segments need an age?  and timestamps?
    //
    // segment info list contains block list for each segment.
    // every segment in the current segment list must be in this
    // info list.  but the info list can also have other segments
    // in it, which are in the process of being deleted.  (or we
    // could delete them and just rely on the page mgr to know
    // that it can't give out those pages until the last cursor
    // on that segment is gone.)
    //
    // format of the first page:
    //
    //     magic number
    //     version
    //     flags
    //     page size
    //     block size
    //     etc
    //
    //     address of segment info list (firstPage, lastPage, innerPageSize, lengthInBytes)
    //
    //     current segment list
    //
    // if the current segment list gets too big to fit in the header,
    // then it has to shrink.  each segment entry is 20 bytes.
    // or should the age be in there too?
    // merge some segments to make it smaller.
    //
    // free page list isn't stored.  it's just
    // found when the db is opened by searching all the segments
    // and building a list of which pages aren't used.
    //
    // should we allow a database file to contain more than one
    // keyspace?  each keyspace is a segment list.
    //
    // a segment consists of
    //   its root page
    //   its guid
    //   its age
    //   a list of all its blocks
    //
    // multiple db connections to the same file.  the object
    // representing the state of the file is global to the process,
    // a singleton, shared by all connections thread-safe.
    //
    // page mgr needs to have a list of free blocks/pages, but
    // it's only in RAM.  init on startup.  keep it updated as
    // segments get released.  try to reuse pages as much as
    // possible rather than appending everything to the end of
    // the file and making it larger.
    //
    // need to know about all the cursors currently open for
    // any segment.  we can't release a segment unless we
    // know it has no cursors open.

    public interface ITransaction
    {
        // one you have one of these (the only one of these), you
        // have the write lock.
        //
        // TODO commit should take a dictionary-ish thing as a param
        void Commit()
        {
            // create a btreesegment containing the mem segment
            // prepend the new seg to the seglist
            // release the write lock
        }

        void Rollback()
        {
            // throw away the memory segment, dispose
            // the cursors.  nothing has been written to the
            // disk yet.
            // release the write lock.
        }

        // TODO need a way to get a cursor which includes a dictionary
        // as the first element.  a way to see/query stuff that the tx
        // knows about but has not been committed yet.

    }

    public class db : IPages, ITransaction
    {
        public db(string filename)
        {
            // read segment list from disk
            // read block lists and look for free pages
        }

        private Dictionary<Guid,List<ICursor>> cursors;
        private ICursor getCursorForSegment()
        {
            // all cursors come through here so we can keep an
            // accurate list of which cursors exist.  a cursor
            // is kind of a read lock.  if a cursor exists for
            // a given segment, we can't delete that segment
            // or reuse its pages.
        }

        ITransaction BeginWrite()
        {
            // grab the write lock for this db
            // grab the seglist -- TODO atomically grab the seglist and all its cursors
            // (now no segment in the seglist can be deleted)
            // get a multicursor for that seglist

            // interesting that the only reason to grab a write lock
            // here and hold it is to make sure that when we finally do
            // want to write the seglist, nothing has changed in the
            // meantime.  we don't need a lock to allow the caller
            // to work with the in memory segment.  we don't even
            // need a lock to flush the in memory segment out do
            // a btree segment.  we just don't want to arrive at
            // the end to prepend our new segment to the seglist
            // and then find out that somebody else prepended one
            // before us.  because then we would have to either
            // find conflicts or just fail.
        }

        ICursor BeginRead()
        {
            // grab the seglist
            // (no segment in the seglist can be deleted)
            // get a multicursor for that seglist
        }

        void Work()
        {
            // pick two adjacent segs from the seg list
            // open cursors and each of them
            // merge A and B to create C.  now locks needed.
            // wait for the write lock
            // (with the lock) read the seg list
            // replace A+B with C
            // write the seglist
            // release the lock
            // note that A and B are free to be removed after their cursors are done
            // note that there should be no way to get a cursor on a segment no in the seglist
        }
    }
#endif

	#if true
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

		Tuple<int,int> IPages.GetRange(IPendingSegment token)
		{
			return new Tuple<int,int> (1, -1);
		}

	}
	#endif

	public class SimplePageManager : IPages
	{
		private class PendingSegment : IPendingSegment
		{
			private List<Tuple<int,int>> blockList = new List<Tuple<int, int>>();

			public void Add(Tuple<int,int> t)
			{
				blockList.Add (t);
			}

			public Tuple<Guid,List<Tuple<int,int>>> End(int lastPage)
			{
				var lastBlock = blockList[blockList.Count-1];
				// assert lastPage >= lastBlock.Item1;
				if (lastPage < lastBlock.Item2) {
					// this segment did not use all the pages we gave it
					blockList.Remove (lastBlock);
					blockList.Add (new Tuple<int, int> (lastBlock.Item1, lastPage));

					// TODO
					// if cur has not changed in the meantime, 
					// we could just adjust cur?  otherwise we need to add
					// the unused pages to a free list.
				}
				return new Tuple<Guid,List<Tuple<int,int>>> (Guid.NewGuid (), blockList);
			}
		}

		private readonly Stream fs;
		int cur = 1; // TODO make room for header
		private readonly Dictionary<Guid,List<Tuple<int,int>>> segments;
		int pageSize;

		// TODO could be a param
		const int PAGES_PER_BLOCK = 10; // TODO very low, for testing purposes

		// surprisingly enough, the test suite passes with only ONE page per block.
		// this is still absurd and should probably be disallowed.

		const int WASTE_PAGES_AFTER_EACH_BLOCK = 3; // obviously, for testing purposes only

		public SimplePageManager(Stream _fs, int _pageSize)
		{
			fs = _fs;
			pageSize = _pageSize;
			segments = new Dictionary<Guid, List<Tuple<int, int>>> ();
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

		#if not
        private byte[] buildBlockList(List<Tuple<int,int>> blocks)
        {
            int space = 0;
            for (int i=0; i<blocks.Count; i++) {
                var t = blocks[i];
                space += Zumero.LSM.cs.Varint.SpaceNeededFor(t.Item1);
                space += Zumero.LSM.cs.Varint.SpaceNeededFor(t.Item2);
            }
			space += Zumero.LSM.cs.Varint.SpaceNeededFor(blocks.Count);
			Zumero.LSM.cs.PageBuilder pb = new Zumero.LSM.cs.PageBuilder (space);
			pb.PutVarint (blocks.Count);
			for (int i=0; i<blocks.Count; i++) {
				var t = blocks[i];
				pb.PutVarint (t.Item1);
				pb.PutVarint (t.Item2);
			}
			return pb.Buffer;
        }

        private MemoryStream buildSegmentInfoList(Dictionary<Guid,List<Tuple<int,int>>> sd)
        {
            var ms = new MemoryStream();
            IPages pm = new trivialMemoryPageManager(512);
            var d = new Dictionary<byte[],Stream>();
            foreach (Guid g in sd.Keys) {
                d[g.ToByteArray()] = new MemoryStream(buildBlockList(sd[g]));
            }
			Zumero.LSM.cs.BTreeSegment.Create (ms, pm, d);
            return ms;
        }

		private Tuple<int,int> saveSegmentInfoList(Dictionary<Guid,List<Tuple<int,int>>> sd)
        {
            var ms = buildSegmentInfoList(sd);
			byte[] buf = ms.GetBuffer ();
			int len = (int)ms.Length;
            int pages = len / pageSize;
            if (0 != (len % pageSize)) {
                pages++;
            }
            var range = GetRange(pages);
			Zumero.LSM.cs.utils.SeekPage(fs, pageSize, range.Item1);
			fs.Write (buf, 0, len);
			return range;
        }
		#endif

		Guid IPages.End(IPendingSegment token, int lastPage)
		{
			var ps = (token as PendingSegment);
			var end = ps.End (lastPage);
			lock (this) {
				segments [end.Item1] = end.Item2;
			}
			return end.Item1;
		}

		private Tuple<int,int> GetRange(int num)
        {
			lock (this) {
				var t = new Tuple<int,int> (cur, cur + num - 1);
				cur = cur + num + WASTE_PAGES_AFTER_EACH_BLOCK;
				return t;
			}
        }

		Tuple<int,int> IPages.GetRange(IPendingSegment token)
		{
			var ps = (token as PendingSegment);
            var t = GetRange(PAGES_PER_BLOCK);
			ps.Add (t);

            return t;
		}

	}

}

