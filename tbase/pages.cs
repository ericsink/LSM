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
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;

namespace lsm_tests
{
#if not

    // need to allow multiple db connections to the same file.
    // need a process-global list of files, each with their state.
    //
    // actually, we need multiple db connections to the same
    // file, but we want them all to get the same copy of
    // this class instance, right?  we don't need each one to
    // have their own state.  we want them all to share the
    // same state.

    // ability to "attach" multiple files to a connection?
    // or is that a concept that exists at a higher level like sql?
    //
    public class db
    {
        // for each segment, we'll need a list of its blocks.
        // this'll get written into a segmentinfo page perhaps?
        //
        // need to know about all the cursors currently open for
        // any segment.  we can't release a segment unless we
        // know it has no cursors open.
        //
        // need a file-wide list of all the segments.  once a
        // segment is no longer used in the segment list (which
        // can only happen after Work), then it is eligible to
        // be removed (after it has no cursors left).
        //
        // removing a segment would be adding its pages/blocks
        // back to a free list.  page manager needs to keep track
        // of this and give out freed pages when it can.
        //
        // is there any reason we would need more than one
        // segment list around?  an open multicursor has implicit
        // knowledge of the seglist, but it doesn't need to be
        // stored anywhere, right?  there is no need to be able
        // to open old seglists?  the segment list for an open
        // reader (mc) exists only in ram.

        // the so-called "write lock" could just be a field
        // that stores the current IWrite?

        public db(string filename)
        {
            // read segment list from disk
        }

        ITransaction BeginWrite()
        {
            // ITransaction implements IWrite-ish, with
            // insert and delete, but its constructor takes a
            // cursor which is the snapshot of the seglist when
            // the tx was opened, and its OpenCursor returns a
            // multicursor containing (memsegcursor, seglistmulti)
            //
            // grab the write lock for this db
            // create a new memory segment
            // grab the seglist
            // (no segment in the seglist can be deleted)
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

        // IWrite.OpenCursor needs to snapshot even the dictionary
        // for that tx.  subsequent changes to that dictionary don't
        // affect what a previously allocated cursor sees.

        ICursor BeginRead()
        {
            // grab the seglist
            // (no segment in the seglist can be deleted)
            // get a multicursor for that seglist
        }

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

	public class MemoryPageManager : IPages
	{
		MemoryStream fs;
		int pageSize;

		public MemoryPageManager(MemoryStream _fs, int _pageSize)
		{
			fs = _fs;
			pageSize = _pageSize;
		}

		int IPages.PageSize
		{
			get {
				return pageSize;
			}
		}

		Guid IPages.Begin()
		{
			return Guid.NewGuid();
		}

		void IPages.End(Guid token, int lastPage)
		{
		}

		Tuple<int,int> IPages.GetRange(Guid token)
		{
			return new Tuple<int,int> (1, -1);
		}

	}

	public class SimplePageManager : IPages
	{
		private readonly Stream fs;
		int cur = 1;
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

		Guid IPages.Begin()
		{
			lock (this) {
				Guid token = Guid.NewGuid();
				segments [token] = new List<Tuple<int, int>> ();
				return token;
			}
		}

		void IPages.End(Guid token, int lastPage)
		{
			var blocks = segments [token];
			//Console.WriteLine ("{0} is done", token);
		}

		Tuple<int,int> IPages.GetRange(Guid token)
		{
			lock (this) {
				var t = new Tuple<int,int> (cur, cur + PAGES_PER_BLOCK - 1);
				cur = cur + PAGES_PER_BLOCK + WASTE_PAGES_AFTER_EACH_BLOCK;
				segments [token].Add (t);
				//Console.WriteLine ("{0} gets {1} --> {2}", token, t.Item1, t.Item2);
				return t;
			}
		}

	}

}

