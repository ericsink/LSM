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

namespace Zumero.LSM.cs
{
	using System;
	using System.IO;
	using System.Collections.Generic;

	using Zumero.LSM;

	public static class utils
	{
		public static void SeekPage(Stream fs, int pageSize, int pageNumber)
		{
			if (0 == pageNumber) {
				throw new Exception();
			}
			long pos = (((long) pageNumber) - 1) * pageSize;
			long newpos = fs.Seek (pos, SeekOrigin.Begin);
			if (pos != newpos) {
				throw new Exception();
			}
		}

		// Like Stream.Read, but it loops until it gets all of what it wants.
		public static void ReadFully(Stream s, byte[] buf, int off, int len)
		{
			int sofar = 0;
			while (sofar < len) {
				int got = s.Read (buf, off + sofar, len - sofar);
				if (0 == got) {
                    throw new Exception();
				}
				sofar += got;
			}
		}

		// read until the end of the stream
		public static byte[] ReadAll(Stream s)
		{
			// TODO this code seems to assume s.Position is 0
			byte[] a = new byte[s.Length];
			int sofar = 0;
			while (sofar < a.Length) {
				int got = s.Read (a, sofar, (int) (a.Length - sofar));
				if (0 == got) {
					break; // TODO shouldn't this be an error?
				}
				sofar += got;
			}
			return a;
		}

	}

	public static class Varint
	{
		// http://sqlite.org/src4/doc/trunk/www/varint.wiki

		public static int SpaceNeededFor(long v)
		{
			if (v <= 240) {
				return 1;
			} else if (v <= 2287) {
				return 2;
			} else if (v <= 67823) {
				return 3;
			} else if (v <= 16777215) {
				return 4;
			} else if (v <= 4294967295) {
				return 5;
			} else if (v <= 1099511627775) {
				return 6;
			} else if (v <= 281474976710655) {
				return 7;
			} else if (v <= 72057594037927935) {
				return 8;
			} else {
				return 9;
			}
		}

			
	}

	class ByteComparer : IComparer<byte[]>
	{
		public static int compareWithin(byte[] buf, int bufOffset, int bufLen, byte[] y)
		{
			int n2 = y.Length;
			int len = bufLen<n2 ? bufLen : n2;
			for (var i = 0; i < len; i++)
			{
				var c = buf[i+bufOffset].CompareTo(y[i]);
				if (c != 0)
				{
					return c;
				}
			}

			return bufLen.CompareTo(y.Length);
		}

		public static int cmp(byte[] x, byte[] y)
		{
			int n1 = x.Length;
			int n2 = y.Length;
			int len = n1<n2 ? n1 : n2;
			for (var i = 0; i < len; i++)
			{
				var c = x[i].CompareTo(y[i]);
				if (c != 0)
				{
					return c;
				}
			}

			return x.Length.CompareTo(y.Length);
		}

		public int Compare(byte[] x, byte[] y)
		{
			return cmp(x,y);
		}
	}

	public class PageReader
	{
		private readonly byte[] buf;
		private int cur;

		public PageReader(int pageSize)
		{
			buf = new byte[pageSize];
		}

        public int PageSize
        {
            get
            {
                return buf.Length;
            }
        }

		public void Read(Stream fs)
		{
			utils.ReadFully(fs, buf, 0, buf.Length);
		}

		public void Reset()
		{
			cur = 0;
		}

		public void Skip(int c)
		{
			cur += c;
		}

		public int Compare(int len, byte[] other)
		{
			return ByteComparer.compareWithin (buf, cur, len, other);
		}

		public int Position
		{
			get { return cur; }
		}

		public void SetPosition(int c)
		{
			cur = c;
		}

		public byte PageType
		{
			get {
				return buf [0];
			}
		}

		public byte GetByte()
		{
			return buf [cur++];
		}

		public int GetInt32()
		{
			uint val = buf [cur++];
			val = val << 8 | buf [cur++];
			val = val << 8 | buf [cur++];
			val = val << 8 | buf [cur++];
            // assert fits in 32 bit int
			return (int) val;
		}

		public int GetInt32At(int at)
		{
			uint val = buf [at];
			val = val << 8 | buf [at+1];
			val = val << 8 | buf [at+2];
			val = val << 8 | buf [at+3];
            // assert fits in 32 bit int
			return (int) val;
		}

        public bool CheckPageFlag(byte f)
        {
            return 0 != (buf[1] & f);
        }

        public int GetBoundaryNextPageField()
        {
            return GetInt32At(buf.Length - 4);
        }

		public int GetInt16()
		{
			uint val = buf [cur++];
			val = (uint) (val << 8 | buf [cur++]);
            ushort r2 = (ushort) val;
			return (int) r2;
		}

		public byte[] GetArray(int len)
		{
			byte[] k = new byte[len];
			Array.Copy (buf, cur, k, 0, len);
			cur += len;
			return k;
		}

		public long GetVarint()
		{
			byte a0 = buf [cur++];
            ulong r;
			if (a0 <= 240) {
				r = a0;
			} else if (a0 <= 248) {
				byte a1 = buf [cur++];
				r = (ulong) (240 + 256 * (a0 - 241) + a1);
			} else if (249 == a0) {
				byte a1 = buf [cur++];
				byte a2 = buf [cur++];
				r = (ulong) (2288 + 256 * a1 + a2);
			} else if (250 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				r = v;
			} else if (251 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				r = v;
			} else if (252 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				r = v;
			} else if (253 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				r = v;
			} else if (254 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				r = v;
			} else {
				// assert a0 is 255
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				r = v;
			}
            // assert r fits
            return (long) r;
		}
	}

	public class PageBuilder
	{
		private readonly byte[] buf;
		private int cur;

		public PageBuilder(int pageSize)
		{
			buf = new byte[pageSize];
		}

        public int PageSize
        {
			get {
				return buf.Length;
			}
        }

		public void Reset()
		{
			cur = 0;
		}

		public void Flush(Stream s)
		{
			s.Write (buf, 0, buf.Length);
		}

		public int Position
		{
			get { return cur; }
		}

		public int Available
		{
			get {
				return buf.Length - cur;
			}
		}

		public void PutByte(byte b)
		{
			buf [cur++] = b;
		}

		public void PutInt32(int ov)
		{
            // assert ov >= 0
			uint v = (uint) ov;
			buf[cur++] = (byte)(v >> 24);
			buf[cur++] = (byte)(v >> 16);
			buf[cur++] = (byte)(v >> 8);
			buf[cur++] = (byte)(v >> 0);
		}

		public void PutInt32At(int at, int ov)
		{
            // assert ov >= 0
			uint v = (uint) ov;
			buf[at++] = (byte)(v >> 24);
			buf[at++] = (byte)(v >> 16);
			buf[at++] = (byte)(v >> 8);
			buf[at++] = (byte)(v >> 0);
		}

        public void SetPageFlag(byte f)
        {
            buf[1] |= f;
        }

        public void SetBoundaryNextPageField(int page)
        {
            PutInt32At(buf.Length - 4, page);
        }

		public void PutInt16(ushort ov)
		{
            // assert ov >= 0
			uint v = (uint) ov;
			buf[cur++] = (byte)(v >> 8);
			buf[cur++] = (byte)(v >> 0);
		}

		public void PutInt16At(int at, ushort ov)
		{
            // assert ov >= 0
			uint v = (uint) ov;
			buf[at++] = (byte)(v >> 8);
			buf[at++] = (byte)(v >> 0);
		}

		public void PutVarint(long ov)
		{
            // assert ov >= 0
            ulong v = (ulong) ov;
			if (v <= 240) {
				buf [cur++] = (byte) v;
			} else if (v <= 2287) {
				buf [cur++] = (byte) ((v - 240) / 256 + 241);
				buf [cur++] = (byte) ((v - 240) % 256);
			} else if (v <= 67823) {
				buf [cur++] = 249;
				buf [cur++] = (byte) ((v - 2288) / 256);
				buf [cur++] = (byte) ((v - 2288) % 256);
			} else if (v <= 16777215) {
				buf [cur++] = 250;
				buf[cur++] = (byte)(v >> 16);
				buf[cur++] = (byte)(v >> 8);
				buf[cur++] = (byte)(v >> 0);
			} else if (v <= 4294967295) {
				buf [cur++] = 251;
				buf[cur++] = (byte)(v >> 24);
				buf[cur++] = (byte)(v >> 16);
				buf[cur++] = (byte)(v >> 8);
				buf[cur++] = (byte)(v >> 0);
			} else if (v <= 1099511627775) {
				buf [cur++] = 252;
				buf[cur++] = (byte)(v >> 32);
				buf[cur++] = (byte)(v >> 24);
				buf[cur++] = (byte)(v >> 16);
				buf[cur++] = (byte)(v >> 8);
				buf[cur++] = (byte)(v >> 0);
			} else if (v <= 281474976710655) {
				buf [cur++] = 253;
				buf[cur++] = (byte)(v >> 40);
				buf[cur++] = (byte)(v >> 32);
				buf[cur++] = (byte)(v >> 24);
				buf[cur++] = (byte)(v >> 16);
				buf[cur++] = (byte)(v >> 8);
				buf[cur++] = (byte)(v >> 0);
			} else if (v <= 72057594037927935) {
				buf [cur++] = 254;
				buf[cur++] = (byte)(v >> 48);
				buf[cur++] = (byte)(v >> 40);
				buf[cur++] = (byte)(v >> 32);
				buf[cur++] = (byte)(v >> 24);
				buf[cur++] = (byte)(v >> 16);
				buf[cur++] = (byte)(v >> 8);
				buf[cur++] = (byte)(v >> 0);
			} else {
				buf [cur++] = 255;
				buf[cur++] = (byte)(v >> 56);
				buf[cur++] = (byte)(v >> 48);
				buf[cur++] = (byte)(v >> 40);
				buf[cur++] = (byte)(v >> 32);
				buf[cur++] = (byte)(v >> 24);
				buf[cur++] = (byte)(v >> 16);
				buf[cur++] = (byte)(v >> 8);
				buf[cur++] = (byte)(v >> 0);
			}
		}

		public void PutArray(byte[] ba)
		{
			Array.Copy (ba, 0, buf, cur, ba.Length);
			cur += ba.Length;
		}

		public void PutStream(Stream ba, int num)
		{
			utils.ReadFully (ba, buf, cur, num);
			cur += (int) num;
		}

	}

	public class MemorySegment : IWrite
	{
		private Dictionary<byte[],Stream> pairs = new Dictionary<byte[],Stream>();

		private class myCursor : ICursor
		{
			private readonly byte[][] keys;
			private readonly Dictionary<byte[],Stream> pairs;
			private int cur = -1;

			public myCursor(Dictionary<byte[],Stream> _pairs)
			{
				pairs = _pairs;
				keys = new byte[pairs.Count][];
				pairs.Keys.CopyTo(keys, 0);
				Array.Sort(keys, new ByteComparer());
			}

			bool ICursor.IsValid()
			{
				return (cur >= 0) && (cur < pairs.Count);
			}

			private int search(byte[] k, int min, int max, SeekOp sop)
			{
				int le = -1;
				int ge = -1;
				while (max >= min) {
					int mid = (max + min) / 2;
					byte[] kmid = keys [mid];
					int cmp = ByteComparer.cmp (kmid, k);
					if (0 == cmp) {
						return mid;
					} else if (cmp < 0) {
						le = mid;
						min = mid + 1;
					} else {
						// assert cmp > 0
						ge = mid;
						max = mid - 1;
					}
				}
				if (SeekOp.SEEK_EQ == sop) {
					return -1;
				} else if (SeekOp.SEEK_GE == sop) {
					return ge;
				} else {
					// assert SeekOp.SEEK_LE == sop
					return le;
				}
			}

			void ICursor.Seek(byte[] k, SeekOp sop)
			{
				cur = search (k, 0, pairs.Count - 1, sop);
			}

			void ICursor.First()
			{
				cur = 0;
			}

			void ICursor.Last()
			{
				cur = pairs.Count - 1;
			}

			void ICursor.Next()
			{
				cur++;
			}

			void ICursor.Prev()
			{
				cur--;
			}

			int ICursor.KeyCompare(byte[] k)
			{
				return ByteComparer.cmp ((this as ICursor).Key (), k);
			}

			byte[] ICursor.Key()
			{
				return keys[cur];
			}

			Stream ICursor.Value()
			{
				Stream v = pairs [keys [cur]];
				if (v != null) {
					v.Seek (0, SeekOrigin.Begin);
				}
				return v;
			}

			int ICursor.ValueLength()
			{
				Stream v = pairs[keys[cur]];
				if (null == v) {
					return -1;
				} else {
					return (int) v.Length;
				}
			}
		}

		ICursor IWrite.OpenCursor()
		{
			return new myCursor(pairs);
		}

		void IWrite.Insert(byte[] k, Stream v)
		{
			pairs.Add (k, v);
		}

		void IWrite.Delete(byte[] k)
		{
			pairs.Add (k, null); // tombstone
		}

		public static IWrite Create()
		{
			return new MemorySegment ();
		}
	}

	public class MultiCursor : ICursor
	{
		private enum Direction
		{
			FORWARD,
			BACKWARD,
			WANDERING
		}

		private readonly List<ICursor> subcursors;
		private ICursor cur;
		private Direction dir;

		public static ICursor create(params ICursor[] _subcursors)
		{
			return new MultiCursor(_subcursors);
		}

		public MultiCursor(IEnumerable<ICursor> _subcursors)
		{
			subcursors = new List<ICursor> ();
			foreach (var oc in _subcursors)
			{
				subcursors.Add(oc);
			}
		}

		bool ICursor.IsValid()
		{
			return (cur != null) && cur.IsValid ();
		}

		private ICursor find(Func<int,bool> f)
		{
			ICursor cur = null;
			byte[] kcur = null;
			for (int i = 0; i < subcursors.Count; i++) {
				ICursor csr = subcursors [i];
				if (!csr.IsValid())
				{
					continue;
				}
				if (null == cur) {
					cur = csr;
					kcur = csr.Key ();
				} else {
					int cmp = csr.KeyCompare (kcur);
					if (f(cmp)) {
						cur = csr;
						kcur = csr.Key ();
					}
				}
			}
			return cur;
		}

		private ICursor findMin()
		{
			return find (x => (x < 0));
		}

		private ICursor findMax()
		{
			return find (x => (x > 0));
		}

		void ICursor.Seek(byte[] k, SeekOp sop)
		{
			cur = null;
			for (int i = 0; i < subcursors.Count; i++) {
				ICursor csr = subcursors [i];
				csr.Seek (k, sop);
				if (csr.IsValid() && ( (SeekOp.SEEK_EQ == sop) || (0 == csr.KeyCompare (k)) ) ) {
					cur = csr;
					break;
				}
			}

			dir = Direction.WANDERING;

			if (null == cur) {
				if (SeekOp.SEEK_GE == sop) {
					cur = findMin ();
					if (null != cur) {
						dir = Direction.FORWARD;
					}
				} else if (SeekOp.SEEK_LE == sop) {
					cur = findMax ();
					if (null != cur) {
						dir = Direction.BACKWARD;
					}
				}
			}
		}

		void ICursor.First()
		{
			for (int i = 0; i < subcursors.Count; i++) {
				ICursor csr = subcursors [i];
				csr.First();
			}
			cur = findMin ();
			dir = Direction.FORWARD;
		}

		void ICursor.Last()
		{
			for (int i = 0; i < subcursors.Count; i++) {
				ICursor csr = subcursors [i];
				csr.Last();
			}
			cur = findMax ();
			dir = Direction.BACKWARD;
		}

		byte[] ICursor.Key()
		{
			return cur.Key ();
		}

		int ICursor.KeyCompare(byte[] k)
		{
			return cur.KeyCompare (k);
		}

		Stream ICursor.Value()
		{
			return cur.Value ();
		}

		int ICursor.ValueLength()
		{
			return cur.ValueLength ();
		}

		void ICursor.Next()
		{
			byte[] k = cur.Key ();

			for (int i = 0; i < subcursors.Count; i++) {
				if ((dir != Direction.FORWARD) && (cur != subcursors[i])) {
					subcursors [i].Seek (k, SeekOp.SEEK_GE);
				}
				if (subcursors [i].IsValid ()) {
					if (0 == subcursors [i].KeyCompare (k)) {
						subcursors [i].Next ();
					}
				}
			}

			cur = findMin ();
			dir = Direction.FORWARD;
		}

		void ICursor.Prev()
		{
			byte[] k = cur.Key ();

			for (int i = 0; i < subcursors.Count; i++) {
				if ((dir != Direction.BACKWARD) && (cur != subcursors[i])) {
					subcursors [i].Seek (k, SeekOp.SEEK_LE);
				}
				if (subcursors [i].IsValid ()) {
					if (0 == subcursors [i].KeyCompare (k)) {
						subcursors [i].Prev ();
					}
				}
			}

			cur = findMax ();
			dir = Direction.BACKWARD;
		}

	}

	public class LivingCursor : ICursor
	{
		private readonly ICursor chain;

		public LivingCursor(ICursor _chain)
		{
			chain = _chain;
		}

		bool ICursor.IsValid()
		{
			return chain.IsValid () && (chain.ValueLength() >= 0);
		}

		private void skipTombstonesForward()
		{
			while (chain.IsValid() && (chain.ValueLength () < 0)) {
				chain.Next ();
			}
		}

		private void skipTombstonesBackward()
		{
			while (chain.IsValid() && (chain.ValueLength () < 0)) {
				chain.Prev ();
			}
		}

		void ICursor.Seek(byte[] k, SeekOp sop)
		{
			chain.Seek (k, sop);
			if (SeekOp.SEEK_GE == sop) {
				skipTombstonesForward ();
			} else if (SeekOp.SEEK_LE == sop) {
				skipTombstonesBackward ();
			}
		}

		void ICursor.First()
		{
			chain.First ();
			skipTombstonesForward ();
		}

		void ICursor.Last()
		{
			chain.Last ();
			skipTombstonesBackward ();
		}

		byte[] ICursor.Key()
		{
			return chain.Key ();
		}

		int ICursor.KeyCompare(byte[] k)
		{
			return chain.KeyCompare (k);
		}

		Stream ICursor.Value()
		{
			return chain.Value ();
		}

		int ICursor.ValueLength()
		{
			return chain.ValueLength ();
		}

		void ICursor.Next()
		{
			chain.Next ();
			skipTombstonesForward ();
		}

		void ICursor.Prev()
		{
			chain.Prev ();
			skipTombstonesBackward ();
		}

	}

	public static class BTreeSegment
	{
		private const byte LEAF_NODE = 1;
		private const byte PARENT_NODE = 2;
		private const byte OVERFLOW_NODE = 3;

        // flags on values
		private const byte FLAG_OVERFLOW = 1;
		private const byte FLAG_TOMBSTONE = 2;

        // flags on pages
		private const byte FLAG_ROOT_NODE = 1;
		private const byte FLAG_BOUNDARY_NODE = 2;

		private const int OVERFLOW_PAGE_HEADER_SIZE = 6;

		private const int PARENT_NODE_HEADER_SIZE = 8;

		private const int LEAF_HEADER_SIZE = 8;
		private const int OFFSET_COUNT_PAIRS = 6;

		private class node
		{
			public int PageNumber;
			public byte[] Key;
		}

		private static void putArrayWithLength(PageBuilder pb, byte[] ba)
		{
			if (null == ba) {
				pb.PutByte(FLAG_TOMBSTONE);
				pb.PutVarint(0);
			} else {
				pb.PutByte (0);
				pb.PutVarint (ba.Length);
				pb.PutArray (ba);
			}
		}

		private static void putStreamWithLength(PageBuilder pb, Stream ba)
		{
			if (null == ba) {
				pb.PutByte(FLAG_TOMBSTONE);
				pb.PutVarint(0);
			} else {
				pb.PutByte (0);
				pb.PutVarint (ba.Length);
				pb.PutStream (ba, (int) ba.Length);
			}
		}

		private static void buildParentPage(byte flags, int firstLeaf, int lastLeaf, Dictionary<int,int> overflows, PageBuilder pb, List<node> children, int stop, int start)
		{
			// assert stop >= start
			int countKeys = (stop - start); 

			pb.Reset ();
			pb.PutByte (PARENT_NODE);
			pb.PutByte (flags);

			pb.PutInt16 ((ushort) countKeys);

			if (0 != (flags & FLAG_ROOT_NODE)) {
				pb.PutInt32 (firstLeaf);
				pb.PutInt32 (lastLeaf);
			}

			// store all the pointers (n+1 of them).  
			// note q<=stop below
			for (int q = start; q <= stop; q++) {
				pb.PutVarint(children[q].PageNumber);
			}

			// now store the keys (n) of them.
			// note q<stop below
			for (int q = start; q < stop; q++) {
				byte[] k = children [q].Key;

				if ((overflows != null) && overflows.ContainsKey (q)) {
					pb.PutByte(FLAG_OVERFLOW); // means overflow
					pb.PutVarint(k.Length);
					pb.PutInt32 (overflows[q]);
				} else {
					putArrayWithLength (pb, k);
				}
			}
		}

		/*
		 * Each overflow page, after 1 byte for the page type and 1 byte for flags,
		 * has a 32-bit int which is the number of pages left in this overflow value.
		 * 
		 * It would be nice to make this a varint, but that would be problematic.
		 * We need to know in advance how many pages the value will consume.
		 */

		private static int countOverflowPagesFor(int pageSize, int len)
		{
			int bytesPerPage = pageSize - OVERFLOW_PAGE_HEADER_SIZE;
			int needed = len / bytesPerPage;
			if ((len % bytesPerPage) != 0) {
				needed++;
			}
			return needed;
		}

		private static int writeOverflowFromArray(IPages pageManager, int nextPageNumber, int boundaryPageNumber, PageBuilder pb, Stream fs, byte[] ba)
		{
			return writeOverflowFromStream (pageManager, nextPageNumber, boundaryPageNumber, pb, fs, new MemoryStream (ba));
		}

		private static int writeOverflowFromStream(IPages pageManager, int nextPageNumber, int boundaryPageNumber, PageBuilder pb, Stream fs, Stream ba)
		{
			int sofar = 0;
			int needed = countOverflowPagesFor (pb.PageSize, (int) ba.Length);

            // TODO fix this to do boundary stuff
			int count = 0;
			while (sofar < ba.Length) {
				pb.Reset ();
				pb.PutByte (OVERFLOW_NODE);
				pb.PutByte (0);
				pb.PutInt32 (needed - count);
				int num = Math.Min ((pb.PageSize - OVERFLOW_PAGE_HEADER_SIZE), (int) (ba.Length - sofar));
				pb.PutStream (ba, num);
				sofar += num;
				pb.Flush (fs);
				count++;
			}
            return nextPageNumber + count;
		}

		private static int calcAvailable(int pageSize, int currentSize, bool couldBeRoot, bool isBoundary)
		{
			int n = (pageSize - currentSize);
			if (couldBeRoot)
			{
				// make space for the firstLeaf and lastLeaf fields
				n -= (2 * sizeof(int));
			}
			if (isBoundary) 
			{
				// make space for the nextPage field on a boundary page
				n -= sizeof(int);
			}
			return n;
		}

		private static List<node> writeParentNodes(int firstLeaf, int lastLeaf, List<node> children, IPages pageManager, int startingPageNumber, int boundaryPageNumber, Stream fs, PageBuilder pb, PageBuilder pbOverflow)
		{
			int nextPageNumber = startingPageNumber;
			var nextGeneration = new List<node> ();

			int sofar = 0;
			Dictionary<int,int> overflows = new Dictionary<int, int> ();
			int first = 0;

			// assert children.Count > 1
			for (int i = 0; i < children.Count; i++) {
				node n = children [i];
				byte[] k = n.Key;

				int neededForInline = 1 
					+ Varint.SpaceNeededFor (k.Length) 
					+ k.Length 
					+ Varint.SpaceNeededFor (n.PageNumber);

				int neededForOverflow = 1 
					+ Varint.SpaceNeededFor (k.Length) 
					+ sizeof(int)
					+ Varint.SpaceNeededFor (n.PageNumber);

				bool isLastChild = (i == (children.Count - 1));

				if (sofar > 0) {
					var flushThisPage = false;
					var isBoundary = (nextPageNumber == boundaryPageNumber);
                    var couldBeRoot = (nextGeneration.Count == 0);
                    var avail = calcAvailable(pb.PageSize, sofar, couldBeRoot, isBoundary);
					if (isLastChild) {
						flushThisPage = true;
					} else if (avail >= neededForInline) {
						// no problem.
					} else if ((pb.PageSize - PARENT_NODE_HEADER_SIZE) >= neededForInline) {
						// it won't fit here, but it would fully fit on the next page.
						flushThisPage = true;
					} else if (avail < neededForOverflow) {
						// we can't even put this key in this page if we overflow it.
						flushThisPage = true;
					}

					bool isRootNode = false;
					if (isLastChild && couldBeRoot) {
						isRootNode = true;
					}

					if (flushThisPage) {
						int thisPageNumber = nextPageNumber;

						byte flags = isRootNode ? FLAG_ROOT_NODE : (isBoundary ? FLAG_BOUNDARY_NODE : (byte) 0);

						buildParentPage (flags, firstLeaf, lastLeaf, overflows, pb, children, i, first);

						if (!isRootNode) {
							if (isBoundary) {
								// TODO ask for next range
                                pb.SetBoundaryNextPageField(0); // TODO
							} else {
								nextPageNumber++;
							}
						}

						pb.Flush (fs);

						nextGeneration.Add (new node {PageNumber = thisPageNumber, Key=children[i-1].Key});

						sofar = 0;
						first = 0;
						overflows.Clear();
					}
				}

				if (!isLastChild) {
					if (0 == sofar) {
						first = i;
						overflows.Clear();

						sofar += 2; // for the page type and the flags
						sofar += 2; // for the stored count
						sofar += 5; // for the extra pointer we'll add at the end, which is a varint, so 5 is the worst case
					}
						
					bool isBoundary = (nextPageNumber == boundaryPageNumber);
					if (calcAvailable(pb.PageSize, sofar, (nextGeneration.Count == 0), isBoundary) >= neededForInline) {
						sofar += k.Length;
					} else {
						int keyOverflowFirstPage = nextPageNumber;
						nextPageNumber = writeOverflowFromArray (pageManager, nextPageNumber, boundaryPageNumber, pbOverflow, fs, k);
						sofar += sizeof(int);
						overflows [i] = keyOverflowFirstPage;
					}

					// inline or not, we need space for the following things

					sofar++; // for the flag
					sofar += Varint.SpaceNeededFor((int) k.Length);
					sofar += Varint.SpaceNeededFor(n.PageNumber);
				}
			}

			// assert cur is null

			return nextGeneration;
		}

		public static int Create(Stream fs, int pageSize, IPages pageManager, ICursor csr)
		{
			// TODO if !(fs.CanSeek()) throw?
			PageBuilder pb = new PageBuilder(pageSize);
			PageBuilder pbOverflow = new PageBuilder(pageSize);

            var range = pageManager.GetRange();
			int nextPageNumber = range.Item1;
            int boundaryPageNumber = range.Item2;

			var nodelist = new List<node> ();

			ushort countPairs = 0;
			byte[] lastKey = null;

			int prevPageNumber = 0;

			csr.First ();
			while (csr.IsValid ()) {
				byte[] k = csr.Key ();
				Stream v = csr.Value ();

				// TODO get vlen here and don't call v.Length so much

				// assert k != null
				// for a tombstone, v might be null

				var neededForOverflowPageNumber = sizeof(int);
				var neededForKeyBase = 1 + Varint.SpaceNeededFor(k.Length);
				var neededForKeyInline = neededForKeyBase + k.Length;
				var neededForKeyOverflow = neededForKeyBase + neededForOverflowPageNumber;

				var neededForValueInline = 1 + ((v!=null) ? Varint.SpaceNeededFor(v.Length) + v.Length : 0);
				var neededForValueOverflow = 1 + ((v!=null) ? Varint.SpaceNeededFor(v.Length) + neededForOverflowPageNumber : 0);

				var neededForBothInline = neededForKeyInline + neededForValueInline;
				var neededForKeyInlineValueOverflow = neededForKeyInline + neededForValueOverflow;
				var neededForOverflowBoth = neededForKeyOverflow + neededForValueOverflow;

				csr.Next ();

				if (pb.Position > 0) {
					// figure out if we need to just flush this page

					int avail = pb.Available - ((nextPageNumber == boundaryPageNumber) ? 4 : 0);

					bool flushThisPage = false;
					if (avail >= neededForBothInline) {
						// no problem.  both the key and the value are going to fit
					} else if ((pb.PageSize - LEAF_HEADER_SIZE) >= neededForBothInline) {
						// it won't fit here, but it would fully fit on the next page.
						flushThisPage = true;
					} else if (avail >= neededForKeyInlineValueOverflow) {
						// the key will fit inline if we just overflow the val
					} else if (avail < neededForOverflowBoth) {
						// we can't even put this pair in this page if we overflow both.
						flushThisPage = true;
					}

					if (flushThisPage) {
						// note that this code is duplicated with slight differences below, after the loop

						int thisPageNumber = nextPageNumber;

						// assert -- it is not possible for this to be the last leaf.  so, at
						// this point in the code, we can be certain that there is going to be
						// another page.

						if (thisPageNumber == boundaryPageNumber) {
                            pb.SetPageFlag(FLAG_BOUNDARY_NODE);
                            // TODO ask for another range
                            pb.SetBoundaryNextPageField(0); // TODO
						} else {
							nextPageNumber++;
						}

						// now that we know how many pairs are in this page, we can write that out
						pb.PutInt16At (OFFSET_COUNT_PAIRS, countPairs);

						pb.Flush (fs);

						nodelist.Add (new node { PageNumber = thisPageNumber, Key = lastKey });

						prevPageNumber = thisPageNumber;
						pb.Reset ();
						countPairs = 0;
						lastKey = null;
					}
				}

				if (pb.Position == 0) {
					// we could be here because we just flushed the page.
					// or because this is the very first page.
					countPairs = 0;
					lastKey = null;

					// 8 byte header

					pb.PutByte(LEAF_NODE);
					pb.PutByte(0); // flags

					pb.PutInt32 (prevPageNumber); // prev page num.
					pb.PutInt16 (0); // number of pairs in this page. zero for now. written at end.
				} 

				/*
				 * one of the following cases must now be true:
				 * 
				 * - both the key and val will fit
				 * - key inline and overflow the val
				 * - overflow both
				 * 
				 * note that we don't care about the case where the
				 * val would fit if we overflowed the key.  if the key
				 * needs to be overflowed, then we're going to overflow
				 * the val as well, even if it would fit.
				 * 
				 * if bumping to the next page would help, we have
				 * already done it above.
				 * 
				 */

				int available = pb.Available - ((nextPageNumber == boundaryPageNumber) ? 4 : 0);
				if (available >= neededForBothInline) {
					// no problem.  both the key and the value are going to fit
					putArrayWithLength (pb, k);
					putStreamWithLength (pb, v);
				} else {
					if (available >= neededForKeyInlineValueOverflow) {
						// the key will fit inline if we just overflow the val
						putArrayWithLength (pb, k);
					} else {
						// assert available >= needed_for_overflow_both

						int keyOverflowFirstPage = nextPageNumber;
						nextPageNumber = writeOverflowFromArray (pageManager, nextPageNumber, boundaryPageNumber, pbOverflow, fs, k);

						pb.PutByte (FLAG_OVERFLOW);
						pb.PutVarint (k.Length);
						pb.PutInt32 (keyOverflowFirstPage);
					}

					int valueOverflowFirstPage = nextPageNumber;
					nextPageNumber = writeOverflowFromStream (pageManager, nextPageNumber, boundaryPageNumber, pbOverflow, fs, v);

					pb.PutByte (FLAG_OVERFLOW);
					pb.PutVarint (v.Length);
					pb.PutInt32 (valueOverflowFirstPage);
				}

				lastKey = k;
				countPairs++;
			}

			if (pb.Position > 0) {
				// note that this code is duplicated with slight differences from above

				int thisPageNumber = nextPageNumber;

				if (!csr.IsValid () && (0 == nodelist.Count)) {
					// this is the last page.  the only page.  the root page.
					// even though it's a leaf.
				} else {
					if (thisPageNumber == boundaryPageNumber) {
                        pb.SetPageFlag(FLAG_BOUNDARY_NODE);
                        // TODO ask for another range
                        pb.SetBoundaryNextPageField(0); // TODO
					} else {
						nextPageNumber++;
					}
				}

				// now that we know how many pairs are in this page, we can write that out
				pb.PutInt16At (OFFSET_COUNT_PAIRS, countPairs);

				pb.Flush (fs);

				nodelist.Add (new node { PageNumber = thisPageNumber, Key = lastKey });
			}

			if (nodelist.Count > 0) {
				int firstLeaf = nodelist [0].PageNumber;
				int lastLeaf = nodelist [nodelist.Count - 1].PageNumber;

				// now write the parent pages, maybe more than one level of them.  we have to get
				// down to a level with just one parent page in it, the root page.

				while (nodelist.Count > 1) {
					nodelist = writeParentNodes (firstLeaf, lastLeaf, nodelist, pageManager, nextPageNumber, boundaryPageNumber, fs, pb, pbOverflow);
					nextPageNumber += nodelist.Count; // TODO this is wrong
				}

				// assert nodelist.Count == 1

				return nodelist [0].PageNumber;
			} else {
				return 0;
			}
		}

		private class myOverflowReadStream : Stream
		{
			private readonly Stream fs;
			private readonly int len;
			private int sofarOverall;
			private int sofarThisPage;
			private int currentPage;
			private readonly byte[] buf;

			// TODO consider supporting seek

			public myOverflowReadStream(Stream _fs, int pageSize, int firstPage, int _len)
			{
				fs = _fs;
				len = _len;

				currentPage = firstPage;

                buf = new byte[pageSize];
				ReadPage();
			}

			public override long Length {
				get {
					return len;
				}
			}

			private void ReadPage()
			{
                utils.SeekPage(fs, buf.Length, currentPage);
				utils.ReadFully (fs, buf, 0, buf.Length);
				// assert buf[0] == OVERFLOW
				sofarThisPage = 0;
			}

			public override bool CanRead {
				get {
					return sofarOverall < len;
				}
			}

			public override int Read (byte[] ba, int offset, int wanted)
			{
				if (sofarThisPage >= (buf.Length - OVERFLOW_PAGE_HEADER_SIZE)) {
					if (sofarOverall < len) {
						currentPage++;
						ReadPage ();
					} else {
						return 0;
					}
				}

				int available = (int) Math.Min ((buf.Length - OVERFLOW_PAGE_HEADER_SIZE), len - sofarOverall);
				int num = (int)Math.Min (available, wanted);
				Array.Copy (buf, OVERFLOW_PAGE_HEADER_SIZE + sofarThisPage, ba, offset, num);
				sofarOverall += num;
				sofarThisPage += num;

				return num;
			}

			public override bool CanWrite {
				get {
					return false;
				}
			}

			public override bool CanSeek {
				get {
					return false;
				}
			}

			public override long Position {
				get {
					throw new NotSupportedException ();
				}
				set {
					throw new NotSupportedException ();
				}
			}

			public override void SetLength (long value)
			{
				throw new NotSupportedException ();
			}

			public override void Flush ()
			{
				throw new NotSupportedException ();
			}

			public override long Seek (long offset, SeekOrigin origin)
			{
				throw new NotSupportedException ();
			}

			public override void Write (byte[] buffer, int offset, int count)
			{
				throw new NotSupportedException ();
			}
		}

		private static byte[] readOverflow(int len, Stream fs, int pageSize, int firstPage)
		{
			var ostrm = new myOverflowReadStream (fs, pageSize, firstPage, len);
			return utils.ReadAll (ostrm);
		}

		private class myCursor : ICursor
		{
			private readonly Stream fs;
			private readonly int rootPage;
			private readonly int firstLeaf;
			private readonly int lastLeaf;
			private readonly PageReader pr;

			private int currentPage = 0;

			private int[] leafKeys;
			private int previousLeaf;
			private int currentKey;

			public myCursor(Stream _fs, int pageSize, int _rootPage)
			{
				// TODO if !(strm.CanSeek()) throw?
				rootPage = _rootPage;
				fs = _fs;
                pr = new PageReader(pageSize);
				if (!setCurrentPage(rootPage)) {
					throw new Exception();
				}
				if (pr.PageType == LEAF_NODE) {
					firstLeaf = lastLeaf = rootPage;
				} else if (pr.PageType == PARENT_NODE) {
					pr.Reset ();
					if (pr.GetByte() != PARENT_NODE) {
						throw new Exception ();
					}
					byte pflag = pr.GetByte ();
					pr.Skip (sizeof(ushort));

					if (0 == (pflag & FLAG_ROOT_NODE)) {
						throw new Exception ();
					}
					firstLeaf = pr.GetInt32 ();
					lastLeaf = pr.GetInt32 ();
				}
				else {
					throw new Exception();
				}
			}

			private void resetLeaf()
			{
				leafKeys = null;
				previousLeaf = 0;
				currentKey = -1;
			}

			private bool nextInLeaf()
			{
				if ((currentKey + 1) < leafKeys.Length) {
					currentKey++;
					return true;
				} else {
					return false;
				}
			}

			private bool prevInLeaf()
			{
				if (currentKey > 0) {
					currentKey--;
					return true;
				} else {
					return false;
				}
			}

            private void skipKey()
            {
                byte kflag = pr.GetByte();
                int klen = (int) pr.GetVarint ();
                if (0 == (kflag & FLAG_OVERFLOW)) {
                    pr.Skip (klen);
                } else {
                    pr.Skip(sizeof(int));
                }
            }

			private void readLeaf()
			{
				resetLeaf ();
				pr.Reset ();
				if (pr.GetByte() != LEAF_NODE) { // TODO page_type()
					// TODO or, we could just return, and leave things in !valid() state
					throw new Exception ();
				}
				pr.GetByte (); // TODO pflag
				previousLeaf = pr.GetInt32 ();
				int count = pr.GetInt16 ();
				// TODO in the fs version, leafKeys is only reallocated when it is too small
				leafKeys = new int[count];
				for (int i = 0; i < count; i++) {
					leafKeys [i] = pr.Position;

                    skipKey();

					// TODO in the fs version, this is a func called skipValue
					// need to skip the val
					byte vflag = pr.GetByte();
					int vlen = (int) pr.GetVarint();
					if (0 != (vflag & FLAG_TOMBSTONE)) {
						// assert vlen is 0
					} else if (0 != (vflag & FLAG_OVERFLOW)) {
						// this is an overflow key.  ignore it.
						// just skip past its pagenum.  
                        pr.Skip(sizeof(int));
					} else {
						pr.Skip(vlen);
					}
				}
			}

			private int compareKeyInLeaf(int n, byte[] other)
			{
				pr.SetPosition(leafKeys [n]);
				byte kflag = pr.GetByte();
				int klen = (int) pr.GetVarint();
				if (0 == (kflag & FLAG_OVERFLOW)) {
					return pr.Compare (klen, other);
				} else {
					// TODO need to cmp the given key against an overflowed
					// key.  for now, we just retrieve the overflowed key
					// and compare it.  but this comparison could be done
					// without retrieving the whole thing.
					int pagenum = pr.GetInt32 ();
					byte[] k = readOverflow(klen, fs, pr.PageSize, pagenum);
					return ByteComparer.cmp (k, other);
				}
			}

			private byte[] keyInLeaf(int n)
			{
				pr.SetPosition(leafKeys [n]);
				byte kflag = pr.GetByte();
				int klen = (int) pr.GetVarint();
				if (0 == (kflag & FLAG_OVERFLOW)) {
					return pr.GetArray (klen);
				} else {
					int pagenum = pr.GetInt32 ();
					return readOverflow(klen, fs, pr.PageSize, pagenum);
				}
			}

			private int searchLeaf(byte[] k, int min, int max, SeekOp sop)
			{
				int le = -1;
				int ge = -1;
				while (max >= min) {
					int mid = (max + min) / 2;
					int cmp = compareKeyInLeaf (mid, k);
					if (0 == cmp) {
						return mid;
					} else if (cmp < 0) {
						le = mid;
						min = mid + 1;
					} else {
						// assert cmp > 0
						ge = mid;
						max = mid - 1;
					}
				}
				if (SeekOp.SEEK_EQ == sop) {
					return -1;
				} else if (SeekOp.SEEK_GE == sop) {
					return ge;
				} else {
					// assert SeekOp.SEEK_LE == sop
					return le;
				}
			}

			private bool setCurrentPage(int pagenum)
			{
				// TODO if pagenum == currentPage, do nothing?
				currentPage = pagenum;
				resetLeaf();
				// TODO this guard doesn't do much anymore.  specifically,
				// in a single-file design, we don't really have any way of
				// checking to see if this pagenum is actually part of the
				// segment.
				if (0 == pagenum) {
					return false;
				}
				if (pagenum <= rootPage) {
                    utils.SeekPage(fs, pr.PageSize, pagenum);
					pr.Read (fs);
					return true;
				} else {
					// TODO does this actually ever happen?
					return false;
				}
			}

			private Tuple<int[],byte[][]> readParentPage()
			{
				pr.Reset ();
				if (pr.GetByte() != PARENT_NODE) {
					throw new Exception ();
				}
				byte pflag = pr.GetByte ();
				int count = (int) pr.GetInt16 ();
				var ptrs = new int[count+1];
				var keys = new byte[count][];

				if (0 != (pflag & FLAG_ROOT_NODE)) {
					pr.Skip (2 * sizeof(int));
				}
				// note "<= count" below
				for (int i = 0; i <= count; i++) {
					ptrs[i] = (int) pr.GetVarint();
				}
				// note "< count" below
				for (int i = 0; i < count; i++) {
					byte flag = pr.GetByte();
					int klen = (int) pr.GetVarint();
					if (0 == (flag & FLAG_OVERFLOW)) {
						keys[i] = pr.GetArray(klen);
					} else {
						int pagenum = pr.GetInt32 ();
						keys[i] = readOverflow (klen, fs, pr.PageSize, pagenum);
					}
				}
				return new Tuple<int[],byte[][]> (ptrs, keys);
			}

			// this is used when moving forward through the leaf pages.
			// we need to skip any overflow pages.  when moving backward,
			// this is not necessary, because each leaf has a pointer to
			// the leaf before it.
            // TODO this will go away
			private bool searchForwardForLeaf()
			{
				while (true) {
					if (LEAF_NODE == pr.PageType) {
						return true;
					}
					else if (PARENT_NODE == pr.PageType) {
						// if we bump into a parent node, that means there are
						// no more leaves.
						return false;
					}
					else {
						// assert OVERFLOW == _buf[0]
						pr.SetPosition (2); // offset of the pages_remaining
						int skip = pr.GetInt32 ();
                        // TODO this code assumes the pages for this overflow
                        // are contiguous, and it assumes that the next page
                        // in this segment is immediately after this overflow.
						if (!setCurrentPage (currentPage + skip)) {
							return false;
						}
					}
				}
			}

			bool ICursor.IsValid()
			{
				// TODO curpagenum >= 1 ?
				return (leafKeys != null) && (leafKeys.Length > 0) && (currentKey >= 0) && (currentKey < leafKeys.Length);
			}

			byte[] ICursor.Key()
			{
				return keyInLeaf(currentKey);
			}

			Stream ICursor.Value()
			{
				pr.SetPosition(leafKeys [currentKey]);

                skipKey();

				// read the val

				byte vflag = pr.GetByte();
				int vlen = (int) pr.GetVarint();
				if (0 != (vflag & FLAG_TOMBSTONE)) {
					return null;
				} else if (0 != (vflag & FLAG_OVERFLOW)) {
					int pagenum = pr.GetInt32 ();
					return new myOverflowReadStream (fs, pr.PageSize, pagenum, vlen);
				} else {
					return new MemoryStream(pr.GetArray (vlen));
				}
			}

			int ICursor.ValueLength()
			{
				pr.SetPosition(leafKeys [currentKey]);

                skipKey();

				// read the val

				byte vflag = pr.GetByte();
				if (0 != (vflag & FLAG_TOMBSTONE)) {
					return -1;
				}
				int vlen = (int) pr.GetVarint();
				return vlen;
			}

			int ICursor.KeyCompare(byte[] k)
			{
				return compareKeyInLeaf (currentKey, k);
			}

			void ICursor.Seek(byte[] k, SeekOp sop)
			{
				// start at the last page, which is always the root of the tree.  
				// it might be the leaf, in a tree with just one node.

				int pagenum = rootPage;

				while (true) {
					if (!setCurrentPage (pagenum)) {
						break;
					}

					if (pr.PageType == LEAF_NODE) {
						readLeaf ();

						currentKey = searchLeaf (k, 0, leafKeys.Length - 1, sop);

						if (SeekOp.SEEK_EQ == sop) {
							break; // once we get to a leaf, we're done, whether the key was found or not
						} else {
							ICursor me = this as ICursor;
							if (me.IsValid ()) {
								break;
							} else {
								// if LE or GE failed on a given page, we might need
								// to look at the next/prev leaf.
								if (SeekOp.SEEK_GE == sop) {
                                    int nextPage;
									if (pr.CheckPageFlag(FLAG_BOUNDARY_NODE)) {
                                        nextPage = pr.GetBoundaryNextPageField();
                                    } else {
                                        nextPage = currentPage + 1;
                                    }
									if (setCurrentPage (nextPage) && searchForwardForLeaf ()) {
										readLeaf ();
										currentKey = 0;
									} else {
										break;
									}
								} else {
									// assert SeekOp.SEEK_LE == sop
									if (0 == previousLeaf) {
										resetLeaf(); // TODO probably not needed?
										break;
									} else if (setCurrentPage(previousLeaf)) {
										readLeaf ();
										currentKey = leafKeys.Length - 1;
									}
								}
							}
						}

					} else if (pr.PageType == PARENT_NODE) {
						Tuple<int[],byte[][]> tp = readParentPage ();
						var ptrs = tp.Item1;
						var keys = tp.Item2;

						// TODO is linear search here the fastest way?
						int found = 0;
						for (int i = 0; i < keys.Length; i++) {
							int cmp = ByteComparer.cmp (k, keys [i]);
							if (cmp <= 0) {
								found = ptrs [i];
								break;
							}
						}
						if (found == 0) {
							found = ptrs [ptrs.Length - 1];
						}
						pagenum = found;
					}
				}
			}

			void ICursor.First()
			{
				if (setCurrentPage (firstLeaf)) {
					readLeaf ();
					currentKey = 0;
				}
			}

			void ICursor.Last()
			{
				if (setCurrentPage (lastLeaf)) {
					readLeaf ();
					currentKey = leafKeys.Length - 1;
				}
			}

			void ICursor.Next()
			{
				if (!nextInLeaf()) {
					// need a new page
                    int nextPage;
                    if (pr.CheckPageFlag(FLAG_BOUNDARY_NODE)) {
                        nextPage = pr.GetBoundaryNextPageField();
                    } else {
                        nextPage = currentPage + 1;
                    }
					if (setCurrentPage (nextPage) && searchForwardForLeaf ()) {
						readLeaf ();
						currentKey = 0;
					}
				}
			}

			void ICursor.Prev()
			{
				if (!prevInLeaf()) {
					// need a new page
					if (0 == previousLeaf) {
						resetLeaf();
					} else if (setCurrentPage(previousLeaf)) {
						readLeaf ();
						currentKey = leafKeys.Length - 1;
					}
				}
			}

		}

		public static ICursor OpenCursor(Stream fs, int pageSize, int rootPage)
		{
			return new myCursor(fs, pageSize, rootPage);
		}

	}

}
