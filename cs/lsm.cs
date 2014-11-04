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
			byte[] a = new byte[(int) (s.Length - s.Position)];
			int sofar = 0;
			while (sofar < a.Length) {
				int got = s.Read (a, sofar, (int) (a.Length - sofar));
				if (0 == got) {
                    throw new Exception();
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

        public int GetSecondToLastInt32()
        {
            return GetInt32At(buf.Length - 8);
        }

        public int GetLastInt32()
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

		public byte[] Buffer
		{
			get {
				return buf;
			}
		}

		public void Reset()
		{
			cur = 0;
		}

		public void Write(Stream s)
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

		private void PutInt32At(int at, int ov)
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

        public void SetSecondToLastInt32(int page)
        {
            if (cur > buf.Length - 8) {
				throw new Exception();
            }
            PutInt32At(buf.Length - 8, page);
        }

        public void SetLastInt32(int page)
        {
            if (cur > buf.Length - 4) {
				throw new Exception();
            }
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

#if not
	public class MemorySegment : IWrite
	{
		#if not
		private class ByteArrayComparer : IEqualityComparer<byte[]> {
			public bool Equals(byte[] left, byte[] right) {
				if ( left == null || right == null ) {
					return left == right;
				}
				return 0 == ByteComparer.cmp (left, right);
			}
			public int GetHashCode(byte[] key) {
				if (key == null) {
					throw new ArgumentNullException ("key");
				}
				int sum = 0;
				// TODO faster hash needed here
				for (int i = 0; i < key.Length; i++) {
					sum += key [i];
				}
				return sum;
			}
		}
		#endif

		// TODO without using the ByteArrayComparer class, this is broken.
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
#endif

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

		protected void Dispose(bool itIsSafeToAlsoFreeManagedObjects)
		{
			// TODO call Dispose on subcursors?
		}

		public void Dispose()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		~MultiCursor()
		{
			Dispose (false);
		}

		public static ICursor Create(params ICursor[] _subcursors)
		{
			return new MultiCursor(_subcursors);
		}

        public static ICursor Create(IEnumerable<ICursor> _subcursors)
        {
			return new MultiCursor(_subcursors);
        }

		private MultiCursor(IEnumerable<ICursor> _subcursors)
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

		protected void Dispose(bool itIsSafeToAlsoFreeManagedObjects)
		{
			// TODO call Dispose on chain?
		}

		public void Dispose()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		~LivingCursor()
		{
			Dispose (false);
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
        // pages types
		private const byte LEAF_NODE = 1;
		private const byte PARENT_NODE = 2;
		private const byte OVERFLOW_NODE = 3;

        // flags on values
		private const byte FLAG_OVERFLOW = 1;
		private const byte FLAG_TOMBSTONE = 2;

        // flags on pages
		private const byte FLAG_ROOT_NODE = 1;
		private const byte FLAG_BOUNDARY_NODE = 2;
		private const byte FLAG_ENDS_ON_BOUNDARY = 4;

		// TODO in the F# version, pgitem is a struct
		private class pgitem
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

		private static void buildParentPage(Dictionary<int,int> overflows, PageBuilder pb, List<pgitem> children, int stop, int start)
		{
			// assert stop >= start
			int countKeys = (stop - start); 

			pb.Reset ();
			pb.PutByte (PARENT_NODE);
			pb.PutByte (0);

			pb.PutInt16 ((ushort) countKeys);

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

        private static int buildOverflowFirstPage(PageBuilder pb, Stream ba, int len, int sofar)
        {
            pb.Reset ();
            pb.PutByte (OVERFLOW_NODE);
            pb.PutByte (0); // starts at 0, may be changed before page is written
            // check for the partial page at the end
            int num = Math.Min ((pb.PageSize - (2 + sizeof(int))), (int) (len - sofar));
            pb.PutStream (ba, num);
            // something will be put in lastInt32 before the page is written
            return sofar + num;
        }

        private static int writeOverflowRegularPages(PageBuilder pb, Stream fs, Stream ba, int numPagesToWrite, int len, int sofar)
        {
            for (int i=0; i<numPagesToWrite; i++) {
                pb.Reset ();
                // check for the partial page at the end
				int num = Math.Min (pb.PageSize, (int) (len - sofar));
				pb.PutStream (ba, num);
				sofar += num;
				pb.Write (fs);
			}
            return sofar;
        }

        private static int buildOverflowBoundaryPage(PageBuilder pb, Stream ba, int len, int sofar)
        {
            pb.Reset ();
            // check for the partial page at the end
            int num = Math.Min ((pb.PageSize - sizeof(int)), (int) (len - sofar));
            pb.PutStream (ba, num);
            // something will be put in lastInt32 before the page is written

            return sofar + num;
        }

		private static int countOverflowPagesFor(int pageSize, int len)
		{
            int pages = len / pageSize;
			if ((len % pageSize) != 0) {
				pages++;
			}
			return pages;
		}

		private static Tuple<int,int> writeOverflow(IPages pageManager, Guid token, int startingNextPageNumber, int startingBoundaryPageNumber, PageBuilder pb, Stream fs, Stream ba)
		{
			int sofar = 0;
			int len = (int) ba.Length;
            int nextPageNumber = startingNextPageNumber;
            int boundaryPageNumber = startingBoundaryPageNumber;

            while (sofar < len) {
                // we're going to write out an overflow first page,
                // followed by zero-or-more "regular" overflow pages,
                // which have no header.  we'll stop at the boundary
                // if the whole thing won't fit.
                sofar = buildOverflowFirstPage(pb, ba, len, sofar);
                // note that we haven't written this page yet.
                var thisPageNumber = nextPageNumber;
                if (thisPageNumber == boundaryPageNumber) {
                    // the first page landed on a boundary
                    pb.SetPageFlag(FLAG_BOUNDARY_NODE);
                    var newRange = pageManager.GetRange(token);
                    nextPageNumber = newRange.Item1;
                    boundaryPageNumber = newRange.Item2;
                    pb.SetLastInt32(nextPageNumber);
                    pb.Write(fs);
                    utils.SeekPage(fs, pb.PageSize, nextPageNumber);
                }
                else {
                    nextPageNumber = nextPageNumber + 1;
                    // assert sofar <= len
                    if (sofar == len) {
                        // the first page is also the last one
						pb.SetLastInt32 (0); // number of regular pages following
                        pb.Write(fs);
                    }
                    else {
                        // assert sofar < len

                        // needed gives us the number of pages, NOT including the first one
                        // which would be necessary to finish this overflow.
                        var needed = countOverflowPagesFor (pb.PageSize, len - sofar);

                        // availableBeforeBoundary is the number of pages until the boundary,
                        // NOT counting the boundary page.  nextPageNumber has already been incremented
                        // for the first page in the block, so we're just talking about data pages.
                        var availableBeforeBoundary = (boundaryPageNumber > 0) ? (boundaryPageNumber - nextPageNumber) : needed;

                        // if needed <= availableBeforeBoundary then this will fit

                        // if needed = (1 + availableBeforeBoundary) then this might fit, 
                        // depending on whether the loss of the 4 bytes on the boundary
                        // page makes a difference or not.  Either way, for this block,
                        // the overflow ends on the boundary

                        // if needed > (1 + availableBeforeBoundary), then this block will end 
                        // on the boundary, but it will continue

                        var numRegularPages = Math.Min(needed, availableBeforeBoundary);
                        pb.SetLastInt32(numRegularPages);

                        if (needed > availableBeforeBoundary) {
                            // this part of the overflow will end on the boundary,
                            // perhaps because it finishes exactly there, or perhaps
                            // because it doesn't fit and needs to continue into the
                            // next block.
                            pb.SetPageFlag(FLAG_ENDS_ON_BOUNDARY);
                        }

                        // now we can write the first page
                        pb.Write(fs);

                        // write out the regular pages.  these are full pages
                        // of data, with no header and no footer.  the last
                        // page might not be full, since it might be a
                        // partial page at the end of the overflow.

                        sofar = writeOverflowRegularPages(pb, fs, ba, numRegularPages, len, sofar);
                        nextPageNumber = nextPageNumber + numRegularPages;

                        if (needed > availableBeforeBoundary) {
                            // assert sofar < len
                            // assert nextPageNumber = boundaryPageNumber
                            //
                            // we need to write out a regular page with a
                            // boundary pointer in it.  if this is happening,
                            // then FLAG_ENDS_ON_BOUNDARY was set on the first
                            // overflow page in this block, since we can't set it
                            // here on this page, because this page has no header.
                            sofar = buildOverflowBoundaryPage(pb, ba, len, sofar);
                            var newRange = pageManager.GetRange(token);
                            nextPageNumber = newRange.Item1;
                            boundaryPageNumber = newRange.Item2;
                            pb.SetLastInt32(nextPageNumber);
                            pb.Write(fs);
                            utils.SeekPage(fs, pb.PageSize, nextPageNumber);
                        }
                    }
                }
            }

            return new Tuple<int,int>(nextPageNumber, boundaryPageNumber);
		}

		private static int calcAvailable(int pageSize, int currentSize, bool couldBeRoot)
		{
			int n = (pageSize - currentSize - sizeof(int)); // for the lastInt32
			if (couldBeRoot)
			{
				// make space for the firstLeaf and lastLeaf fields (lastInt32 already there)
				n -= sizeof(int);
			}
			return n;
		}

		private static Tuple<int,int,List<pgitem>> writeParentNodes(IPages pageManager, Guid token, int firstLeaf, int lastLeaf, List<pgitem> children, int startingPageNumber, int startingBoundaryPageNumber, Stream fs, PageBuilder pb, PageBuilder pbOverflow)
		{
			int nextPageNumber = startingPageNumber;
            int boundaryPageNumber = startingBoundaryPageNumber;
			var nextGeneration = new List<pgitem> ();

            // 2 for the page type and flags
            // 2 for the stored count
            // 5 for the extra ptr we will add at the end, a varint, 5 is worst case
            // 4 for lastInt32
			const int PAGE_OVERHEAD = 2 + 2 + 5 + 4;

			int sofar = 0;
			Dictionary<int,int> overflows = new Dictionary<int, int> ();
			int first = 0;

			// assert children.Count > 1
			for (int i = 0; i < children.Count; i++) {
				pgitem n = children [i];
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
					var writeThisPage = false;
					var isBoundary = (nextPageNumber == boundaryPageNumber);
                    var couldBeRoot = (nextGeneration.Count == 0);
                    var avail = calcAvailable(pb.PageSize, sofar, couldBeRoot);
					if (isLastChild) {
						writeThisPage = true;
					} else if (avail >= neededForInline) {
						// no problem.
					} else if ((pb.PageSize - PAGE_OVERHEAD) >= neededForInline) {
						// it won't fit here, but it would fully fit on the next page.
						writeThisPage = true;
					} else if (avail < neededForOverflow) {
						// we can't even put this key in this page if we overflow it.
						writeThisPage = true;
					}

					bool isRootNode = false;
					if (isLastChild && couldBeRoot) {
						isRootNode = true;
					}

					if (writeThisPage) {
						int thisPageNumber = nextPageNumber;

						buildParentPage (overflows, pb, children, i, first);

						if (isRootNode) {
                            pb.SetPageFlag(FLAG_ROOT_NODE);
                            pb.SetSecondToLastInt32(firstLeaf);
                            pb.SetLastInt32(lastLeaf);
                        } else {
							if (isBoundary) {
                                pb.SetPageFlag(FLAG_BOUNDARY_NODE);
                                var newRange = pageManager.GetRange(token);
                                nextPageNumber = newRange.Item1;
                                boundaryPageNumber = newRange.Item2;
                                pb.SetLastInt32(nextPageNumber);
							} else {
								nextPageNumber++;
							}
						}

						pb.Write (fs);

                        if (nextPageNumber != (thisPageNumber+1)) {
                            utils.SeekPage(fs, pb.PageSize, nextPageNumber);
                        }

						nextGeneration.Add (new pgitem {PageNumber = thisPageNumber, Key=children[i-1].Key});

						sofar = 0;
						first = 0;
						overflows.Clear();
					}
				}

				if (!isLastChild) {
					if (0 == sofar) {
						first = i;
						overflows.Clear();
                        sofar = PAGE_OVERHEAD;
					}
						
					if (calcAvailable(pb.PageSize, sofar, (nextGeneration.Count == 0)) >= neededForInline) {
						sofar += k.Length;
					} else {
						int keyOverflowFirstPage = nextPageNumber;
						var kRange = writeOverflow (pageManager, token, nextPageNumber, boundaryPageNumber, pbOverflow, fs, new MemoryStream(k));
                        nextPageNumber = kRange.Item1;
                        boundaryPageNumber = kRange.Item2;
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

			return new Tuple<int,int,List<pgitem>>(nextPageNumber, boundaryPageNumber, nextGeneration);
		}

		public static Tuple<Guid,int> Create(Stream fs, IPages pageManager, IEnumerable<KeyValuePair<byte[],Stream>> source)
		{
			// TODO if !(fs.CanSeek()) throw?
            int pageSize = pageManager.PageSize;
			PageBuilder pb = new PageBuilder(pageSize);
			PageBuilder pbOverflow = new PageBuilder(pageSize);

			var token = pageManager.Begin ();
            var range = pageManager.GetRange(token);
			int nextPageNumber = range.Item1;
            int boundaryPageNumber = range.Item2;

            // 2 for the page type and flags
            // 4 for the prev page
            // 2 for the stored count
            // 4 for lastInt32 (which isn't in pb.Available)
			const int PAGE_OVERHEAD = 2 + 4 + 2 + 4;
            const int OFFSET_COUNT_PAIRS = 6;

			var nodelist = new List<pgitem> ();

			ushort countPairs = 0;
			byte[] lastKey = null;

			int prevPageNumber = 0;

            utils.SeekPage(fs, pb.PageSize, nextPageNumber);

            foreach (var kv in source) {
				byte[] k = kv.Key;
				Stream v = kv.Value;

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

				if (pb.Position > 0) {
					// figure out if we need to just write this page

					int avail = pb.Available - sizeof(int); // for lastInt32

					bool writeThisPage = false;
					if (avail >= neededForBothInline) {
						// no problem.  both the key and the value are going to fit
					} else if ((pb.PageSize - PAGE_OVERHEAD) >= neededForBothInline) {
						// it won't fit here, but it would fully fit on the next page.
						writeThisPage = true;
					} else if (avail >= neededForKeyInlineValueOverflow) {
						// the key will fit inline if we just overflow the val
					} else if (avail < neededForOverflowBoth) {
						// we can't even put this pair in this page if we overflow both.
						writeThisPage = true;
					}

					if (writeThisPage) {
						// note that this code is duplicated with slight differences below, after the loop

						int thisPageNumber = nextPageNumber;

						// assert -- it is not possible for this to be the last leaf.  so, at
						// this point in the code, we can be certain that there is going to be
						// another page.

						if (thisPageNumber == boundaryPageNumber) {
                            pb.SetPageFlag(FLAG_BOUNDARY_NODE);
                            var newRange = pageManager.GetRange(token);
                            nextPageNumber = newRange.Item1;
                            boundaryPageNumber = newRange.Item2;
                            pb.SetLastInt32(nextPageNumber);
						} else {
							nextPageNumber++;
						}

						// now that we know how many pairs are in this page, we can write that out
						pb.PutInt16At (OFFSET_COUNT_PAIRS, countPairs);

						pb.Write (fs);

                        if (nextPageNumber != (thisPageNumber+1)) {
                            utils.SeekPage(fs, pb.PageSize, nextPageNumber);
                        }

						nodelist.Add (new pgitem { PageNumber = thisPageNumber, Key = lastKey });

						prevPageNumber = thisPageNumber;
						pb.Reset ();
						countPairs = 0;
						lastKey = null;
					}
				}

				if (pb.Position == 0) {
					// we could be here because we just wrote the page.
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

				int available = pb.Available - sizeof(int); // for lastInt32
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
						var kRange = writeOverflow (pageManager, token, nextPageNumber, boundaryPageNumber, pbOverflow, fs, new MemoryStream(k));
                        nextPageNumber = kRange.Item1;
                        boundaryPageNumber = kRange.Item2;

						pb.PutByte (FLAG_OVERFLOW);
						pb.PutVarint (k.Length);
						pb.PutInt32 (keyOverflowFirstPage);
					}

                    int valueOverflowFirstPage = nextPageNumber;
                    var vRange = writeOverflow (pageManager, token, nextPageNumber, boundaryPageNumber, pbOverflow, fs, v);
                    nextPageNumber = vRange.Item1;
                    boundaryPageNumber = vRange.Item2;

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

				if (0 == nodelist.Count) {
					// this is the last page.  the only page.  the root page.
					// even though it's a leaf.
				} else {
					if (thisPageNumber == boundaryPageNumber) {
                        pb.SetPageFlag(FLAG_BOUNDARY_NODE);
                        var newRange = pageManager.GetRange(token);
                        nextPageNumber = newRange.Item1;
                        boundaryPageNumber = newRange.Item2;
                        pb.SetLastInt32(nextPageNumber);
					} else {
						nextPageNumber++;
					}
				}

				// now that we know how many pairs are in this page, we can write that out
				pb.PutInt16At (OFFSET_COUNT_PAIRS, countPairs);

				pb.Write (fs);

                if (nextPageNumber != (thisPageNumber+1)) {
                    utils.SeekPage(fs, pb.PageSize, nextPageNumber);
                }

				nodelist.Add (new pgitem { PageNumber = thisPageNumber, Key = lastKey });
			}

			if (nodelist.Count > 0) {
				int firstLeaf = nodelist [0].PageNumber;
				int lastLeaf = nodelist [nodelist.Count - 1].PageNumber;

				// now write the parent pages, maybe more than one level of them.  we have to get
				// down to a level with just one parent page in it, the root page.

				while (nodelist.Count > 1) {
					var results = writeParentNodes (pageManager, token, firstLeaf, lastLeaf, nodelist, nextPageNumber, boundaryPageNumber, fs, pb, pbOverflow);
                    nextPageNumber = results.Item1;
                    boundaryPageNumber = results.Item2;
                    nodelist = results.Item3;
				}

				// assert nodelist.Count == 1

				pageManager.End (token, nodelist [0].PageNumber);

				return new Tuple<Guid,int>(token, nodelist [0].PageNumber);
			} else {
				pageManager.End (token, 0);

				return new Tuple<Guid,int>(token, 0);
			}
		}

		private class myOverflowReadStream : Stream
		{
			private readonly Stream fs;
			private readonly int len;
			private readonly int firstPage;
			private readonly byte[] buf;
			private int currentPage;
			private int sofarOverall;
			private int sofarThisPage;
            private int firstPageInBlock;
            private int countRegularDataPagesInBlock;
            private int boundaryPageNumber;
            private int bytesOnThisPage;
            private int offsetOnThisPage;

			// TODO consider supporting seek

			public myOverflowReadStream(Stream _fs, int pageSize, int _firstPage, int _len)
			{
				fs = _fs;
				len = _len;
                firstPage = _firstPage;

				currentPage = firstPage;

                buf = new byte[pageSize];
				ReadFirstPage();
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
                if (currentPage == firstPageInBlock) {
                    bytesOnThisPage = buf.Length - (2 + sizeof(int));
                    offsetOnThisPage = 2;
                }
                else if (currentPage == boundaryPageNumber) {
                    bytesOnThisPage = buf.Length - sizeof(int);
                    offsetOnThisPage = 0;
                }
                else {
                    // assert currentPage > firstPageInBlock
                    // assert currentPage < boundaryPageNumber OR boundaryPageNumber = 0
                    bytesOnThisPage = buf.Length;
                    offsetOnThisPage = 0;
                }

			}

            private void ReadFirstPage() 
            {
                firstPageInBlock = currentPage;
                ReadPage();
                if (PageType() != OVERFLOW_NODE) {
                    throw new Exception();
                }
                if (CheckPageFlag(FLAG_BOUNDARY_NODE)) {
                    // first page landed on a boundary node
                    // lastInt32 is the next page number, which we'll fetch later
                    boundaryPageNumber = currentPage;
                    countRegularDataPagesInBlock = 0;
                }
                else {
                    countRegularDataPagesInBlock = GetLastInt32();
                    if (CheckPageFlag(FLAG_ENDS_ON_BOUNDARY)) {
                        boundaryPageNumber = currentPage + countRegularDataPagesInBlock + 1;
                    }
                    else {
                        boundaryPageNumber = 0;
                    }
                }
            }

			public override bool CanRead {
				get {
					return sofarOverall < len;
				}
			}

            private bool isBoundary
            {
                get {
                    return (0 != (buf[1] & FLAG_BOUNDARY_NODE));
                }
            }

            private int GetInt32At(int at)
            {
                uint val = buf [at];
                val = val << 8 | buf [at+1];
                val = val << 8 | buf [at+2];
                val = val << 8 | buf [at+3];
                // assert fits in 32 bit int
                return (int) val;
            }

            private int GetLastInt32()
            {
                return GetInt32At(buf.Length - 4);
            }

            private byte PageType()
            {
                return buf[0];
            }

            public bool CheckPageFlag(byte f)
            {
                return 0 != (buf[1] & f);
            }

			public override int Read (byte[] ba, int offset, int wanted)
			{
                if (sofarOverall >= len) {
                    return 0;
                } else {
                    var direct = false;
                    if (sofarThisPage >= bytesOnThisPage) {
                        if (currentPage == boundaryPageNumber) {
                            currentPage = GetLastInt32();
                            ReadFirstPage();
                        } else {
                            // we need a new page.  and if it's a full data page,
                            // and if wanted is big enough to take all of it, then
                            // we want to read (at least) it directly into the
                            // buffer provided by the caller.  we already know
                            // this candidate page cannot be the first page in a
                            // block.
                            var maybeDataPage = currentPage + 1;
                            var isDataPage = 
                                (boundaryPageNumber > 0)
                                    ? (((len - sofarOverall) >= buf.Length) && (countRegularDataPagesInBlock > 0) && (maybeDataPage > firstPageInBlock) && (maybeDataPage < boundaryPageNumber))
                                    : ((len - sofarOverall) >= buf.Length) && (countRegularDataPagesInBlock > 0) && (maybeDataPage > firstPageInBlock) && (maybeDataPage <= (firstPageInBlock + countRegularDataPagesInBlock))
                                    ;

                            if (isDataPage && (wanted >= buf.Length)) {
                                // assert (currentPage + 1) > firstPageInBlock
                                //
                                // don't increment currentPage here because below, we will
                                // calculate how many pages we actually want to do.
                                direct = true;
                                bytesOnThisPage = buf.Length;
                                sofarThisPage = 0;
                                offsetOnThisPage = 0;
                            } else {
                                currentPage = currentPage + 1;
                                ReadPage();
                            }
                        }
                    }

                    if (direct) {
                        // currentPage has not been incremented yet
                        //
                        // skip the buffer.  note, therefore, that the contents of the
                        // buffer are "invalid" in that they do not correspond to currentPage
                        //
                        var numPagesWanted = wanted / buf.Length;
                        // assert countRegularDataPagesInBlock > 0
                        var lastDataPageInThisBlock = firstPageInBlock + countRegularDataPagesInBlock ;
                        var theDataPage = currentPage + 1;
                        var numPagesAvailable = 
                            (boundaryPageNumber>0)
                                ? (boundaryPageNumber - theDataPage)
                                : (lastDataPageInThisBlock - theDataPage + 1)
                                ;
                        var numPagesToFetch = Math.Min(numPagesWanted, numPagesAvailable);
                        var bytesToFetch = numPagesToFetch * buf.Length;
                        // assert bytesToFetch <= wanted

                        utils.SeekPage(fs, buf.Length, theDataPage);
                        utils.ReadFully(fs, ba, offset, bytesToFetch);
                        sofarOverall = sofarOverall + bytesToFetch;
                        currentPage = currentPage + numPagesToFetch;
                        sofarThisPage = buf.Length;
                        return bytesToFetch;
                    } else {
                        var available = Math.Min (bytesOnThisPage - sofarThisPage, len - sofarOverall);
                        var num = Math.Min (available, wanted);
                        System.Array.Copy (buf, offsetOnThisPage + sofarThisPage, ba, offset, num);
                        sofarOverall = sofarOverall + num;
                        sofarThisPage = sofarThisPage + num;
                        return num;
                    }
                }
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
                    return sofarOverall;
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
			private readonly Action<ICursor> hook;

			private int currentPage = 0;

			private int[] leafKeys;
			private int previousLeaf;
			private int currentKey;

			protected void Dispose(bool itIsSafeToAlsoFreeManagedObjects)
			{
				if (hook != null) {
					hook (this);
				}
			}

			public void Dispose()
			{
				Dispose (true);
				GC.SuppressFinalize (this);
			}

			~myCursor()
			{
				Dispose (false);
			}

			public myCursor(Stream _fs, int pageSize, int _rootPage, Action<ICursor> _hook)
			{
				// TODO if !(strm.CanSeek()) throw?
				rootPage = _rootPage;
				fs = _fs;
				hook = _hook;
                pr = new PageReader(pageSize);
				if (!setCurrentPage(rootPage)) {
					throw new Exception();
				}
				if (pr.PageType == LEAF_NODE) {
					firstLeaf = lastLeaf = rootPage;
				} else if (pr.PageType == PARENT_NODE) {
                    if (!pr.CheckPageFlag(FLAG_ROOT_NODE)) {
						throw new Exception ();
					}
					firstLeaf = pr.GetSecondToLastInt32 ();
					lastLeaf = pr.GetLastInt32 ();
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
                        int lastInt32 = pr.GetLastInt32();
                        //
                        // an overflow page has a value in its LastInt32 which
                        // is one of two things.
                        //
                        // if it's a boundary node, it's the page number of the
                        // next page in the segment.
                        //
                        // otherwise, it's the number of pages to skip ahead.
                        // this skip might take us to whatever follows this
                        // overflow (which could be a leadf or a parent or
                        // another overflow), or it might just take us to a
                        // boundary page (in the case where the overflow didn't
                        // fit).  it doesn't matter.  we just skip ahead.
                        //
                        if (pr.CheckPageFlag(FLAG_BOUNDARY_NODE)) {
                            if (!setCurrentPage(lastInt32)) {
                                return false;
                            }
                        } else {
                            var endsOnBoundary = pr.CheckPageFlag(FLAG_ENDS_ON_BOUNDARY);
                            if (setCurrentPage(currentPage + lastInt32)) {
                                if (endsOnBoundary) {
                                    var next = pr.GetLastInt32();
                                    if (!setCurrentPage(next)) {
                                        return false;
                                    }
                                }
                            } else {
                                return false;
                            }
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
                                        nextPage = pr.GetLastInt32();
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
                        nextPage = pr.GetLastInt32();
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

		public static ICursor OpenCursor(Stream fs, int pageSize, int rootPage, Action<ICursor> hook)
		{
			return new myCursor(fs, pageSize, rootPage, hook);
		}

	}

}
