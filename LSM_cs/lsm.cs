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
		// Like Stream.Read, but it loops until it gets all of what it wants,
		// or until the end of the stream
		public static int ReadFully(Stream s, byte[] buf, int off, int len)
		{
			int sofar = 0;
			while (sofar < len) {
				int got = s.Read (buf, off + sofar, len - sofar);
				if (0 == got) {
					break;
				}
				sofar += got;
			}
			return sofar;
		}

		// read until the end of the stream
		public static byte[] ReadAll(Stream s)
		{
			byte[] a = new byte[s.Length]; // TODO does this give us the length remaining?
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

	public static class varint
	{
		// http://sqlite.org/src4/doc/trunk/www/varint.wiki

		public static int space_needed(ulong v)
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
		public static int cmp_within(byte[] buf, int buf_offset, int buf_len, byte[] y)
		{
			int n2 = y.Length;
			int len = buf_len<n2 ? buf_len : n2;
			for (var i = 0; i < len; i++)
			{
				var c = buf[i+buf_offset].CompareTo(y[i]);
				if (c != 0)
				{
					return c;
				}
			}

			return buf_len.CompareTo(y.Length);
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

		public PageReader(int pgsz)
		{
			buf = new byte[pgsz];
		}

		public void Read(Stream fs)
		{
			fs.Read (buf, 0, buf.Length);
		}

		public void Reset()
		{
			cur = 0;
		}

		public void Skip(int c)
		{
			cur += c;
		}

		public int cmp(int len, byte[] other)
		{
			return ByteComparer.cmp_within (buf, cur, len, other);
		}

		public int Position
		{
			get { return cur; }
		}

		public void SetPosition(int c)
		{
			cur = c;
		}

		public byte page_type()
		{
			return buf [0];
		}

		public byte read_byte()
		{
			return buf [cur++];
		}

		public uint read_uint32()
		{
			uint val = buf [cur++];
			val = val << 8 | buf [cur++];
			val = val << 8 | buf [cur++];
			val = val << 8 | buf [cur++];
			return val;
		}

		public ushort read_uint16()
		{
			ushort val = buf [cur++];
			val = (ushort) (val << 8 | buf [cur++]);
			return val;
		}

		public byte[] read_byte_array(int len)
		{
			byte[] k = new byte[len];
			Array.Copy (buf, cur, k, 0, len);
			cur += len;
			return k;
		}

		public ulong read_varint()
		{
			byte a0 = buf [cur++];
			if (a0 <= 240) {
				return a0;
			} else if (a0 <= 248) {
				byte a1 = buf [cur++];
				return (ulong) (240 + 256 * (a0 - 241) + a1);
			} else if (249 == a0) {
				byte a1 = buf [cur++];
				byte a2 = buf [cur++];
				return (ulong) (2288 + 256 * a1 + a2);
			} else if (250 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				return v;
			} else if (251 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				return v;
			} else if (252 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				return v;
			} else if (253 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				return v;
			} else if (254 == a0) {
				ulong v = 0;
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				v = (v << 8) | buf [cur++];
				return v;
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
				return v;
			}
		}
	}

	public class PageBuilder
	{
		private readonly byte[] buf;
		private int cur;

		public PageBuilder(int pgsz)
		{
			buf = new byte[pgsz];
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

		// TODO could be a property
		public int Available()
		{
			return buf.Length - cur;
		}

		public void write_byte(byte b)
		{
			buf [cur++] = b;
		}

		public void write_uint32(uint val)
		{
			buf[cur++] = (byte)(val >> 24);
			buf[cur++] = (byte)(val >> 16);
			buf[cur++] = (byte)(val >> 8);
			buf[cur++] = (byte)(val >> 0);
		}

		public void write_uint32_at(int at, uint val)
		{
			buf[at++] = (byte)(val >> 24);
			buf[at++] = (byte)(val >> 16);
			buf[at++] = (byte)(val >> 8);
			buf[at++] = (byte)(val >> 0);
		}

		public void write_uint16(ushort val)
		{
			buf[cur++] = (byte)(val >> 8);
			buf[cur++] = (byte)(val >> 0);
		}

		public void write_uint16_at(int at, ushort val)
		{
			buf[at++] = (byte)(val >> 8);
			buf[at++] = (byte)(val >> 0);
		}

		public void write_varint(ulong v)
		{
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

		public void write_byte_array(byte[] ba)
		{
			Array.Copy (ba, 0, buf, cur, ba.Length);
			cur += ba.Length;
		}

		public void write_byte_stream(Stream ba, int num)
		{
			int got = utils.ReadFully (ba, buf, cur, num);
			// assert got == num
			cur += (int) num;
		}

	}

	public class MemorySegment : IWrite
	{
		private Dictionary<byte[],Stream> _pairs = new Dictionary<byte[],Stream>();

		private class my_cursor : ICursor
		{
			private readonly byte[][] _keys;
			private readonly Dictionary<byte[],Stream> _pairs;
			private int _cur = -1;

			public my_cursor(Dictionary<byte[],Stream> pairs)
			{
				_pairs = pairs;
				_keys = new byte[_pairs.Count][];
				_pairs.Keys.CopyTo(_keys, 0);
				Array.Sort(_keys, new ByteComparer());
			}

			bool ICursor.IsValid()
			{
				return (_cur >= 0) && (_cur < _pairs.Count);
			}

			private int search(byte[] k, int min, int max, SeekOp sop)
			{
				int le = -1;
				int ge = -1;
				while (max >= min) {
					int mid = (max + min) / 2;
					byte[] kmid = _keys [mid];
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
				_cur = search (k, 0, _pairs.Count - 1, sop);
			}

			void ICursor.First()
			{
				_cur = 0;
			}

			void ICursor.Last()
			{
				_cur = _pairs.Count - 1;
			}

			void ICursor.Next()
			{
				_cur++;
			}

			void ICursor.Prev()
			{
				_cur--;
			}

			int ICursor.KeyCompare(byte[] k)
			{
				return ByteComparer.cmp ((this as ICursor).Key (), k);
			}

			byte[] ICursor.Key()
			{
				return _keys[_cur];
			}

			Stream ICursor.Value()
			{
				Stream v = _pairs [_keys [_cur]];
				if (v != null) {
					v.Seek (0, SeekOrigin.Begin);
				}
				return v;
			}

			int ICursor.ValueLength()
			{
				Stream v = _pairs[_keys[_cur]];
				if (null == v) {
					return -1;
				} else {
					return (int) v.Length;
				}
			}
		}

		ICursor IWrite.OpenCursor()
		{
			return new my_cursor(_pairs);
		}

		void IWrite.Insert(byte[] k, Stream v)
		{
			_pairs.Add (k, v);
		}

		void IWrite.Delete(byte[] k)
		{
			_pairs.Add (k, null); // tombstone
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

		private readonly List<ICursor> _csrs;
		private ICursor _cur;
		private Direction _dir;

		public static ICursor create(params ICursor[] csrs)
		{
			return new MultiCursor(csrs);
		}

		public MultiCursor(IEnumerable<ICursor> others)
		{
			_csrs = new List<ICursor> ();
			foreach (var oc in others)
			{
				_csrs.Add(oc);
			}
		}

		bool ICursor.IsValid()
		{
			return (_cur != null) && _cur.IsValid ();
		}

		private ICursor find_cur(Func<int,bool> f)
		{
			ICursor cur = null;
			byte[] kcur = null;
			for (int i = 0; i < _csrs.Count; i++) {
				ICursor csr = _csrs [i];
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

		private ICursor find_min()
		{
			return find_cur (x => (x < 0));
		}

		private ICursor find_max()
		{
			return find_cur (x => (x > 0));
		}

		void ICursor.Seek(byte[] k, SeekOp sop)
		{
			_cur = null;
			for (int i = 0; i < _csrs.Count; i++) {
				ICursor csr = _csrs [i];
				csr.Seek (k, sop);
				if (csr.IsValid() && ( (SeekOp.SEEK_EQ == sop) || (0 == csr.KeyCompare (k)) ) ) {
					_cur = csr;
					break;
				}
			}

			_dir = Direction.WANDERING;

			if (null == _cur) {
				if (SeekOp.SEEK_GE == sop) {
					_cur = find_min ();
					if (null != _cur) {
						_dir = Direction.FORWARD;
					}
				} else if (SeekOp.SEEK_LE == sop) {
					_cur = find_max ();
					if (null != _cur) {
						_dir = Direction.BACKWARD;
					}
				}
			}
		}

		void ICursor.First()
		{
			for (int i = 0; i < _csrs.Count; i++) {
				ICursor csr = _csrs [i];
				csr.First();
			}
			_cur = find_min ();
			_dir = Direction.FORWARD;
		}

		void ICursor.Last()
		{
			for (int i = 0; i < _csrs.Count; i++) {
				ICursor csr = _csrs [i];
				csr.Last();
			}
			_cur = find_max ();
			_dir = Direction.BACKWARD;
		}

		byte[] ICursor.Key()
		{
			return _cur.Key ();
		}

		int ICursor.KeyCompare(byte[] k)
		{
			return _cur.KeyCompare (k);
		}

		Stream ICursor.Value()
		{
			return _cur.Value ();
		}

		int ICursor.ValueLength()
		{
			return _cur.ValueLength ();
		}

		void ICursor.Next()
		{
			byte[] k = _cur.Key ();

			for (int i = 0; i < _csrs.Count; i++) {
				if ((_dir != Direction.FORWARD) && (_cur != _csrs[i])) {
					_csrs [i].Seek (k, SeekOp.SEEK_GE);
				}
				if (_csrs [i].IsValid ()) {
					if (0 == _csrs [i].KeyCompare (k)) {
						_csrs [i].Next ();
					}
				}
			}

			_cur = find_min ();
			_dir = Direction.FORWARD;
		}

		void ICursor.Prev()
		{
			byte[] k = _cur.Key ();

			for (int i = 0; i < _csrs.Count; i++) {
				if ((_dir != Direction.BACKWARD) && (_cur != _csrs[i])) {
					_csrs [i].Seek (k, SeekOp.SEEK_LE);
				}
				if (_csrs [i].IsValid ()) {
					if (0 == _csrs [i].KeyCompare (k)) {
						_csrs [i].Prev ();
					}
				}
			}

			_cur = find_max ();
			_dir = Direction.BACKWARD;
		}

	}

	public class LivingCursor : ICursor
	{
		private readonly ICursor _chain;

		public LivingCursor(ICursor chain)
		{
			_chain = chain;
		}

		bool ICursor.IsValid()
		{
			return _chain.IsValid () && (_chain.ValueLength() >= 0);
		}

		private void next_until()
		{
			while (_chain.IsValid() && (_chain.ValueLength () < 0)) {
				_chain.Next ();
			}
		}

		private void prev_until()
		{
			while (_chain.IsValid() && (_chain.ValueLength () < 0)) {
				_chain.Prev ();
			}
		}

		void ICursor.Seek(byte[] k, SeekOp sop)
		{
			_chain.Seek (k, sop);
			if (SeekOp.SEEK_GE == sop) {
				next_until ();
			} else if (SeekOp.SEEK_LE == sop) {
				prev_until ();
			}
		}

		void ICursor.First()
		{
			_chain.First ();
			next_until ();
		}

		void ICursor.Last()
		{
			_chain.Last ();
			prev_until ();
		}

		byte[] ICursor.Key()
		{
			return _chain.Key ();
		}

		int ICursor.KeyCompare(byte[] k)
		{
			return _chain.KeyCompare (k);
		}

		Stream ICursor.Value()
		{
			return _chain.Value ();
		}

		int ICursor.ValueLength()
		{
			return _chain.ValueLength ();
		}

		void ICursor.Next()
		{
			_chain.Next ();
			next_until ();
		}

		void ICursor.Prev()
		{
			_chain.Prev ();
			prev_until ();
		}

	}

	public static class BTreeSegment
	{
		private const byte LEAF_NODE = 1;
		private const byte PARENT_NODE = 2;
		private const byte OVERFLOW_NODE = 3;

		private const byte FLAG_OVERFLOW = 1;
		private const byte FLAG_TOMBSTONE = 2;

		private const byte FLAG_ROOT_NODE = 1;

		private const int PAGE_SIZE = 4096;

		private const int OVERFLOW_PAGE_HEADER_SIZE = 6;

		private const int PARENT_NODE_HEADER_SIZE = 8;

		private const int LEAF_HEADER_SIZE = 8;
		private const int OFFSET_COUNT_PAIRS = 6;

		private class node
		{
			public uint PageNumber;
			public byte[] Key;
		}

		private static void write_byte_array_with_length(PageBuilder pb, byte[] ba)
		{
			if (null == ba) {
				pb.write_byte(FLAG_TOMBSTONE);
				pb.write_varint(0);
			} else {
				pb.write_byte (0);
				pb.write_varint ((uint)ba.Length);
				pb.write_byte_array (ba);
			}
		}

		private static void write_byte_array_with_length(PageBuilder pb, Stream ba)
		{
			if (null == ba) {
				pb.write_byte(FLAG_TOMBSTONE);
				pb.write_varint(0);
			} else {
				pb.write_byte (0);
				pb.write_varint ((uint)ba.Length);
				pb.write_byte_stream (ba, (int) ba.Length);
			}
		}

		private static void build_parent_page(bool root, uint first_leaf, uint last_leaf, Dictionary<int,uint> overflows, PageBuilder pb, List<node> children, int stop, int start)
		{
			// assert stop >= start
			int count_keys = (stop - start); 

			pb.Reset ();
			pb.write_byte (PARENT_NODE);
			pb.write_byte( (byte) (root ? FLAG_ROOT_NODE : 0) );

			pb.write_uint16 ((ushort) count_keys);

			if (root) {
				pb.write_uint32 (first_leaf);
				pb.write_uint32 (last_leaf);
			}

			// store all the pointers (n+1 of them).  
			// note q<=stop below
			for (int q = start; q <= stop; q++) {
				pb.write_varint(children[q].PageNumber);
			}

			// now store the keys (n) of them.
			// note q<stop below
			for (int q = start; q < stop; q++) {
				byte[] k = children [q].Key;

				if ((overflows != null) && overflows.ContainsKey (q)) {
					pb.write_byte(FLAG_OVERFLOW); // means overflow
					pb.write_varint((uint) k.Length);
					pb.write_uint32 (overflows[q]);
				} else {
					write_byte_array_with_length (pb, k);
				}
			}

			// assert cur <= PAGE_SIZE.  actually, it has to be, since
			// an exception would have been thrown above if we overran the
			// buffer.
		}

		/*
		 * Each overflow page, after 1 byte for the page type and 1 byte for flags,
		 * has a 32-bit int which is the number of pages left in this overflow value.
		 * 
		 * It would be nice to make this a varint, but that would be problematic.
		 * We need to know in advance how many pages the value will consume.
		 */

		private static int count_overflow_pages_needed_for(int len)
		{
			int bytes_per_overflow_page = PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE;
			int this_overflow_needed = len / bytes_per_overflow_page;
			if ((len % bytes_per_overflow_page) != 0) {
				this_overflow_needed++;
			}
			return this_overflow_needed;
		}

		private static uint write_overflow_from_bytearray(PageBuilder pb, Stream fs, byte[] ba)
		{
			return write_overflow_from_stream (pb, fs, new MemoryStream (ba));
		}

		private static uint write_overflow_from_stream(PageBuilder pb, Stream fs, Stream ba)
		{
			int sofar = 0;
			int needed = count_overflow_pages_needed_for ((int) ba.Length);

			int count = 0;
			while (sofar < ba.Length) {
				pb.Reset ();
				pb.write_byte (OVERFLOW_NODE);
				pb.write_byte (0);
				pb.write_uint32 ((uint) (needed - count));
				int num = Math.Min ((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), (int) (ba.Length - sofar));
				pb.write_byte_stream (ba, num);
				sofar += num;
				pb.Flush (fs);
				count++;
			}
			return (uint) count;
		}

		private class leaf_node_state
		{
			public ushort count_pairs = 0;
			public byte[] last_key = null;
		}

		private static int calc_available(int current_size, bool could_be_root)
		{
			int n = (PAGE_SIZE - current_size);
			if (could_be_root)
			{
				// make space for the first_page and last_page fields
				n -= (2 * sizeof(UInt32));
			}
			return n;
		}

		private static List<node> write_parent_nodes(uint first_leaf, uint last_leaf, List<node> children, uint next_page_number, Stream fs, PageBuilder pb)
		{
			var next_generation = new List<node> ();

			int current_size = 0;
			Dictionary<int,uint> overflows = new Dictionary<int, uint> ();
			int first = 0;

			// assert children.Count > 1
			for (int i = 0; i < children.Count; i++) {
				node n = children [i];
				byte[] k = n.Key;

				int needed_for_inline = 1 
					+ varint.space_needed ((uint)k.Length) 
					+ k.Length 
					+ varint.space_needed (n.PageNumber);

				int needed_for_overflow = 1 
					+ varint.space_needed ((uint)k.Length) 
					+ sizeof(uint)
					+ varint.space_needed (n.PageNumber);

				bool b_last_child = false;
				if (i == (children.Count - 1)) {
					// there must be >1 items in the children list, so:
					// assert i>0
					// assert cur != null
					b_last_child = true;
				}
					
				if (current_size > 0) {
					bool flush_this_page = false;
					if (b_last_child) {
						flush_this_page = true;
					} else if (calc_available(current_size, (next_generation.Count == 0)) >= needed_for_inline) {
						// no problem.
					} else if ((PAGE_SIZE - PARENT_NODE_HEADER_SIZE) >= needed_for_inline) {
						// it won't fit here, but it would fully fit on the next page.
						flush_this_page = true;
					} else if (calc_available(current_size, (next_generation.Count == 0)) < needed_for_overflow) {
						// we can't even put this key in this page if we overflow it.
						flush_this_page = true;
					}

					bool b_this_is_the_root_node = false;
					if (b_last_child && (next_generation.Count == 0)) {
						b_this_is_the_root_node = true;
					}

					if (flush_this_page) {
						build_parent_page (b_this_is_the_root_node, first_leaf, last_leaf, overflows, pb, children, i, first);

						pb.Flush (fs);

						next_generation.Add (new node {PageNumber = next_page_number++, Key=children[i-1].Key});

						current_size = 0;
						first = 0;
						overflows.Clear();

                        if (b_last_child) {
							break;
						}					
					}
				}

				if (0 == current_size) {
					first = i;
					overflows.Clear();

					current_size += 2; // for the page type and the flags
					current_size += 2; // for the stored count
					current_size += 5; // for the extra pointer we'll add at the end, which is a varint, so 5 is the worst case
				}
					
				if (calc_available(current_size, (next_generation.Count == 0)) >= needed_for_inline) {
					current_size += k.Length;
				} else {
					// it's okay to pass our PageBuilder here for working purposes.  we're not
					// really using it yet, until we call build_parent_page
					uint overflow_page_number = next_page_number;
					uint overflow_page_count = write_overflow_from_bytearray (pb, fs, k);
					next_page_number += overflow_page_count;
					current_size += sizeof(uint);
					overflows [i] = overflow_page_number;
				}

				// inline or not, we need space for the following things

				current_size++; // for the flag
				current_size += varint.space_needed((uint) k.Length);
				current_size += varint.space_needed(n.PageNumber);
			}

			// assert cur is null

			return next_generation;
		}

		// TODO we probably want this function to accept a pagesize and base pagenumber
		public static uint Create(Stream fs, ICursor csr)
		{
			PageBuilder pb = new PageBuilder(PAGE_SIZE);
			PageBuilder pb2 = new PageBuilder(PAGE_SIZE);

			uint next_page_number = 1;

			var nodelist = new List<node> ();

			ushort count_pairs = 0;
			byte[] last_key = null;

			uint prev_page_number = 0;

			csr.First ();
			while (csr.IsValid ()) {
				byte[] k = csr.Key ();
				Stream v = csr.Value ();

				// assert k != null
				// for a tombstone, v might be null

				var needed_overflow_page_number = sizeof(uint);
				var needed_k_base = 1 + varint.space_needed((ulong) k.Length);
				var needed_k_inline = needed_k_base + k.Length;
				var needed_k_overflow = needed_k_base + needed_overflow_page_number;

				var needed_v_inline = 1 + ((v!=null) ? varint.space_needed((ulong) v.Length) + v.Length : 0);
				var needed_v_overflow = 1 + ((v!=null) ? varint.space_needed((ulong) v.Length) + needed_overflow_page_number : 0);

				var needed_for_inline_both = needed_k_inline + needed_v_inline;
				var needed_for_inline_key_overflow_val = needed_k_inline + needed_v_overflow;
				var needed_for_overflow_both = needed_k_overflow + needed_v_overflow;

				csr.Next ();

				if (pb.Position > 0) {
					// figure out if we need to just flush this page

					int avail = pb.Available();

					bool flush_this_page = false;
					if (avail >= needed_for_inline_both) {
						// no problem.  both the key and the value are going to fit
					} else if ((PAGE_SIZE - LEAF_HEADER_SIZE) >= needed_for_inline_both) {
						// it won't fit here, but it would fully fit on the next page.
						flush_this_page = true;
					} else if (avail >= needed_for_inline_key_overflow_val) {
						// the key will fit inline if we just overflow the val
					} else if (avail < needed_for_overflow_both) {
						// we can't even put this pair in this page if we overflow both.
						flush_this_page = true;
					}

					if (flush_this_page) {
						// TODO this code is duplicated with slight differences below, after the loop

						// now that we know how many pairs are in this page, we can write that out
						pb.write_uint16_at (OFFSET_COUNT_PAIRS, count_pairs);

						pb.Flush (fs);

						nodelist.Add (new node { PageNumber = next_page_number, Key = last_key });

						prev_page_number = next_page_number++;
						pb.Reset ();
						count_pairs = 0;
						last_key = null;
					}
				}

				if (pb.Position == 0) {
					// we could be here because we just flushed the page.
					// or because this is the very first page.
					count_pairs = 0;
					last_key = null;

					// 8 byte header

					pb.write_byte(LEAF_NODE);
					pb.write_byte(0); // flags

					pb.write_uint32 (prev_page_number); // prev page num.
					pb.write_uint16 (0); // number of pairs in this page. zero for now. written at end.
				} 

				int available = pb.Available();

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

				if (available >= needed_for_inline_both) {
					// no problem.  both the key and the value are going to fit
					write_byte_array_with_length (pb, k);
					write_byte_array_with_length (pb, v);
				} else {
					if (available >= needed_for_inline_key_overflow_val) {
						// the key will fit inline if we just overflow the val
						write_byte_array_with_length (pb, k);
					} else {
						// assert available >= needed_for_overflow_both

						uint k_overflow_page_number = next_page_number;
						uint k_overflow_page_count = write_overflow_from_bytearray (pb2, fs, k);
						next_page_number += k_overflow_page_count;

						pb.write_byte (FLAG_OVERFLOW);
						pb.write_varint ((uint)k.Length);
						pb.write_uint32 (k_overflow_page_number);
					}

					uint v_overflow_page_number = next_page_number;
					uint v_overflow_page_count = write_overflow_from_stream (pb2, fs, v);
					next_page_number += v_overflow_page_count;

					pb.write_byte (FLAG_OVERFLOW);
					pb.write_varint ((uint)v.Length);
					pb.write_uint32 (v_overflow_page_number);
				}

				last_key = k;
				count_pairs++;
			}

			if (pb.Position > 0) {
				// TODO this code is duplicated with slight differences from above

				// now that we know how many pairs are in this page, we can write that out
				pb.write_uint16_at (OFFSET_COUNT_PAIRS, count_pairs);

				pb.Flush (fs);

				nodelist.Add (new node { PageNumber = next_page_number++, Key = last_key });
			}

			if (nodelist.Count > 0) {
				uint first_leaf = nodelist [0].PageNumber;
				uint last_leaf = nodelist [nodelist.Count - 1].PageNumber;

				// now write the parent pages, maybe more than one level of them.  we have to get
				// down to a level with just one parent page in it, the root page.

				while (nodelist.Count > 1) {
					nodelist = write_parent_nodes (first_leaf, last_leaf, nodelist, next_page_number, fs, pb);
					next_page_number += (uint) nodelist.Count;
				}

				// assert nodelist.Count == 1

				return nodelist [0].PageNumber;
			} else {
				return 0;
			}
		}

		private class OverflowReadStream : Stream
		{
			private readonly Stream _fs;
			private readonly int _len;
			private int sofar_overall;
			private int sofar_thispage;
			private uint curpage;
			private byte[] buf = new byte[PAGE_SIZE];

			// TODO I suppose if the underlying stream can seek and if we kept
			// the first_page, we could seek or reset as well.

			public OverflowReadStream(Stream fs, uint first_page, int len)
			{
				_fs = fs;
				_len = len;

				curpage = first_page;

				ReadPage();
			}

			public override long Length {
				get {
					return _len;
				}
			}

			private void ReadPage()
			{
				uint pos = (curpage - 1) * PAGE_SIZE;
				_fs.Seek (pos, SeekOrigin.Begin);
				int got = utils.ReadFully (_fs, buf, 0, PAGE_SIZE);
				// assert got == PAGE_SIZE
				// assert buf[0] == OVERFLOW
				sofar_thispage = 0;
			}

			public override bool CanRead {
				get {
					return sofar_overall < _len;
				}
			}

			public override int Read (byte[] ba, int offset, int wanted)
			{
				if (sofar_thispage >= (PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE)) {
					if (sofar_overall < _len) {
						curpage++;
						ReadPage ();
					} else {
						return 0;
					}
				}

				int available = (int) Math.Min ((PAGE_SIZE - OVERFLOW_PAGE_HEADER_SIZE), _len - sofar_overall);
				int num = (int)Math.Min (available, wanted);
				Array.Copy (buf, OVERFLOW_PAGE_HEADER_SIZE + sofar_thispage, ba, offset, num);
				sofar_overall += num;
				sofar_thispage += num;

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

		private static byte[] read_overflow(int len, Stream fs, uint first_page)
		{
			var ostrm = new OverflowReadStream (fs, first_page, len);
			return utils.ReadAll (ostrm);
		}

		private class my_cursor : ICursor
		{
			private readonly Stream fs;
			private readonly long fs_length;
			private readonly PageReader pr = new PageReader(PAGE_SIZE);

			private uint cur_page = 0;

			private int[] leaf_keys;
			private uint prev_leaf;
			private int cur_key;

			public my_cursor(Stream strm, long length)
			{
				fs_length = length;
				fs = strm;
				leaf_reset();
			}

			private void leaf_reset()
			{
				leaf_keys = null;
				prev_leaf = 0;
				cur_key = -1;
			}

			private bool leaf_forward()
			{
				if ((cur_key + 1) < leaf_keys.Length) {
					cur_key++;
					return true;
				} else {
					return false;
				}
			}

			private bool leaf_backward()
			{
				if (cur_key > 0) {
					cur_key--;
					return true;
				} else {
					return false;
				}
			}

            private void skip_key()
            {
                byte kflag = pr.read_byte();
                int klen = (int) pr.read_varint ();
                if (0 == (kflag & FLAG_OVERFLOW)) {
                    pr.Skip (klen);
                } else {
                    pr.Skip(sizeof(uint));
                }
            }

			private void leaf_read()
			{
				leaf_reset ();
				pr.Reset ();
				if (pr.read_byte() != LEAF_NODE) { // TODO page_type()
					// TODO or, we could just return, and leave things in !valid() state
					throw new Exception ();
				}
				pr.read_byte (); // TODO pflag
				prev_leaf = pr.read_uint32 ();
				int count = pr.read_uint16 ();
				leaf_keys = new int[count];
				for (int i = 0; i < count; i++) {
					leaf_keys [i] = pr.Position;

                    skip_key();

					// need to skip the val
					byte vflag = pr.read_byte();
					int vlen = (int) pr.read_varint();
					if (0 != (vflag & FLAG_TOMBSTONE)) {
						// assert vlen is 0
					} else if (0 != (vflag & FLAG_OVERFLOW)) {
						// this is an overflow key.  ignore it.
						// just skip past its pagenum.  
                        pr.Skip(sizeof(uint));
					} else {
						pr.Skip(vlen);
					}
				}
			}

			private int leaf_cmp_key(int n, byte[] other)
			{
				pr.SetPosition(leaf_keys [n]);
				byte kflag = pr.read_byte();
				int klen = (int) pr.read_varint();
				if (0 == (kflag & FLAG_OVERFLOW)) {
					return pr.cmp (klen, other);
				} else {
					// TODO need to cmp the given key against an overflowed
					// key.  for now, we just retrieve the overflowed key
					// and compare it.  but this comparison could be done
					// without retrieving the whole thing.
					uint pagenum = pr.read_uint32 ();
					byte[] k = read_overflow(klen, fs, pagenum);
					return ByteComparer.cmp (k, other);
				}
			}

			private byte[] leaf_key(int n)
			{
				pr.SetPosition(leaf_keys [n]);
				byte kflag = pr.read_byte();
				int klen = (int) pr.read_varint();
				if (0 == (kflag & FLAG_OVERFLOW)) {
					return pr.read_byte_array (klen);
				} else {
					uint pagenum = pr.read_uint32 ();
					return read_overflow(klen, fs, pagenum);
				}
			}

			private int leaf_search(byte[] k, int min, int max, SeekOp sop)
			{
				int le = -1;
				int ge = -1;
				while (max >= min) {
					int mid = (max + min) / 2;
					int cmp = leaf_cmp_key (mid, k);
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

			private bool set_current_page(uint pagenum)
			{
				cur_page = pagenum;
				leaf_reset();
				if (0 == pagenum) {
					return false;
				}
				uint pos = (pagenum - 1) * PAGE_SIZE;
				if ((pos + PAGE_SIZE) <= fs_length) {
					fs.Seek (pos, SeekOrigin.Begin);
					pr.Read (fs);
					return true;
				} else {
					return false;
				}
			}

			private void start_rootpage_read()
			{
				pr.Reset ();
				if (pr.read_byte() != PARENT_NODE) {
					throw new Exception ();
				}
				byte pflag = pr.read_byte ();
				pr.Skip (sizeof(ushort));

				if (0 == (pflag & FLAG_ROOT_NODE)) {
					throw new Exception ();
				}
			}

			private uint get_rootpage_first_leaf()
			{
				start_rootpage_read ();

				var rootpage_first_leaf = pr.read_uint32 ();
				//var rootpage_last_leaf = pr.read_uint32 ();

				return rootpage_first_leaf;
			}

			private uint get_rootpage_last_leaf()
			{
				start_rootpage_read ();

				//var rootpage_first_leaf = pr.read_uint32 ();
				pr.Skip (sizeof(uint));
				var rootpage_last_leaf = pr.read_uint32 ();

				return rootpage_last_leaf;
			}

			private Tuple<uint[],byte[][]> parentpage_read()
			{
				pr.Reset ();
				if (pr.read_byte() != PARENT_NODE) {
					throw new Exception ();
				}
				byte pflag = pr.read_byte ();
				int count = (int) pr.read_uint16 ();
				var my_ptrs = new uint[count+1];
				var my_keys = new byte[count][];

				if (0 != (pflag & FLAG_ROOT_NODE)) {
					pr.Skip (2 * sizeof(uint));
				}
				// note "<= count" below
				for (int i = 0; i <= count; i++) {
					my_ptrs[i] = (uint) pr.read_varint();
				}
				// note "< count" below
				for (int i = 0; i < count; i++) {
					byte flag = pr.read_byte();
					int klen = (int) pr.read_varint();
					if (0 == (flag & FLAG_OVERFLOW)) {
						my_keys[i] = pr.read_byte_array(klen);
					} else {
						uint pagenum = pr.read_uint32 ();
						my_keys[i] = read_overflow (klen, fs, pagenum);
					}
				}
				return new Tuple<uint[],byte[][]> (my_ptrs, my_keys);
			}

			// this is used when moving forward through the leaf pages.
			// we need to skip any overflow pages.  when moving backward,
			// this is not necessary, because each leaf has a pointer to
			// the leaf before it.
			private bool search_forward_for_leaf()
			{
				while (true) {
					if (LEAF_NODE == pr.page_type()) {
						return true;
					}
					else if (PARENT_NODE == pr.page_type()) {
						// if we bump into a parent node, that means there are
						// no more leaves.
						return false;
					}
					else {
						// assert OVERFLOW == _buf[0]
						int cur = 2; // offset of the pages_remaining
						uint skip = pr.read_uint32 ();
						if (!set_current_page (cur_page + skip)) {
							return false;
						}
					}
				}
			}

			bool ICursor.IsValid()
			{
				// TODO curpagenum >= 1 ?
				return (leaf_keys != null) && (leaf_keys.Length > 0) && (cur_key >= 0) && (cur_key < leaf_keys.Length);
			}

			byte[] ICursor.Key()
			{
				return leaf_key(cur_key);
			}

			Stream ICursor.Value()
			{
				pr.SetPosition(leaf_keys [cur_key]);

                skip_key();

				// read the val

				byte vflag = pr.read_byte();
				int vlen = (int) pr.read_varint();
				if (0 != (vflag & FLAG_TOMBSTONE)) {
					return null;
				} else if (0 != (vflag & FLAG_OVERFLOW)) {
					uint pagenum = pr.read_uint32 ();
					return new OverflowReadStream (fs, pagenum, vlen);
				} else {
					return new MemoryStream(pr.read_byte_array (vlen));
				}
			}

			int ICursor.ValueLength()
			{
				pr.SetPosition(leaf_keys [cur_key]);

                skip_key();

				// read the val

				byte vflag = pr.read_byte();
				if (0 != (vflag & FLAG_TOMBSTONE)) {
					return -1;
				}
				int vlen = (int) pr.read_varint();
				return vlen;
			}

			int ICursor.KeyCompare(byte[] k)
			{
				return leaf_cmp_key (cur_key, k);
			}

			void ICursor.Seek(byte[] k, SeekOp sop)
			{
				// start at the last page, which is always the root of the tree.  
				// it might be the leaf, in a tree with just one node.

				uint pagenum = (uint) (fs_length / PAGE_SIZE);

				while (true) {
					if (!set_current_page (pagenum)) {
						break;
					}

					if (pr.page_type() == LEAF_NODE) {
						leaf_read ();

						cur_key = leaf_search (k, 0, leaf_keys.Length - 1, sop);

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
									if (set_current_page (cur_page + 1) && search_forward_for_leaf ()) {
										leaf_read ();
										cur_key = 0;
									} else {
										break;
									}
								} else {
									// assert SeekOp.SEEK_LE == sop
									if (0 == prev_leaf) {
										leaf_reset(); // TODO probably not needed?
										break;
									} else if (set_current_page(prev_leaf)) {
										leaf_read ();
										cur_key = leaf_keys.Length - 1;
									}
								}
							}
						}

					} else if (pr.page_type() == PARENT_NODE) {
						Tuple<uint[],byte[][]> tp = parentpage_read ();
						var my_ptrs = tp.Item1;
						var my_keys = tp.Item2;

						// TODO is linear search here the fastest way?
						uint found = 0;
						for (int i = 0; i < my_keys.Length; i++) {
							int cmp = ByteComparer.cmp (k, my_keys [i]);
							if (cmp <= 0) {
								found = my_ptrs [i];
								break;
							}
						}
						if (found == 0) {
							found = my_ptrs [my_ptrs.Length - 1];
						}
						pagenum = found;
					}
				}
			}

			void ICursor.First()
			{
				// start at the last page, which is always the root of the tree.  
				// it might be the leaf, in a tree with just one node.

				uint pagenum = (uint) (fs_length / PAGE_SIZE);
				if (set_current_page (pagenum)) {
					if (LEAF_NODE != pr.page_type()) {
						// assert _buf[1] & FLAG_ROOT_NODE
						set_current_page (get_rootpage_first_leaf()); // TODO don't ignore return val
					}
					leaf_read ();
					cur_key = 0;
				}
			}

			void ICursor.Last()
			{
				// start at the last page, which is always the root of the tree.  
				// it might be the leaf, in a tree with just one node.

				uint pagenum = (uint) (fs_length / PAGE_SIZE);
				if (set_current_page (pagenum)) {
					if (LEAF_NODE != pr.page_type()) {
						// assert _buf[1] & FLAG_ROOT_NODE
						set_current_page (get_rootpage_last_leaf()); // TODO don't ignore return val
					}
					leaf_read ();
					cur_key = leaf_keys.Length - 1;
				}
			}

			void ICursor.Next()
			{
				if (!leaf_forward()) {
					// need a new page
					if (set_current_page (cur_page + 1) && search_forward_for_leaf ()) {
						leaf_read ();
						cur_key = 0;
					}
				}
			}

			void ICursor.Prev()
			{
				if (!leaf_backward()) {
					// need a new page
					if (0 == prev_leaf) {
						leaf_reset();
					} else if (set_current_page(prev_leaf)) {
						leaf_read ();
						cur_key = leaf_keys.Length - 1;
					}
				}
			}

		}

		// TODO pass in a page size
		public static ICursor OpenCursor(Stream strm, long length)
		{
			return new my_cursor(strm, length);
		}

	}

}
