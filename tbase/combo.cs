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
using System.IO;
using System.Collections.Generic;

using Zumero.LSM;

namespace lsm_tests
{
	public class myCursor : ICursor
	{
		private readonly byte[][] keys;
		private readonly Dictionary<byte[],Stream> pairs;
		private int cur = -1;

		private class ByteComparer : IComparer<byte[]>
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

		public void Dispose()
		{
		}

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

	public static class exd
	{
		public static byte[] ToUTF8(this string s)
		{
			return System.Text.Encoding.UTF8.GetBytes (s);
		}

		public static string UTF8ToString(this byte[] ba)
		{
			return System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length);
		}

		public static void Seek(this ICursor csr, string k, SeekOp sop)
		{
			csr.Seek (k.ToUTF8(), sop);
		}

		public static void Insert(this Dictionary<byte[],Stream> d, byte[] k, byte[] v)
		{
			d [k] = new MemoryStream (v);
		}

		public static void Insert(this Dictionary<byte[],Stream> d, string k, string v)
		{
			d [k.ToUTF8()] = new MemoryStream(v.ToUTF8());
		}

		public static void Delete(this Dictionary<byte[],Stream> d, string k)
		{
			d [k.ToUTF8()] = null;
		}

		public static ICursor OpenCursor(this Dictionary<byte[],Stream> d)
		{
			return new myCursor(d);
		}
	}

	public abstract class combo
	{

		public Dictionary<byte[],Stream> create_memory_segment()
		{
			return new Dictionary<byte[], Stream> ();
		}

		public Tuple<Guid,int> create_btree_segment(Stream fs,IPages pageManager,ICursor csr)
		{
			return create_btree_segment (fs, pageManager, ICursorExtensions.ToSequenceOfKeyValuePairs (csr));
		}

		public int create_btree_segment(Stream fs,IPages pageManager,Dictionary<string,string> d)
		{
			return -1; // TODO
		}

		public abstract Tuple<Guid,int> create_btree_segment(Stream fs,IPages pageManager,IEnumerable<KeyValuePair<byte[],Stream>> source);
		public abstract ICursor open_btree_segment(Stream fs,int pageSize,int rootPage,Action<ICursor> hook);
		public abstract ICursor create_living_cursor(ICursor csr);
		public abstract ICursor create_multicursor(params ICursor[] a);

		public ICursor open_btree_segment(Stream fs,int pageSize,int rootPage)
		{
			return open_btree_segment (fs, pageSize, rootPage, null);
		}

		public static List<combo> get_combos()
		{
			List<combo> a = new List<combo>();
			a.Add(new combo_fs());
			a.Add(new combo_cs());
			a.Add(new combo_fs_cs());
			a.Add(new combo_cs_fs());
			return a;
		}
	}

    public class combo_cs : combo
    {
		public override Tuple<Guid,int> create_btree_segment(Stream fs,IPages pageManager,IEnumerable<KeyValuePair<byte[],Stream>> source)
        {
			return Zumero.LSM.cs.BTreeSegment.Create(fs, pageManager, source);
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage,Action<ICursor> hook)
        {
            return Zumero.LSM.cs.BTreeSegment.OpenCursor(fs,pageSize,rootPage,hook);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.cs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.cs.MultiCursor.Create(a);
        }

    }

    public class combo_fs : combo
    {
		public override Tuple<Guid,int> create_btree_segment(Stream fs,IPages pageManager,IEnumerable<KeyValuePair<byte[],Stream>> source)
        {
			return Zumero.LSM.fs.BTreeSegment.Create(fs, pageManager, source);
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage,Action<ICursor> hook)
        {
            return Zumero.LSM.fs.BTreeSegment.OpenCursor(fs,pageSize,rootPage,hook);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.fs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.fs.MultiCursor.Create(a);
        }

    }

    public class combo_cs_fs : combo
    {
		public override Tuple<Guid,int> create_btree_segment(Stream fs,IPages pageManager,IEnumerable<KeyValuePair<byte[],Stream>> source)
        {
			return Zumero.LSM.cs.BTreeSegment.Create(fs, pageManager, source);
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage,Action<ICursor> hook)
        {
            return Zumero.LSM.fs.BTreeSegment.OpenCursor(fs,pageSize,rootPage,hook);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.fs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.fs.MultiCursor.Create(a);
        }

    }

    public class combo_fs_cs : combo
    {
		public override Tuple<Guid,int> create_btree_segment(Stream fs,IPages pageManager,IEnumerable<KeyValuePair<byte[],Stream>> source)
        {
			return Zumero.LSM.fs.BTreeSegment.Create(fs, pageManager, source);
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage,Action<ICursor> hook)
        {
            return Zumero.LSM.cs.BTreeSegment.OpenCursor(fs,pageSize,rootPage,hook);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.cs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.cs.MultiCursor.Create(a);
        }

    }

}

