
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;

namespace lsm_tests
{
	public static class foo
	{
		private const int PAGE_SIZE = 256;

		private static int lastPage(Stream fs)
		{
			return (int)(fs.Length / PAGE_SIZE);
		}

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

		public static string UTF8StreamToString(this Stream s)
		{
			return ReadAll (s).UTF8ToString ();
		}

		private static Stream openFile(string s)
		{
			return new FileStream (s, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
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

		private static class Assert
		{
			public static void True(bool b)
			{
				if (!b) {
					throw new Exception ();
				}
			}

			public static void Equal(long x, long y)
			{
				if (x != y) {
					throw new Exception ();
				}
			}
		}

	    public static void Main(string[] argv)
	    {
			Random r = new Random (501);
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				for (int i=0; i<1000; i++) {
					byte[] k = new byte[r.Next(10000)];
					byte[] v = new byte[r.Next(10000)];
					for (int q=0; q<k.Length; q++) {
						k[q] = (byte) r.Next(255);
					}
					for (int q=0; q<v.Length; q++) {
						v[q] = (byte) r.Next(255);
					}
					t1.Insert(k,v);
				}

				using (var fs = new FileStream ("blobs", FileMode.Create)) {
					IPages pageManager = new SimplePageManager(fs, PAGE_SIZE);
					int pg = c.create_btree_segment (fs, pageManager, t1.OpenCursor ());

					ICursor t1csr = t1.OpenCursor();
					ICursor btcsr = c.open_btree_segment(fs, PAGE_SIZE, pg);
					t1csr.First();
					while (t1csr.IsValid()) {

						var k = t1csr.Key();

						btcsr.Seek(k, SeekOp.SEEK_EQ);
						Assert.True(btcsr.IsValid());

						Assert.Equal(t1csr.ValueLength(), btcsr.ValueLength());

						var tv = ReadAll(t1csr.Value());
						var tb = ReadAll(btcsr.Value());
						int d = cmp(tv,tb);
						Assert.Equal(0, d);

						t1csr.Next();
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
	    }
	}

}
