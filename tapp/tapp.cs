
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
			Action<combo> f = (combo c) => {
				const string filename = "ten";

				var rootPages = new int[10];

				using (var fs = new FileStream (filename, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)) {
					IPages pageManager = new SimplePageManager(PAGE_SIZE);

					for (int i=0; i<10; i++) {
						var t1 = new Dictionary<byte[],Stream>();
						for (int q=0; q<50; q++) {
							t1.Insert((q*10+i).ToString("0000"), (i+q).ToString());
						}
						rootPages[i] = c.create_btree_segment (fs, pageManager, t1.OpenCursor ()).Item2;
						Console.WriteLine("rootPage[{0}] = {1}", i, rootPages[i]);
					}

					Console.WriteLine("pos home: {0}", (int) fs.Position);
					fs.Seek(0L, SeekOrigin.Begin);
					Console.WriteLine("pos now: {0}", (int) fs.Position);
					fs.Seek(0L, SeekOrigin.End);
					Console.WriteLine("pos now: {0}", (int) fs.Position);

					int s5;
					ICursor[] csrs = new ICursor[10];
					FileStream[] strms = new FileStream[10];
					for (int i=0; i<10; i++) {
						strms[i] = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
						csrs[i] = c.open_btree_segment(strms[i], PAGE_SIZE, rootPages[i]);
					}

					var mc = c.create_multicursor(csrs);
					s5 = c.create_btree_segment (fs, pageManager, mc).Item2;
					csrs = null;

					for (int i=0; i<10; i++) {
						strms[i].Close();
						strms[i] = null;
					}
					strms = null;

					{
						var csr = c.open_btree_segment(fs, PAGE_SIZE, s5);

						csr.First();
						int prev = -1;
						while (csr.IsValid()) {
							int cur = int.Parse(csr.Key().UTF8ToString());
							Assert.Equal(prev+1, cur);
							prev = cur;
							csr.Next();
						}
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
	    }
	}

}
