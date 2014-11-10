
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;
using Zumero.LSM.fs;

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

		private static string tid()
		{
			return Guid.NewGuid ().ToString ().Replace ("{", "").Replace ("}", "").Replace ("-", "");
		}

	    public static void Main(string[] argv)
	    {
			var f = new dbf (tid());
			using (var db = new Zumero.LSM.fs.Database (f) as IDatabase) {
				var ta = new Thread[5];
				var ts = new Guid[ta.Length];

				const int AFTER = 100;

				ta [0] = new Thread (() => {
					//Console.WriteLine("0 sleeping");
					//Console.Out.Flush();
					//Thread.Sleep(1000);
					//Console.WriteLine("0 awake");
					//Console.Out.Flush();
					var t1 = new Dictionary<byte[],Stream> ();
					for (int i = 0; i < 5000; i++) {
						t1.Insert ((i * 2).ToString (), i.ToString ());
					}
					//Console.WriteLine("0 writing");
					//Console.Out.Flush();
					ts[0] = db.WriteSegment (t1);
					//Console.WriteLine("0 done");
					//Console.Out.Flush();
					Thread.Sleep(AFTER);
				});

				ta [1] = new Thread (() => {
					//Console.WriteLine("1 sleeping");
					//Console.Out.Flush();
					//Thread.Sleep(1000);
					//Console.WriteLine("1 awake");
					//Console.Out.Flush();
					var t1 = new Dictionary<byte[],Stream> ();
					for (int i = 0; i < 5000; i++) {
						t1.Insert ((i * 3).ToString (), i.ToString ());
					}
					//Console.WriteLine("1 writing");
					//Console.Out.Flush();
					ts [1] = db.WriteSegment (t1);
					//Console.WriteLine("1 done");
					//Console.Out.Flush();
					Thread.Sleep(AFTER);
				});

				ta [2] = new Thread (() => {
					//Console.WriteLine("2 sleeping");
					//Console.Out.Flush();
					//Thread.Sleep(1000);
					//Console.WriteLine("2 awake");
					//Console.Out.Flush();
					var t1 = new Dictionary<byte[],Stream> ();
					for (int i = 0; i < 5000; i++) {
						t1.Insert ((i * 5).ToString (), i.ToString ());
					}
					//Console.WriteLine("2 writing");
					//Console.Out.Flush();
					ts [2] = db.WriteSegment (t1);
					//Console.WriteLine("2 done");
					//Console.Out.Flush();
					Thread.Sleep(AFTER);
				});

				ta [3] = new Thread (() => {
					//Console.WriteLine("3 sleeping");
					//Console.Out.Flush();
					//Thread.Sleep(1000);
					//Console.WriteLine("3 awake");
					//Console.Out.Flush();
					var t1 = new Dictionary<byte[],Stream> ();
					for (int i = 0; i < 5000; i++) {
						t1.Insert ((i * 7).ToString (), i.ToString ());
					}
					//Console.WriteLine("3 writing");
					//Console.Out.Flush();
					ts [3] = db.WriteSegment (t1);
					//Console.WriteLine("3 done");
					//Console.Out.Flush();
					Thread.Sleep(AFTER);
				});

				ta [4] = new Thread (() => {
					//Console.WriteLine("4 sleeping");
					//Console.Out.Flush();
					//Thread.Sleep(1000);
					//Console.WriteLine("4 awake");
					//Console.Out.Flush();
					var t1 = new Dictionary<byte[],Stream> ();
					for (int i = 0; i < 5000; i++) {
						t1.Insert ((i * 11).ToString (), i.ToString ());
					}
					//Console.WriteLine("4 writing");
					//Console.Out.Flush();
					ts [4] = db.WriteSegment (t1);
					//Console.WriteLine("4 done");
					//Console.Out.Flush();
					Thread.Sleep(AFTER);
				});

				Console.WriteLine ("starting threads");
				Console.Out.Flush ();

				foreach (Thread t in ta) {
					t.Start ();
				}

				foreach (Thread t in ta) {
					t.Join ();
				}

				using (var tx = db.RequestWriteLock ()) {
					tx.PrependSegments (ts);
				}

				using (var csr = db.OpenCursor ()) {
					csr.First ();
					int count = 0;
					while (csr.IsValid ()) {
						count++;
						csr.Next ();
					}
					//Assert.Equal (20000, count);
				}
			}
	    }
	}

}
