
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

		private static async void wait(IDatabase db, Guid[] ts)
		{
			using (var tx = await db.RequestWriteLock ()) {
				tx.PrependSegments (ts);
			}

		}

		private static Task<Guid> start(int i, Random rand, IDatabase db)
		{
			return Task<Guid>.Run(async () => {
				var q1 = DateTime.Now;
				var t1 = new Dictionary<byte[],Stream>();
				int count = rand.Next(10000);
				for (int q=0; q<count; q++) {
					t1.Insert(rand.Next().ToString(), rand.Next().ToString());
				}
				var q2 = DateTime.Now;
				Console.WriteLine("{0}: dict = {1}", i, (q2-q1).TotalMilliseconds);
				var g = db.WriteSegment (t1);
				var q3 = DateTime.Now;
				Console.WriteLine("{0}: segment = {1}", i, (q3-q2).TotalMilliseconds);
				using (var tx = await db.RequestWriteLock ()) {
					var q4 = DateTime.Now;
					Console.WriteLine("{0}: lock = {1}", i, (q4-q3).TotalMilliseconds);
					tx.PrependSegments (new List<Guid> {g});
					var q5 = DateTime.Now;
					Console.WriteLine("{0}: commit = {1}", i, (q5-q4).TotalMilliseconds);
				}
				return g;
			});
		}

	    public static void Main(string[] argv)
	    {
			Random rand = new Random ();
			var f = new dbf ("several_tasks_" + tid());
			using (var db = new Zumero.LSM.fs.Database (f) as IDatabase) {
				const int NUM_TASKS = 500;
				Task<Guid>[] ta = new Task<Guid>[NUM_TASKS];
				for (int i = 0; i < NUM_TASKS; i++) {
					ta [i] = start (i, rand, db);
				}

				Console.WriteLine("WAITING");

				var q6 = DateTime.Now;
				Task.WaitAll (ta);
				var q7 = DateTime.Now;
				Console.WriteLine("FINISH = {0}", (q7-q6).TotalMilliseconds);

				#if not
				using (var csr = db.OpenCursor ()) {
					csr.First ();
					int count = 0;
					while (csr.IsValid ()) {
						count++;
						csr.Next ();
					}
					Console.Out.WriteLine ("total {0} records", count);
				}
				var q8 = DateTime.Now;
				Console.WriteLine("    cursor = {0}", (q8-q7).TotalMilliseconds);
				#endif
			}
	    }
	}

}
