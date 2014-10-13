
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;

namespace lsm_tests
{
	// TODO maybe move this to tbase
	public static class hack
	{
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

	}

	public class foo
	{
		private const int PAGE_SIZE = 256;

		private static int lastPage(Stream fs)
		{
			return (int)(fs.Length / PAGE_SIZE);
		}

		private static Stream openFile(string s)
		{
			return new FileStream (s, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

	    public static void Main(string[] argv)
	    {
			Action<combo> f = (combo c) => {
				var s = "this is a longer string";
				for (int i = 0; i < 10; i++) {
					s = s + s;
				}

				{
					var t1 = c.create_memory_segment();
					t1.Insert ("k1", s);
					t1.Insert ("k2", s);
					t1.Insert ("k3", s);
					t1.Insert ("k4", s);

					using (var fs = new FileStream ("long_vals", FileMode.Create, FileAccess.ReadWrite)) {
						IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs = new FileStream ("long_vals", FileMode.Open, FileAccess.Read)) {
					var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

					csr.First ();
					while (csr.IsValid ()) {
						var k = csr.Key();
						if (2 != k.Length) {
							throw new Exception();
						}
						if (s.Length != csr.ValueLength()) {
							throw new Exception();
						}
						var v = csr.Value ();
						if (s != v.UTF8StreamToString()) {
							throw new Exception();
						}
						csr.Next ();
					}

					csr.Last ();
					while (csr.IsValid ()) {
						var k = csr.Key();
						if (2 != k.Length) {
							throw new Exception();
						}
						if (s.Length != csr.ValueLength()) {
							throw new Exception();
						}
						var v = csr.Value ();
						if (s != v.UTF8StreamToString()) {
							throw new Exception();
						}
						csr.Prev ();
					}
				}

				{
					var t1 = c.create_memory_segment();
					t1.Insert (s, "k1");
					t1.Insert (s + s, "k1");
					t1.Insert (s + s + s, "k1");
					t1.Insert (s + s + s + s, "k1");

					using (var fs = new FileStream ("long_keys", FileMode.Create, FileAccess.ReadWrite)) {
						IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, pageManager, t1.OpenCursor ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
	    }
	}

}
