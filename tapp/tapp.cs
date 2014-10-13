
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;
//using Zumero.LSM.fs;

namespace lsm_tests
{

	public static class hack
	{
	    public static string from_utf8(this Stream s)
	    {
	        // note the arbitrary choice of getting this function from cs instead of fs
	        // maybe utils should move into LSM_base
	        return Zumero.LSM.fs.utils.ReadAll (s).UTF8ToString ();
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
				int s1;
				int s2;
				int s3;
				int s4;

				using (var fsPageManager = new FileStream ("one_file", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)) {
					IPages pageManager = new SimplePageManager(fsPageManager);

					var ta = new Thread[4];
					var ts = new int[4];

					ta[0] = new Thread(() => {
					using (var fs = new FileStream ("one_file", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
						var t1 = c.create_memory_segment();
						for (int i=0; i<50000; i++) {
							t1.Insert((i*2).ToString(), i.ToString());
						}
						ts[0] = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
					});

					ta[1] = new Thread(() => {
						using (var fs = new FileStream ("one_file", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
						var t1 = c.create_memory_segment();
						for (int i=0; i<50000; i++) {
							t1.Insert((i*3).ToString(), i.ToString());
						}
						ts[1] = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
					});

					ta[2] = new Thread(() => {
						using (var fs = new FileStream ("one_file", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
						var t1 = c.create_memory_segment();
						for (int i=0; i<50000; i++) {
							t1.Insert((i*5).ToString(), i.ToString());
						}
						ts[2] = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
					});

					ta[3] = new Thread(() => {
						using (var fs = new FileStream ("one_file", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
						var t1 = c.create_memory_segment();
						for (int i=0; i<50000; i++) {
							t1.Insert((i*7).ToString(), i.ToString());
						}
						ts[3] = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
					});

					foreach (Thread t in ta) {
						t.Start();
					}
						
					foreach (Thread t in ta) {
						t.Join();
					}

					s1 = ts[0];
					s2 = ts[1];
					s3 = ts[2];
					s4 = ts[3];

					// TODO combo needs a way to get a multicursor with >2 subs

					int s1_2;
					int s3_4;
					using (var fs = new FileStream ("one_file", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {

						using (var fs1 = openFile("one_file")) {
							var csr1 = c.open_btree_segment (fs1, PAGE_SIZE, s1);
							using (var fs2 = openFile("one_file")) {
								var csr2 = c.open_btree_segment (fs2, PAGE_SIZE, s2);
								var mc = c.create_multicursor(csr1, csr2);
								s1_2 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, mc);
							}
						}

						using (var fs3 = openFile("one_file")) {
							var csr3 = c.open_btree_segment (fs3, PAGE_SIZE, s3);
							using (var fs4 = openFile("one_file")) {
								var csr4 = c.open_btree_segment (fs4, PAGE_SIZE, s4);
								var mc = c.create_multicursor(csr3, csr4);
								s3_4 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, mc);
							}
						}
					}

					using (var fs = new FileStream ("one_file", FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
						int s5;

						using (var fs1_2 = openFile("one_file")) {
							var csr1_2 = c.open_btree_segment (fs1_2, PAGE_SIZE, s1_2);
							using (var fs3_4 = openFile("one_file")) {
								var csr3_4 = c.open_btree_segment (fs3_4, PAGE_SIZE, s3_4);
								var mc = c.create_multicursor(csr1_2, csr3_4);
								s5 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, mc);
							}
						}

						{
							var csr = c.open_btree_segment(fs, PAGE_SIZE, s5);

							csr.First();
							while (csr.IsValid()) {
								csr.Next();
							}
						}
					}
				}
			};
			f (combo.make_cs ());
	    }
	}

}
