using System;
using System.IO;
using System.Collections.Generic;

using Xunit;

using Zumero.LSM;
using Zumero.LSM.fs;
using System.Threading;
using System.Threading.Tasks;
using lsm_tests;

namespace newTests
{
	public class MyClass
	{
		private static string tid()
		{
			return Guid.NewGuid ().ToString ().Replace ("{", "").Replace ("}", "").Replace ("-", "");
		}

		[Fact]
		public void empty_cursor()
		{
			var f = new dbf (tid());
			var db = new Zumero.LSM.fs.Database (f) as IDatabase;
			var csr = db.OpenCursor ();
			csr.First ();
			Assert.False (csr.IsValid ());
			csr.Last ();
			Assert.False (csr.IsValid ());
		}

		[Fact]
		public void several_threads()
		{
			var f = new dbf (tid());
			using (var db = new Zumero.LSM.fs.Database (f) as IDatabase) {
				var ta = new Thread[5];
				var ts = new Guid[ta.Length];

				ta[0] = new Thread(() => {
					var t1 = new Dictionary<byte[],Stream>();
					for (int i=0; i<5000; i++) {
						t1.Insert((i*2).ToString(), i.ToString());
					}
					ts[0] = db.WriteSegment (t1);
				});

				ta[1] = new Thread(() => {
					var t1 = new Dictionary<byte[],Stream>();
					for (int i=0; i<5000; i++) {
						t1.Insert((i*3).ToString(), i.ToString());
					}
					ts[1] = db.WriteSegment (t1);
				});

				ta[2] = new Thread(() => {
					var t1 = new Dictionary<byte[],Stream>();
					for (int i=0; i<5000; i++) {
						t1.Insert((i*5).ToString(), i.ToString());
					}
					ts[2] = db.WriteSegment (t1);
				});

				ta[3] = new Thread(() => {
					var t1 = new Dictionary<byte[],Stream>();
					for (int i=0; i<5000; i++) {
						t1.Insert((i*7).ToString(), i.ToString());
					}
					ts[3] = db.WriteSegment (t1);
				});

				ta[4] = new Thread(() => {
					var t1 = new Dictionary<byte[],Stream>();
					for (int i=0; i<5000; i++) {
						t1.Insert((i*11).ToString(), i.ToString());
					}
					ts[4] = db.WriteSegment (t1);
				});

				foreach (Thread t in ta) {
					t.Start();
				}

				foreach (Thread t in ta) {
					t.Join();
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

		[Fact]
		public void first_write()
		{
			var f = new dbf (tid());
			using (var db = new Zumero.LSM.fs.Database (f) as IDatabase) {
				// TODO consider whether we need IDatabase at all.  only
				// if we're going to do a C# version too, right?

				// build our data to be committed in memory in a
				// standard .NET dictionary.  note that are not
				// specifying IComparer here, so we're getting
				// reference compares, which is not safe unless you
				// know you're not inserting any duplicates.
				var mem = new Dictionary<byte[], Stream> ();
				for (int i = 0; i < 100; i++) {
					// extension method for .Insert(string,string)
					mem.Insert (i.ToString (), i.ToString ());
				}
				
				// write the segment to the file.  nobody knows about it
				// but us.  it will be written to the file, but the header
				// will not be touched yet, so its pages will be reclaimed
				// later unless this segment becomes official as a member
				// of the current state, listed in the header.
				var seg = db.WriteSegment (mem);

				// open a tx and add our segment to the current state.
				// after we do this, the segment is real.  "committed".
				using (var tx = db.RequestWriteLock ()) {
					var a = new List<Guid> { seg };
					tx.PrependSegments (a);
				}

				// open a cursor on the db and see if stuff looks okay
				using (var csr = db.OpenCursor ()) {
					csr.Seek (42.ToString (), SeekOp.SEEK_EQ);
					Assert.True (csr.IsValid ());
					csr.Next ();
					Assert.True (csr.IsValid ());
					string k = csr.Key ().UTF8ToString ();
					Assert.Equal ("43", k);
				}
			}
		}
	}
}

