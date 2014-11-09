using System;
using System.IO;
using System.Collections.Generic;

using Xunit;

using Zumero.LSM;
using Zumero.LSM.fs;
using lsm_tests;

namespace newTests
{
	public class MyClass
	{
		[Fact]
		public void empty_cursor()
		{
			var f = new dbf (Guid.NewGuid().ToString().Replace("{", "").Replace("}","").Replace("-",""));
			var db = new Zumero.LSM.fs.Database (f) as IDatabase;
			var csr = db.BeginRead ();
			csr.First ();
			Assert.False (csr.IsValid ());
			csr.Last ();
			Assert.False (csr.IsValid ());
		}

		[Fact]
		public void first_write()
		{
			var f = new dbf ("foo");
			var db = new Zumero.LSM.fs.Database (f) as IDatabase;
			// TODO consider whether we need IDatabase at all.  only
			// if we're going to a C# version too, right?

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
			// but us.  it will be written to the segment info list,
			// but it's not in the current state, so its pages will be
			// reclaimed later unless it gets there.
			var seg = db.WriteSegment (mem);

			// open a tx and add our segment to the current state
			var tx = db.BeginTransaction ();
			var a = new List<Guid> { seg.Item1 };
			tx.Commit (a);
		}
	}
}

