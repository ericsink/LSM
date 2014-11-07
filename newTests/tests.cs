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
		public void first_write()
		{
			var f = new dbf ("foo");
			var db = new Zumero.LSM.fs.Database (f) as IDatabase;
			// TODO consider whether we need IDatabase at all.  only
			// if we're going to a C# version too, right?
			var mem = new Dictionary<byte[], Stream> ();
			for (int i = 0; i < 100; i++) {
				mem.Insert (i.ToString (), i.ToString ());
			}
			var csr = mem.OpenCursor ();
			var seq = ICursorExtensions.ToSequenceOfKeyValuePairs (csr);
			db.WriteSegment (seq);
		}
	}
}

