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

//using Xunit;

using Zumero.LSM;

namespace lsm_tests
{
	public class combo
	{
		public Func<IWrite> create_memory_segment;
		public Func<Stream,int,IPages,ICursor,int> create_btree_segment;
		public Func<Stream,int,int,ICursor> open_btree_segment;
		public Func<ICursor,ICursor> create_living_cursor;
		public Func<ICursor,ICursor,ICursor> create_multicursor;

		public static combo make_cs()
		{
			combo c = new combo ();
			c.create_memory_segment = Zumero.LSM.cs.MemorySegment.Create;
			c.create_btree_segment = Zumero.LSM.cs.BTreeSegment.Create;
			c.open_btree_segment = Zumero.LSM.cs.BTreeSegment.OpenCursor;
			c.create_living_cursor = (ICursor csr) => new Zumero.LSM.cs.LivingCursor(csr);
			c.create_multicursor = (ICursor a, ICursor b) => Zumero.LSM.cs.MultiCursor.create (a, b);
			return c;
		}

		public static combo make_fs()
		{
			combo c = new combo ();
			c.create_memory_segment = Zumero.LSM.fs.MemorySegment.Create;
			c.create_btree_segment = Zumero.LSM.fs.BTreeSegment.Create;
			c.open_btree_segment = Zumero.LSM.fs.BTreeSegment.OpenCursor;
			c.create_living_cursor = (ICursor csr) => new Zumero.LSM.fs.LivingCursor(csr);
			c.create_multicursor = (ICursor a, ICursor b) => Zumero.LSM.fs.MultiCursor.create (a, b);
			return c;
		}

		public static combo make_cs_fs()
		{
			combo c = new combo ();
			c.create_memory_segment = Zumero.LSM.cs.MemorySegment.Create;
			c.create_btree_segment = Zumero.LSM.cs.BTreeSegment.Create;
			c.open_btree_segment = Zumero.LSM.fs.BTreeSegment.OpenCursor;
			c.create_living_cursor = (ICursor csr) => new Zumero.LSM.fs.LivingCursor(csr);
			c.create_multicursor = (ICursor a, ICursor b) => Zumero.LSM.fs.MultiCursor.create (a, b);
			return c;
		}

		public static combo make_fs_cs()
		{
			combo c = new combo ();
			c.create_memory_segment = Zumero.LSM.fs.MemorySegment.Create;
			c.create_btree_segment = Zumero.LSM.fs.BTreeSegment.Create;
			c.open_btree_segment = Zumero.LSM.cs.BTreeSegment.OpenCursor;
			c.create_living_cursor = (ICursor csr) => new Zumero.LSM.cs.LivingCursor(csr);
			c.create_multicursor = (ICursor a, ICursor b) => Zumero.LSM.cs.MultiCursor.create (a, b);
			return c;
		}

		public static List<combo> get_combos()
		{
			List<combo> a = new List<combo>();
			a.Add(combo.make_cs());
			a.Add(combo.make_fs());
			a.Add(combo.make_cs_fs());
			a.Add(combo.make_fs_cs());
			return a;
		}
	}

}

