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

using Zumero.LSM;

namespace lsm_tests
{
	public abstract class combo
	{
		public abstract IWrite create_memory_segment();
		public abstract int create_btree_segment(Stream fs,IPages pageManager,ICursor csr);
		public abstract ICursor open_btree_segment(Stream fs,int pageSize,int rootPage);
		public abstract ICursor create_living_cursor(ICursor csr);
		public abstract ICursor create_multicursor(params ICursor[] a);

		public static List<combo> get_combos()
		{
			List<combo> a = new List<combo>();
			a.Add(new combo_fs());
			//a.Add(new combo_cs());
			//a.Add(new combo_fs_cs());
			//a.Add(new combo_cs_fs());
			return a;
		}
	}

    public class combo_cs : combo
    {
		public override IWrite create_memory_segment()
        {
            return Zumero.LSM.cs.MemorySegment.Create();
        }

		public override int create_btree_segment(Stream fs,IPages pageManager,ICursor csr)
        {
			return Zumero.LSM.cs.BTreeSegment.Create(fs, pageManager, csr.ToSequenceOfTuples());
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage)
        {
            return Zumero.LSM.cs.BTreeSegment.OpenCursor(fs,pageSize,rootPage);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.cs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.cs.MultiCursor.Create(a);
        }

    }

    public class combo_fs : combo
    {
		public override IWrite create_memory_segment()
        {
            return Zumero.LSM.fs.MemorySegment.Create();
        }

		public override int create_btree_segment(Stream fs,IPages pageManager,ICursor csr)
        {
			return Zumero.LSM.fs.BTreeSegment.Create(fs, pageManager, csr.ToSequenceOfTuples());
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage)
        {
            return Zumero.LSM.fs.BTreeSegment.OpenCursor(fs,pageSize,rootPage);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.fs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.fs.MultiCursor.Create(a);
        }

    }

    public class combo_cs_fs : combo
    {
		public override IWrite create_memory_segment()
        {
            return Zumero.LSM.cs.MemorySegment.Create();
        }

		public override int create_btree_segment(Stream fs,IPages pageManager,ICursor csr)
        {
			return Zumero.LSM.cs.BTreeSegment.Create(fs, pageManager, csr.ToSequenceOfTuples());
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage)
        {
            return Zumero.LSM.fs.BTreeSegment.OpenCursor(fs,pageSize,rootPage);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.fs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.fs.MultiCursor.Create(a);
        }

    }

    public class combo_fs_cs : combo
    {
		public override IWrite create_memory_segment()
        {
            return Zumero.LSM.fs.MemorySegment.Create();
        }

		public override int create_btree_segment(Stream fs,IPages pageManager,ICursor csr)
        {
			return Zumero.LSM.fs.BTreeSegment.Create(fs, pageManager, csr.ToSequenceOfTuples());
        }

		public override ICursor open_btree_segment(Stream fs,int pageSize,int rootPage)
        {
            return Zumero.LSM.cs.BTreeSegment.OpenCursor(fs,pageSize,rootPage);
        }

		public override ICursor create_living_cursor(ICursor csr)
        {
            return new Zumero.LSM.cs.LivingCursor(csr);
        }

		public override ICursor create_multicursor(params ICursor[] a)
        {
            return Zumero.LSM.cs.MultiCursor.Create(a);
        }

    }

}

