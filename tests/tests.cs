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

using Xunit;

using Zumero.LSM;

namespace lsm_tests
{
    // TODO move page manager into combo and implement it in both cs and ls
    public class SimplePageManager : IPages
    {
        private readonly Stream fs;
		int cur = 1;

        public SimplePageManager(Stream _fs)
        {
            fs = _fs;
        }

        Tuple<int,int> IPages.GetRange()
        {
			var t = new Tuple<int,int>(cur,cur + 9);
			cur = cur + 13;
			return t;
        }
    }

	public class Class1
	{
		private const int PAGE_SIZE = 256;

		private int lastPage(Stream fs)
		{
			return (int)(fs.Length / PAGE_SIZE);
		}

		private Stream openFile(string s)
		{
			return new FileStream (s, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

		[Fact]
		public void one_file()
		{
			Action<combo> f = (combo c) => {
				int s1;
				int s2;
				int s3;
				int s4;
				IPages pageManager = new SimplePageManager(null); // TODO doesn't use fs anyway
				using (var fs = new FileStream ("one_file", FileMode.Create, FileAccess.ReadWrite, FileShare.None)) {
					{
						var t1 = c.create_memory_segment();
						for (int i=0; i<500; i++) {
							t1.Insert((i*2).ToString(), i.ToString());
						}
						s1 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}

					{
						var t1 = c.create_memory_segment();
						for (int i=0; i<500; i++) {
							t1.Insert((i*3).ToString(), i.ToString());
						}
						s2 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}

					{
						var t1 = c.create_memory_segment();
						for (int i=0; i<500; i++) {
							t1.Insert((i*5).ToString(), i.ToString());
						}
						s3 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}

					{
						var t1 = c.create_memory_segment();
						for (int i=0; i<500; i++) {
							t1.Insert((i*7).ToString(), i.ToString());
						}
						s4 = c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

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
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void lexographic()
		{
			Action<ICursor> do_checks = (ICursor csr) => {
				// --------
				csr.First();
				Assert.True(csr.IsValid());
				Assert.Equal ("10", csr.Key ().FromUTF8 ());

				csr.Next();
				Assert.True(csr.IsValid());
				Assert.Equal ("20", csr.Key ().FromUTF8 ());

				csr.Next();
				Assert.True(csr.IsValid());
				Assert.Equal ("8", csr.Key ().FromUTF8 ());

				csr.Next();
				Assert.False(csr.IsValid());

				// --------
				csr.Last();
				Assert.True(csr.IsValid());
				Assert.Equal ("8", csr.Key ().FromUTF8 ());

				csr.Prev();
				Assert.True(csr.IsValid());
				Assert.Equal ("20", csr.Key ().FromUTF8 ());

				csr.Prev();
				Assert.True(csr.IsValid());
				Assert.Equal ("10", csr.Key ().FromUTF8 ());

				csr.Prev();
				Assert.False(csr.IsValid());
			};

			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				t1.Insert("8", "");
				t1.Insert("10", "");
				t1.Insert("20", "");

				{
					ICursor csr = t1.OpenCursor();

					do_checks(csr);

					using (var fs = new FileStream ("lexographic", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment (fs, PAGE_SIZE, pageManager, csr);

						do_checks(c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs)));
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void weird()
		{
			Action<combo> f = (combo c) => {
				{
					var t1 = c.create_memory_segment();
					for (int i=0; i<100; i++) {
						t1.Insert(i.ToString("000"), i.ToString());
					}

					using (var fs = new FileStream ("weird1", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}
				{
					var t1 = c.create_memory_segment();
					for (int i=0; i<1000; i++) {
						t1.Insert(i.ToString("00000"), i.ToString());
					}

					using (var fs = new FileStream ("weird2", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs1 = new FileStream ("weird1", FileMode.Open, FileAccess.Read)) {
					var csr1 = c.open_btree_segment(fs1, PAGE_SIZE, lastPage(fs1));
					using (var fs2 = new FileStream ("weird2", FileMode.Open, FileAccess.Read)) {
						var csr2 = c.open_btree_segment(fs2, PAGE_SIZE, lastPage(fs2));

						var mc = c.create_multicursor(csr1, csr2);

						mc.First();
						for (int i=0; i<100; i++) {
							mc.Next();
							Assert.True(mc.IsValid());
						}
						for (int i=0; i<50; i++) {
							mc.Prev();
							Assert.True(mc.IsValid());
						}
						for (int i=0; i<100; i++) {
							mc.Next();
							Assert.True(mc.IsValid());
							mc.Next();
							Assert.True(mc.IsValid());
							mc.Prev();
							Assert.True(mc.IsValid());
						}
						for (int i=0; i<50; i++) {
							mc.Seek(mc.Key(), SeekOp.SEEK_EQ);
							Assert.True(mc.IsValid());
							mc.Next();
							Assert.True(mc.IsValid());
						}
						for (int i=0; i<50; i++) {
							mc.Seek(mc.Key(), SeekOp.SEEK_EQ);
							Assert.True(mc.IsValid());
							mc.Prev();
							Assert.True(mc.IsValid());
						}
						for (int i=0; i<50; i++) {
							mc.Seek(mc.Key(), SeekOp.SEEK_LE);
							Assert.True(mc.IsValid());
							mc.Prev();
							Assert.True(mc.IsValid());
						}
						for (int i=0; i<50; i++) {
							mc.Seek(mc.Key(), SeekOp.SEEK_GE);
							Assert.True(mc.IsValid());
							mc.Next();
							Assert.True(mc.IsValid());
						}
						string s = mc.Key().FromUTF8();
						// got the following value from the debugger.
						// just want to make sure that it doesn't change
						// and all combos give the same answer.
						Assert.Equal("00148", s); 
						//Console.WriteLine("{0}", s);
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void blobs()
		{
			Random r = new Random ();
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				for (int i=0; i<1000; i++) {
					byte[] k = new byte[r.Next(10000)];
					byte[] v = new byte[r.Next(10000)];
					for (int q=0; q<k.Length; q++) {
						k[q] = (byte) r.Next(255);
					}
					for (int q=0; q<v.Length; q++) {
						v[q] = (byte) r.Next(255);
					}
					t1.Insert(k,v);
				}

				using (var fs = new FileStream ("blobs", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void simple()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				t1.Insert ("c", "3");
				t1.Insert ("e", "5");
				t1.Insert ("g", "7");

				{
					var csr = t1.OpenCursor ();

					csr.First ();
					while (csr.IsValid ()) {
						csr.Next ();
					}
				}

				using (var fs = new FileStream ("simple", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
				}

				using (var fs = new FileStream ("simple", FileMode.Open, FileAccess.Read)) {
					var csr = c.open_btree_segment (fs, PAGE_SIZE, lastPage(fs));

					csr.First ();
					while (csr.IsValid ()) {
						csr.Next ();
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void hundredk()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment ();
				for (int i = 0; i < 100000; i++) {
					t1.Insert ((i * 2).ToString (), i.ToString ());
				}

				using (var fs = new FileStream ("hundredk", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment (fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void no_le_ge_multicursor()
		{
			Action<combo> f = (combo c) => {
				{
					var t1 = c.create_memory_segment();
					t1.Insert ("c", "3");
					t1.Insert ("g", "7");

					using (var fs = new FileStream ("no_le_ge_multicursor_1", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				{
					var t1 = c.create_memory_segment();
					t1.Insert ("e", "5");

					using (var fs = new FileStream ("no_le_ge_multicursor_2", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs1 = new FileStream ("no_le_ge_multicursor_1", FileMode.Open, FileAccess.Read)) {
					var csr1 = c.open_btree_segment(fs1, PAGE_SIZE, lastPage(fs1));
					using (var fs2 = new FileStream ("no_le_ge_multicursor_2", FileMode.Open, FileAccess.Read)) {
						var csr2 = c.open_btree_segment(fs2, PAGE_SIZE, lastPage(fs2));

						var csr = c.create_multicursor(csr2, csr1);

						csr.Seek ("a", SeekOp.SEEK_LE);
						Assert.False (csr.IsValid ());

						csr.Seek ("d", SeekOp.SEEK_LE);
						Assert.True (csr.IsValid ());

						csr.Seek ("f", SeekOp.SEEK_GE);
						Assert.True (csr.IsValid ());

						csr.Seek ("h", SeekOp.SEEK_GE);
						Assert.False (csr.IsValid ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);

		}

		[Fact]
		public void no_le_ge()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				t1.Insert ("c", "3");
				t1.Insert ("e", "5");
				t1.Insert ("g", "7");

				{
					var csr = t1.OpenCursor ();

					csr.Seek ("a", SeekOp.SEEK_LE);
					Assert.False (csr.IsValid ());

					csr.Seek ("d", SeekOp.SEEK_LE);
					Assert.True (csr.IsValid ());

					csr.Seek ("f", SeekOp.SEEK_GE);
					Assert.True (csr.IsValid ());

					csr.Seek ("h", SeekOp.SEEK_GE);
					Assert.False (csr.IsValid ());
				}

				using (var fs = new FileStream ("no_le_ge", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
				}

				using (var fs = new FileStream ("no_le_ge", FileMode.Open, FileAccess.Read)) {
					var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

					csr.Seek ("a", SeekOp.SEEK_LE);
					Assert.False (csr.IsValid ());

					csr.Seek ("d", SeekOp.SEEK_LE);
					Assert.True (csr.IsValid ());

					csr.Seek ("f", SeekOp.SEEK_GE);
					Assert.True (csr.IsValid ());

					csr.Seek ("h", SeekOp.SEEK_GE);
					Assert.False (csr.IsValid ());
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void long_vals()
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
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs = new FileStream ("long_vals", FileMode.Open, FileAccess.Read)) {
					var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

					csr.First ();
					while (csr.IsValid ()) {
						var k = csr.Key();
						Assert.Equal (2, k.Length);
						Assert.Equal (s.Length, csr.ValueLength ());
						csr.Next ();
					}

					csr.Last ();
					while (csr.IsValid ()) {
						var v = csr.Value ();
						Assert.Equal (s, v.from_utf8());
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
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);

		}

		[Fact]
		public void seek_ge_le()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				t1.Insert ("a", "1");
				t1.Insert ("c", "3");
				t1.Insert ("e", "5");
				t1.Insert ("g", "7");
				t1.Insert ("i", "9");
				t1.Insert ("k", "11");
				t1.Insert ("m", "13");
				t1.Insert ("o", "15");
				t1.Insert ("q", "17");
				t1.Insert ("s", "19");
				t1.Insert ("u", "21");
				t1.Insert ("w", "23");
				t1.Insert ("y", "25");

				Assert.Equal (13, count_keys_forward (t1.OpenCursor ()));
				Assert.Equal (13, count_keys_backward (t1.OpenCursor ()));

				using (var fs = new MemoryStream()) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor());

					{
						var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

						Assert.Equal (13, count_keys_forward (csr));
						Assert.Equal (13, count_keys_backward (csr));

						csr.Seek ("n", SeekOp.SEEK_EQ);
						Assert.False (csr.IsValid ());

						csr.Seek ("n", SeekOp.SEEK_LE);
						Assert.True (csr.IsValid ());
						Assert.Equal ("m", csr.Key ().FromUTF8 ());

						csr.Seek ("n", SeekOp.SEEK_GE);
						Assert.True (csr.IsValid ());
						Assert.Equal ("o", csr.Key ().FromUTF8 ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void seek_ge_le_bigger()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				for (int i = 0; i < 10000; i++) {
					t1.Insert ((i * 2).ToString (), i.ToString ());
				}

				using (var fs = new FileStream("test_seek_ge_le_bigger", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor());

					{
						var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

						csr.Seek ("8088", SeekOp.SEEK_EQ);
						Assert.True (csr.IsValid ());

						csr.Seek ("8087", SeekOp.SEEK_EQ);
						Assert.False (csr.IsValid ());

						csr.Seek ("8087", SeekOp.SEEK_LE);
						Assert.True (csr.IsValid ());
						Assert.Equal ("8086", csr.Key ().FromUTF8 ());

						csr.Seek ("8087", SeekOp.SEEK_GE);
						Assert.True (csr.IsValid ());
						Assert.Equal ("8088", csr.Key ().FromUTF8 ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void seek_ge_le_bigger_multicursor()
		{
			Action<combo> f = (combo c) => {
				{
					var t1 = c.create_memory_segment();
					for (int i = 0; i < 10000; i++) {
						t1.Insert ((i * 4).ToString ("0000000000"), i.ToString ());
					}

					using (var fs = new FileStream ("test_seek_ge_le_bigger_multicursor_4", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}
				{
					var t1 = c.create_memory_segment();
					for (int i = 0; i < 10000; i++) {
						t1.Insert ((i * 7).ToString ("0000000000"), i.ToString ());
					}

					using (var fs = new FileStream ("test_seek_ge_le_bigger_multicursor_7", FileMode.Create)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs_4 = new FileStream ("test_seek_ge_le_bigger_multicursor_4", FileMode.Open, FileAccess.Read)) {
					var csr_4 = c.open_btree_segment(fs_4, PAGE_SIZE, lastPage(fs_4));
					using (var fs_7 = new FileStream ("test_seek_ge_le_bigger_multicursor_7", FileMode.Open, FileAccess.Read)) {
						var csr_7 = c.open_btree_segment(fs_7, PAGE_SIZE, lastPage(fs_7));

						var csr = c.create_multicursor(csr_7, csr_4);

						csr.Seek ("0000002330", SeekOp.SEEK_EQ);
						Assert.False (csr.IsValid ());

						csr.Seek ("0000002330", SeekOp.SEEK_LE);
						Assert.True (csr.IsValid ());
						Assert.Equal ("0000002328", csr.Key ().FromUTF8 ());

						csr.Seek ("0000002330", SeekOp.SEEK_GE);
						Assert.True (csr.IsValid ());
						Assert.Equal ("0000002331", csr.Key ().FromUTF8 ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);

		}

		[Fact]
		public void delete_not_there()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();

				Assert.Equal (0, count_keys_forward (t1.OpenCursor ()));
				Assert.Equal (0, count_keys_backward (t1.OpenCursor ()));

				t1.Delete("");
				t1.Delete("2");
				t1.Delete("3");

				Assert.Equal (3, count_keys_forward (t1.OpenCursor ()));
				Assert.Equal (3, count_keys_backward (t1.OpenCursor ()));

				var csr = c.create_living_cursor(t1.OpenCursor());
				Assert.Equal (0, count_keys_forward (csr));
				Assert.Equal (0, count_keys_backward (csr));
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		#if not
		[Fact]
		public void empty_segment()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();

				Assert.Equal (0, count_keys_forward (t1.OpenCursor ()));
				Assert.Equal (0, count_keys_backward (t1.OpenCursor ()));

				using (var fs = new MemoryStream()) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment(fs, PAGE_SIZE, t1.OpenCursor());

					{
						var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

						Assert.Equal (0, count_keys_forward (csr));
						Assert.Equal (0, count_keys_backward (csr));
					}

					{
						var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));
						var t2 = c.create_memory_segment();
						var mc = c.create_multicursor(t2.OpenCursor(),csr);
						mc.Seek("", SeekOp.SEEK_LE);
						Assert.False(mc.IsValid());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}
		#endif

		[Fact]
		public void btree_in_memory()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				t1.Insert ("a", "1");
				t1.Insert ("b", "2");
				t1.Insert ("c", "3");

				Assert.Equal (3, count_keys_forward (t1.OpenCursor ()));
				Assert.Equal (3, count_keys_backward (t1.OpenCursor ()));

				using (var fs = new MemoryStream()) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor());

					{
						var csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));

						csr.Seek ("b", SeekOp.SEEK_EQ);
						Assert.True (csr.IsValid ());
						Assert.Equal ("2", csr.Value ().from_utf8 ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void empty_val()
		{
			Action<combo> f = (combo c) => {
				var t1 = c.create_memory_segment();
				t1.Insert ("_", "");
				var csr = t1.OpenCursor ();

				csr.Seek ("_", SeekOp.SEEK_EQ);
				Assert.True (csr.IsValid ());
				Assert.Equal (0, csr.ValueLength ());

				using (var fs = new FileStream ("empty_val", FileMode.Create, FileAccess.ReadWrite)) {
                        IPages pageManager = new SimplePageManager(fs);
					c.create_btree_segment(fs, PAGE_SIZE, pageManager, csr);
				}

				using (var fs = new FileStream ("empty_val", FileMode.Open, FileAccess.Read)) {
					csr = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));
					csr.Seek ("_", SeekOp.SEEK_EQ);
					Assert.True (csr.IsValid ());
					Assert.Equal (0, csr.ValueLength ());
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void overwrite_val_mem()
		{
			Action<combo> f = (combo c) => {
				{
					var t1 = c.create_memory_segment();
					t1.Insert ("a", "1");
					t1.Insert ("b", "2");
					t1.Insert ("c", "3");
					t1.Insert ("d", "4");

					{
						var csr = t1.OpenCursor ();
						csr.Seek ("b", SeekOp.SEEK_EQ);
						Assert.True (csr.IsValid ());
						Assert.Equal ("2", csr.Value ().from_utf8 ());
					}

					using (var fs = new FileStream ("overwrite_val_mem", FileMode.Create, FileAccess.ReadWrite)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs = new FileStream ("overwrite_val_mem", FileMode.Open, FileAccess.Read)) {
					var csr_b1 = c.open_btree_segment(fs, PAGE_SIZE, lastPage(fs));
					csr_b1.Seek ("b", SeekOp.SEEK_EQ);
					Assert.True (csr_b1.IsValid ());
					Assert.Equal ("2", csr_b1.Value ().from_utf8());

					var t1 = c.create_memory_segment();
					t1.Insert ("b", "5");
					{
						var csr = t1.OpenCursor ();
						csr.Seek ("b", SeekOp.SEEK_EQ);
						Assert.True (csr.IsValid ());
						Assert.Equal ("5", csr.Value ().from_utf8 ());
					}

					{
						var mc = c.create_multicursor(t1.OpenCursor (), csr_b1);
						mc.Seek ("b", SeekOp.SEEK_EQ);
						Assert.True (mc.IsValid ());
						Assert.Equal ("5", mc.Value ().from_utf8 ());
					}

					{
						var mc = c.create_multicursor(csr_b1, t1.OpenCursor ());
						mc.Seek ("b", SeekOp.SEEK_EQ);
						Assert.True (mc.IsValid ());
						Assert.Equal ("2", mc.Value ().from_utf8 ());
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		[Fact]
		public void tombstone()
		{
			Action<combo> f = (combo c) => {
				{
					var t1 = c.create_memory_segment();
					t1.Insert ("a", "1");
					t1.Insert ("b", "2");
					t1.Insert ("c", "3");
					t1.Insert ("d", "4");

					Assert.Equal (4, count_keys_forward (t1.OpenCursor ()));
					Assert.Equal (4, count_keys_backward (t1.OpenCursor ()));

					using (var fs = new FileStream ("tombstone_1", FileMode.Create, FileAccess.ReadWrite)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				{
					var t1 = c.create_memory_segment();
					t1.Delete ("b");

					Assert.Equal (1, count_keys_forward (t1.OpenCursor ()));
					Assert.Equal (1, count_keys_backward (t1.OpenCursor ()));
					Assert.Equal (0, count_keys_forward (c.create_living_cursor(t1.OpenCursor ())));
					Assert.Equal (0, count_keys_backward (c.create_living_cursor(t1.OpenCursor ())));

					using (var fs = new FileStream ("tombstone_2", FileMode.Create, FileAccess.ReadWrite)) {
                        IPages pageManager = new SimplePageManager(fs);
						c.create_btree_segment(fs, PAGE_SIZE, pageManager, t1.OpenCursor ());
					}
				}

				using (var fs1 = new FileStream ("tombstone_1", FileMode.Open, FileAccess.Read)) {
					var csr1 = c.open_btree_segment(fs1, PAGE_SIZE, lastPage(fs1));
					using (var fs2 = new FileStream ("tombstone_2", FileMode.Open, FileAccess.Read)) {
						var csr2 = c.open_btree_segment(fs2, PAGE_SIZE, lastPage(fs2));

						{
							var mc = c.create_multicursor(csr2, csr1);

							mc.Seek ("b", SeekOp.SEEK_EQ);
							Assert.True (mc.IsValid ());
							Assert.Equal (-1, mc.ValueLength ());
							Assert.Null (mc.Value ());
							mc.Prev ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("a", mc.Key ().FromUTF8 ());
							Assert.Equal ("1", mc.Value ().from_utf8 ());

							Assert.Equal (4, count_keys_forward (mc));
							Assert.Equal (4, count_keys_backward (mc));

							// ----

							mc.First ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("a", mc.Key ().FromUTF8 ());
							Assert.Equal ("1", mc.Value ().from_utf8 ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("b", mc.Key ().FromUTF8 ());
							Assert.Equal (null, mc.Value ());
							Assert.Equal (-1, mc.ValueLength ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("c", mc.Key ().FromUTF8 ());
							Assert.Equal ("3", mc.Value ().from_utf8 ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("d", mc.Key ().FromUTF8 ());
							Assert.Equal ("4", mc.Value ().from_utf8 ());

							mc.Next ();
							Assert.False (mc.IsValid ());

							// ----

							mc.First ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("a", mc.Key ().FromUTF8 ());
							Assert.Equal ("1", mc.Value ().from_utf8 ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("b", mc.Key ().FromUTF8 ());
							Assert.Equal (null, mc.Value ());
							Assert.Equal (-1, mc.ValueLength ());

							mc.Prev ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("a", mc.Key ().FromUTF8 ());
							Assert.Equal ("1", mc.Value ().from_utf8 ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("b", mc.Key ().FromUTF8 ());
							Assert.Equal (null, mc.Value ());
							Assert.Equal (-1, mc.ValueLength ());

							// ----

							mc.Seek ("b", SeekOp.SEEK_LE);
							Assert.True (mc.IsValid ());
							Assert.Equal (-1, mc.ValueLength ());
							Assert.Equal ("b", mc.Key ().FromUTF8 ());

							mc.Prev ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("a", mc.Key ().FromUTF8 ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal (-1, mc.ValueLength ());
							Assert.Equal ("b", mc.Key ().FromUTF8 ());

							mc.Next ();
							Assert.True (mc.IsValid ());
							Assert.Equal ("c", mc.Key ().FromUTF8 ());

							// ----

							var lc = c.create_living_cursor(mc) as ICursor;

							lc.First ();
							Assert.True (lc.IsValid ());
							Assert.Equal ("a", lc.Key ().FromUTF8 ());
							Assert.Equal ("1", lc.Value ().from_utf8 ());

							lc.Next ();
							Assert.True (lc.IsValid ());
							Assert.Equal ("c", lc.Key ().FromUTF8 ());
							Assert.Equal ("3", lc.Value ().from_utf8 ());

							lc.Next ();
							Assert.True (lc.IsValid ());
							Assert.Equal ("d", lc.Key ().FromUTF8 ());
							Assert.Equal ("4", lc.Value ().from_utf8 ());

							lc.Next ();
							Assert.False (lc.IsValid ());

							Assert.Equal (3, count_keys_forward (lc));
							Assert.Equal (3, count_keys_backward (lc));

							lc.Seek ("b", SeekOp.SEEK_EQ);
							Assert.False (lc.IsValid ());

							lc.Seek ("b", SeekOp.SEEK_LE);
							Assert.True (lc.IsValid ());
							Assert.Equal ("a", lc.Key ().FromUTF8 ());
							lc.Next ();
							Assert.True (lc.IsValid ());
							Assert.Equal ("c", lc.Key ().FromUTF8 ());

							lc.Seek ("b", SeekOp.SEEK_GE);
							Assert.True (lc.IsValid ());
							Assert.Equal ("c", lc.Key ().FromUTF8 ());
							lc.Prev ();
							Assert.Equal ("a", lc.Key ().FromUTF8 ());
						}
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}

		private static int count_keys_forward(ICursor csr)
		{
			int count = 0;
			csr.First ();
			while (csr.IsValid ()) {
				count++;
				csr.Next ();
			}
			return count;
		}

		private static int count_keys_backward(ICursor csr)
		{
			int count = 0;
			csr.Last ();
			while (csr.IsValid ()) {
				count++;
				csr.Prev ();
			}
			return count;
		}

	}
}

