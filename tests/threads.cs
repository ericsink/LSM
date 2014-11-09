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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

using Zumero.LSM;

namespace lsm_tests
{
	public class test_threads
	{
		private const int PAGE_SIZE = 256;

		private static Stream openFile(string s)
		{
			return new FileStream (s, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

		[Fact]
		public void single_file_threaded()
		{
			const string filename = "single_file_threaded";

			Action<combo> f = (combo c) => {
				int s1;
				int s2;
				int s3;
				int s4;

				using (var fsPageManager = new FileStream (filename, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite)) {
					IPages pageManager = new SimplePageManager(PAGE_SIZE);

					var ta = new Thread[4];
					var ts = new int[4];

					ta[0] = new Thread(() => {
						using (var fs = new FileStream (filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
							var t1 = new Dictionary<byte[],Stream>();
							for (int i=0; i<5000; i++) {
								t1.Insert((i*2).ToString(), i.ToString());
							}
							ts[0] = c.create_btree_segment (fs, pageManager, t1.OpenCursor ()).Item2;
						}
					});

					ta[1] = new Thread(() => {
						using (var fs = new FileStream (filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
							var t1 = new Dictionary<byte[],Stream>();
							for (int i=0; i<5000; i++) {
								t1.Insert((i*3).ToString(), i.ToString());
							}
							ts[1] = c.create_btree_segment (fs, pageManager, t1.OpenCursor ()).Item2;
						}
					});

					ta[2] = new Thread(() => {
						using (var fs = new FileStream (filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
							var t1 = new Dictionary<byte[],Stream>();
							for (int i=0; i<5000; i++) {
								t1.Insert((i*5).ToString(), i.ToString());
							}
							ts[2] = c.create_btree_segment (fs, pageManager, t1.OpenCursor ()).Item2;
						}
					});

					ta[3] = new Thread(() => {
						using (var fs = new FileStream (filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
							var t1 = new Dictionary<byte[],Stream>();
							for (int i=0; i<5000; i++) {
								t1.Insert((i*7).ToString(), i.ToString());
							}
							ts[3] = c.create_btree_segment (fs, pageManager, t1.OpenCursor ()).Item2;
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
					using (var fs = new FileStream (filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {

						using (var fs1 = openFile(filename)) {
							var csr1 = c.open_btree_segment (fs1, PAGE_SIZE, s1);
							using (var fs2 = openFile(filename)) {
								var csr2 = c.open_btree_segment (fs2, PAGE_SIZE, s2);
								var mc = c.create_multicursor(csr1, csr2);
								s1_2 = c.create_btree_segment (fs, pageManager, mc).Item2;
							}
						}

						using (var fs3 = openFile(filename)) {
							var csr3 = c.open_btree_segment (fs3, PAGE_SIZE, s3);
							using (var fs4 = openFile(filename)) {
								var csr4 = c.open_btree_segment (fs4, PAGE_SIZE, s4);
								var mc = c.create_multicursor(csr3, csr4);
								s3_4 = c.create_btree_segment (fs, pageManager, mc).Item2;
							}
						}
					}

					using (var fs = new FileStream (filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
						int s5;

						using (var fs1_2 = openFile(filename)) {
							var csr1_2 = c.open_btree_segment (fs1_2, PAGE_SIZE, s1_2);
							using (var fs3_4 = openFile(filename)) {
								var csr3_4 = c.open_btree_segment (fs3_4, PAGE_SIZE, s3_4);
								var mc = c.create_multicursor(csr1_2, csr3_4);
								s5 = c.create_btree_segment (fs, pageManager, mc).Item2;
							}
						}

						{
							var csr = c.open_btree_segment(fs, PAGE_SIZE, s5);

							csr.First();
							while (csr.IsValid()) {
								csr.Next();
							}

							csr.Seek((42*3).ToString(), SeekOp.SEEK_EQ);
							Assert.True(csr.IsValid());
						}
					}
				}
			};
			foreach (combo c in combo.get_combos()) f(c);
		}
	}
}

