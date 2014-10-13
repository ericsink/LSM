
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;

namespace lsm_tests
{
	public class SimplePageManager : IPages
	{
		private readonly Stream fs;
		int cur = 1;
		private readonly Dictionary<string,List<Tuple<int,int>>> segments;

		public SimplePageManager(Stream _fs)
		{
			fs = _fs;
			segments = new Dictionary<string, List<Tuple<int, int>>> ();
		}

		string IPages.Begin()
		{
			lock (this) {
				int count = segments.Count;
				string token = count.ToString ("0000");
				segments [token] = new List<Tuple<int, int>> ();
				return token;
			}
		}

		void IPages.End(string token, int lastPage)
		{
			var blocks = segments [token];
			Console.WriteLine ("{0} is done", token);
			//Console.WriteLine ("{0}", blocks);
		}

		Tuple<int,int> IPages.GetRange(string token)
		{
			lock (this) {
				var t = new Tuple<int,int> (cur, cur + 9);
				cur = cur + 13;
				segments [token].Add (t);
				Console.WriteLine ("{0} gets {1} --> {2}", token, t.Item1, t.Item2);
				return t;
			}
		}

	}

}

