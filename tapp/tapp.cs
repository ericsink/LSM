
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Zumero.LSM;

namespace lsm_tests
{

	public class foo
	{
		private const int PAGE_SIZE = 256;

		private static Stream openFile(string s)
		{
			return new FileStream (s, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
		}

	    public static void Main(string[] argv)
	    {
	    }
	}

}
