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
	public static class hack
	{
		public static string from_utf8(this Stream s)
		{
			// note the arbitrary choice of getting this function from cs instead of fs
			// maybe utils should move into LSM_base
			return Zumero.LSM.cs.utils.ReadAll (s).FromUTF8 ();
		}


	}

}

