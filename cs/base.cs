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

namespace Zumero.LSM
{
	using System;
	using System.IO;
	using System.Collections.Generic;

    public interface IPages
    {
		int PageSize { get; }
        Guid Begin();
        Tuple<int,int> GetRange(Guid token); // TODO consider making this a struct
		void End(Guid token, int lastPage);
    }

	public enum SeekOp
	{
		SEEK_EQ,
		SEEK_LE,
		SEEK_GE
	}

	public interface ICursor : IDisposable
	{
		void Seek(byte[] k, SeekOp sop);

		void First();
		void Last();
		void Next();
		void Prev();

		bool IsValid(); // TODO property?
		byte[] Key(); // TODO property?
		Stream Value(); // TODO property?
		int ValueLength(); // TODO property?

		int KeyCompare(byte[] k);
	}

	public static class ICursorExtensions
	{
		public static IEnumerable<Tuple<byte[],Stream>> ToSequenceOfTuples(ICursor csr)
		{
			csr.First ();
			while (csr.IsValid ()) {
				yield return new Tuple<byte[], Stream> (csr.Key (), csr.Value ());
				csr.Next ();
			}
		}

		public static IEnumerable<KeyValuePair<byte[],Stream>> ToSequenceOfKeyValuePairs(ICursor csr)
		{
			csr.First ();
			while (csr.IsValid ()) {
				yield return new KeyValuePair<byte[], Stream> (csr.Key (), csr.Value ());
				csr.Next ();
			}
		}

	}

}
