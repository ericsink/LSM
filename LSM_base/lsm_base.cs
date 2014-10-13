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
        string Begin();
        Tuple<int,int> GetRange(string token);
		void End(string token, int lastPage);
    }

	public interface IWrite
	{
		void Insert(byte[] k, Stream v);
		void Delete(byte[] k);
		// TODO delete_range
		ICursor OpenCursor();
	}

	public enum SeekOp
	{
		SEEK_EQ,
		SEEK_LE,
		SEEK_GE
	}

	public interface ICursor
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

	public static class ex
	{
		public static void Insert(this IWrite w, byte[] k, byte[] v)
		{
			w.Insert (k, new MemoryStream(v) );
		}

		public static void Insert(this IWrite w, string k, byte[] v)
		{
			w.Insert (k.ToUTF8 (), new MemoryStream(v) );
		}

		public static void Insert(this IWrite w, string k, string v)
		{
			w.Insert (k.ToUTF8 (), new MemoryStream(v.ToUTF8 ()) );
		}

		public static void Delete(this IWrite w, string k)
		{
			w.Delete (k.ToUTF8 ());
		}

		public static void Seek(this ICursor csr, string k, SeekOp sop)
		{
			csr.Seek (k.ToUTF8(), sop);
		}

		public static byte[] ToUTF8(this string s)
		{
			return System.Text.Encoding.UTF8.GetBytes (s);
		}

		public static string UTF8ToString(this byte[] ba)
		{
			return System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length);
		}
	}

}
