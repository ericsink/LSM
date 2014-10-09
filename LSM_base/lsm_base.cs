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

		bool IsValid();
		byte[] Key();
		Stream Value();
		int ValueLength();

		int KeyCompare(byte[] k);
	}

}
