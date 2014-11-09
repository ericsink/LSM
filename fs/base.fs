(*
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
*)

namespace Zumero.LSM

open System
open System.IO
open System.Collections.Generic

type IPendingSegment = interface end

type IPages =
    abstract member PageSize : int with get
    abstract member Begin : unit->IPendingSegment
    abstract member GetRange : IPendingSegment->int*int // TODO consider struct instead of tuple
    abstract member End : IPendingSegment*int->Guid

type SeekOp = SEEK_EQ=0 | SEEK_LE=1 | SEEK_GE=2

type ICursor =
    inherit IDisposable
    abstract member Seek : k:byte[] * sop:SeekOp -> unit
    abstract member First : unit -> unit
    abstract member Last : unit -> unit
    abstract member Next : unit -> unit
    abstract member Prev : unit -> unit
    // the following are methods instead of properties because
    // ICursor doesn't know how expensive they are to implement.
    abstract member IsValid : unit -> bool
    abstract member Key : unit -> byte[]
    abstract member Value : unit -> Stream
    abstract member ValueLength : unit -> int

    abstract member KeyCompare : k:byte[] -> int

type IWriteLock =
    inherit IDisposable
    abstract member PrependSegments : seq<Guid> -> unit
    // TODO we could have NeverMind(seq<Guid>) which explicitly 
    // frees segments in waiting.  but this wouldn't necessarily
    // need to be here in IWriteLock.

type IDatabaseFile =
    abstract member OpenForReading : unit -> Stream
    abstract member OpenForWriting : unit -> Stream

type IDatabase = 
    inherit IDisposable
    abstract member WriteSegmentFromSortedSequence : seq<KeyValuePair<byte[],Stream>> -> Guid * int
    abstract member WriteSegment : System.Collections.Generic.IDictionary<byte[],Stream> -> Guid * int

    abstract member OpenCursor : unit->ICursor 
    // TODO consider name such as OpenLivingCursorOnCurrentState()
    // TODO consider OpenCursorOnSegmentsInWaiting(seq<Guid>)
    // TODO consider ListSegmentsInCurrentState()
    // TODO consider OpenCursorOnSpecificSegment(seq<Guid>)

    abstract member RequestWriteLock : unit->IWriteLock
    // TODO consider name TryGetWriteLock
    // TODO what happens if it can't get the write lock?  throw?  null?  fs option?  wait?  async?

    // TODO need a way to tell the db to merge segments.  should it always just choose?
    // or can the caller get a list of segments, plus info about them, and help decide?

module CursorUtils =
    let ToSortedSequenceOfKeyValuePairs (csr:ICursor) = seq { csr.First(); while csr.IsValid() do yield new KeyValuePair<byte[],Stream>(csr.Key(), csr.Value()); csr.Next(); done }
    let ToSortedSequenceOfTuples (csr:ICursor) = seq { csr.First(); while csr.IsValid() do yield (csr.Key(), csr.Value()); csr.Next(); done }

