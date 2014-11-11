
# Overview

This is my "Learn F#" project.  It's a key-value store, implemented as
a log structured merge tree, conceptually similar to LevelDB or the 
storage layer of SQLite4.

This is not ready for production use.  Big pieces are still missing.
For example, there is nothing to reuse pages and nothing to decide
when to merge segments.

There are two implementations here, one in C# and one in 
F#.  Originally, the C# version was written first.  The F# version 
started as mostly a straight port, and has been evolving to be more
idiomatic and functional.

At the time of this writing, the F# version is the focus of my 
attention and the C# version is falling behind.  For example, the F#
code has an initial implementation of a layer to manage transactions,
while the C# code is still just raw pieces to read/write B+Tree
segments.  I now think of the C# implementation as part of the test
suite.

# What is a log structured merge tree?

The basic idea of an LSM tree is that a database is implemented as
list of lists.  Each list is usually called a "sorted run" or a "segment".

The first segment is kept in memory, and is the only
one which is mutable.  Inserts, updates, and deletes happen only
to the memory segment.  

When the memory segment gets too large, it is flushed out to disk.
Once a segment is on disk, it is immutable.

Searching or walking the database requires checking all of the segments, 
in the proper order.  The more segments there are, the trickier and
slower this will be.  So we can improve things by merging two disk
segments together to form a new one.

A key can exist in multiple segments.  If so, when searching or iterating,
the first segment wins.  In this way, the database can support
updates (overwriting the value for a key).  Deletes are implemented
in the same way, by adding a "tombstone" value, a flag that serves 
to indicate that the key has been deleted.

The memory segment can be implemented with whatever data structure
makes sense.  The disk segment is usually something like a B+tree,
except it doesn't need to support insert/update/delete, so it's
much simpler.

# Design: B+Tree segments and cursors

In principle, both keys and values are byte arrays.  A key really is
a byte[].  But a value is actually represented as a System.IO.Stream, 
just in case it is too large to fit in memory.

The main concept here is an interface called ICursor.
It defines the methods which can be used to search or iterate
over one segment, whether it be in memory or on disk.

This code base doesn't really implement a memory segment object,
preferring instead to let the caller use standard .NET collections
for that.

To construct a disk segment, call BTreeSegment.CreateFromSortedSequence.  
It needs a sequence (aka IEnumerable) of KeyValuePair objects,
properly sorted.

To create a segment from an ICursor, use CursorUtils.ToSortedSequenceOfKeyValuePairs
to get the sequence you need.

To create a segment from a Dictionary, use BtreeSegment.SortAndCreate.

BTreeSegment.OpenCursor() does what you would expect.

The object that makes it all work is MultiCursor.  This is an ICursor
that has one or more sub-cursors.  You can search or iterate over
a MultiCursor just like any other.  Under the hood, it will deal
with all the sub-cursors and present the illusion that they are one
segment.

So, you can flush a memory segment to disk by passing its cursor
to BTreeSegment.Create.

You can combine any two (or more) segments into one by creating a 
MultiCursor on them and passing it to BTreeSegment.Create.

There is one more ICursor implementation I have not mentioned, and
that is something I call LivingCursor.  This is a cursor that has
one subcursor, and all it does is filter out the tombstones.

# The B+Tree

This is basically a bulk-loaded B+tree.  Since it doesn't need to support
insert/update/delete, every node is "full".

The pages are written in order in a single pass.  First all the leaves
are written.  Then the parent nodes of the tree are written depth-first
until we get a tree layer that has only one node, which becomes the root
node.  The root node gets two extra pieces of info in it, which are the
page numbers of the first and last leaf.  In this way, if you have the
page number of the root node (which is always the last page in the file),
you can find anything else.

Every leaf or parent node may be preceded by one or more overflow nodes.
These are used to store any key or value which is too large to fit on
a page.

Each leaf node knows the page number of the previous leaf, which, because of
overflow nodes, may not be the immediately preceding page.

Construction of a B+Tree segment relies on a "page manager" which provides
with pages from the file.  Multiple B+Tree segments can exist in the
same file.  The page manager divides the file into blocks of pages
and hands them out on request.

# The Code

I have taken a lot of inspiration and ideas from SQLite4.  The varint
concept comes directly from there.  ICursor is almost exactly the same.

There's a small xUnit test suite for testing the B+Tree and cursor
layer.  It is configured to run every test four times:

 * The F# implementation

 * The C# implementation

 * B+trees written by F# and read by C#

 * B+trees written by C# and read by F#

All the tests are in C#.

There is another xUnit test suite called dbTests.  This one exercises
the Database layer.

All the work so far has been done on Mono in Xamarin Studio on my
Mac.  I assume this code will run on .NET/Windows, but I haven't tried
it yet.

The C# version is a profile 78 PCL.  So far it would seem that F#
(under Xamarin Studio anyway) does not have tooling support for PCLs.

