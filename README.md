
# Overview

This is my "Learn F#" project.  It's a key-value store, implemented as
a log structured merge tree, conceptually similar to LevelDB or the 
storage layer of SQLite4.

This is not a complete implementation.  You can't grab this and just use
it like LevelDB.  Lots of stuff is missing.  There is no support for 
transactions.  There is no code to manage the merge between levels.

There are two implementations here, one in C# and one in 
F#.  Both implement the same API.  Originally, the C# version was written 
first.  The F# version started as mostly a straight port, and its quality 
of code is probably dreadful by F# standards.  But hopefully it is
improving.  And as the project moves forward, some features are implemented
the other direction, in F# first and then ported to C#.

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

# Design

In principle, both keys and values are byte arrays.  A key really is
a byte[].  But a value is actually represented as a System.IO.Stream, 
just in case it is too large to fit in memory.

The main concept here is an interface called ICursor.
It defines the methods which can be used to search or iterate
over one segment, whether it be in memory or on disk.

MemorySegment is little more than a wrapper around a .NET Dictionary.
It has a method called OpenCursor() which returns an ICursor.

To construct a disk segment, call BTreeSegment.Create.  Its parameters
are a Stream (into which the B+tree will be written) and an ICursor
(from which the keys and values will be obtained).

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

As I said above, this is not a complete implementation.  There needs
to be something that owns the memory segment and all the disk segments
and makes smart decisions about when to flush the memory segment
and when to merge disk segments together.  That piece of code is
currently absent here.

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

There's a small xUnit test suite.  It is configured to run every
test four times:

 * The F# implementation

 * The C# implementation

 * B+trees written by F# and read by C#

 * B+trees written by C# and read by F#

All the tests are written in C#.

All the work so far has been done on Mono in Xamarin Studio on my
Mac.  I assume this code will run on .NET/Windows, but I haven't tried
it yet.

Both implementations exist in a single source code file.  lsm.fs is
predictably a lot shorter than lsm.cs, but that comparison isn't totally
fair, since the C# version currently has more comments.

The F# version is slightly slower.  I consider this to be strongly
related to my skill differential and probably-not-at-all related
to any broad perf differences between the two languages.

The C# version is a profile 78 PCL.  So far it would seem that F#
(under Xamarin Studio anyway) does not have tooling support for PCLs.

# The F# Code

It's rather object oriented.  In some ways, it probably needs to be.  I could
find no purely functional way of dealing with the basic idea of ICursor.

But even if this code needs to stay object-oriented in the large, 
I suspect there are lots of ways this can be improved to make the code more
idiomatic.

I've learned a lot about F# just by doing the port, but I want to go
further.  

 - I want the code to have far fewer uses of the word mutable.
The mutable class members don't bother me so much, but the mutable
variables in functions seem like glaring symptoms of a C# port.

 - And I used an Option type in only one place.

 - And I represent a tombstone with a null (this might actually stay that way).

 - And there are lots of if-then expressions and hardly any match expressions.

 - And there are while/for loops.

 - And I mostly used mutable collections from System.Collections.Generic.

Basically, lsm.fs is a functional programming nightmare, but it's a starting point.

Because of the tricks I use in the test suite, ICursor (and its friends) are
defined in a C# assembly called LSM\_base.  Both the C# and F# implementations
reference this assembly.

It's nice that this demonstrates the use of C# to call through an F# implementation
of an interface defined in C#.  But before I did the the test suite trick,
the F# had its own definition of ICursor, and that was cool too.  The code is
still there in lsm.fs, commented out.  Just for fun, I could have used the
F# definitions for LSM\_base, but LSM\_base has to be a PCL because LSM\_cs
is a PCL.

