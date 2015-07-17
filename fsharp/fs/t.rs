
// The code below results in an error message I do not understand.
//
// This code is a Rust port of some F# code.  Specifically, this:
//
//     https://github.com/ericsink/LSM
//
// The snippet below has been pared down a lot to approximately
// the minimum necessary to retain the confusing error message.
//
// In general, the problem area seems to be the fact that I am
// writing a function that takes a closure.
// 
// Porting this code from F# to Rust is mostly just an exercise
// in learning Rust.  I intend to do this in multiple passes.
// In the first pass, I wanted to try to preserve as much of the
// current F# approach as possible, even if doing so is not
// particularly idiomatic for Rust.  In a second pass, presumably
// armed with more knowledge about Rust, I would improve things
// further.
//
// My primary goal in posting here is to understand the error
// message I am getting, not to workaround it.  I'm pretty sure
// I could make this work if I simply rewrote the code using
// a different approach (which does not use a closure).  But
// first I want to know what I'm doing wrong with my attempts
// to use *this* approach, since it kinda seems like it should
// work.


// ICursor is a trait for iterating over key-value pairs.  The
// reason I am not using Iterator is that ICursor supports
// iteration in both directions (which has been deleted here
// for brevity).  The ICursor below has been stripped of all
// its dignity, mostly leaving only the methods necessary to get 
// the confusing error.
trait ICursor {
    fn First(&mut self); // included here for informational purposes only
    fn Key(&self) -> Box<[u8]>;
    fn KeyCompare(&self, k:&[u8]) -> i32;
}

// MultiCursor is an ICursor which contains a bunch of cursors
// inside it.  It can support iteration over its subcursors
// while pretending it is all just one.
struct MultiCursor<'a> { 

    // BTW:
    // I am not at all confident that this is the best way to
    // represent the subcursor collection.  To follow the
    // original F# design, MultiCursor would want to own its
    // subcursors and "dispose of" (drop) them when it gets
    // "disposed" (dropped).  For now, I'm using an array of
    // references to closures.  The references are mut because
    // several of the methods of ICursor (like First) do modify
    // the contents.
    subcursors : Box<[&'a mut ICursor]>, 

    // the current cursor, if there is one, represented as an
    // index into subcursors.
    cur : Option<usize>,
}

impl<'a> MultiCursor<'a> {

    // Down a bit further, you'll see findMin() and findMax().  All I'm
    // really doing here is trying to avoid duplicating logic in those
    // two functions.  In F#, the somewhat natural way to do this was to
    // factor out the logic into find() and have each of the other two
    // pass in a different comparison function.
    fn find(&self, compare_func : &Fn(&ICursor,&ICursor) -> i32) -> Option<usize> {
        if self.subcursors.is_empty() {
            None
        } else {
            // Confession:  This function has already been altered quite a bit
            // relative to its F# origins.  The loop below used to be a fold.
            let mut res = None;
            for i in 0 .. self.subcursors.len() {
                match res {
                    Some(winning) => {
                        let x = self.subcursors[i];
                        let y = self.subcursors[winning]; // THIS IS THE LINE WHERE THE ERROR HAPPENS
                        let c = compare_func(x,y);
                        if c<0 {
                            res = Some(i)
                        }
                    },
                    None => {
                        res = Some(i)
                    }
                }
            }
            res
        }
    }

    // So here's the error:

    // t.rs:82:33: 82:57 error: type mismatch resolving `<[&'a mut ICursor + 'a] as core::ops::Index<usize>>::Output == &ICursor`: values differ in mutability [E0271]
    // t.rs:82                         let y = self.subcursors[winning]; // THIS IS THE LINE WHERE THE ERROR HAPPENS
    //                                         ^~~~~~~~~~~~~~~~~~~~~~~~

    // I think I'm trying to pass a mut reference into a non-mut reference.
    //
    // If the situation were reversed (passing a non-mut reference into
    // a mut reference), then I would expect an error, albeit a different
    // one than what I am seeing.
    //
    // It seems like my code is equivalent to the following, which works:

    // fn foo(x:&i32) {
    //     println!("{}", x);
    // }

    // fn main() {
    //     let mut q = 5;
    //     foo(&q);
    //     q = 3;
    //     foo(&q);
    // }

    // So anyway, that's it.  Insights welcomed.  Thanks in advance!

    // ----------------------------------------

    // All of the functions below are informational.  I get the same error
    // if I delete them all.

    fn findMin(&self) -> Option<usize> {
        let compare_func = |a:&ICursor,b:&ICursor| a.KeyCompare(&*b.Key());
        self.find(&compare_func)
    }

    fn findMax(&self) -> Option<usize> {
        let compare_func = |a:&ICursor,b:&ICursor| b.KeyCompare(&*a.Key());
        self.find(&compare_func)
    }

    fn Create(subs: Vec<&'a mut ICursor>) -> MultiCursor<'a> {
        let s = subs.into_boxed_slice();
        MultiCursor { subcursors: s, cur : None }
    }

}

fn main() 
{

}

