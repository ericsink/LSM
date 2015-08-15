
use std;
use std::cmp::Ordering;

use super::Result;

extern crate bson;

pub enum QueryDoc {
    QueryDoc(Vec<QueryItem>),
}

enum QueryItem {
    Compare(String, Vec<Pred>),
    AND(Vec<QueryDoc>),
    OR(Vec<QueryDoc>),
    NOR(Vec<QueryDoc>),
    Where(bson::Value),
    Text(String),
}

enum Pred {
    Exists(bool),
    Size(i32),
    Type(i32),
    Mod(i64, i64),
    ElemMatchObjects(QueryDoc),
    ElemMatchPreds(Vec<Pred>),
    Not(Vec<Pred>),
    In(Vec<bson::Value>),
    Nin(Vec<bson::Value>),
    All(Vec<bson::Value>),
    AllElemMatchObjects(Vec<QueryDoc>),
    EQ(bson::Value),
    NE(bson::Value),
    GT(bson::Value),
    LT(bson::Value),
    GTE(bson::Value),
    LTE(bson::Value),
    // TODO regex should be in compiled form, not a string
    REGEX(String),
    Near(bson::Value),
    NearSphere(bson::Value),
    GeoWithin(bson::Value),
    GeoIntersects(bson::Value),
}

fn cmp_f64(m: f64, litv: f64) -> Ordering {
    if m == litv {
        Ordering::Equal
    } else if m.is_nan() && litv.is_nan() {
        Ordering::Equal
    } else if m.is_nan() {
        Ordering::Less
    } else if litv.is_nan() {
        Ordering::Greater
    } else if m < litv {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

fn cmp(d: &bson::Value, lit: &bson::Value) -> Ordering {
    match (d,lit) {
        (&bson::Value::BObjectID(m), &bson::Value::BObjectID(litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BInt32(m), &bson::Value::BInt32(litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BInt64(m), &bson::Value::BInt64(litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BDateTime(m), &bson::Value::BDateTime(litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BTimeStamp(m), &bson::Value::BTimeStamp(litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BDouble(m), &bson::Value::BDouble(litv)) => {
            cmp_f64(m, litv)
        },
        (&bson::Value::BString(ref m), &bson::Value::BString(ref litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BBoolean(m), &bson::Value::BBoolean(litv)) => {
            m.cmp(&litv)
        },
        (&bson::Value::BUndefined, &bson::Value::BUndefined) => {
            Ordering::Equal
        },
        (&bson::Value::BNull, &bson::Value::BNull) => {
            Ordering::Equal
        },
        (&bson::Value::BInt32(m), &bson::Value::BInt64(litv)) => {
            let m = m as i64;
            m.cmp(&litv)
        },
        (&bson::Value::BInt32(m), &bson::Value::BDouble(litv)) => {
            let m = m as f64;
            cmp_f64(m, litv)
        },
        (&bson::Value::BInt64(m), &bson::Value::BInt32(litv)) => {
            let litv = litv as i64;
            m.cmp(&litv)
        },
        (&bson::Value::BInt64(m), &bson::Value::BDouble(litv)) => {
            let m = m as f64;
            cmp_f64(m, litv)
        },
        (&bson::Value::BDouble(m), &bson::Value::BInt32(litv)) => {
            // when comparing double and int, cast the int to double, regardless of ordering
            let litv = litv as f64;
            cmp_f64(m, litv)
        },
        (&bson::Value::BDouble(m), &bson::Value::BInt64(litv)) => {
            // when comparing double and int, cast the int to double, regardless of ordering
            // TODO this can overflow
            let litv = litv as f64;
            cmp_f64(m, litv)
        },
        (&bson::Value::BArray(ref ba_m), &bson::Value::BArray(ref ba_litv)) => {
            let lenm = ba_m.items.len();
            let lenlitv = ba_litv.items.len();
            let len = std::cmp::min(lenm, lenlitv);
            for i in 0 .. len {
                let c = cmp(&ba_m.items[i], &ba_litv.items[i]);
                if c != Ordering::Equal {
                    return c;
                }
            }
            lenm.cmp(&lenlitv)
        },
        (&bson::Value::BDocument(ref bd_m), &bson::Value::BDocument(ref bd_litv)) => {
            let lenm = bd_m.pairs.len();
            let lenlitv = bd_litv.pairs.len();
            let len = std::cmp::min(lenm, lenlitv);
            for i in 0 .. len {
                if bd_m.pairs[i].0 < bd_litv.pairs[i].0 {
                    return Ordering::Less;
                } else if bd_m.pairs[i].0 > bd_litv.pairs[i].0 {
                    return Ordering::Greater;
                } else {
                    let c = cmp(&bd_m.pairs[i].1, &bd_litv.pairs[i].1);
                    if c != Ordering::Equal {
                        return c;
                    }
                }
            }
            lenm.cmp(&lenlitv)
        },
        _ => {
            let torder_d = d.get_type_order();
            let torder_lit = lit.get_type_order();
            assert!(torder_d != torder_lit);
            torder_d.cmp(&torder_lit)
        },
    }
}

fn array_min_max(a: &Vec<bson::Value>, judge: Ordering) -> Option<&bson::Value> {
    let mut cur = None;
    for v in a {
        match cur {
            Some(win) => {
                let c = cmp(v, win);
                if c == judge {
                    cur = Some(v);
                }
            },
            None => {
                cur = Some(v);
            },
        }
    }
    cur
}

fn array_min(a: &Vec<bson::Value>) -> Option<&bson::Value> {
    array_min_max(a, Ordering::Less)
}

fn array_max(a: &Vec<bson::Value>) -> Option<&bson::Value> {
    array_min_max(a, Ordering::Greater)
}

fn cmpdir(d: &bson::Value, lit: &bson::Value, reverse: bool) -> Ordering {
    // when comparing an array against something else during sort:
    // if two arrays, compare element by element.
    // if array vs. not-array, find the min or max (depending on the
    // sort direction) of the array and compare against that.

    let c = 
        match (d, lit) {
            (&bson::Value::BArray(_), &bson::Value::BArray(_)) => {
                cmp(d, lit)
            },
            (&bson::Value::BArray(ref ba), _) => {
                let om =
                    if reverse {
                        array_max(&ba.items)
                    } else {
                        array_min(&ba.items)
                    };
                match om {
                    Some(m) => cmp(m, lit),
                    // TODO is the following the correct behavior for an empty array?
                    None => cmp(d, lit),
                }
            },
            (_, &bson::Value::BArray(ref ba)) => {
                let om =
                    if reverse {
                        array_max(&ba.items)
                    } else {
                        array_min(&ba.items)
                    };
                match om {
                    Some(m) => cmp(d, m),
                    // TODO is the following the correct behavior for an empty array?
                    None => cmp(d, lit),
                }
            },
            _ => {
                cmp(d, lit)
            },
        };
    if reverse {
        c.reverse()
    } else {
        c
    }
}

fn cmp_eq(d: &bson::Value, lit: &bson::Value) -> bool {
    let torder_d = d.get_type_order();
    let torder_lit = lit.get_type_order();

    if torder_d == torder_lit {
        cmp(d, lit) == Ordering::Equal
    } else {
        false
    }
}

fn cmp_in(d: &bson::Value, lit: &bson::Value) -> bool {
    match lit {
        &bson::Value::BRegex(ref expr, ref options) => {
            match d {
                &bson::Value::BString(ref s) => {
                    // TODO use expr and options to construct a regex and match s
                    unimplemented!();
                },
                _ => {
                    false
                },
            }
        },
        _ => {
            cmp_eq(d, lit)
        },
    }
}

fn cmp_lt_gt(d: &bson::Value, lit: &bson::Value, judge: Ordering) -> bool {
    if d.is_nan() || lit.is_nan() {
        false
    } else {
        let torder_d = d.get_type_order();
        let torder_lit = lit.get_type_order();

        if torder_d == torder_lit {
            cmp(d, lit) == judge
        } else {
            false
        }
    }
}

fn cmp_lt(d: &bson::Value, lit: &bson::Value) -> bool {
    cmp_lt_gt(d, lit, Ordering::Less)
}

fn cmp_gt(d: &bson::Value, lit: &bson::Value) -> bool {
    cmp_lt_gt(d, lit, Ordering::Greater)
}

fn cmp_lte_gte(d: &bson::Value, lit: &bson::Value, judge: Ordering) -> bool {
    let dnan = d.is_nan();
    let litnan = lit.is_nan();
    if dnan || litnan {
        dnan && litnan
    } else {
        let torder_d = d.get_type_order();
        let torder_lit = lit.get_type_order();

        if torder_d == torder_lit {
            let c = cmp(d, lit);
            if c == Ordering::Equal {
                true
            } else if c == judge {
                true
            } else {
                false
            }
        } else {
            // TODO this seems wrong.  shouldn't we compare the type orders?
            false
        }
    }
}

fn cmp_lte(d: &bson::Value, lit: &bson::Value) -> bool {
    cmp_lte_gte(d, lit, Ordering::Less)
}

fn cmp_gte(d: &bson::Value, lit: &bson::Value) -> bool {
    cmp_lte_gte(d, lit, Ordering::Greater)
}

fn do_elem_match_objects<F: Fn(&str, usize)>(doc: &QueryDoc, d: &bson::Value, cb_array_pos: &F) -> bool {
    match d {
        &bson::Value::BArray(ref ba) => {
            for vsub in &ba.items {
                match vsub {
                    &bson::Value::BArray(_) | &bson::Value::BDocument(_) => {
                        if match_query_doc(doc, vsub, cb_array_pos) {
                            return true;
                        }
                    },
                    _ => {
                    },
                }
            }
            false
        },
        _ => {
            false
        },
    }
}

fn match_predicate<F: Fn(&str, usize)>(pred: &Pred, d: &bson::Value, cb_array_pos: &F) -> bool {
    match pred {
        &Pred::Exists(b) => {
            unreachable!();
        },
        &Pred::Not(ref preds) => {
            let any_matches = preds.iter().any(|p| match_predicate(p, d, cb_array_pos));
            !any_matches
        },
        &Pred::ElemMatchObjects(ref doc) => {
            match d {
                &bson::Value::BArray(ref ba) => {
                    let found = 
                        ba.items.iter().position(|vsub| {
                            match vsub {
                                &bson::Value::BDocument(_) | &bson::Value::BArray(_) => match_query_doc(doc, vsub, cb_array_pos),
                                _ => false,
                            }
                        });
                    match found {
                        Some(n) => {
                            cb_array_pos("TODO", n);
                            true
                        },
                        None => false
                    }
                },
                _ => false,
            }
        },
        &Pred::ElemMatchPreds(ref preds) => {
            match d {
                &bson::Value::BArray(ref ba) => {
                    let found = 
                        ba.items.iter().position(|vsub| preds.iter().any(|p| !match_predicate(p, vsub, cb_array_pos)));
                    match found {
                        Some(n) => {
                            cb_array_pos("TODO", n);
                            true
                        },
                        None => false
                    }
                },
                _ => false,
            }
        },
        &Pred::AllElemMatchObjects(ref docs) => {
            // for each elemMatch doc in the $all array, run it against
            // the candidate array.  if any elemMatch doc fails, false.
            docs.iter().any(|doc| !do_elem_match_objects(doc, d, cb_array_pos))
        },
        &Pred::All(ref lits) => {
            // TODO does this ever happen, now that it is handled earlier?
            if lits.len() == 0 {
                false
            } else {
                !lits.iter().any(|lit| {
                    let b =
                        if cmp_eq(d, lit) {
                            true
                        } else {
                            match d {
                                &bson::Value::BArray(ref ba) => {
                                    ba.items.iter().any(|v| cmp_eq(v, lit))
                                },
                                _ => false,
                            }
                        };
                    !b
                })
            }
        },
        &Pred::EQ(ref lit) => cmp_eq(d, lit),
        &Pred::NE(ref lit) => !cmp_eq(d, lit),
        &Pred::LT(ref lit) => cmp_lt(d, lit),
        &Pred::GT(ref lit) => cmp_gt(d, lit),
        &Pred::LTE(ref lit) => cmp_lte(d, lit),
        &Pred::GTE(ref lit) => cmp_gte(d, lit),
        &Pred::REGEX(_) => {
            match d {
                &bson::Value::BString(ref s) => {
                    // TODO use regex to match s
                    unimplemented!();
                },
                _ => false,
            }
        },
        &Pred::Near(_) => unimplemented!(),
        &Pred::NearSphere(_) => unimplemented!(),
        &Pred::GeoWithin(_) => unimplemented!(),
        &Pred::GeoIntersects(_) => unimplemented!(),
        &Pred::Type(n) => (d.getTypeNumber_u8() as i32) == n,
        &Pred::In(ref lits) => lits.iter().any(|v| cmp_in(d, v)),
        &Pred::Nin(ref lits) => !lits.iter().any(|v| cmp_in(d, v)),
        &Pred::Size(n) => {
            match d {
                &bson::Value::BArray(ref ba) => ba.items.len() == (n as usize),
                _ => false,
            }
        },
        &Pred::Mod(div, rem) => {
            match d {
                &bson::Value::BInt32(n) => ((n as i64) % div) == rem,
                &bson::Value::BInt64(n) => (n % div) == rem,
                &bson::Value::BDouble(n) => ((n as i64) % div) == rem,
                _ => false,
            }
        },
    }
}

fn match_pair_exists(pred: &Pred, path: &str, start: &bson::Value) -> bool {
    let dot = path.find('.');
    let name = match dot { 
        None => path,
        Some(ndx) => &path[0 .. ndx]
    };
    match start.tryGetValueEither(name) {
        Some(v) => {
            match dot {
                None => true,
                Some(dot) => {
                    let subpath = &path[dot+1 ..];
                    match v {
                        &bson::Value::BDocument(_) => {
                            match_pair_exists(pred, subpath, v)
                        },
                        &bson::Value::BArray(ref ba) => {
                            let b = match_pair_exists(pred, subpath, v);
                            if b {
                                true
                            } else {
                                ba.items.iter().any(|vsub| {
                                    match vsub {
                                        &bson::Value::BDocument(_) => match_pair_exists(pred, subpath, v),
                                        _ => false,
                                    }
                                })
                            }
                        },
                        _ => false,
                    }
                },
            }
        },
        None => false,
    }
}

fn match_pair_other<F: Fn(&str, usize)>(pred: &Pred, path: &str, start: &bson::Value, arr: bool, cb_array_pos: &F) -> bool {
    let dot = path.find('.');
    let name = match dot { 
        None => path,
        Some(ndx) => &path[0 .. ndx]
    };
    match start.tryGetValueEither(name) {
        Some(v) => {
            match dot {
                None => {
                    if match_predicate(pred, v, cb_array_pos) {
                        true
                    } else if !arr {
                        match pred {
                            &Pred::Size(_) => false,
                            &Pred::All(_) => false,
                            &Pred::ElemMatchPreds(_) => false,
                            _ => {
                                match v {
                                    &bson::Value::BArray(ref ba) => {
                                        match ba.items.iter().position(|vsub| match_predicate(pred, vsub, cb_array_pos)) {
                                            Some(ndx) => {
                                                cb_array_pos("TODO", ndx);
                                                true
                                            },
                                            None => false,
                                        }
                                    },
                                    _ => false,
                                }
                            },
                        }
                    } else {
                        false
                    }
                },
                Some(dot) => {
                    let subpath = &path[dot+1 ..];
                    match v {
                        &bson::Value::BDocument(_) => {
                            match_pair_other(pred, subpath, v, false, cb_array_pos)
                        },
                        &bson::Value::BArray(ref ba) => {
                            let b = match_pair_other(pred, subpath, v, true, cb_array_pos);
                            if b {
                                true
                            } else {
                                let f = |vsub| {
                                    match vsub {
                                        &bson::Value::BDocument(_) => match_pair_other(pred, subpath, v, false, cb_array_pos),
                                        _ => false,
                                    }
                                };
                                match ba.items.iter().position(f) {
                                    Some(ndx) => {
                                        cb_array_pos("TODO", ndx);
                                        true
                                    },
                                    None => false,
                                }
                            }
                        },
                        _ => {
                            match pred {
                                &Pred::Type(n) => false,
                                _ => match_predicate(pred, &bson::Value::BNull, cb_array_pos),
                            }
                        },
                    }
                },
            }
        },
        None => {
            if arr {
                false
            } else {
                match pred {
                    &Pred::Type(n) => false,
                    _ => match_predicate(pred, &bson::Value::BNull, cb_array_pos),
                }
            }
        },
    }
}

fn match_pair<F: Fn(&str, usize)>(pred: &Pred, path: &str, start: &bson::Value, cb_array_pos: &F) -> bool {
    // not all predicates do their path searching in the same way
    // TODO consider a reusable function which generates all possible paths
    
    match pred {
        &Pred::All(ref a) => {
            if a.len() == 0 {
                false
            } else {
                // TODO clone below is awful
                a.iter().all(|lit| match_pair(&Pred::EQ(lit.clone()), path, start, cb_array_pos))
            }
        },
        &Pred::Exists(b) => {
            b == match_pair_exists(pred, path, start)
        },
        &Pred::Not(ref a) => {
            let any_matches = a.iter().any(|p| !match_pair(p, path, start, cb_array_pos));
            any_matches
        },
        &Pred::NE(ref a) => {
            // TODO since this is implemented in matchPredicate, it seems like we should
            // be able to remove this implementation.  but if we do, some tests fail.
            // figure out exactly why.
            // TODO clone below is awful
            !match_pair(&Pred::EQ(a.clone()), path, start, cb_array_pos)
        },
        &Pred::Nin(ref a) => {
            // TODO since this is implemented in matchPredicate, it seems like we should
            // be able to remove this implementation.  but if we do, some tests fail.
            // figure out exactly why.
            // TODO clone below is awful
            !match_pair(&Pred::In(a.clone()), path, start, cb_array_pos)
        },
        _ => {
            match_pair_other(pred, path, start, false, cb_array_pos)
        },
    }
}

fn match_query_item<F: Fn(&str, usize)>(qit: &QueryItem, d: &bson::Value, cb_array_pos: &F) -> bool {
    match qit {
        &QueryItem::Compare(ref path, ref preds) => {
            preds.iter().all(|v| match_pair(v, path, d, cb_array_pos))
        },
        &QueryItem::AND(ref qd) => {
            qd.iter().all(|v| match_query_doc(v, d, cb_array_pos))
        },
        &QueryItem::OR(ref qd) => {
            qd.iter().any(|v| match_query_doc(v, d, cb_array_pos))
        },
        &QueryItem::NOR(ref qd) => {
            !qd.iter().any(|v| match_query_doc(v, d, cb_array_pos))
        },
        &QueryItem::Where(ref v) => {
            panic!("TODO $where is not supported"); //16395 in agg
        },
        &QueryItem::Text(ref s) => {
            // TODO is there more work to do here?  or does the index code deal with it all now?
            true
        },
    }
}

fn match_query_doc<F: Fn(&str, usize)>(q: &QueryDoc, d: &bson::Value, cb_array_pos: &F) -> bool {
    let &QueryDoc::QueryDoc(ref items) = q;
    // AND
    for qit in items {
        if !match_query_item(qit, d, cb_array_pos) {
            return false;
        }
    }
    true
}

fn contains_no_dollar_keys(v: &bson::Value) -> bool {
    match v {
        &bson::Value::BDocument(ref bd) => {
            bd.pairs.iter().all(|&(ref k, _)| !k.starts_with("$"))
        },
        _ => true,
    }
}

fn is_valid_within_all(v: &bson::Value) -> bool {
    contains_no_dollar_keys(v)
}

fn is_valid_within_in(v: &bson::Value) -> bool {
    contains_no_dollar_keys(v)
}

// TODO I suppose this func could return &str slices into the QueryDoc?
fn get_paths(q: &QueryDoc) -> Vec<String> {
    fn f(a: &mut Vec<String>, q: &QueryDoc) {
        let &QueryDoc::QueryDoc(ref items) = q;
        for qit in items {
            match qit {
                &QueryItem::Compare(ref path, ref preds) => {
                    a.push(path.clone());
                },
                &QueryItem::AND(ref docs) => {
                    for d in docs {
                        f(a, d);
                    }
                },
                _ => {
                },
            }
        }
    }
    let mut a = Vec::new();
    f(&mut a, q);
    let a = a.into_iter().collect::<std::collections::HashSet<_>>();
    let a = a.into_iter().collect::<Vec<_>>();
    a
}

fn get_eqs(q: &QueryDoc) -> Vec<(&str, &bson::Value)> {
    fn f<'q>(a: &mut Vec<(&'q str, &'q bson::Value)>, q: &'q QueryDoc) {
        let &QueryDoc::QueryDoc(ref items) = q;
        for qit in items {
            match qit {
                &QueryItem::Compare(ref path, ref preds) => {
                    for psub in preds {
                        match psub {
                            &Pred::EQ(ref v) => {
                                a.push((path, v));
                            },
                            _ => {
                            },
                        }
                    }
                },
                &QueryItem::AND(ref docs) => {
                    for d in docs {
                        f(a, d);
                    }
                },
                _ => {
                },
            }
        }
    }
    let mut a = Vec::new();
    f(&mut a, q);
    // TODO error if there are any duplicate keys
    a
}

fn is_query_doc(v: &bson::Value) -> bool {
    match v {
        &bson::Value::BDocument(ref bd) => {
            let has_path = bd.pairs.iter().any(|&(ref k, _)| !k.starts_with("$"));
            let has_and = bd.pairs.iter().any(|&(ref k, _)| k == "$and");
            let has_or = bd.pairs.iter().any(|&(ref k, _)| k == "$or");
            let has_nor = bd.pairs.iter().any(|&(ref k, _)| k == "$nor");
            has_path || has_and || has_or || has_nor
        },
        _ => {
            // TODO or panic?
            false
        }
    }
}

fn parse_pred_list(pairs: &Vec<(String,bson::Value)>) -> Vec<Pred> {
    unimplemented!();
}

fn parse_compare(k: &str, v: &bson::Value) -> Result<QueryItem> {
    if k.starts_with("$") {
        return Err(super::Error::Misc("parse_compare $"));
    }
    let qit = 
        match v {
            &bson::Value::BDocument(ref bd) => {
                if bd.is_dbref() {
                    QueryItem::Compare(String::from(k), vec![Pred::EQ(v.clone())])
                } else if bd.pairs.iter().any(|&(ref k, _)| k.starts_with("$")) {
                    let preds = parse_pred_list(&bd.pairs);
                    QueryItem::Compare(String::from(k), preds)
                } else {
                    QueryItem::Compare(String::from(k), vec![Pred::EQ(v.clone())])
                }
            },
            &bson::Value::BRegex(ref expr, ref options) => {
                QueryItem::Compare(String::from(k), vec![Pred::REGEX(String::from("TODO"))])
            },
            _ => {
                // TODO clone
                QueryItem::Compare(String::from(k), vec![Pred::EQ(v.clone())])
            },
        };
    Ok(qit)
}

// TODO this func kinda wants to consume its argument
fn parse_query_doc(bd: &bson::Document) -> Result<Vec<QueryItem>> {
    fn do_and_or(result: &mut Vec<QueryItem>, a: &Vec<bson::Value>, op: &str) -> Result<()> {
        if a.len() == 0 {
            // TODO no panic
            panic!("array for $and $or cannot be empty");
        } else if a.len() == 1 {
            let d = try!(a[0].as_document());
            let subpairs = try!(parse_query_doc(d));
            for it in subpairs {
                result.push(it);
            }
        } else {
            // TODO this wants to be a map+closure, but the error handling is weird
            let mut m = Vec::new();
            for d in a {
                let d = try!(d.as_document());
                let d = try!(parse_query_doc(d));
                let d = QueryDoc::QueryDoc(d);
                m.push(d);
            }
            // TODO grrr.  this str compare hack is ugly.
            if op == "$and" {
                result.push(QueryItem::AND(m));
            } else if op == "$or" {
                result.push(QueryItem::OR(m));
            } else {
                unreachable!();
            }
        }
        Ok(())
    }

    let mut result = Vec::new();
    for &(ref k, ref v) in &bd.pairs {
        match k.as_str() {
            "$comment" => {
            },
            "$atomic" => {
            },
            "$where" => {
                // TODO clone
                result.push(QueryItem::Where(v.clone()));
            },
            "$and" => {
                let ba = try!(v.as_array());
                do_and_or(&mut result, &ba.items, k);
            },
            "$or" => {
                let ba = try!(v.as_array());
                do_and_or(&mut result, &ba.items, k);
            },
            "$text" => {
                match v {
                    &bson::Value::BDocument(ref bd) => {
                        match bd.pairs.iter().find(|&&(ref k, _)| k == "$search") {
                            Some(&(_, bson::Value::BString(ref s))) => {
                                result.push(QueryItem::Text(s.clone()));
                            },
                            _ => panic!("invalid $text"),
                        }
                    },
                    _ => panic!("invalid $text"),
                }
            },
            "$nor" => {
                let ba = try!(v.as_array());
                if ba.items.len() == 0 {
                    // TODO no panic
                    panic!("array for $and $or cannot be empty");
                }
                // TODO what if just one?  canonicalize?
                // TODO this wants to be a map+closure, but the error handling is weird
                let mut m = Vec::new();
                for d in &ba.items {
                    let d = try!(d.as_document());
                    let d = try!(parse_query_doc(d));
                    let d = QueryDoc::QueryDoc(d);
                    m.push(d);
                }
                result.push(QueryItem::NOR(m));
            },
            _ => {
                result.push(try!(parse_compare(k, v)));
            },
        }
    }
    Ok(result)
}

pub fn parse_query(v: bson::Document) -> Result<QueryDoc> {
    let a = try!(parse_query_doc(&v));
    let q = QueryDoc::QueryDoc(a);
    Ok(q)
}

