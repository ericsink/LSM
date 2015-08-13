
use std;
use std::cmp::Ordering;

extern crate bson;
use bson::BsonValue;

enum QueryDoc {
    QueryDoc(Vec<QueryItem>),
}

enum QueryItem {
    Compare(String, Vec<Pred>),
    AND(Vec<QueryDoc>),
    OR(Vec<QueryDoc>),
    NOR(Vec<QueryDoc>),
    Where(BsonValue),
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
    In(Vec<BsonValue>),
    Nin(Vec<BsonValue>),
    All(Vec<BsonValue>),
    AllElemMatchObjects(Vec<QueryDoc>),
    EQ(BsonValue),
    NE(BsonValue),
    GT(BsonValue),
    LT(BsonValue),
    GTE(BsonValue),
    LTE(BsonValue),
    REGEX(String),
    Near(BsonValue),
    NearSphere(BsonValue),
    GeoWithin(BsonValue),
    GeoIntersects(BsonValue),
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

fn cmp(d: &BsonValue, lit: &BsonValue) -> Ordering {
    match (d,lit) {
        (&BsonValue::BObjectID(m), &BsonValue::BObjectID(litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BInt32(m), &BsonValue::BInt32(litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BInt64(m), &BsonValue::BInt64(litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BDateTime(m), &BsonValue::BDateTime(litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BTimeStamp(m), &BsonValue::BTimeStamp(litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BDouble(m), &BsonValue::BDouble(litv)) => {
            cmp_f64(m, litv)
        },
        (&BsonValue::BString(ref m), &BsonValue::BString(ref litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BBoolean(m), &BsonValue::BBoolean(litv)) => {
            m.cmp(&litv)
        },
        (&BsonValue::BUndefined, &BsonValue::BUndefined) => {
            Ordering::Equal
        },
        (&BsonValue::BNull, &BsonValue::BNull) => {
            Ordering::Equal
        },
        (&BsonValue::BInt32(m), &BsonValue::BInt64(litv)) => {
            let m = m as i64;
            m.cmp(&litv)
        },
        (&BsonValue::BInt32(m), &BsonValue::BDouble(litv)) => {
            let m = m as f64;
            cmp_f64(m, litv)
        },
        (&BsonValue::BInt64(m), &BsonValue::BInt32(litv)) => {
            let litv = litv as i64;
            m.cmp(&litv)
        },
        (&BsonValue::BInt64(m), &BsonValue::BDouble(litv)) => {
            let m = m as f64;
            cmp_f64(m, litv)
        },
        (&BsonValue::BDouble(m), &BsonValue::BInt32(litv)) => {
            // when comparing double and int, cast the int to double, regardless of ordering
            let litv = litv as f64;
            cmp_f64(m, litv)
        },
        (&BsonValue::BDouble(m), &BsonValue::BInt64(litv)) => {
            // when comparing double and int, cast the int to double, regardless of ordering
            // TODO this can overflow
            let litv = litv as f64;
            cmp_f64(m, litv)
        },
        (&BsonValue::BArray(ref m), &BsonValue::BArray(ref litv)) => {
            let lenm = m.len();
            let lenlitv = litv.len();
            let len = std::cmp::min(lenm, lenlitv);
            for i in 0 .. len {
                let c = cmp(&m[i], &litv[i]);
                if c != Ordering::Equal {
                    return c;
                }
            }
            lenm.cmp(&lenlitv)
        },
        (&BsonValue::BDocument(ref m), &BsonValue::BDocument(ref litv)) => {
            let lenm = m.len();
            let lenlitv = litv.len();
            let len = std::cmp::min(lenm, lenlitv);
            for i in 0 .. len {
                if m[i].0 < litv[i].0 {
                    return Ordering::Less;
                } else if m[i].0 > litv[i].0 {
                    return Ordering::Greater;
                } else {
                    let c = cmp(&m[i].1, &litv[i].1);
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

fn array_min_max(a: &Vec<BsonValue>, judge: Ordering) -> Option<&BsonValue> {
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

fn array_min(a: &Vec<BsonValue>) -> Option<&BsonValue> {
    array_min_max(a, Ordering::Less)
}

fn array_max(a: &Vec<BsonValue>) -> Option<&BsonValue> {
    array_min_max(a, Ordering::Greater)
}

fn cmpdir(d: &BsonValue, lit: &BsonValue, reverse: bool) -> Ordering {
    // when comparing an array against something else during sort:
    // if two arrays, compare element by element.
    // if array vs. not-array, find the min or max (depending on the
    // sort direction) of the array and compare against that.

    let c = 
        match (d, lit) {
            (&BsonValue::BArray(_), &BsonValue::BArray(_)) => {
                cmp(d, lit)
            },
            (&BsonValue::BArray(ref a), _) => {
                let om =
                    if reverse {
                        array_max(a)
                    } else {
                        array_min(a)
                    };
                match om {
                    Some(m) => cmp(m, lit),
                    // TODO is the following the correct behavior for an empty array?
                    None => cmp(d, lit),
                }
            },
            (_, &BsonValue::BArray(ref a)) => {
                let om =
                    if reverse {
                        array_max(a)
                    } else {
                        array_min(a)
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

fn cmp_eq(d: &BsonValue, lit: &BsonValue) -> bool {
    let torder_d = d.get_type_order();
    let torder_lit = lit.get_type_order();

    if torder_d == torder_lit {
        cmp(d, lit) == Ordering::Equal
    } else {
        false
    }
}

fn cmp_in(d: &BsonValue, lit: &BsonValue) -> bool {
    match lit {
        &BsonValue::BRegex(ref expr, ref options) => {
            match d {
                &BsonValue::BString(ref s) => {
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

fn cmp_lt_gt(d: &BsonValue, lit: &BsonValue, judge: Ordering) -> bool {
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

fn cmp_lt(d: &BsonValue, lit: &BsonValue) -> bool {
    cmp_lt_gt(d, lit, Ordering::Less)
}

fn cmp_gt(d: &BsonValue, lit: &BsonValue) -> bool {
    cmp_lt_gt(d, lit, Ordering::Greater)
}

fn cmp_lte_gte(d: &BsonValue, lit: &BsonValue, judge: Ordering) -> bool {
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

fn cmp_lte(d: &BsonValue, lit: &BsonValue) -> bool {
    cmp_lte_gte(d, lit, Ordering::Less)
}

fn cmp_gte(d: &BsonValue, lit: &BsonValue) -> bool {
    cmp_lte_gte(d, lit, Ordering::Greater)
}

fn do_elem_match_objects<F: Fn(&str, usize)>(doc: &QueryDoc, d: &BsonValue, cb_array_pos: &F) -> bool {
    match d {
        &BsonValue::BArray(ref a) => {
            for vsub in a {
                match vsub {
                    &BsonValue::BArray(_) | &BsonValue::BDocument(_) => {
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

fn match_predicate<F: Fn(&str, usize)>(pred: &Pred, d: &BsonValue, cb_array_pos: &F) -> bool {
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
                &BsonValue::BArray(ref a) => {
                    let found = 
                        a.iter().position(|vsub| {
                            match vsub {
                                &BsonValue::BDocument(_) | &BsonValue::BArray(_) => match_query_doc(doc, vsub, cb_array_pos),
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
                &BsonValue::BArray(ref a) => {
                    let found = 
                        a.iter().position(|vsub| preds.iter().any(|p| !match_predicate(p, vsub, cb_array_pos)));
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
                                &BsonValue::BArray(ref a) => {
                                    a.iter().any(|v| cmp_eq(v, lit))
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
        &Pred::REGEX(_) => unimplemented!(),
        &Pred::Near(_) => unimplemented!(),
        &Pred::NearSphere(_) => unimplemented!(),
        &Pred::GeoWithin(_) => unimplemented!(),
        &Pred::GeoIntersects(_) => unimplemented!(),
        &Pred::Type(n) => (d.getTypeNumber_u8() as i32) == n,
        &Pred::In(ref lits) => lits.iter().any(|v| cmp_in(d, v)),
        &Pred::Nin(ref lits) => !lits.iter().any(|v| cmp_in(d, v)),
        &Pred::Size(n) => {
            match d {
                &BsonValue::BArray(ref a) => a.len() == (n as usize),
                _ => false,
            }
        },
        &Pred::Mod(div, rem) => {
            match d {
                &BsonValue::BInt32(n) => ((n as i64) % div) == rem,
                &BsonValue::BInt64(n) => (n % div) == rem,
                &BsonValue::BDouble(n) => ((n as i64) % div) == rem,
                _ => false,
            }
        },
    }
}

fn match_pair_exists(pred: &Pred, path: &str, start: &BsonValue) -> bool {
    // TODO
    true
}

fn match_pair_other<F: Fn(&str, usize)>(pred: &Pred, path: &str, start: &BsonValue, arr: bool, cb_array_pos: &F) -> bool {
    // TODO
    true
}

fn match_pair<F: Fn(&str, usize)>(pred: &Pred, path: &str, start: &BsonValue, cb_array_pos: &F) -> bool {
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

fn match_query_item<F: Fn(&str, usize)>(qit: &QueryItem, d: &BsonValue, cb_array_pos: &F) -> bool {
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

fn match_query_doc<F: Fn(&str, usize)>(q: &QueryDoc, d: &BsonValue, cb_array_pos: &F) -> bool {
    let &QueryDoc::QueryDoc(ref items) = q;
    // AND
    for qit in items {
        if !match_query_item(qit, d, cb_array_pos) {
            return false;
        }
    }
    true
}


