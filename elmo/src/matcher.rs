
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


