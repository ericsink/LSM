
extern crate bson;

use bson::BsonValue;

#[test]
fn bson_simple() {
    fn f() -> bson::Result<()> {
        let mut pairs = Vec::new();
        pairs.push((String::from("i32"), BsonValue::BInt32(40)));
        pairs.push((String::from("string"), BsonValue::BString(String::from("forty"))));
        let mut a = Vec::new();
        a.push(BsonValue::BInt64(40));
        a.push(BsonValue::BDouble(40.0));
        a.push(BsonValue::BNull);
        pairs.push((String::from("array"), BsonValue::BArray(a)));
        let bv = BsonValue::BDocument(pairs);
        let mut buf = Vec::new();
        bv.to_bson(&mut buf);
        println!("{:?}", buf);
        match try!(BsonValue::from_bson(&buf)) {
            BsonValue::BDocument(a) => {
                assert_eq!(3, a.len());

                let (ref k,ref v) = a[0];
                assert_eq!(k, "i32");
                match v {
                    &BsonValue::BInt32(n) => assert_eq!(n, 40),
                    _ => panic!(),
                }

                let (ref k,ref v) = a[1];
                assert_eq!(k, "string");
                match v {
                    &BsonValue::BString(ref s) => assert_eq!(s, "forty"),
                    _ => panic!(),
                }

                let (ref k,ref v) = a[2];
                assert_eq!(k, "array");
                match v {
                    &BsonValue::BArray(ref a) => assert_eq!(a.len(), 3),
                    _ => panic!(),
                }

            },
            _ => panic!(),
        }
        Ok(())
    }
    assert!(f().is_ok());
}


