pub mod v2 {
    pub mod requests;
    pub mod responses;

    use std::collections::HashSet;
    use std::fmt;
    use std::str::Utf8Error;

    use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    use serde::de;
    use serde::de::{Deserializer, Error, SeqAccess};
    use serde::ser;

    pub type Int = u64;
    pub type Map = serde_json::Map<String, serde_json::Value>;

    struct BucketTypeListVisitor;

    /// The set of characters to percent encode.
    ///
    /// B2 docs approve of encoding absolutely everything but the `/` cannot be
    /// encoded in paths so this takes the conservative approach and encodes
    /// everything except alpha-numeric characters and the `/` character.
    /// Encoding spaces as `%20` is fine even though B2 will return spaces
    /// encoded as `+`.
    const ENCODE_SET: AsciiSet = NON_ALPHANUMERIC.remove(b'/');

    pub fn percent_decode(value: &str) -> Result<String, Utf8Error> {
        // Must first convert `+` characters back to spaces.
        let string = value.replace('+', " ");

        Ok(percent_decode_str(&string).decode_utf8()?.into_owned())
    }

    pub fn percent_encode(value: &str) -> String {
        utf8_percent_encode(value, &ENCODE_SET).collect()
    }

    impl<'de> de::Visitor<'de> for BucketTypeListVisitor {
        type Value = BucketTypes;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "An array of 'all', 'allPublic', 'allPrivate' or 'snapshot'"
            )
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let first: String = match seq.next_element()? {
                Some(item) => item,
                None => return Err(A::Error::invalid_length(0, &self)),
            };

            if &first == "all" {
                Ok(BucketTypes::All)
            } else {
                let visitor = BucketTypeVisitor;
                let bucket_type = visitor.visit_str(&first)?;

                let mut set: HashSet<BucketType> = Default::default();
                set.insert(bucket_type);

                loop {
                    match seq.next_element()? {
                        Some(bucket_type) => {
                            set.insert(bucket_type);
                        }
                        None => return Ok(BucketTypes::Some(set)),
                    }
                }
            }
        }
    }

    struct BucketTypesVisitor;

    impl<'de> de::Visitor<'de> for BucketTypesVisitor {
        type Value = BucketTypes;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "An optional array of 'all', 'allPublic', 'allPrivate' or 'snapshot'"
            )
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(BucketTypeListVisitor)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Default::default())
        }
    }

    #[derive(Debug, Clone)]
    pub enum BucketTypes {
        All,
        Some(HashSet<BucketType>),
        Any,
    }

    impl BucketTypes {
        pub fn includes(&self, bucket_type: BucketType) -> bool {
            match self {
                Self::All => {
                    if let BucketType::Unknown(_) = bucket_type {
                        false
                    } else {
                        true
                    }
                }
                Self::Some(set) => set.contains(&bucket_type),
                Self::Any => true,
            }
        }
    }

    impl Default for BucketTypes {
        fn default() -> Self {
            Self::Any
        }
    }

    impl ser::Serialize for BucketTypes {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ser::Serializer,
        {
            match self {
                Self::All => serializer.serialize_some(&["all"]),
                Self::Some(set) => {
                    let types: Vec<BucketType> = set.iter().cloned().collect();
                    serializer.serialize_some(&types)
                }
                Self::Any => serializer.serialize_none(),
            }
        }
    }

    impl<'de> de::Deserialize<'de> for BucketTypes {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            deserializer.deserialize_option(BucketTypesVisitor)
        }
    }

    struct BucketTypeVisitor;

    impl<'de> de::Visitor<'de> for BucketTypeVisitor {
        type Value = BucketType;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "'allPublic', 'allPrivate' or 'snapshot'")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v {
                "allPublic" => Ok(BucketType::Public),
                "allPrivate" => Ok(BucketType::Private),
                "snapshot" => Ok(BucketType::Snapshot),
                s => Ok(BucketType::Unknown(s.to_owned())),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub enum BucketType {
        Public,
        Private,
        Snapshot,
        Unknown(String),
    }

    impl ser::Serialize for BucketType {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ser::Serializer,
        {
            match self {
                Self::Public => serializer.serialize_str("allPublic"),
                Self::Private => serializer.serialize_str("allPrivate"),
                Self::Snapshot => serializer.serialize_str("snapshot"),
                Self::Unknown(s) => serializer.serialize_str(&s),
            }
        }
    }

    impl<'de> de::Deserialize<'de> for BucketType {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: de::Deserializer<'de>,
        {
            deserializer.deserialize_str(BucketTypeVisitor)
        }
    }
}