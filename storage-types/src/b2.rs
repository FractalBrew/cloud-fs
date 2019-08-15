pub mod v2 {
    pub mod requests;
    pub mod responses;

    use std::collections::HashSet;
    use std::fmt;

    use serde::de;
    use serde::de::{Deserializer, Error, SeqAccess};
    use serde::ser;

    struct BucketTypeListVisitor;

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
