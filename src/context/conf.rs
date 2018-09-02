//! Module to provide representation of the Hadoop `Configuration` class.
use std::collections::HashMap;
use std::env;

/// Configuration struct to represent a Hadoop configuration.
///
/// Internally this is simply a `String` -> `String` map, as
/// we don't have enough information to parse with. The struct
/// implementation exists as a compatibility layer.
#[derive(Debug)]
pub struct Configuration {
    inner: HashMap<String, String>,
}

impl Configuration {
    /// Constructs a new `Configuration` using Hadoop's input.
    pub fn new() -> Self {
        Self::with_env(env::vars())
    }

    /// Constructs a new `Configuration` using a custom input.
    pub fn with_env<I, T>(pairs: I) -> Self
    where
        T: Into<String>,
        I: Iterator<Item = (T, T)>,
    {
        // create container
        let mut conf = Self {
            inner: HashMap::new(),
        };

        // iterate all pairs
        for (key, val) in pairs {
            let key = key.into();
            let val = val.into();

            // hadoop never has uppercased values
            if key.chars().any(|c| c.is_uppercase()) {
                continue;
            }

            // insert the key/value pair
            conf.insert(key, val);
        }

        conf
    }

    /// Retrieves a potential `Configuration` value.
    pub fn get(&self, key: &str) -> Option<&str> {
        // shimming for hadoop
        let opt = if key.contains(".") {
            self.inner.get(&key.replace(".", "_"))
        } else {
            self.inner.get(key)
        };

        // better than &String
        opt.map(|s| s.as_ref())
    }

    /// Inserts a key/value pair into the `Configuration`.
    pub fn insert<T>(&mut self, key: T, val: T)
    where
        T: Into<String>,
    {
        // convert to String
        let mut key_str = key.into();

        // hadoop compatibility
        if key_str.contains(".") {
            key_str = key_str.replace(".", "_");
        }

        // insert into the internal mapping
        self.inner.insert(key_str, val.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let env = vec![
            ("FAKE_VAR", "1"),
            ("mapred.job.id", "123"),
            ("mapred_job_id", "123"),
        ];

        let conf = Configuration::with_env(env.into_iter());

        assert_eq!(conf.inner.get("FAKE_VAR"), None);
        assert_eq!(conf.inner.get("mapred.job.id"), None);
        assert_eq!(conf.inner.get("mapred_job_id"), Some(&"123".to_owned()));
    }

    #[test]
    fn test_retrieval_shimming() {
        let env = vec![("mapred.job.id", "123"), ("mapred_job_id", "123")];
        let conf = Configuration::with_env(env.into_iter());

        assert_eq!(conf.get("mapred.job.id"), Some("123"));
        assert_eq!(conf.get("mapred_job_id"), Some("123"));
    }

    #[test]
    fn test_insertion_shimming() {
        let env = Vec::<(String, String)>::new();
        let mut conf = Configuration::with_env(env.into_iter());

        conf.insert("mapred.job.id", "123");

        assert_eq!(conf.get("mapred_job_id"), Some("123"));
    }
}
