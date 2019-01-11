//! Delimiter bindings to provide byte offsets for all stages.
use super::conf::Configuration;

/// Delimiters struct to store the input/output separators
/// for all stages of a MapReduce lifecycle. Once created,
/// this structure should be considered immutable.
#[derive(Debug)]
pub struct Delimiters {
    input: Vec<u8>,
    output: Vec<u8>,
}

impl Delimiters {
    /// Creates a new `Delimiters` from a job `Configuration`.
    pub fn new(conf: &Configuration) -> Self {
        // check to see if this is map/reduce stage
        let stage = match conf.get("mapreduce.task.ismap") {
            Some(val) if val == "true" => "map",
            _ => "reduce",
        };

        // fetch the input/output separators for the current stage
        let input_key = format!("stream.{}.input.field.separator", stage);
        let output_key = format!("stream.{}.output.field.separator", stage);

        Self {
            // separators are optional, so default to a tab
            input: conf.get(&input_key).unwrap_or("\t").as_bytes().to_vec(),
            output: conf.get(&output_key).unwrap_or("\t").as_bytes().to_vec(),
        }
    }

    /// Returns a reference to the input delimiter.
    #[inline]
    pub fn input(&self) -> &[u8] {
        &self.input
    }

    /// Returns a reference to the output delimiter.
    #[inline]
    pub fn output(&self) -> &[u8] {
        &self.output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_delimiters_creation() {
        let env = vec![
            ("mapreduce.task.ismap", "true"),
            ("stream.map.input.field.separator", ":"),
            ("stream.map.output.field.separator", "|"),
        ];

        let conf = Configuration::with_env(env.into_iter());
        let delim = Delimiters::new(&conf);

        assert_eq!(delim.input(), b":");
        assert_eq!(delim.output(), b"|");
    }

    #[test]
    fn test_reduce_delimiters_creation() {
        let env = vec![
            ("mapreduce.task.ismap", "false"),
            ("stream.reduce.input.field.separator", ":"),
            ("stream.reduce.output.field.separator", "|"),
        ];

        let conf = Configuration::with_env(env.into_iter());
        let delim = Delimiters::new(&conf);

        assert_eq!(delim.input(), b":");
        assert_eq!(delim.output(), b"|");
    }

    #[test]
    fn test_delimiter_defaults() {
        let env = Vec::<(String, String)>::new();

        let conf = Configuration::with_env(env.into_iter());
        let delim = Delimiters::new(&conf);

        assert_eq!(delim.input(), b"\t");
        assert_eq!(delim.output(), b"\t");
    }
}
