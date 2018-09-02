//! Delimiter bindings to provide byte offsets for all stages.
use super::conf::Configuration;

/// Delimiters struct to store the input/output separators
/// for all stages of a MapReduce lifecycle. Once created,
/// this structure should be considered immutable.
#[derive(Debug)]
pub struct Delimiters {
    input: String,
    output: String,
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
            input: conf.get(&input_key).unwrap_or("\t").to_owned(),
            output: conf.get(&output_key).unwrap_or("\t").to_owned(),
        }
    }

    /// Returns a reference to the input delimiter.
    #[inline]
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Returns a reference to the output delimiter.
    #[inline]
    pub fn output(&self) -> &str {
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

        assert_eq!(delim.input(), ":");
        assert_eq!(delim.output(), "|");
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

        assert_eq!(delim.input(), ":");
        assert_eq!(delim.output(), "|");
    }

    #[test]
    fn test_delimiter_defaults() {
        let env = Vec::<(String, String)>::new();

        let conf = Configuration::with_env(env.into_iter());
        let delim = Delimiters::new(&conf);

        assert_eq!(delim.input(), "\t");
        assert_eq!(delim.output(), "\t");
    }
}
