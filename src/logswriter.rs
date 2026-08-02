use clap::ValueEnum;
use std::fmt::{Display, Formatter};
use tracing_subscriber::fmt::writer::BoxMakeWriter;

#[derive(Clone, ValueEnum)]
pub enum LogsWriter {
    StdErr,
}

impl Display for LogsWriter {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let w = match self {
            Self::StdErr => "stderr",
        };
        f.write_str(w)
    }
}

impl LogsWriter {
    pub fn into_make_writer(self) -> BoxMakeWriter {
        match self {
            LogsWriter::StdErr => BoxMakeWriter::new(std::io::stderr),
        }
    }
}
