use std::borrow::Cow;
use std::env;
use std::path::{Path, PathBuf};

/// A _probably_ unique chunk ID.
#[derive(PartialEq, Debug, Clone)]
pub struct ChunkId<'a>(pub(crate) Cow<'a, str>);

impl ChunkId<'_> {
    /// Returns the path to the chunk in `upload/` corresponding to this ID.
    pub fn file_path(&self) -> PathBuf {
        let root = env::var("UPLOAD_DIR").unwrap_or(String::from("./upload"));
        let id_str = self.id();

        if id_str.len() < 2 {
            return Path::new(&root).join("null").join(id_str);
        }

        let first_char = &id_str[0..1];
        let second_char = &id_str[1..2];

        Path::new(&root)
            .join(first_char)
            .join(second_char)
            .join(id_str)
    }

    pub fn id(&self) -> &str {
        self.0.as_ref()
    }

    /// Returns the path to the chunk in `upload/` corresponding to this ID.
    pub fn is_present(&self) -> bool {
        if self.0.as_ref() == "" {
            return true;
        }

        self.file_path().exists()
    }
}

impl<'a> ChunkId<'a> {
    /// Returns an instance of `ChunkId` if the path segment is a valid ID.
    /// Otherwise returns the invalid ID as the `Err` value.
    ///
    /// Replaces Rocket's `FromParam` impl; the caller (`chunks::retrieve`)
    /// applies it to the captured path segment and turns an `Err` into the
    /// same 422 Rocket produced when a param guard failed.
    pub fn from_param(param: &'a str) -> Result<Self, &'a str> {
        param
            .chars()
            .all(|c| c.is_ascii_alphanumeric())
            .then(|| ChunkId(param.into()))
            .ok_or(param)
    }
}

impl<'a> From<&'a str> for ChunkId<'a> {
    fn from(file_name: &'a str) -> Self {
        // Convert FileName to a string slice (&str)

        // Convert &str to Cow<str>
        ChunkId(Cow::from(file_name))
    }
}
