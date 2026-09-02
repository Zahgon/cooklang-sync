use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum CommitResultStatus {
    Success(i32),
    NeedChunks(String),
}
