pub const InsertError = error {
    EmptyBody,
    MaxBodySize,
    DecompressFailed,
    ContentTypeNotSupported,
    ContentEncodingNotSupported,
    FailedToProccess,
};
