pub const ApiError = error{
    EmptyBody,
    MaxBodySize,
    DecompressFailed,
    ContentTypeNotSupported,
    ContentEncodingNotSupported,
    FailedToProccess,
    FailedToParse,
    InvalidTimestamp,
    InvalidBody,
    FailedToWriteResponse,
    Timeout,
    InternalError,
};
