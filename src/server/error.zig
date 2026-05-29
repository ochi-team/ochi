pub const ApiError = error{
    EmptyBody,
    MaxBodySize,
    DecompressFailed,
    ContentTypeNotSupported,
    ContentEncodingNotSupported,
    FailedToProccess,
    FailedToParse,
    InvalidBody,
    FailedToWriteResponse,
    InternalError,
};
