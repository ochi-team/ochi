pub const ApiError = error{
    EmptyBody,
    MaxBodySize,
    DecompressFailed,
    ContentTypeNotSupported,
    ContentEncodingNotSupported,
    FailedToProccess,
    FailedToParse,
    FailedToWriteResponse,
    InternalError,
};
