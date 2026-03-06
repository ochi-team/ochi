const InsertError = error {
    EmptyBody,
    MaxBodySize,
    DecompressFailed,
    ContentTypeNotSupported,
    ContentEncodingNotSupported,
    FailedToProccess,
};

const GeneralError = error {
    InternalServerError,
};

pub const ServerError = InsertError || GeneralError;
