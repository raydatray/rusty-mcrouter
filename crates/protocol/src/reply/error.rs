use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorReply {
    Error,                 // ERROR
    Client(Option<Bytes>), // CLIENT_ERROR [message]
    Server(Option<Bytes>), // SERVER_ERROR [message]
}
