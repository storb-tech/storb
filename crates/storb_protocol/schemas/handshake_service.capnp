@0xe61bf670800089db;

using T = import "types.capnp";
using ProtocolVersion = T.ProtocolVersion;

struct KeyRegistrationInfo {
    using AccountId = List(UInt8);

    uid @0 :UInt16;
    accountId @1 :AccountId;
}

struct VerificationMessage {
    netuid @0 :UInt16;
    sender @1 :KeyRegistrationInfo;
    receiver @2 :KeyRegistrationInfo;
}

struct HandshakePayload {
    using KeypairSignature = List(UInt8);

    protocolVersion @0 :ProtocolVersion;
    signature @1 :KeypairSignature;
    message @2 :VerificationMessage;
}

struct HandshakeResult {
    protocolVersion @0 :ProtocolVersion;
    status @1 :Status;

    enum Status {
        ok @0;
        internalServerError @1;
        unauthorized @2;
    }
}

interface HandshakeService {
    handshake @0 (payload :HandshakePayload) -> (result :HandshakeResult);
}
