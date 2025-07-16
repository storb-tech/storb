@0xb42c8313853a9d6a;

using T = import "types.capnp";
using ProtocolVersion = T.ProtocolVersion;

struct LocalNodeInfo {
    using Multiaddr = List(UInt8);

    uid @0 :Uid;
    quicAddress @1 :Address;

    struct Uid {
        union {
            none @0 :Void;
            some @1 :UInt16;
        }
    }

    struct Address {
        union {
            none @0 :Void;
            some @1 :Multiaddr;
        }
    }
}

struct InfoResult {
    protocolVersion @0 :ProtocolVersion;
    status @1 :Status;
    nodeInfo @2 :LocalNodeInfo;

    enum Status {
        ok @0;
        internalServerError @1;
        unauthorized @2;
    }
}

interface InfoService {
    info @0 () -> (result :InfoResult);
}
