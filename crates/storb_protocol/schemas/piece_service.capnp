@0x986e747693ae66ae;

using T = import "types.capnp";
using ProtocolVersion = T.ProtocolVersion;
using PieceHash = T.PieceHash;

struct Piece {
    protocolVersion @0 :ProtocolVersion;
    size @1 :UInt64;
    data @2 :Data;
}

interface PieceService {
    storePiece @0 (piece :Piece) -> (pieceHash :PieceHash);
    getPiece @1 (pieceHash :PieceHash) -> (piece :Piece);
}
