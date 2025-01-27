@0xca5797f8b9e17f24;

struct PieceInfo {
    pieceId @0 :Text;
    piece @1 :Data;
}

interface PieceInfoInterface {
    storePiece @0 (p :PieceInfo) -> (totalPoints :UInt64);
}
