@0xd48f1e581f586fef;

struct FileUploadRequest {
  filename @0 :Text;
  data @1 :Data;  # Binary file data
  contentType @2 :Text;
}

struct FileUploadResponse {
  success @0 :Bool;
  message @1 :Text;
  fileId @2 :Text;  # Return a unique ID for the uploaded file
}

struct MinerStoreRequest {
  data @0 :Data;  # Binary file data
}

struct MinerStoreResponse {
  pieceId @0 :Text;  # Return a unique ID for the uploaded file
}
