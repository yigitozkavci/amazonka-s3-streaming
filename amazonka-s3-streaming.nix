{ mkDerivation, amazonka, amazonka-core, amazonka-s3, async, base
, bytestring, conduit, dlist, exceptions, http-client, lens, mmorph
, mtl, stdenv
}:
mkDerivation {
  pname = "amazonka-s3-streaming";
  version = "1.1.0.0";
  src = ./.;
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    amazonka amazonka-core amazonka-s3 async base bytestring conduit
    dlist exceptions http-client lens mmorph mtl
  ];
  homepage = "https://github.com/Axman6/amazonka-s3-streaming#readme";
  description = "Provides conduits to upload data to S3 using the Multipart API";
  license = stdenv.lib.licenses.bsd3;
}
