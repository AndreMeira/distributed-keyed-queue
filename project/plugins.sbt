// ScalaPB + zio-grpc — generates ZIO-native gRPC stubs from src/main/protobuf during compile.
// Versions match homelab-schemas so the generated code is identical to what a real service would get.
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
