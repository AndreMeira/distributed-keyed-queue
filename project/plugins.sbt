// ScalaPB + zio-grpc — generates ZIO-native gRPC stubs from src/main/protobuf during compile.
// Versions match homelab-schemas so the generated code is identical to what a real service would get.
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")

// Packages the service as a Docker image (`sbt Docker/publishLocal`), which is what the end-to-end suite
// runs: several real instances against one Valkey, driven over the wire. See docs/testing/end-to-end.md.
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1")

addSbtPlugin("com.github.sbt" % "sbt-javaagent" % "0.1.8")
