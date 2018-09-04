// swift-tools-version:4.0
import PackageDescription

let package = Package(
  name: "FoundationDB",
  products: [
    .library(name: "FoundationDB", targets: ["FoundationDB"]),
    .executable(name: "FoundationDBBindingTestRunner", targets: ["FoundationDBBindingTestRunner"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio", from: "1.2.0"),
    .package(url: "https://github.com/FoundationDB/fdb-swift-c-packaging", .branch("master"))
  ],
  targets: [
    .target(name: "FoundationDB", dependencies: ["NIO", "CFoundationDB"]),
    .target(name: "CFoundationDB"),
    .target(name: "FoundationDBBindingTest", dependencies: ["FoundationDB"]),
    .target(name: "FoundationDBBindingTestRunner", dependencies: ["FoundationDBBindingTest"]),
    .testTarget(name: "FoundationDBTests", dependencies: ["FoundationDB"]),
    .testTarget(name: "FoundationDBBindingTestTests", dependencies: ["FoundationDBBindingTest"]),
  ]
)
