// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Dyno",
    platforms: [.macOS(.v10_15), .iOS(.v13)],
    products: [
        .library(
            name: "Dyno",
            targets: ["Dyno"])
    ],
    dependencies: [
        .package(url: "https://github.com/jrsonline/StrictlySwiftLib.git", .branch("master")),
        .package(url: "https://github.com/jrsonline/StrictlySwiftTestLib.git", .branch("master")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "Dyno",
            dependencies: ["StrictlySwiftLib"]),
        .testTarget(
            name: "DynoTests",
            dependencies: ["Dyno", "StrictlySwiftLib", "StrictlySwiftTestLib"]),
    ]
)



