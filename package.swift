// swift-tools-version:5.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Dyno",
    products: [
        .library(
            name: "Dyno",
            targets: ["Dyno"])
    ],
    dependencies: [
	.package(url: "https://github.com/pvieito/PythonKit.git", .branch("master")),
        .package(url: "https://github.com/ReactiveX/RxSwift.git", "4.0.0" ..< "5.0.0")

        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "Dyno",
            dependencies: ["PythonKit","RxSwift"]),
        .testTarget(
            name: "DynoTests",
            dependencies: ["Dyno","PythonKit","RxSwift", "RxBlocking"]),
    ]
)
