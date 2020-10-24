// swift-tools-version:5.3

import PackageDescription

let package = Package(
    name: "FMDB",
    products: [
        .library(name: "FMDB", targets: ["FMDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .systemLibrary(
            name: "CSQLite",
            pkgConfig: "sqlite3"
        ),
        .target(
            name: "FMDB",
            dependencies: [
                "CSQLite",
                .product(name: "Logging", package: "swift-log")
            ],
            path: "Sources/fmdb",
            swiftSettings: [
                .unsafeFlags(["-warnings-as-errors"])
            ])

        ]
)
