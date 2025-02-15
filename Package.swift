// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

var PlatformCSettings: [CSetting] = []
var PlatformLinkerSettings: [LinkerSetting] = []

PlatformCSettings = [.unsafeFlags(["-I", "/usr/include/libnl3"])]

PlatformLinkerSettings += [
  .linkedLibrary("nl-3"),
  .linkedLibrary("nl-route-3"),
  .linkedLibrary("nl-nf-3"),
]

let package = Package(
  name: "NetLinkSwift",
  products: [
    .library(
      name: "NetLink",
      targets: ["NetLink"]
    ),
    .executable(
      name: "nldump",
      targets: ["nldump"]
    ),
    .executable(
      name: "nlmonitor",
      targets: ["nlmonitor"]
    ),
    .executable(
      name: "nltool",
      targets: ["nltool"]
    ),
  ],
  dependencies: [
    .package(url: "https://github.com/PADL/IORingSwift", branch: "main"),
    .package(url: "https://github.com/apple/swift-async-algorithms", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-system", from: "1.2.1"),
    .package(url: "https://github.com/apple/swift-argument-parser", from: "1.2.0"),
    .package(url: "https://github.com/PADL/SocketAddress", from: "0.0.1"),
    .package(url: "https://github.com/lhoward/AsyncExtensions", from: "0.9.2"),
  ],
  targets: [
    .systemLibrary(
      name: "CNetLink",
      providers: [
        .apt(["libnl-3-dev", "libnl-route-3-dev", "libnl-nf-3-dev"]),
      ]
    ),
    .target(
      name: "NetLink",
      dependencies: ["CNetLink",
                     .product(name: "CLinuxSockAddr", package: "SocketAddress"),
                     .product(name: "SocketAddress", package: "SocketAddress"),
                     .product(name: "SystemPackage", package: "swift-system"),
                     .product(name: "AsyncAlgorithms", package: "swift-async-algorithms"),
                     "AsyncExtensions"],
      cSettings: PlatformCSettings,
      swiftSettings: [
        .swiftLanguageMode(.v5),
        .enableExperimentalFeature("StrictConcurrency"),
      ],
      linkerSettings: PlatformLinkerSettings
    ),
    .executableTarget(
      name: "nldump",
      dependencies: ["NetLink"],
      path: "Examples/nldump",
      swiftSettings: [
        .swiftLanguageMode(.v5),
        .enableExperimentalFeature("StrictConcurrency"),
      ]
    ),
    .executableTarget(
      name: "nlmonitor",
      dependencies: ["NetLink"],
      path: "Examples/nlmonitor",
      swiftSettings: [
        .swiftLanguageMode(.v5),
        .enableExperimentalFeature("StrictConcurrency"),
      ]
    ),
    .executableTarget(
      name: "nltool",
      dependencies: ["NetLink", .product(name: "IORingUtils", package: "IORingSwift")],
      path: "Examples/nltool",
      swiftSettings: [
        .swiftLanguageMode(.v5),
        .enableExperimentalFeature("StrictConcurrency"),
      ]
    ),
  ]
)
