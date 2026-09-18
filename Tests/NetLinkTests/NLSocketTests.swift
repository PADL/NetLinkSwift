//
// Copyright (c) 2026 PADL Software Pty Ltd
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

import CNetLink
@testable import NetLink
import XCTest

final class NLSocketTests: XCTestCase {
  /// What a descriptor refers to, `socket:[inode]` for a socket, or nil if it is closed.
  private func target(of fd: CInt) -> String? {
    try? FileManager.default.destinationOfSymbolicLink(atPath: "/proc/self/fd/\(fd)")
  }

  /// A socket nothing refers to is deallocated, which ends its notifications, and its
  /// descriptor is closed once its read source is cancelled.
  func testSocketIsReleasedWithItsLastReference() async throws {
    var socket: NLSocket? = try NLSocket(protocol: NETLINK_ROUTE)
    try socket?.subscribeLinks()
    weak let weakSocket = socket
    let notifications = try XCTUnwrap(socket?.notifications)
    let fd = nl_socket_get_fd(socket?._sk)
    let open = try XCTUnwrap(target(of: fd))

    socket = nil
    // a retained socket never ends its notifications
    guard weakSocket == nil else { return XCTFail("the socket is retained by its read source") }
    for try await _ in notifications {}

    // the descriptor is closed by the source's cancel handler, which runs asynchronously
    let deadline = ContinuousClock.now + .seconds(2)
    while target(of: fd) == open, ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(10))
    }
    XCTAssertNotEqual(target(of: fd), open)
  }
}
