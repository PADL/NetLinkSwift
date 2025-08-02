//
// Copyright (c) 2025 PADL Software Pty Ltd
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

import CLinuxSockAddr
import CNetLink
import Dispatch
import NetLink
import SocketAddress
import SystemPackage

private func usage() {
  print(
    "Usage: \(CommandLine.arguments[0]) [iface] [mac-address]"
  )
  exit(1)
}

private func findInterface(named name: String, socket: NLSocket) async throws -> RTNLLink {
  guard let link = try await socket.getLinks(family: sa_family_t(AF_PACKET))
    .first(where: { $0.name == name })
  else {
    print("interface \(name) not found")
    throw Errno.noSuchFileOrDirectory
  }
  return link
}

@MainActor
private var gSocket: NLSocket!

@main
enum setmacaddr {
  public static func main() async throws {
    if CommandLine.arguments.count < 3 {
      usage()
    }

    do {
      let socket = try NLSocket(protocol: NETLINK_ROUTE)
      gSocket = socket
      let link = try await findInterface(named: CommandLine.arguments[1], socket: socket)
      let macAddress = try RTNLLink.parseMacAddressString(CommandLine.arguments[2])
      try await link.set(address: macAddress, socket: gSocket)
    } catch {
      print("failed to set MAC address: \(error)")
      exit(3)
    }
  }
}
