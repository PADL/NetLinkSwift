//
// Copyright (c) 2024 PADL Software Pty Ltd
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

enum Command: CaseIterable {
  case add_vlan
  case del_vlan
  case show_vlan
  case add_fdb
  case del_fdb
  case show_fdb
  case add_mdb
  case add_srp_mdb
  case del_mdb
  case show_mdb
  case add_mqprio
  case del_mqprio
  case add_cbs
  case del_cbs

  var needsArg: Bool {
    switch self {
    case .show_vlan, .show_fdb, .show_mdb: false
    default: true
    }
  }
}

typealias CommandHandler = (Command, NLSocket, RTNLLink, String) async throws -> ()

func usage() -> Never {
  print(
    "Usage: \(CommandLine.arguments[0]) [add_vlan|del_vlan|show_vlan|add_fdb|del_fdb|show_fdb|add_mdb|add_srp_mdb|del_mdb|show_mdb|add_mqprio|del_mqprio|add_cbs|del_cbs] [ifname] [vid|mac-address|parent:handle]"
  )
  exit(1)
}

func findLink(named name: String, socket: NLSocket) async throws -> RTNLLink {
  guard let link = try await socket.getLinks(family: sa_family_t(AF_UNSPEC))
    .first(where: { $0.name == name })
  else {
    print("interface \(name) not found")
    throw Errno.noSuchFileOrDirectory
  }
  return link
}

func findBridge(named name: String, socket: NLSocket) async throws -> RTNLLinkBridge {
  guard let bridge = try await findLink(named: name, socket: socket) as? RTNLLinkBridge else {
    print("interface \(name) is not a bridge")
    throw Errno.invalidArgument
  }
  return bridge
}

func findLink(index: Int, socket: NLSocket) async throws -> RTNLLink {
  guard let link = try await socket.getLinks(family: sa_family_t(AF_UNSPEC))
    .first(where: { $0.index == index })
  else {
    print("interface \(index) not found")
    throw Errno.noSuchFileOrDirectory
  }
  return link
}

func findBridge(index: Int, socket: NLSocket) async throws -> RTNLLinkBridge {
  guard let bridge = try await findLink(index: index, socket: socket) as? RTNLLinkBridge else {
    print("interface \(index) is not a bridge")
    throw Errno.invalidArgument
  }
  return bridge
}

func add_vlan(command: Command, socket: NLSocket, link: RTNLLink, arg: String) async throws {
  guard let link = link as? RTNLLinkBridge, let vlan = UInt16(arg) else { usage() }
  try await link.add(vlans: Set([vlan]), socket: socket)
}

func del_vlan(command: Command, socket: NLSocket, link: RTNLLink, arg: String) async throws {
  guard let link = link as? RTNLLinkBridge, let vlan = UInt16(arg) else { usage() }
  try await link.remove(vlans: Set([vlan]), socket: socket)
}

func add_fdb(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let bridge = try await findBridge(index: link.master, socket: socket)
  let macAddress = try RTNLLink.parseMacAddressString(arg)
  try await bridge.add(link: link, fdbEntry: macAddress, socket: socket)
}

func del_fdb(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let bridge = try await findBridge(index: link.master, socket: socket)
  let macAddress = try RTNLLink.parseMacAddressString(arg)
  try await bridge.remove(link: link, fdbEntry: macAddress, socket: socket)
}

func add_mdb(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let bridge = try await findBridge(index: link.master, socket: socket)
  let groupAddress = try RTNLLink.parseMacAddressString(arg)
  let flags: RTNLLinkBridge.MDBFlags = command == .add_srp_mdb ? [.streamReserved] : []
  try await bridge.add(
    link: link,
    groupAddresses: [groupAddress],
    flags: flags,
    socket: socket
  )
}

func del_mdb(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let bridge = try await findBridge(index: link.master, socket: socket)
  let groupAddress = try RTNLLink.parseMacAddressString(arg)
  try await bridge.remove(link: link, groupAddresses: [groupAddress], socket: socket)
}

func formatMac(_ addr: RTNLLink.LinkAddress) -> String {
  func hex(_ b: UInt8) -> String {
    let h = Array("0123456789abcdef".utf8)
    return String(unsafeUninitializedCapacity: 2) { p in
      p[0] = h[Int(b / 16)]; p[1] = h[Int(b % 16)]; return 2
    }
  }
  return (0..<6).map { hex(addr[$0]) }.joined(separator: ":")
}

func show_vlan(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  guard let bridge = link as? RTNLLinkBridge else {
    print("interface \(link.name) is not a bridge port")
    throw Errno.invalidArgument
  }
  let tagged = bridge.bridgeTaggedVLANs ?? []
  let untagged = bridge.bridgeUntaggedVLANs ?? []
  let pvid = bridge.bridgePVID
  print(
    "bridge port \(link.name) (ifindex \(link.index)) master \(link.master) bridge-flags 0x\(String(bridge.bridgeFlags, radix: 16)) port-state \(bridge.bridgePortState) pvid \(pvid.map(String.init) ?? "none") hasVLAN \(bridge.bridgeHasVLAN)"
  )
  if tagged.isEmpty {
    print("  (no VLANs)")
    return
  }
  for vid in tagged.sorted() {
    var flags: [String] = []
    if untagged.contains(vid) { flags.append("untagged") }
    if pvid == vid { flags.append("PVID") }
    if flags.isEmpty { flags.append("tagged") }
    print("  vid \(vid) [\(flags.joined(separator: ", "))]")
  }
}

func show_fdb(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  // Match FDB entries on this interface or with this bridge as master.
  var any = false
  for try await neigh in try await socket.getNeighbors(family: sa_family_t(AF_BRIDGE)) {
    guard neigh.ifIndex == link.index || neigh.master == link.index else { continue }
    any = true
    let mac = neigh.linkLayerAddress.map(formatMac) ?? "?"
    let vlan = neigh.vlanID.map(String.init) ?? "-"
    let state = RTNLNeighbor.stateString(neigh.state)
    let flags = RTNLNeighbor.flagsString(neigh.flags)
    print(
      "  dev-ifindex \(neigh.ifIndex) master \(neigh.master) lladdr \(mac) vlan \(vlan) state \(state) flags \(flags)"
    )
  }
  if !any { print("  (no FDB entries)") }
}

func show_mdb(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  // The bridge MDB dump emits one rtnl_mdb per bridge device. If `link` is
  // a bridge master (no upper master of its own), filter by its own
  // ifindex; if it is a bridge port (slave), filter by its master.
  let bridgeIndex: Int
  if link is RTNLLinkBridge, link.master == 0 || link.master == link.index {
    bridgeIndex = link.index
  } else if link.master != 0 {
    bridgeIndex = link.master
  } else {
    print("interface \(link.name) is not a bridge or bridge port")
    throw Errno.invalidArgument
  }
  var any = false
  for try await mdb in try await socket.getMDB() {
    guard mdb.bridgeIndex == bridgeIndex else { continue }
    for entry in mdb.entries {
      any = true
      var flags: [String] = []
      flags.append(entry.isPermanent ? "permanent" : "temporary")
      if entry.isStreamReserved { flags.append("stream-reserved") }
      print(
        "  port-ifindex \(entry.ifIndex) vid \(entry.vid) proto 0x\(String(entry.proto, radix: 16)) addr \(entry.addressString) flags [\(flags.joined(separator: ", "))]"
      )
    }
  }
  if !any { print("  (no MDB entries)") }
}

func stringToHandle(_ string: String) throws -> (UInt32, UInt32) {
  let s = string.split(separator: ":")
  guard s.count == 2 else {
    throw Errno.invalidArgument
  }
  guard let h1 = UInt32(s[0], radix: 16), let h2 = UInt32(s[1], radix: 16) else {
    throw Errno.invalidArgument
  }
  return (h1, h2)
}

func add_mqprio(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let (parent, handle) = try stringToHandle(arg)
  let mqprio = try RTNLMQPrioQDisc(
    handle: parent << 16 | handle,
    parent: UInt32.max,
    numTC: 3,
    priorityMap: [2: 1, 3: 2],
    hwOffload: true,
    count: [2, 1, 1],
    offset: [2, 1, 0],
    mode: .dcb,
    shaper: .dcb
  )

  try await link.add(mqprio: mqprio, socket: socket)
}

func del_mqprio(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let (parent, handle) = try stringToHandle(arg)
  let mqprio = try RTNLMQPrioQDisc(
    handle: parent << 16 | handle,
    parent: UInt32.max,
    numTC: 3,
    priorityMap: [2: 1, 3: 2],
    hwOffload: true,
    count: [2, 1, 1],
    offset: [2, 1, 0],
    mode: .dcb,
    shaper: .dcb
  )

  try await link.remove(mqprio: mqprio, socket: socket)
}

func add_cbs(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let (parent, handle) = try stringToHandle(arg)
  // TODO: make these configurable
  // https://tsn.readthedocs.io/qdiscs.html
  try await link.add(
    handle: 0,
    parent: parent << 16 | handle,
    hiCredit: 153,
    loCredit: -1389,
    idleSlope: 98688,
    sendSlope: -901_312,
    socket: socket
  )
}

func del_cbs(
  command: Command,
  socket: NLSocket,
  link: RTNLLink,
  arg: String
) async throws {
  let (parent, handle) = try stringToHandle(arg)
  try await link.remove(
    handle: 0,
    parent: parent << 16 | handle,
    socket: socket
  )
}

@MainActor
private var gSocket: NLSocket!

@main
enum nltool {
  public static func main() async throws {
    if CommandLine.arguments.count < 3 {
      usage()
    }

    guard let command = Command.allCases
      .first(where: { String(describing: $0) == CommandLine.arguments[1] })
    else {
      usage()
    }

    if command.needsArg, CommandLine.arguments.count < 4 {
      usage()
    }

    do {
      let socket = try NLSocket(protocol: NETLINK_ROUTE)
      gSocket = socket
      let link = try await findLink(named: CommandLine.arguments[2], socket: socket)
      let commands: [Command: CommandHandler] = [
        .add_vlan: add_vlan,
        .del_vlan: del_vlan,
        .show_vlan: show_vlan,
        .add_fdb: add_fdb,
        .del_fdb: del_fdb,
        .show_fdb: show_fdb,
        .add_mdb: add_mdb,
        .add_srp_mdb: add_mdb,
        .del_mdb: del_mdb,
        .show_mdb: show_mdb,
        .add_mqprio: add_mqprio,
        .del_mqprio: del_mqprio,
        .add_cbs: add_cbs,
        .del_cbs: del_cbs,
      ]
      let commandHandler = commands[command]!
      let arg = CommandLine.arguments.count >= 4 ? CommandLine.arguments[3] : ""
      try await commandHandler(command, socket, link, arg)
    } catch {
      print("failed to \(command): \(error)")
      exit(3)
    }
  }
}
