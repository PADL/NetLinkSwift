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

// Experimental devlink (generic netlink) support, sufficient to enumerate
// devlink ports and set a per-port parameter. The devlink uAPI constants are
// duplicated here as Swift enums because the build host's <linux/devlink.h>
// may predate the per-port parameter commands.

import CNetLink
import Glibc
import SystemPackage

// GENL_HDRLEN = NLMSG_ALIGN(sizeof(struct genlmsghdr)); genlmsghdr is 4 bytes.
private let _genlHdrLen: CInt = 4

enum DevlinkCommand: UInt8 {
  case portGet = 5
  case portNew = 7
  case portParamGet = 47
  case portParamSet = 48
}

enum DevlinkAttr: CInt {
  case busName = 1
  case devName = 2
  case portIndex = 3
  case portNetdevIfIndex = 6
  case paramName = 81
  case paramType = 83
  case paramValueData = 86
  case paramValueCmode = 87
}

// The wire value of DEVLINK_ATTR_PARAM_TYPE mirrors the internal NLA_* values
// (DEVLINK_VAR_ATTR_TYPE_*): U8 = 1, U16 = 2, U32 = 3, U64 = 4.
public enum DevlinkParamType: UInt8, Sendable {
  case u8 = 1
  case u16 = 2
  case u32 = 3
  case u64 = 4
}

public enum DevlinkParamCmode: UInt8, Sendable {
  case runtime = 0
  case driverinit = 1
  case permanent = 2
}

/// A devlink port, as reported by DEVLINK_CMD_PORT_GET.
public struct RTNLDevlinkPort: NLObjectConstructible, Sendable, CustomStringConvertible {
  public let busName: String
  public let devName: String
  public let portIndex: UInt32
  public let netdevIfIndex: UInt32?

  /// Not used — built directly in `NLSocket_CB_VALID` from the raw genl message.
  public init(object: NLObject) throws { throw NLError.invalidArgument }

  init(rawHeader nlh: UnsafeMutablePointer<nlmsghdr>) throws {
    var busName: String?
    var devName: String?
    var portIndex: UInt32?
    var ifIndex: UInt32?

    var rem = nlmsg_attrlen(nlh, _genlHdrLen)
    var pos = nlmsg_attrdata(nlh, _genlHdrLen)
    while nla_ok(pos, rem) != 0 {
      defer { pos = nla_next(pos, &rem) }
      guard let attr = pos else { continue }
      switch nla_type(attr) {
      case DevlinkAttr.busName.rawValue:
        if let s = nla_get_string(attr) { busName = String(cString: s) }
      case DevlinkAttr.devName.rawValue:
        if let s = nla_get_string(attr) { devName = String(cString: s) }
      case DevlinkAttr.portIndex.rawValue:
        portIndex = nla_get_u32(attr)
      case DevlinkAttr.portNetdevIfIndex.rawValue:
        ifIndex = nla_get_u32(attr)
      default:
        break
      }
    }

    guard let busName, let devName, let portIndex else {
      throw NLError.invalidArgument
    }
    self.busName = busName
    self.devName = devName
    self.portIndex = portIndex
    netdevIfIndex = ifIndex
  }

  public var description: String {
    "RTNLDevlinkPort(\(busName)/\(devName)/\(portIndex), ifindex: \(netdevIfIndex.map(String.init) ?? "nil"))"
  }
}

/// Experimental devlink client over a dedicated NETLINK_GENERIC socket.
public final class RTNLDevlink: @unchecked Sendable {
  // Exposed so NLSocket_CB_VALID can recognise dump replies.
  static let portNewCommand = DevlinkCommand.portNew.rawValue

  private let _socket: NLSocket
  private let _family: CInt

  public init() throws {
    _socket = try NLSocket(protocol: NETLINK_GENERIC)
    _family = try RTNLDevlink._resolveGenlFamily("devlink")
  }

  // Resolve the genl family id on a throwaway blocking socket so we do not
  // interfere with the async receive loop on _socket.
  private static func _resolveGenlFamily(_ name: String) throws -> CInt {
    guard let sk = nl_socket_alloc() else { throw NLError.noMemory }
    defer { nl_socket_free(sk) }
    try throwingNLError { nl_connect(sk, NETLINK_GENERIC) }
    let id = genl_ctrl_resolve(sk, name)
    guard id >= 0 else { throw Errno.noSuchFileOrDirectory }
    return id
  }

  private func _message(
    command: DevlinkCommand,
    flags: NLMessage.Flags
  ) throws -> NLMessage {
    let message = try NLMessage(socket: _socket, type: Int(_family), flags: flags)
    var ghdr = genlmsghdr()
    ghdr.cmd = command.rawValue
    ghdr.version = 1 // DEVLINK_GENL_VERSION
    try message.append(opaque: &ghdr)
    return message
  }

  /// Enumerate all devlink ports across all instances.
  public func ports() async throws -> [RTNLDevlinkPort] {
    let message = try _message(command: .portGet, flags: [.request, .dump])
    var result = [RTNLDevlinkPort]()
    for try await object in try _socket.streamRequest(message: message) {
      if let port = object as? RTNLDevlinkPort { result.append(port) }
    }
    return result
  }

  /// Look up the devlink port whose backing netdev has the given ifindex.
  public func port(forIfIndex ifIndex: Int) async throws -> RTNLDevlinkPort? {
    try await ports().first { $0.netdevIfIndex == UInt32(ifIndex) }
  }

  /// Set a u16 per-port devlink parameter.
  public func setPortParam(
    busName: String,
    devName: String,
    portIndex: UInt32,
    name: String,
    u16Value: UInt16,
    cmode: DevlinkParamCmode = .runtime
  ) async throws {
    let message = try _message(command: .portParamSet, flags: [.request, .ack])
    try message.put(string: busName, for: DevlinkAttr.busName.rawValue)
    try message.put(string: devName, for: DevlinkAttr.devName.rawValue)
    try message.put(u32: portIndex, for: DevlinkAttr.portIndex.rawValue)
    try message.put(string: name, for: DevlinkAttr.paramName.rawValue)
    try message.put(u8: DevlinkParamType.u16.rawValue, for: DevlinkAttr.paramType.rawValue)
    try message.put(u16: u16Value, for: DevlinkAttr.paramValueData.rawValue)
    try message.put(u8: cmode.rawValue, for: DevlinkAttr.paramValueCmode.rawValue)
    try await _socket.ackRequest(message: message)
  }

  /// Convenience: resolve the devlink port for a netdev ifindex and set a u16
  /// per-port parameter on it.
  public func setPortParam(
    ifIndex: Int,
    name: String,
    u16Value: UInt16,
    cmode: DevlinkParamCmode = .runtime
  ) async throws {
    guard let port = try await port(forIfIndex: ifIndex) else {
      throw Errno.noSuchAddressOrDevice
    }
    try await setPortParam(
      busName: port.busName,
      devName: port.devName,
      portIndex: port.portIndex,
      name: name,
      u16Value: u16Value,
      cmode: cmode
    )
  }
}
