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

import AsyncAlgorithms
import AsyncExtensions
import CLinuxSockAddr
import CNetLink
import Glibc
import Synchronization
import SystemPackage

// MARK: - parsed generic netlink message

/// A single generic netlink attribute, parsed from a response payload. The
/// payload is copied so the value is `Sendable` and outlives the libnl message.
public struct GENLAttribute: Sendable {
  public let type: Int
  public let payload: [UInt8]

  public var uint8: UInt8? {
    payload.count >= 1 ? payload[0] : nil
  }

  public var uint16: UInt16? {
    guard payload.count >= 2 else { return nil }
    return payload.withUnsafeBytes { $0.loadUnaligned(as: UInt16.self) }
  }

  public var uint32: UInt32? {
    guard payload.count >= 4 else { return nil }
    return payload.withUnsafeBytes { $0.loadUnaligned(as: UInt32.self) }
  }

  public var string: String? {
    guard !payload.isEmpty else { return nil }
    let bytes = payload.firstIndex(of: 0).map { Array(payload[..<$0]) } ?? payload
    return String(decoding: bytes, as: UTF8.self)
  }

  /// Re-parse this attribute's payload as a sequence of nested attributes.
  public var nested: [GENLAttribute] {
    GENLAttribute.parse(payload)
  }

  static func parse(_ buffer: UnsafeRawBufferPointer) -> [GENLAttribute] {
    var result = [GENLAttribute]()
    guard let base = buffer.baseAddress else { return result }
    let total = buffer.count
    var offset = 0
    // struct nlattr { __u16 nla_len; __u16 nla_type; } in host byte order,
    // payload follows, each attribute padded to NLA_ALIGNTO (4) bytes.
    while offset + 4 <= total {
      let len = Int(base.loadUnaligned(fromByteOffset: offset, as: UInt16.self))
      let rawType = base.loadUnaligned(fromByteOffset: offset + 2, as: UInt16.self)
      guard len >= 4, offset + len <= total else { break }
      let payload = Array(UnsafeRawBufferPointer(start: base + offset + 4, count: len - 4))
      // mask off NLA_F_NESTED / NLA_F_NET_BYTEORDER
      result.append(GENLAttribute(type: Int(rawType & 0x3FFF), payload: payload))
      offset += (len + 3) & ~3
    }
    return result
  }

  static func parse(_ data: [UInt8]) -> [GENLAttribute] {
    data.withUnsafeBytes { parse($0) }
  }

  func first(_ type: CInt) -> GENLAttribute? {
    self.type == Int(type) ? self : nil
  }
}

public extension [GENLAttribute] {
  func first(type: CInt) -> GENLAttribute? {
    first { $0.type == Int(type) }
  }
}

/// A decoded generic netlink response: the genl command/version plus the
/// top-level attributes that follow the `genlmsghdr`.
public struct GENLMessage: Sendable {
  public let cmd: UInt8
  public let version: UInt8
  public let attributes: [GENLAttribute]

  init(nlh: UnsafeMutablePointer<nlmsghdr>) {
    let gnlh = genlmsg_hdr(nlh)!
    cmd = gnlh.pointee.cmd
    version = gnlh.pointee.version
    let len = genlmsg_attrlen(gnlh, 0)
    if let data = genlmsg_attrdata(gnlh, 0), len > 0 {
      attributes = GENLAttribute.parse(UnsafeRawBufferPointer(
        start: UnsafeRawPointer(data),
        count: Int(len)
      ))
    } else {
      attributes = []
    }
  }
}

// MARK: - nlctrl controller types

public struct GENLMulticastGroup: Sendable, CustomStringConvertible {
  public let name: String
  public let id: UInt32

  public var description: String {
    "GENLMulticastGroup(name: \(name), id: \(id))"
  }
}

/// A generic netlink family as resolved by the `nlctrl` controller
/// (`CTRL_CMD_GETFAMILY`).
public struct GENLFamily: Sendable, CustomStringConvertible {
  public let id: UInt16
  public let name: String
  public let version: UInt32?
  public let multicastGroups: [GENLMulticastGroup]

  init(message: GENLMessage) throws {
    guard let id = message.attributes.first(type: CInt(CTRL_ATTR_FAMILY_ID))?.uint16 else {
      throw NLError.invalidArgument
    }
    self.id = id
    name = message.attributes.first(type: CInt(CTRL_ATTR_FAMILY_NAME))?.string ?? ""
    version = message.attributes.first(type: CInt(CTRL_ATTR_VERSION))?.uint32

    var groups = [GENLMulticastGroup]()
    if let mcast = message.attributes.first(type: CInt(CTRL_ATTR_MCAST_GROUPS)) {
      // CTRL_ATTR_MCAST_GROUPS nests one entry per group, each entry itself a
      // nest of CTRL_ATTR_MCAST_GRP_{NAME,ID}; the entry's own type is an index.
      for entry in mcast.nested {
        let attrs = GENLAttribute.parse(entry.payload)
        if let name = attrs.first(type: CInt(CTRL_ATTR_MCAST_GRP_NAME))?.string,
           let id = attrs.first(type: CInt(CTRL_ATTR_MCAST_GRP_ID))?.uint32
        {
          groups.append(GENLMulticastGroup(name: name, id: id))
        }
      }
    }
    multicastGroups = groups
  }

  public var description: String {
    "GENLFamily(name: \(name), id: \(id), version: \(version.map(String.init) ?? "?"), groups: \(multicastGroups.count))"
  }
}

// MARK: - ethtool

public struct EthtoolPauseParameters: Sendable, CustomStringConvertible {
  public let autoneg: Bool?
  public let rx: Bool?
  public let tx: Bool?

  public var description: String {
    func fmt(_ b: Bool?) -> String { b.map { $0 ? "on" : "off" } ?? "n/a" }
    return "EthtoolPauseParameters(autoneg: \(fmt(autoneg)), rx: \(fmt(rx)), tx: \(fmt(tx)))"
  }
}

// MARK: - libnl callbacks

private func GENLSocket_CB_VALID(
  _ msg: OpaquePointer!,
  _ arg: UnsafeMutableRawPointer!
) -> CInt {
  let genl = Unmanaged<GENLSocket>.fromOpaque(arg).takeUnretainedValue()
  guard let nlh = nlmsg_hdr(msg) else { return CInt(NL_SKIP.rawValue) }
  let message = GENLMessage(nlh: nlh)
  genl.resume(sequence: nlh.pointee.nlmsg_seq, with: .success(message))
  return CInt(NL_OK.rawValue)
}

private func GENLSocket_ErrCB(
  _ nla: UnsafeMutablePointer<sockaddr_nl>!,
  _ err: UnsafeMutablePointer<nlmsgerr>!,
  _ arg: UnsafeMutableRawPointer!
) -> CInt {
  let genl = Unmanaged<GENLSocket>.fromOpaque(arg).takeUnretainedValue()
  let hdr = err.pointee.msg
  genl.resume(sequence: hdr.nlmsg_seq, with: .failure(Errno(rawValue: -err.pointee.error)))
  return CInt(NL_SKIP.rawValue)
}

// MARK: - generic netlink socket

/// Generic netlink (`NETLINK_GENERIC`) layer over the core `NLSocket`. The core
/// `NLSocket_CB_VALID` dispatch only understands rtnetlink and netfilter
/// objects, so this layer installs its own genl-aware valid/error callbacks on
/// the underlying socket and tracks pending requests by sequence number itself.
public final class GENLSocket: @unchecked Sendable {
  private let _socket: NLSocket

  private enum Request {
    case continuation(CheckedContinuation<GENLMessage, Error>)
  }

  private let _requests = Mutex<[UInt32: Request]>([:])

  public init() throws {
    _socket = try NLSocket(protocol: NETLINK_GENERIC)
    // A single genl GET reply carries no ACK; disabling auto-ack keeps the
    // success path to exactly one valid message (errors still arrive as
    // NLMSG_ERROR via the error callback).
    _socket.setAutoAck(false)

    nl_socket_modify_cb(
      _socket._sk,
      NL_CB_VALID,
      NL_CB_CUSTOM,
      GENLSocket_CB_VALID,
      Unmanaged.passUnretained(self).toOpaque()
    )
    nl_socket_modify_err_cb(
      _socket._sk,
      NL_CB_CUSTOM,
      GENLSocket_ErrCB,
      Unmanaged.passUnretained(self).toOpaque()
    )
  }

  fileprivate func resume(sequence: UInt32, with result: Result<GENLMessage, Error>) {
    let request = _requests.withLock { $0.removeValue(forKey: sequence) }
    guard let request else { return }
    switch request {
    case let .continuation(continuation):
      continuation.resume(with: result)
    }
  }

  private func makeMessage(
    family: CInt,
    cmd: UInt8,
    version: UInt8,
    flags: CInt = 0
  ) throws -> NLMessage {
    guard let msg = nlmsg_alloc() else { throw NLError.noMemory }
    let sequence = _socket.useNextSequenceNumber()
    guard genlmsg_put(
      msg,
      UInt32(NL_AUTO_PID),
      sequence,
      family,
      0,
      NLM_F_REQUEST | flags,
      cmd,
      version
    ) != nil else {
      nlmsg_free(msg)
      throw NLError.noMemory
    }
    return NLMessage(consuming: msg)
  }

  private func request(message: consuming NLMessage) async throws -> GENLMessage {
    let sequence = message.sequence
    precondition(sequence != 0)
    return try await withTaskCancellationHandler(operation: {
      try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<
        GENLMessage,
        Error
      >) in
        _requests.withLock { $0[sequence] = .continuation(continuation) }
        do {
          try message.send(on: _socket)
        } catch {
          resume(sequence: sequence, with: .failure(error))
        }
      }
    }, onCancel: {
      resume(sequence: sequence, with: .failure(CancellationError()))
    })
  }

  /// Resolve a generic netlink family by name via the `nlctrl` controller,
  /// returning its id and multicast groups.
  public func resolveFamily(name: String) async throws -> GENLFamily {
    let message = try makeMessage(
      family: CInt(GENL_ID_CTRL),
      cmd: UInt8(CTRL_CMD_GETFAMILY),
      version: 1
    )
    try message.put(string: name, for: CInt(CTRL_ATTR_FAMILY_NAME))
    let reply = try await request(message: message)
    return try GENLFamily(message: reply)
  }

  /// Issue `ETHTOOL_MSG_PAUSE_GET` for an interface and decode the pause
  /// (flow control) parameters from the reply.
  public func ethtoolPauseParameters(interfaceName: String) async throws
    -> EthtoolPauseParameters
  {
    let family = try await resolveFamily(name: "ethtool")
    let message = try makeMessage(
      family: CInt(family.id),
      cmd: UInt8(ETHTOOL_MSG_PAUSE_GET),
      version: 1
    )
    let header = message.nestStart(attr: CInt(ETHTOOL_A_PAUSE_HEADER))
    try message.put(string: interfaceName, for: CInt(ETHTOOL_A_HEADER_DEV_NAME))
    message.nestEnd(attr: header)

    let reply = try await request(message: message)
    func flag(_ type: CInt) -> Bool? {
      reply.attributes.first(type: type)?.uint8.map { $0 != 0 }
    }
    return EthtoolPauseParameters(
      autoneg: flag(CInt(ETHTOOL_A_PAUSE_AUTONEG)),
      rx: flag(CInt(ETHTOOL_A_PAUSE_RX)),
      tx: flag(CInt(ETHTOOL_A_PAUSE_TX))
    )
  }
}
