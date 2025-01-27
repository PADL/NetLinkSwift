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
import SocketAddress
import SystemPackage

protocol RTNLFactory {}

extension RTNLFactory {
  init(reassigningSelfTo other: Self) {
    self = other
  }
}

public class RTNLLink: NLObjectConstructible, @unchecked
Sendable, CustomStringConvertible,
  RTNLFactory
{
  private let _object: NLObject

  fileprivate init(_ object: NLObject) {
    _object = object
  }

  public required convenience init(object: NLObject) throws {
    guard object.messageType == RTM_NEWLINK || object.messageType == RTM_DELLINK else {
      debugPrint("Unknown message type \(object.messageType) returned")
      throw NLError.invalidArgument
    }
    if rtnl_link_is_bridge(object._obj) != 0 {
      self.init(reassigningSelfTo: RTNLLinkBridge(object) as! Self)
    } else if rtnl_link_is_vlan(object._obj) != 0 {
      self.init(reassigningSelfTo: RTNLLinkVLAN(object) as! Self)
    } else {
      self.init(object)
    }
  }

  fileprivate var _obj: OpaquePointer {
    _object._obj
  }

  public var name: String {
    String(cString: rtnl_link_get_name(_obj))
  }

  public var index: Int {
    Int(rtnl_link_get_ifindex(_obj))
  }

  public var mtu: UInt {
    UInt(rtnl_link_get_mtu(_obj))
  }

  public var description: String {
    "\(Swift.type(of: self))(\(index):\(name):\(family):\(addressString))"
  }

  public var flags: Int {
    Int(rtnl_link_get_flags(_obj))
  }

  public var group: UInt32 {
    rtnl_link_get_group(_obj)
  }

  private func byteToHex(_ byte: UInt8) -> String {
    let hexAlphabet = Array("0123456789abcdef".utf8)
    return String(unsafeUninitializedCapacity: 2) { ptr -> Int in
      ptr[0] = hexAlphabet[Int(byte / 16)]
      ptr[1] = hexAlphabet[Int(byte % 16)]
      return 2
    }
  }

  public var addressString: String {
    byteToHex(address.0) +
      ":" + byteToHex(address.1) +
      ":" + byteToHex(address.2) +
      ":" + byteToHex(address.3) +
      ":" + byteToHex(address.4) +
      ":" + byteToHex(address.5)
  }

  public typealias LinkAddress = (UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)

  public static func parseMacAddressString(_ macAddress: String) throws -> LinkAddress {
    let ll = try sockaddr_ll(
      family: sa_family_t(AF_PACKET),
      presentationAddress: macAddress
    )
    return (
      ll.sll_addr.0,
      ll.sll_addr.1,
      ll.sll_addr.2,
      ll.sll_addr.3,
      ll.sll_addr.4,
      ll.sll_addr.5
    )
  }

  private func _makeAddress(_ addr: OpaquePointer) -> LinkAddress {
    var mac = [UInt8](repeating: 0, count: Int(nl_addr_get_len(addr)))
    precondition(mac.count == Int(ETH_ALEN))
    _ = mac.withUnsafeMutableBytes {
      memcpy($0.baseAddress!, nl_addr_get_binary_addr(addr), Int(nl_addr_get_len(addr)))
    }
    return (mac[0], mac[1], mac[2], mac[3], mac[4], mac[5])
  }

  public var address: LinkAddress {
    _makeAddress(rtnl_link_get_addr(_obj))
  }

  public var nlAddress: NLAddress {
    NLAddress(addr: rtnl_link_get_addr(_obj))
  }

  public var broadcastAddress: LinkAddress {
    _makeAddress(rtnl_link_get_broadcast(_obj))
  }

  public var nlBroadcastAddress: NLAddress {
    NLAddress(addr: rtnl_link_get_broadcast(_obj))
  }

  public var family: sa_family_t {
    sa_family_t(rtnl_link_get_family(_obj))
  }

  public var arpType: UInt16 {
    UInt16(rtnl_link_get_arptype(_obj))
  }

  public var txQLen: Int {
    Int(rtnl_link_get_txqlen(_obj))
  }

  public var master: Int {
    Int(rtnl_link_get_master(_obj))
  }

  public var slaveOf: Int {
    Int(rtnl_link_get_link(_obj))
  }

  public var carrier: UInt8 {
    rtnl_link_get_carrier(_obj)
  }

  // IF_OPER_XXX
  public var operationalState: UInt8 {
    rtnl_link_get_operstate(_obj)
  }

  // LINK_MODE_XXX
  public var linkMode: UInt8 {
    rtnl_link_get_linkmode(_obj)
  }

  public var aliasName: String? {
    if let alias = rtnl_link_get_ifalias(_obj) {
      String(cString: alias)
    } else {
      nil
    }
  }

  public var qDisc: String? {
    if let qdisc = rtnl_link_get_qdisc(_obj) {
      String(cString: qdisc)
    } else {
      nil
    }
  }

  public var numVF: Int {
    get throws {
      var numVF = UInt32(0)
      let r = rtnl_link_get_num_vf(_obj, &numVF)
      if r < 0 {
        throw NLError(rawValue: -r)
      }
      return Int(numVF)
    }
  }

  public func getStatistics(id: rtnl_link_stat_id_t) -> UInt64 {
    rtnl_link_get_stat(_obj, id)
  }

  public var type: String? {
    if let type = rtnl_link_get_type(_obj) {
      String(cString: type)
    } else {
      nil
    }
  }

  public var slaveType: String? {
    if let type = rtnl_link_get_slave_type(_obj) {
      String(cString: type)
    } else {
      nil
    }
  }

  public var promiscuity: UInt32 {
    rtnl_link_get_promiscuity(_obj)
  }

  public var numTXQueues: UInt32 {
    rtnl_link_get_num_tx_queues(_obj)
  }

  public var numRXQueues: UInt32 {
    rtnl_link_get_num_rx_queues(_obj)
  }

  public var physicalPortName: String? {
    if let name = rtnl_link_get_phys_port_name(_obj) {
      String(cString: name)
    } else {
      nil
    }
  }

  public var physicalPortID: NLData? {
    NLData(data: rtnl_link_get_phys_port_id(_obj))
  }

  public var physicalSwitchID: NLData? {
    NLData(data: rtnl_link_get_phys_switch_id(_obj))
  }
}

public final class RTNLLinkBridge: RTNLLink, @unchecked Sendable {
  var bridgeHasExtendedInfo: Bool {
    rtnl_link_bridge_has_ext_info(_obj) != 0
  }

  private var _bridgeFlags: UInt16 {
    var bridgeFlags: UInt16 = 0
    if master == index { bridgeFlags |= UInt16(BRIDGE_FLAGS_SELF) }
    return bridgeFlags
  }

  public func add(
    vlans: Set<UInt16>,
    flags: UInt16 = 0,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    try await socket._vlanRequest(
      vlans: vlans,
      interfaceIndex: index,
      flags: flags,
      moreFlags: _bridgeFlags,
      operation: updateIfPresent ? .addOrUpdate : .add
    )
  }

  public func remove(vlans: Set<UInt16>, flags: UInt16 = 0, socket: NLSocket) async throws {
    try await socket._vlanRequest(
      vlans: vlans,
      interfaceIndex: index,
      flags: flags,
      moreFlags: _bridgeFlags,
      operation: .delete
    )
  }

  public func add(
    link: RTNLLink? = nil,
    fdbEntry macAddress: LinkAddress,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    let bridgeIndex: Int?
    let interfaceIndex: Int

    if let link, link.index != index {
      bridgeIndex = index
      interfaceIndex = link.index
    } else {
      bridgeIndex = nil
      interfaceIndex = index
    }

    try await socket._neighborRequest(
      bridgeIndex: bridgeIndex,
      interfaceIndex: interfaceIndex,
      macAddress: macAddress,
      moreFlags: _bridgeFlags,
      operation: updateIfPresent ? .addOrUpdate : .add
    )
  }

  public func remove(
    link: RTNLLink? = nil,
    fdbEntry macAddress: LinkAddress,
    socket: NLSocket
  ) async throws {
    let bridgeIndex: Int?
    let interfaceIndex: Int

    if let link, link.index != index {
      bridgeIndex = index
      interfaceIndex = link.index
    } else {
      bridgeIndex = nil
      interfaceIndex = index
    }

    try await socket._neighborRequest(
      bridgeIndex: bridgeIndex,
      interfaceIndex: interfaceIndex,
      macAddress: macAddress,
      moreFlags: _bridgeFlags,
      operation: .delete
    )
  }

  public func add(
    link: RTNLLink,
    groupAddresses: [LinkAddress],
    vlanID: UInt16? = nil,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    try await socket._groupRequest(
      bridgeIndex: index,
      interfaceIndex: link.index,
      groupAddresses: groupAddresses,
      vlanID: vlanID,
      operation: updateIfPresent ? .addOrUpdate : .add
    )
  }

  public func remove(
    link: RTNLLink,
    groupAddresses: [LinkAddress],
    vlanID: UInt16? = nil,
    socket: NLSocket
  ) async throws {
    try await socket._groupRequest(
      bridgeIndex: index,
      interfaceIndex: link.index,
      groupAddresses: groupAddresses,
      vlanID: vlanID,
      operation: .delete
    )
  }

  public var bridgePortState: UInt8 {
    UInt8(rtnl_link_bridge_get_port_state(_obj))
  }

  public var bridgePriority: UInt16 {
    UInt16(rtnl_link_bridge_get_priority(_obj))
  }

  public var bridgeCost: UInt32 {
    var cost = UInt32(0)
    let r = rtnl_link_bridge_get_cost(_obj, &cost)
    precondition(r == 0)
    return cost
  }

  public var bridgeFlags: UInt32 {
    UInt32(rtnl_link_bridge_get_flags(_obj))
  }

  public var bridgeHWMode: UInt16 {
    get throws {
      var hwmode = UInt16(0)
      try throwingNLError {
        rtnl_link_bridge_get_hwmode(_obj, &hwmode)
      }
      return hwmode
    }
  }

  public var bridgePVID: UInt16? {
    let pvid = rtnl_link_bridge_pvid(_obj)
    if pvid <= 0 {
      return nil
    }
    return UInt16(pvid)
  }

  public var bridgeHasVLAN: Bool {
    rtnl_link_bridge_has_vlan(_obj) != 0
  }

  private var _bridgePortVLAN: rtnl_link_bridge_vlan? {
    guard let p = rtnl_link_bridge_get_port_vlan(_obj) else { return nil }
    return p.pointee
  }

  private func _findNextBit(index: inout Int, in bitmap: UInt32) {
    var ret: Int
    if index < 0 {
      ret = Int(ffs(Int32(bitPattern: bitmap)))
    } else {
      ret = Int(ffs(Int32(bitPattern: bitmap >> index)))
      if ret > 0 { ret += index }
      else { ret = 0 }
    }
    index = ret
  }

  private func _expandBitmap(_ bitmap: [UInt32]) -> Set<UInt16> {
    var ret = Set<UInt16>()

    for k in 0..<bitmap.count {
      var index: Int = -1
      repeat {
        _findNextBit(index: &index, in: bitmap[k])
        guard index > 0 else { break }
        ret.insert(UInt16(k * 32 + index) - 1)
      } while true
    }

    return ret
  }

  public var bridgeTaggedVLANs: Set<UInt16>? {
    guard let bpv = _bridgePortVLAN else { return nil }

    return withUnsafePointer(to: bpv.vlan_bitmap) { pointer in
      let start = pointer.propertyBasePointer(to: \.0)!
      let bitmap = [UInt32](UnsafeBufferPointer(
        start: start,
        count: Int(RTNL_LINK_BRIDGE_VLAN_BITMAP_LEN)
      ))
      return _expandBitmap(bitmap)
    }
  }

  public var bridgeUntaggedVLANs: Set<UInt16>? {
    guard let bpv = _bridgePortVLAN else { return nil }

    return withUnsafePointer(to: bpv.untagged_bitmap) { pointer in
      let start = pointer.propertyBasePointer(to: \.0)!
      let bitmap = [UInt32](UnsafeBufferPointer(
        start: start,
        count: Int(RTNL_LINK_BRIDGE_VLAN_BITMAP_LEN)
      ))
      return _expandBitmap(bitmap)
    }
  }
}

public final class RTNLLinkVLAN: RTNLLink, @unchecked Sendable {
  public var vlanID: UInt16? {
    let vid = rtnl_link_vlan_get_id(_obj)
    if vid == 0 { return nil }
    return UInt16(vid)
  }

  public var vlanProtocol: UInt16? {
    let proto = rtnl_link_vlan_get_protocol(_obj)
    if proto == 0 { return nil }
    return UInt16(proto)
  }

  public var vlanFlags: UInt32 {
    UInt32(rtnl_link_vlan_get_flags(_obj))
  }
}

public extension NLSocket {
  func enslave(link slave: RTNLLink, to master: RTNLLink) throws {
    try throwingNLError {
      rtnl_link_enslave(_sk, master._obj, slave._obj)
    }
  }

  func release(link slave: RTNLLink) throws {
    try throwingNLError {
      rtnl_link_release(_sk, slave._obj)
    }
  }
}

public enum RTNLLinkMessage: NLObjectConstructible, Sendable {
  case new(RTNLLink)
  case del(RTNLLink)

  public init(object: NLObject) throws {
    switch object.messageType {
    case RTM_NEWLINK:
      self = try .new(RTNLLink(object: object))
    case RTM_DELLINK:
      self = try .del(RTNLLink(object: object))
    default:
      throw NLError.invalidArgument
    }
  }

  public var link: RTNLLink {
    switch self {
    case let .new(link):
      link
    case let .del(link):
      link
    }
  }
}

public extension NLSocket {
  func getLinks(family: sa_family_t) async throws -> AnyAsyncSequence<RTNLLink> {
    let message = try NLMessage(socket: self, type: RTM_GETLINK, flags: .dump)
    var hdr = ifinfomsg()
    hdr.ifi_family = UInt8(family)
    try withUnsafeBytes(of: &hdr) {
      try message.append(Array($0))
    }
    try message.put(
      u32: UInt32(RTEXT_FILTER_VF | RTEXT_FILTER_BRVLAN | RTEXT_FILTER_MRP),
      for: CInt(IFLA_EXT_MASK)
    )
    return try streamRequest(message: message).map { ($0 as! RTNLLinkMessage).link }
      .eraseToAnyAsyncSequence()
  }

  func subscribeLinks() throws {
    try add(membership: RTNLGRP_LINK)
  }

  func unsubscribeLinks() throws {
    try drop(membership: RTNLGRP_LINK)
  }

  func getAddresses(family: sa_family_t) async throws -> AnyAsyncSequence<NLAddress> {
    let message = try NLMessage(socket: self, type: RTM_GETADDR, flags: .dump)
    var hdr = rtgenmsg()
    hdr.rtgen_family = UInt8(family)
    try withUnsafeBytes(of: &hdr) {
      try message.append(Array($0))
    }
    return try streamRequest(message: message).map { ($0 as! NLAddressMessage).address }
      .eraseToAnyAsyncSequence()
  }

  func subscribeIPv4Addresses() throws {
    try add(membership: RTNLGRP_IPV4_IFADDR)
  }

  func unsubscribeIPv4Addresses() throws {
    try drop(membership: RTNLGRP_IPV4_IFADDR)
  }

  func subscribeIPv6Addresses() throws {
    try add(membership: RTNLGRP_IPV6_IFADDR)
  }

  func unsubscribeIPv6Addresses() throws {
    try drop(membership: RTNLGRP_IPV6_IFADDR)
  }

  func getQDiscs(
    family: sa_family_t,
    interfaceIndex: Int
  ) async throws -> AnyAsyncSequence<RTNLTCQDisc> {
    let message = try NLMessage(socket: self, type: RTM_GETQDISC, flags: .dump)
    var hdr = tcmsg()
    hdr.tcm_family = UInt8(family)
    hdr.tcm_ifindex = Int32(interfaceIndex)
    try withUnsafeBytes(of: &hdr) {
      try message.append(Array($0))
    }
    return try streamRequest(message: message).map { ($0 as! RTNLTCMessage).tc as! RTNLTCQDisc }
      .eraseToAnyAsyncSequence()
  }

  func subscribeTC() throws {
    try add(membership: RTNLGRP_TC)
  }

  func unsubscribeTC() throws {
    try drop(membership: RTNLGRP_TC)
  }

  fileprivate func _vlanRequest(
    vlans: Set<UInt16>,
    interfaceIndex: Int,
    flags: UInt16 = 0,
    moreFlags: UInt16 = 0,
    operation: NLMessage.Operation
  ) async throws {
    let message = try NLMessage(
      socket: self,
      type: operation != .delete ? RTM_SETLINK : RTM_DELLINK,
      operation: operation
    )
    try message.appendIfInfo(index: interfaceIndex)
    let attr = message.nestStart(attr: CInt(IFLA_AF_SPEC))
    if moreFlags != 0 {
      try message.put(u16: moreFlags, for: CInt(IFLA_BRIDGE_FLAGS))
    }
    for vid in vlans {
      var vlanInfo = bridge_vlan_info(flags: flags, vid: vid)
      try message.put(opaque: &vlanInfo, for: CInt(IFLA_BRIDGE_VLAN_INFO))
    }
    message.nestEnd(attr: attr)
    try await ackRequest(message: message)
  }

  fileprivate func _groupRequest(
    bridgeIndex: Int,
    interfaceIndex: Int,
    groupAddresses: [RTNLLink.LinkAddress],
    vlanID: UInt16? = nil,
    flags: UInt8 = 0,
    operation: NLMessage.Operation
  ) async throws {
    let message = try NLMessage(
      socket: self,
      type: operation != .delete ? RTM_NEWMDB : RTM_DELMDB,
      flags: operation.flags
    )
    var portMsg = br_port_msg(family: UInt8(AF_BRIDGE), ifindex: UInt32(bridgeIndex))
    try withUnsafeBytes(of: &portMsg) {
      try message.append(Array($0))
    }
    var entry = br_mdb_entry(
      ifindex: UInt32(interfaceIndex),
      state: UInt8(MDB_PERMANENT),
      flags: flags,
      vid: vlanID ?? 0,
      addr: .init()
    )
    for groupAddress in groupAddresses {
      entry.addr.u.mac_addr = groupAddress
      try message.put(opaque: &entry, for: CInt(MDBA_MDB_ENTRY))
    }
    try await ackRequest(message: message)
  }

  fileprivate func _neighborRequest(
    bridgeIndex: Int? = nil,
    interfaceIndex: Int,
    macAddress: RTNLLink.LinkAddress,
    moreFlags: UInt16 = 0,
    operation: NLMessage.Operation
  ) async throws {
    let message = try NLMessage(
      socket: self,
      type: operation != .delete ? RTM_NEWNEIGH : RTM_DELNEIGH,
      flags: operation.flags
    )
    var msg = ndmsg()
    msg.ndm_ifindex = Int32(interfaceIndex)
    msg.ndm_family = UInt8(AF_BRIDGE)
    msg.ndm_state = UInt16(NUD_NOARP | NUD_PERMANENT)
    msg.ndm_flags = (moreFlags & UInt16(BRIDGE_FLAGS_SELF)) != 0 ? UInt8(NTF_SELF) : 0
    if let _ = bridgeIndex {
      msg.ndm_flags |= UInt8(NTF_MASTER)
    }
    try withUnsafeBytes(of: &msg) {
      try message.append(Array($0))
    }
    try withUnsafePointer(to: macAddress) { pointer in
      let start = pointer.propertyBasePointer(to: \.0)!
      let macAddressBytes = [UInt8](UnsafeBufferPointer(start: start, count: Int(ETH_ALEN)))
      try message.put(data: macAddressBytes, for: CInt(NDA_LLADDR))
    }
    if let bridgeIndex {
      try message.put(u32: UInt32(bridgeIndex), for: CInt(NDA_MASTER))
    }
    try await ackRequest(message: message)
  }
}

public class RTNLTCBase: NLObjectConstructible, @unchecked
Sendable, CustomStringConvertible,
  RTNLFactory
{
  private let _object: NLObject

  fileprivate init(_ object: NLObject) {
    _object = object
  }

  public required convenience init(object: NLObject) throws {
    let kind = String(cString: rtnl_tc_get_kind(object._obj))
    switch object.messageType {
    case RTM_NEWQDISC:
      fallthrough
    case RTM_DELQDISC:
      fallthrough
    case RTM_GETQDISC:
      try self.init(reassigningSelfTo: RTNLTCQDisc(object: object, kind: kind) as! Self)
    case RTM_NEWTCLASS:
      fallthrough
    case RTM_DELTCLASS:
      fallthrough
    case RTM_GETTCLASS:
      self.init(reassigningSelfTo: RTNLTCClass(object) as! Self)
    case RTM_NEWTFILTER:
      fallthrough
    case RTM_DELTFILTER:
      fallthrough
    case RTM_GETTFILTER:
      self.init(reassigningSelfTo: RTNLTCClassifier(object) as! Self)
    default:
      throw NLError.invalidArgument
    }
  }

  fileprivate var _obj: OpaquePointer {
    _object._obj
  }

  public var name: String {
    String(cString: rtnl_link_get_name(_obj))
  }

  public var index: Int {
    Int(rtnl_tc_get_ifindex(_obj))
  }

  public var mtu: UInt32 {
    rtnl_tc_get_mtu(_obj)
  }

  public var handle: UInt32 {
    rtnl_tc_get_handle(_obj)
  }

  public var parent: UInt32 {
    rtnl_tc_get_parent(_obj)
  }

  public var linkType: UInt32 {
    rtnl_tc_get_linktype(_obj)
  }

  public var kind: String {
    String(cString: rtnl_tc_get_kind(_obj))
  }

  public var chain: UInt32 {
    get throws {
      var chain: UInt32 = 0
      try throwingNLError {
        rtnl_tc_get_chain(_obj, &chain)
      }
      return chain
    }
  }

  public var description: String {
    "\(Swift.type(of: self))(index: \(index), handle: \(handle), parent: \(parent), kind: \(kind))"
  }
}

public class RTNLTCQDisc: RTNLTCBase, @unchecked Sendable {
  public convenience init() {
    self.init(object: NLObject(consumingObj: rtnl_qdisc_alloc()))
  }

  public required convenience init(object: NLObject) {
    self.init(object)
  }

  public convenience init(object: NLObject, kind: String) throws {
    switch kind {
    case "mqprio":
      self.init(reassigningSelfTo: RTNLMQPrioQDisc(object) as! Self)
    case "pfifo_fast":
      self.init(reassigningSelfTo: RTNLPFIFOFastQDisc(object) as! Self)
    default:
      self.init(object)
    }
    rtnl_tc_set_kind(_obj, kind)
  }
}

public final class RTNLPFIFOFastQDisc: RTNLTCQDisc, @unchecked Sendable {}

public final class RTNLMQPrioQDisc: RTNLTCQDisc, @unchecked Sendable {
  public enum Mode: UInt16 {
    case dcb = 0
    case channel = 1
  }

  public enum Shaper: UInt16 {
    case dcb = 0
    case bwRate = 1
  }

  public convenience init(
    numTC: Int? = nil,
    priorityMap: [UInt8: UInt8]? = nil,
    hwOffload: Bool? = nil,
    count: [UInt16]? = nil,
    offset: [UInt16]? = nil,
    mode: Mode? = nil,
    shaper: Shaper? = nil,
    minRate: [UInt64]? = nil,
    maxRate: [UInt64]? = nil
  ) throws {
    try self.init(object: NLObject(consumingObj: rtnl_qdisc_alloc()), kind: "mqprio")
    if let numTC {
      try throwingNLError {
        rtnl_qdisc_mqprio_set_num_tc(_obj, CInt(numTC))
      }
    }
    if let priorityMap {
      var priomap = [UInt8](repeating: 0, count: Int(TC_QOPT_BITMASK + 1))
      for (key, value) in priorityMap {
        guard key <= TC_QOPT_BITMASK else { throw NLError.invalidArgument }
        priomap[Int(key)] = value
      }

      try throwingNLError {
        rtnl_qdisc_mqprio_set_priomap(_obj, &priomap, CInt(priomap.count))
      }
    }
    if let hwOffload {
      try throwingNLError {
        rtnl_qdisc_mqprio_hw_offload(_obj, hwOffload ? 1 : 0)
      }
    }
    if let count, let offset {
      guard count.count == offset.count else {
        throw NLError.invalidArgument
      }

      var count = count
      var offset = offset
      try throwingNLError {
        rtnl_qdisc_mqprio_set_queue(_obj, &count, &offset, CInt(count.count))
      }
    } else if (count == nil) != (offset == nil) {
      throw NLError.invalidArgument
    }
    if let mode {
      try throwingNLError {
        rtnl_qdisc_mqprio_set_mode(_obj, mode.rawValue)
      }
    }
    if let shaper {
      try throwingNLError {
        rtnl_qdisc_mqprio_set_shaper(_obj, shaper.rawValue)
      }
    }
    if let minRate {
      var minRate = minRate
      try throwingNLError {
        rtnl_qdisc_mqprio_set_min_rate(_obj, &minRate, CInt(minRate.count))
      }
    }
    if let maxRate {
      var maxRate = maxRate
      try throwingNLError {
        rtnl_qdisc_mqprio_set_max_rate(_obj, &maxRate, CInt(maxRate.count))
      }
    }
  }

  public var numTC: Int {
    Int(rtnl_qdisc_mqprio_get_num_tc(_obj))
  }

  public var hwOffload: Bool {
    rtnl_qdisc_mqprio_get_hw_offload(_obj) > 0
  }

  public var mode: Mode {
    get throws {
      let mode = try throwingNLError {
        rtnl_qdisc_mqprio_get_mode(_obj)
      }
      guard let mode = Mode(rawValue: UInt16(mode)) else {
        throw NLError.invalidArgument
      }
      return mode
    }
  }

  public var shaper: Shaper {
    get throws {
      let shaper = try throwingNLError {
        rtnl_qdisc_mqprio_get_shaper(_obj)
      }
      guard let shaper = Shaper(rawValue: UInt16(shaper)) else {
        throw NLError.invalidArgument
      }
      return shaper
    }
  }

  public var minRate: [UInt64] {
    get throws {
      var rates = [UInt64](repeating: 0, count: Int(TC_QOPT_MAX_QUEUE))
      _ = try throwingNLError {
        rates.withUnsafeMutableBufferPointer {
          rtnl_qdisc_mqprio_get_min_rate(_obj, $0.baseAddress)
        }
      }
      return rates
    }
  }

  public var maxRate: [UInt64] {
    get throws {
      var rates = [UInt64](repeating: 0, count: Int(TC_QOPT_MAX_QUEUE))
      _ = try throwingNLError {
        rates.withUnsafeMutableBufferPointer {
          rtnl_qdisc_mqprio_get_max_rate(_obj, $0.baseAddress)
        }
      }
      return rates
    }
  }

  // maps priorities to TCs
  public var priorityMap: [UInt8: UInt8]? {
    guard let map = rtnl_qdisc_mqprio_get_priomap(_obj) else { return nil }
    var priorityMap = [UInt8: UInt8]()
    for i in 0...Int(TC_QOPT_BITMASK) {
      priorityMap[UInt8(i)] = UInt8(map[i])
    }
    return priorityMap
  }
}

public final class RTNLTCClassifier: RTNLTCBase, @unchecked Sendable {}

public final class RTNLTCClass: RTNLTCBase, @unchecked Sendable {}

private extension NLSocket {
  func _tcRequest(
    family: sa_family_t = sa_family_t(AF_UNSPEC),
    interfaceIndex: Int,
    kind: String? = nil,
    chain: UInt32? = nil,
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    options: UnsafePointer<some Any>,
    optionsAttribute: CInt? = nil,
    operation: NLMessage.Operation
  ) async throws {
    try await _tcRequest(
      family: family,
      interfaceIndex: interfaceIndex,
      kind: kind,
      chain: chain,
      handle: handle,
      parent: parent,
      fillOptions: { message in
        if let optionsAttribute {
          try message.put(opaque: options, for: optionsAttribute)
        }
      },
      operation: operation
    )
  }

  func _tcRequest(
    family: sa_family_t = sa_family_t(AF_UNSPEC),
    interfaceIndex: Int,
    kind: String? = nil,
    chain: UInt32? = nil,
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    fillOptions: (_: borrowing NLMessage) throws -> (),
    operation: NLMessage.Operation
  ) async throws {
    if handle == nil, parent == nil {
      throw NLError(rawValue: NLE_MISSING_ATTR)
    }

    let message = try NLMessage(
      socket: self,
      type: operation != .delete ? RTM_NEWQDISC : RTM_DELQDISC,
      operation: operation
    )

    var tchdr = tcmsg()
    tchdr.tcm_family = UInt8(family)
    tchdr.tcm_ifindex = CInt(interfaceIndex)
    tchdr.tcm_parent = parent ?? 0
    tchdr.tcm_handle = handle ?? 0
    try message.append(opaque: &tchdr)

    if let kind {
      try message.put(string: kind, for: CInt(TCA_KIND))
    }

    if let chain {
      try message.put(u32: chain, for: CInt(TCA_CHAIN))
    }

    if operation != .delete {
      let attr = message.nestStart(attr: CInt(TCA_OPTIONS))
      try fillOptions(message)
      message.nestEnd(attr: attr)
    }

    try await ackRequest(message: message)
  }

  func _mqprioQDiscRequest(
    interfaceIndex: Int,
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    mqprio: RTNLMQPrioQDisc,
    operation: NLMessage.Operation
  ) async throws {
    // options attribute is tc_mqprio_qopt() without padding || mode || shaper || minRate || maxRate

    var qopt = tc_mqprio_qopt()
    qopt.num_tc = UInt8(mqprio.numTC)
    if let priomap = rtnl_qdisc_mqprio_get_priomap(mqprio._obj) {
      memcpy(&qopt.prio_tc_map.0, priomap, Int(TC_QOPT_BITMASK + 1))
    }
    qopt.hw = mqprio.hwOffload ? 1 : 0

    try throwingNLError {
      rtnl_qdisc_mqprio_get_queue(mqprio._obj, &qopt.count.0, &qopt.offset.0)
    }

    try await _tcRequest(
      interfaceIndex: interfaceIndex,
      kind: "mqprio",
      handle: handle,
      parent: parent,
      fillOptions: { message in
        var qopt = qopt
        try withUnsafeBytes(of: &qopt) {
          try message.append(Array($0), pad: NL_DONTPAD)
          if let mode = try? mqprio.mode {
            try message.put(u16: mode.rawValue, for: CInt(TCA_MQPRIO_MODE))
          }
          if let shaper = try? mqprio.shaper {
            try message.put(u16: shaper.rawValue, for: CInt(TCA_MQPRIO_SHAPER))
          }
        }
      },
      operation: operation
    )
  }

  func _cbsQDiscRequest(
    interfaceIndex: Int,
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    offload: Bool = true,
    hiCredit: Int32 = Int32.max,
    loCredit: Int32 = Int32.min,
    idleSlope: Int32 = 0,
    sendSlope: Int32 = 0,
    operation: NLMessage.Operation
  ) async throws {
    var qopt = tc_cbs_qopt()
    qopt.offload = offload ? 1 : 0
    qopt.hicredit = hiCredit
    qopt.locredit = loCredit
    qopt.idleslope = idleSlope
    qopt.sendslope = sendSlope
    try await _tcRequest(
      interfaceIndex: interfaceIndex,
      kind: "cbs",
      handle: handle,
      parent: parent,
      options: &qopt,
      optionsAttribute: CInt(TCA_CBS_PARMS),
      operation: operation
    )
  }

  func _pFifoFastQDiscRequest(
    interfaceIndex: Int,
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    operation: NLMessage.Operation
  ) async throws {
    var dummy = ()
    try await _tcRequest(
      interfaceIndex: interfaceIndex,
      kind: "pfifo_fast",
      handle: handle,
      parent: parent,
      options: &dummy,
      operation: operation
    )
  }
}

public extension RTNLLink {
  func add(
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    mqprio: RTNLMQPrioQDisc,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    try await socket._mqprioQDiscRequest(
      interfaceIndex: index, handle: handle, parent: parent, mqprio: mqprio,
      operation: updateIfPresent ? .addOrUpdate : .add
    )
  }

  func remove(
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    mqprio: RTNLMQPrioQDisc,
    socket: NLSocket
  ) async throws {
    try await socket._mqprioQDiscRequest(
      interfaceIndex: index, handle: handle, parent: parent, mqprio: mqprio,
      operation: .delete
    )
  }

  func add(
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    offload: Bool = true,
    hiCredit: Int32 = Int32.max,
    loCredit: Int32 = Int32.min,
    idleSlope: Int32,
    sendSlope: Int32,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    try await socket._cbsQDiscRequest(
      interfaceIndex: index, handle: handle, parent: parent, offload: offload, hiCredit: hiCredit,
      loCredit: loCredit,
      idleSlope: idleSlope, sendSlope: sendSlope,
      operation: updateIfPresent ? .addOrUpdate : .add
    )
  }

  func remove(
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    socket: NLSocket
  ) async throws {
    try await socket._pFifoFastQDiscRequest(
      interfaceIndex: index,
      handle: handle,
      parent: parent,
      operation: .delete
    )
  }
}

public enum RTNLTCMessage: NLObjectConstructible, Sendable {
  case new(RTNLTCBase)
  case del(RTNLTCBase)

  public init(object: NLObject) throws {
    switch object.messageType {
    case RTM_NEWQDISC:
      fallthrough
    case RTM_NEWTCLASS:
      fallthrough
    case RTM_NEWTFILTER:
      self = try .new(RTNLTCBase(object: object))
    case RTM_DELQDISC:
      fallthrough
    case RTM_DELTCLASS:
      fallthrough
    case RTM_DELTFILTER:
      self = try .del(RTNLTCBase(object: object))
    default:
      throw NLError.invalidArgument
    }
  }

  public var tc: RTNLTCBase {
    switch self {
    case let .new(tc):
      tc
    case let .del(tc):
      tc
    }
  }
}

extension UnsafePointer {
  func propertyBasePointer<Property>(to property: KeyPath<Pointee, Property>)
    -> UnsafePointer<Property>?
  {
    guard let offset = MemoryLayout<Pointee>.offset(of: property) else { return nil }
    return (UnsafeRawPointer(self) + offset).assumingMemoryBound(to: Property.self)
  }
}
