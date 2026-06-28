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
    guard let address else { return "" }
    return byteToHex(address[0]) +
      ":" + byteToHex(address[1]) +
      ":" + byteToHex(address[2]) +
      ":" + byteToHex(address[3]) +
      ":" + byteToHex(address[4]) +
      ":" + byteToHex(address[5])
  }

  public typealias LinkAddress = InlineArray<6, UInt8>

  public static func parseMacAddressString(_ macAddress: String) throws -> LinkAddress {
    let ll = try sockaddr_ll(
      family: sa_family_t(AF_PACKET),
      presentationAddress: macAddress
    )
    var result = LinkAddress(repeating: 0)
    withUnsafeBytes(of: ll.sll_addr) { bytes in
      for i in 0..<6 {
        result[i] = bytes[i]
      }
    }
    return result
  }

  private func _makeAddress(_ addr: OpaquePointer?) -> LinkAddress? {
    guard let addr else { return nil }
    let len = Int(nl_addr_get_len(addr))
    guard len == Int(ETH_ALEN) else { return nil }
    var result = LinkAddress(repeating: 0)
    let bytes = UnsafeBufferPointer(
      start: nl_addr_get_binary_addr(addr).assumingMemoryBound(to: UInt8.self),
      count: len
    )
    for i in 0..<6 {
      result[i] = bytes[i]
    }
    return result
  }

  public var address: LinkAddress? {
    _makeAddress(rtnl_link_get_addr(_obj))
  }

  public func set(address: LinkAddress, socket: NLSocket) async throws {
    let message = try NLMessage(socket: socket, type: RTM_SETLINK, flags: [.request, .ack])
    try message.appendIfInfo(family: sa_family_t(AF_UNSPEC), index: index)
    try message.put(data: address.span.withUnsafeBytes { Array($0) }, for: CInt(IFLA_ADDRESS))
    try await socket.ackRequest(message: message)
  }

  public var nlAddress: NLAddress {
    NLAddress(addr: rtnl_link_get_addr(_obj))
  }

  public var broadcastAddress: LinkAddress? {
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

  public enum BridgeOption: Int {
    case unspec = 0
    case state // Spanning tree state
    case priority // Spanning tree priority (UInt16)
    case cost // Spanning tree cost (UInt32)
    case mode // Mode (hairpin)
    case `guard` // BPDU guard
    case protect // Root port protection
    case fastLeave // Multicast fast leave
    case learning // MAC learning
    case unicastFlood // Flood unicast traffic
    case proxyARP // Proxy ARP
    case learningSync // MAC learning sync from device
    case proxyARPWifi // Proxy ARP for Wi-Fi
    case rootID // Designated root
    case bridgeID // Designated bridge
    case designatedPort
    case designatedCost
    case id
    case no
    case topologyChangeAck
    case configPending
    case messageAgeTimer
    case forwardDelayTimer
    case holdTimer
    case flush
    case multicastRouter
    case pad
    case mcastFlood
    case mcastToUcast
    case vlanTunnel
    case bcastFlood
    case groupFwdMask
    case neighSuppress
    case isolated
    case backupPort
    case mrpRingOpen
    case mrpInOpen
    case mcastEhtHostsLimit
    case mcastEhtHostsCnt
    case locked
    case mab
    case mcastNGroups
    case mcastMaxGroups
    case neighVlanSuppress
    case backupNhid // IFLA_BRPORT_BACKUP_NHID
    case neighForwardGrat // IFLA_BRPORT_NEIGH_FORWARD_GRAT
  }

  public func set(option: BridgeOption, _ value: some Any, socket: NLSocket) async throws {
    try await socket._setBridgeOption(
      interfaceIndex: index,
      option: option.rawValue,
      value
    )
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

  /// The state of an MDB entry (`br_mdb_entry.state`). The raw values mirror
  /// the kernel `MDB_*` UAPI constants; `dynamicReservation` may be absent
  /// from older host `<linux/if_bridge.h>`, so the values are defined here.
  public enum MDBState: UInt8, Sendable {
    /// `MDB_TEMPORARY` — the entry is aged by a group timer.
    case temporary = 0
    /// `MDB_PERMANENT` — the entry persists until explicitly removed.
    case permanent = 1
    /// `MDB_DYNAMIC_RESERVATION` — a permanent entry that additionally marks
    /// the group as an 802.1Qat reserved stream; the kernel propagates this to
    /// switchdev so hardware can apply its own admission policy.
    case dynamicReservation = 2
  }

  public func add(
    link: RTNLLink,
    groupAddresses: [LinkAddress],
    vlanID: UInt16? = nil,
    state: MDBState = .permanent,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    let operation: NLMessage.Operation = updateIfPresent ? .addOrUpdate : .add
    try await socket._groupRequest(
      bridgeIndex: index,
      interfaceIndex: link.index,
      groupAddresses: groupAddresses,
      vlanID: vlanID,
      state: state.rawValue,
      operation: operation
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
      let start = UnsafeRawPointer(pointer).assumingMemoryBound(to: UInt32.self)
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
      let start = UnsafeRawPointer(pointer).assumingMemoryBound(to: UInt32.self)
      let bitmap = [UInt32](UnsafeBufferPointer(
        start: start,
        count: Int(RTNL_LINK_BRIDGE_VLAN_BITMAP_LEN)
      ))
      return _expandBitmap(bitmap)
    }
  }
}

// linux/if_vlan.h vlan_flags. Defined here rather than imported from
// <linux/if_vlan.h> because the CNetLink umbrella header does not include it.
public struct VLANFlags: OptionSet, Sendable {
  public typealias RawValue = UInt32

  public let rawValue: RawValue

  public init(rawValue: RawValue) {
    self.rawValue = rawValue
  }

  public static let reorderHeader = VLANFlags(rawValue: 0x1)
  public static let gvrp = VLANFlags(rawValue: 0x2)
  public static let looseBinding = VLANFlags(rawValue: 0x4)
  public static let mvrp = VLANFlags(rawValue: 0x8)
  public static let bridgeBinding = VLANFlags(rawValue: 0x10)
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

  public var vlanFlags: VLANFlags {
    VLANFlags(rawValue: UInt32(rtnl_link_vlan_get_flags(_obj)))
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

  func subscribeBridgeVLANs() throws {
    try add(membership: RTNLGRP_BRVLAN)
  }

  func unsubscribeBridgeVLANs() throws {
    try drop(membership: RTNLGRP_BRVLAN)
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

  fileprivate func _setBridgeOption(
    interfaceIndex: Int,
    option: Int,
    _ value: some Any
  ) async throws {
    let message = try NLMessage(
      socket: self,
      type: RTM_SETLINK,
      operation: .update
    )
    try message.appendIfInfo(index: interfaceIndex)
    let attr = message.nestStart(attr: CInt(IFLA_PROTINFO))
    switch value {
    case let v as Bool: try message.put(u8: v ? 1 : 0, for: CInt(option))
    case let v as UInt8: try message.put(u8: v, for: CInt(option))
    case let v as UInt16: try message.put(u16: v, for: CInt(option))
    case let v as UInt32: try message.put(u32: v, for: CInt(option))
    case let v as UInt64: try message.put(u64: v, for: CInt(option))
    case let v as [UInt8]: try message.put(data: v, for: CInt(option))
    case let v as String: try message.put(string: v, for: CInt(option))
    default: throw NLError.invalidArgument
    }
    message.nestEnd(attr: attr)
    try await ackRequest(message: message)
  }

  // NLM_F_BULK is not supported for RTM_DELLINK
  private func _vlanRequestSingle(
    vlan vid: UInt16,
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
    var vlanInfo = bridge_vlan_info(flags: flags, vid: vid)
    try message.put(opaque: &vlanInfo, for: CInt(IFLA_BRIDGE_VLAN_INFO))
    message.nestEnd(attr: attr)
    try await ackRequest(message: message)
  }

  fileprivate func _vlanRequest(
    vlans: Set<UInt16>,
    interfaceIndex: Int,
    flags: UInt16 = 0,
    moreFlags: UInt16 = 0,
    operation: NLMessage.Operation
  ) async throws {
    for vlan in vlans {
      try await _vlanRequestSingle(
        vlan: vlan,
        interfaceIndex: interfaceIndex,
        flags: flags,
        moreFlags: moreFlags,
        operation: operation
      )
    }
  }

  fileprivate func _groupRequest(
    bridgeIndex: Int,
    interfaceIndex: Int,
    groupAddresses: [RTNLLink.LinkAddress],
    vlanID: UInt16? = nil,
    state: UInt8 = UInt8(MDB_PERMANENT),
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
      state: state,
      flags: 0,
      vid: vlanID ?? 0,
      addr: .init()
    )

    func linkAddressToTuple(_ addr: RTNLLink
      .LinkAddress) -> (UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)
    {
      (addr[0], addr[1], addr[2], addr[3], addr[4], addr[5])
    }

    for groupAddress in groupAddresses {
      entry.addr.u.mac_addr = linkAddressToTuple(groupAddress)
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
    try withUnsafeBytes(of: macAddress) { bytes in
      let macAddressBytes = [UInt8](bytes)
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
    handle: UInt32? = nil,
    parent: UInt32? = nil,
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
    if let handle {
      rtnl_tc_set_handle(_obj, handle)
    }
    if let parent {
      rtnl_tc_set_parent(_obj, parent)
    }
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

  // per-TC queue allocation: count[tc] queues starting at offset[tc]
  public var queues: (count: [UInt16], offset: [UInt16]) {
    get throws {
      var count = [UInt16](repeating: 0, count: Int(TC_QOPT_MAX_QUEUE))
      var offset = [UInt16](repeating: 0, count: Int(TC_QOPT_MAX_QUEUE))
      try throwingNLError {
        rtnl_qdisc_mqprio_get_queue(_obj, &count, &offset)
      }
      let n = numTC
      return (Array(count.prefix(n)), Array(offset.prefix(n)))
    }
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

  func _mqprioQDiscRequest_addOrUpdate(
    interfaceIndex: Int? = nil,
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
      interfaceIndex: interfaceIndex ?? mqprio.index,
      kind: mqprio.kind,
      handle: mqprio.handle,
      parent: mqprio.parent,
      fillOptions: { message in
        var qopt = qopt
        try withUnsafeBytes(of: &qopt) {
          try message.append(Array($0), pad: NL_DONTPAD)
          if mqprio.hwOffload {
            if let mode = try? mqprio.mode {
              try message.put(u16: mode.rawValue, for: CInt(TCA_MQPRIO_MODE))
            }
            if let shaper = try? mqprio.shaper {
              try message.put(u16: shaper.rawValue, for: CInt(TCA_MQPRIO_SHAPER))
            }
            if let minRate = try? mqprio.minRate {
              var minRate = minRate
              try minRate.withUnsafeMutableBufferPointer { buffer in
                try message.put(
                  data: Array(UnsafeRawBufferPointer(buffer).bindMemory(to: UInt8.self)),
                  for: CInt(TCA_MQPRIO_MIN_RATE64)
                )
              }
            }
            if let maxRate = try? mqprio.maxRate {
              var maxRate = maxRate
              try maxRate.withUnsafeMutableBufferPointer { buffer in
                try message.put(
                  data: Array(UnsafeRawBufferPointer(buffer).bindMemory(to: UInt8.self)),
                  for: CInt(TCA_MQPRIO_MAX_RATE64)
                )
              }
            }
          }
        }
      },
      operation: operation
    )
  }

  func _mqprioQDiscRequest_delete(
    interfaceIndex: Int? = nil,
    mqprio: RTNLMQPrioQDisc
  ) async throws {
    try await _tcRequest(
      interfaceIndex: interfaceIndex ?? mqprio.index,
      kind: mqprio.kind,
      handle: mqprio.handle,
      parent: mqprio.parent,
      fillOptions: { _ in },
      operation: .delete
    )
  }

  func _mqprioQDiscRequest(
    interfaceIndex: Int? = nil,
    mqprio: RTNLMQPrioQDisc,
    operation: NLMessage.Operation
  ) async throws {
    switch operation {
    case .add:
      fallthrough
    case .update:
      fallthrough
    case .addOrUpdate:
      try await _mqprioQDiscRequest_addOrUpdate(
        interfaceIndex: interfaceIndex,
        mqprio: mqprio,
        operation: operation
      )
    case .delete:
      try await _mqprioQDiscRequest_delete(interfaceIndex: interfaceIndex, mqprio: mqprio)
    }
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

  func _fqCoDelQDiscRequest(
    interfaceIndex: Int,
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    operation: NLMessage.Operation
  ) async throws {
    var dummy = ()
    try await _tcRequest(
      interfaceIndex: interfaceIndex,
      kind: "fq_codel",
      handle: handle,
      parent: parent,
      options: &dummy,
      operation: operation
    )
  }
}

public extension RTNLLink {
  func add(
    mqprio: RTNLMQPrioQDisc,
    updateIfPresent: Bool = true,
    socket: NLSocket
  ) async throws {
    try await socket._mqprioQDiscRequest(
      interfaceIndex: index, mqprio: mqprio, operation: updateIfPresent ? .addOrUpdate : .add
    )
  }

  func remove(
    handle: UInt32? = nil,
    parent: UInt32? = nil,
    mqprio: RTNLMQPrioQDisc,
    socket: NLSocket
  ) async throws {
    try await socket._mqprioQDiscRequest(
      interfaceIndex: index, mqprio: mqprio, operation: .delete
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
    socket: NLSocket,
    restoreDefaultQDisc: Bool = true
  ) async throws {
    if restoreDefaultQDisc {
      try await socket._fqCoDelQDiscRequest(
        interfaceIndex: index,
        handle: handle,
        parent: parent,
        operation: .addOrUpdate
      )
    } else {
      // this will remove the CBS QDisc entirely, which may stop traffic flowing
      try await socket._cbsQDiscRequest(
        interfaceIndex: index,
        handle: handle,
        parent: parent,
        operation: .delete
      )
    }
  }
}

/// A DCB application priority table entry (`struct dcb_app`), used to program the
/// DCBNL IEEE 802.1Qaz APP table. For the PCP selector (`DCB_APP_SEL_PCP`) this maps
/// an ingress PCP (`protocol`) to an internal frame priority (`priority`). The frame
/// priority is then mapped to a queue (QPri) by a separate, downstream FPri->QPri step;
/// `priority` here is never a queue index.
public struct RTNLDCBApp: Sendable, Equatable {
  public static let pcpSelector = UInt8(DCB_APP_SEL_PCP)

  public var selector: UInt8
  public var priority: UInt8
  public var protocolID: UInt16

  public init(selector: UInt8, priority: UInt8, protocolID: UInt16) {
    self.selector = selector
    self.priority = priority
    self.protocolID = protocolID
  }

  /// Map an ingress PCP value (0-15, where 8-15 carry DEI) to an internal frame priority.
  public static func pcp(_ pcp: UInt8, priority: UInt8) -> RTNLDCBApp {
    RTNLDCBApp(selector: pcpSelector, priority: priority, protocolID: UInt16(pcp))
  }

  fileprivate var _app: dcb_app {
    var app = dcb_app()
    app.selector = selector
    app.priority = priority
    app.`protocol` = protocolID
    return app
  }
}

extension NLSocket {
  fileprivate func _dcbAppRequest(
    interfaceName: String,
    apps: [RTNLDCBApp],
    cmd: UInt8
  ) async throws {
    let message = try NLMessage(
      socket: self,
      type: Int(RTM_SETDCB),
      flags: [.request, .ack]
    )
    var hdr = dcbmsg()
    hdr.dcb_family = UInt8(AF_UNSPEC)
    hdr.cmd = cmd
    hdr.dcb_pad = 0
    try message.append(opaque: &hdr)

    try message.put(string: interfaceName, for: CInt(DCB_ATTR_IFNAME.rawValue))

    let ieee = message.nestStart(attr: CInt(DCB_ATTR_IEEE.rawValue))
    let table = message.nestStart(attr: CInt(DCB_ATTR_IEEE_APP_TABLE.rawValue))
    for app in apps {
      // PCP selector entries go in DCB_ATTR_DCB_APP; standard selectors in DCB_ATTR_IEEE_APP.
      let attr = app.selector == RTNLDCBApp.pcpSelector ?
        CInt(DCB_ATTR_DCB_APP.rawValue) : CInt(DCB_ATTR_IEEE_APP.rawValue)
      var entry = app._app
      try message.put(opaque: &entry, for: attr)
    }
    message.nestEnd(attr: table)
    message.nestEnd(attr: ieee)

    // dcbnl reports the SET/DEL result as a DCB_ATTR_IEEE u8 in the RTM_SETDCB reply body; the
    // netlink ACK is always success. Read the reply and surface the real error (e.g. a driver
    // that does not implement the op replies EOPNOTSUPP here).
    let reply = try await continuationRequest(message: message)
    let errorCode = (reply as? RTNLDCBMessage)?.dcb.errorCode ?? 0
    if errorCode != 0 {
      throw Errno(rawValue: errorCode)
    }
  }
}

public extension RTNLLink {
  /// Add DCB APP table entries (`DCB_CMD_IEEE_SET`) on this interface.
  func add(dcbApps: [RTNLDCBApp], socket: NLSocket) async throws {
    try await socket._dcbAppRequest(
      interfaceName: name,
      apps: dcbApps,
      cmd: UInt8(DCB_CMD_IEEE_SET.rawValue)
    )
  }

  /// Remove DCB APP table entries (`DCB_CMD_IEEE_DEL`) from this interface.
  func remove(dcbApps: [RTNLDCBApp], socket: NLSocket) async throws {
    try await socket._dcbAppRequest(
      interfaceName: name,
      apps: dcbApps,
      cmd: UInt8(DCB_CMD_IEEE_DEL.rawValue)
    )
  }

  /// Fetch the DCB IEEE APP table (`DCB_CMD_IEEE_GET`) for this interface.
  func getDCBApps(socket: NLSocket) async throws -> [RTNLDCBApp] {
    try await socket._dcbGetApps(interfaceName: name)
  }
}

/// The DCB IEEE APP table returned by a `DCB_CMD_IEEE_GET` query. dcbnl messages are not
/// libnl objects, so (like MDB) they are parsed directly from the raw netlink message.
public final class RTNLDCB: NLObjectConstructible, @unchecked Sendable, CustomStringConvertible {
  public let interfaceName: String?
  public let apps: [RTNLDCBApp]
  /// Positive errno from a `DCB_CMD_IEEE_SET`/`DEL` reply (0 = success). dcbnl returns the
  /// command result as a `DCB_ATTR_IEEE` u8 in the reply body rather than via a netlink ACK,
  /// so callers must inspect this rather than rely on the (always-success) ACK.
  public let errorCode: CInt

  init(interfaceName: String?, apps: [RTNLDCBApp], errorCode: CInt = 0) {
    self.interfaceName = interfaceName
    self.apps = apps
    self.errorCode = errorCode
  }

  /// Not used — built directly in `NLSocket_CB_VALID` for `RTM_GETDCB`.
  public required convenience init(object: NLObject) throws {
    throw NLError.invalidArgument
  }

  convenience init(rawHeader nlh: UnsafeMutablePointer<nlmsghdr>) throws {
    guard Int(nlmsg_datalen(nlh)) >= MemoryLayout<dcbmsg>.size else {
      throw NLError.invalidArgument
    }
    let hdrlen = CInt(MemoryLayout<dcbmsg>.size)
    var interfaceName: String?
    var apps = [RTNLDCBApp]()
    var errorCode: CInt = 0

    var attrRem = nlmsg_attrlen(nlh, hdrlen)
    var attrPos = nlmsg_attrdata(nlh, hdrlen)
    while nla_ok(attrPos, attrRem) != 0 {
      defer { attrPos = nla_next(attrPos, &attrRem) }
      guard let attr = attrPos else { continue }
      switch nla_type(attr) {
      case CInt(DCB_ATTR_IFNAME.rawValue):
        if let s = nla_get_string(attr) { interfaceName = String(cString: s) }
      case CInt(DCB_ATTR_IEEE.rawValue):
        // In a GET reply DCB_ATTR_IEEE is a nested table; in a SET/DEL reply it is a single
        // u8 carrying the (negated) command result. Distinguish by payload length.
        if nla_len(attr) == 1 {
          let raw = Int(nla_get_u8(attr))
          errorCode = raw == 0 ? 0 : CInt(256 - raw) // -errno stored as u8 -> positive errno
        } else {
          apps.append(contentsOf: RTNLDCB._parseAppTable(ieee: attr))
        }
      default:
        break
      }
    }
    self.init(interfaceName: interfaceName, apps: apps, errorCode: errorCode)
  }

  private static func _parseAppTable(ieee: UnsafeMutablePointer<nlattr>) -> [RTNLDCBApp] {
    var apps = [RTNLDCBApp]()
    var tableRem = nla_len(ieee)
    var tablePos = nla_data(ieee)?.assumingMemoryBound(to: nlattr.self)
    while nla_ok(tablePos, tableRem) != 0 {
      defer { tablePos = nla_next(tablePos, &tableRem) }
      guard let table = tablePos,
            nla_type(table) == CInt(DCB_ATTR_IEEE_APP_TABLE.rawValue) else { continue }

      var appRem = nla_len(table)
      var appPos = nla_data(table)?.assumingMemoryBound(to: nlattr.self)
      while nla_ok(appPos, appRem) != 0 {
        defer { appPos = nla_next(appPos, &appRem) }
        guard let appAttr = appPos else { continue }
        let type = nla_type(appAttr)
        guard type == CInt(DCB_ATTR_IEEE_APP.rawValue) ||
          type == CInt(DCB_ATTR_DCB_APP.rawValue),
          let data = nla_data(appAttr),
          Int(nla_len(appAttr)) >= MemoryLayout<dcb_app>.size else { continue }
        let a = data.assumingMemoryBound(to: dcb_app.self).pointee
        apps.append(RTNLDCBApp(
          selector: a.selector,
          priority: a.priority,
          protocolID: a.`protocol`
        ))
      }
    }
    return apps
  }

  public var description: String {
    "RTNLDCB(interface: \(interfaceName ?? "?"), apps: \(apps.count), errorCode: \(errorCode))"
  }
}

public enum RTNLDCBMessage: NLObjectConstructible, Sendable {
  case get(RTNLDCB)

  /// Not used — built directly in `NLSocket_CB_VALID` for `RTM_GETDCB`.
  public init(object: NLObject) throws {
    throw NLError.invalidArgument
  }

  public var dcb: RTNLDCB {
    switch self {
    case let .get(d): d
    }
  }
}

extension NLSocket {
  fileprivate func _dcbGetApps(interfaceName: String) async throws -> [RTNLDCBApp] {
    let message = try NLMessage(
      socket: self,
      type: Int(RTM_GETDCB),
      flags: [.request]
    )
    var hdr = dcbmsg()
    hdr.dcb_family = UInt8(AF_UNSPEC)
    hdr.cmd = UInt8(DCB_CMD_IEEE_GET.rawValue)
    hdr.dcb_pad = 0
    try message.append(opaque: &hdr)
    try message.put(string: interfaceName, for: CInt(DCB_ATTR_IFNAME.rawValue))

    let result = try await continuationRequest(message: message)
    return (result as! RTNLDCBMessage).dcb.apps
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

private func _hex(_ byte: UInt8) -> String {
  let hex = Array("0123456789abcdef".utf8)
  return String(unsafeUninitializedCapacity: 2) { ptr in
    ptr[0] = hex[Int(byte / 16)]
    ptr[1] = hex[Int(byte % 16)]
    return 2
  }
}

private func _hex(_ value: UInt16) -> String {
  _hex(UInt8(value >> 8)) + _hex(UInt8(value & 0xff))
}

private func _nlAddrToBytes(_ addr: OpaquePointer?) -> (sa_family_t, [UInt8])? {
  guard let addr else { return nil }
  let len = Int(nl_addr_get_len(addr))
  guard len > 0 else { return (sa_family_t(nl_addr_get_family(addr)), []) }
  let buf = UnsafeBufferPointer(
    start: nl_addr_get_binary_addr(addr).assumingMemoryBound(to: UInt8.self),
    count: len
  )
  return (sa_family_t(nl_addr_get_family(addr)), Array(buf))
}

private func _nlAddrToMac(_ addr: OpaquePointer?) -> RTNLLink.LinkAddress? {
  guard let addr else { return nil }
  guard Int(nl_addr_get_len(addr)) == Int(ETH_ALEN) else { return nil }
  var result = RTNLLink.LinkAddress(repeating: 0)
  let bytes = UnsafeBufferPointer(
    start: nl_addr_get_binary_addr(addr).assumingMemoryBound(to: UInt8.self),
    count: 6
  )
  for i in 0..<6 { result[i] = bytes[i] }
  return result
}

public final class RTNLNeighbor: NLObjectConstructible, @unchecked Sendable,
  CustomStringConvertible, RTNLFactory
{
  private let _object: NLObject

  fileprivate init(_ object: NLObject) {
    _object = object
  }

  public required convenience init(object: NLObject) throws {
    guard object.messageType == RTM_NEWNEIGH || object.messageType == RTM_DELNEIGH
    else {
      throw NLError.invalidArgument
    }
    self.init(object)
  }

  fileprivate var _obj: OpaquePointer { _object._obj }

  public var family: sa_family_t {
    sa_family_t(rtnl_neigh_get_family(_obj))
  }

  public var ifIndex: Int {
    Int(rtnl_neigh_get_ifindex(_obj))
  }

  public var master: Int {
    Int(rtnl_neigh_get_master(_obj))
  }

  public var state: Int {
    Int(rtnl_neigh_get_state(_obj))
  }

  public var flags: UInt32 {
    UInt32(rtnl_neigh_get_flags(_obj))
  }

  public var vlanID: Int? {
    let v = Int(rtnl_neigh_get_vlan(_obj))
    return v < 0 ? nil : v
  }

  public var linkLayerAddress: RTNLLink.LinkAddress? {
    _nlAddrToMac(rtnl_neigh_get_lladdr(_obj))
  }

  public var destinationAddress: (sa_family_t, [UInt8])? {
    _nlAddrToBytes(rtnl_neigh_get_dst(_obj))
  }

  public static func stateString(_ state: Int) -> String {
    var buf = [CChar](repeating: 0, count: 128)
    let r = buf.withUnsafeMutableBufferPointer {
      rtnl_neigh_state2str(Int32(state), $0.baseAddress, $0.count)
    }
    return r.map { String(cString: $0) } ?? ""
  }

  public static func flagsString(_ flags: UInt32) -> String {
    var buf = [CChar](repeating: 0, count: 128)
    let r = buf.withUnsafeMutableBufferPointer {
      rtnl_neigh_flags2str(Int32(flags), $0.baseAddress, $0.count)
    }
    return r.map { String(cString: $0) } ?? ""
  }

  public var description: String {
    let mac = linkLayerAddress.map { addr in
      (0..<6).map { _hex(addr[$0]) }.joined(separator: ":")
    } ?? "?"
    return "RTNLNeighbor(if: \(ifIndex), master: \(master), lladdr: \(mac), vlan: \(vlanID.map(String.init) ?? "-"), state: \(Self.stateString(state)), flags: \(Self.flagsString(flags)))"
  }
}

public enum RTNLNeighborMessage: NLObjectConstructible, Sendable {
  case new(RTNLNeighbor)
  case del(RTNLNeighbor)

  public init(object: NLObject) throws {
    switch object.messageType {
    case RTM_NEWNEIGH:
      self = try .new(RTNLNeighbor(object: object))
    case RTM_DELNEIGH:
      self = try .del(RTNLNeighbor(object: object))
    default:
      throw NLError.invalidArgument
    }
  }

  public var neighbor: RTNLNeighbor {
    switch self {
    case let .new(n): n
    case let .del(n): n
    }
  }
}

public struct RTNLMDBEntry: Sendable, CustomStringConvertible {
  public let ifIndex: Int
  public let vid: UInt16
  public let state: UInt8
  public let flags: UInt8
  public let proto: UInt16
  public let addressFamily: sa_family_t
  public let address: [UInt8]

  /// The decoded entry state, or `nil` for an unrecognised raw value.
  public var mdbState: RTNLLinkBridge.MDBState? {
    RTNLLinkBridge.MDBState(rawValue: state)
  }

  public var isPermanent: Bool {
    mdbState == .permanent || mdbState == .dynamicReservation
  }

  /// True if the entry is an 802.1Qat reserved stream, i.e. its state is
  /// `MDB_DYNAMIC_RESERVATION`. Such an entry is a permanent entry that
  /// additionally marks the group reserved.
  public var isDynamicReservation: Bool { mdbState == .dynamicReservation }

  public var macAddress: RTNLLink.LinkAddress? {
    guard address.count == 6 else { return nil }
    var result = RTNLLink.LinkAddress(repeating: 0)
    for i in 0..<6 { result[i] = address[i] }
    return result
  }

  public var addressString: String {
    if address.count == 6 {
      return (0..<6).map { _hex(address[$0]) }.joined(separator: ":")
    }
    return address.map { _hex($0) }.joined()
  }

  public var description: String {
    "RTNLMDBEntry(if: \(ifIndex), vid: \(vid), proto: 0x\(_hex(proto)), addr: \(addressString), \(isPermanent ? "permanent" : "temporary"), flags: 0x\(_hex(flags)))"
  }
}

/// Bridge MDB representation. Built by parsing the raw netlink message
/// (`RTM_*MDB`) directly — libnl's `rtnl_mdb_entry` API omits the per-entry
/// flag byte, so we bypass it for dumps and notifications.
public final class RTNLMDB: NLObjectConstructible, @unchecked Sendable,
  CustomStringConvertible, RTNLFactory
{
  public let bridgeIndex: Int
  public let entries: [RTNLMDBEntry]

  init(bridgeIndex: Int, entries: [RTNLMDBEntry]) {
    self.bridgeIndex = bridgeIndex
    self.entries = entries
  }

  /// Not used — MDB messages are parsed directly from the raw nlmsg in
  /// `NLSocket_CB_VALID`. The conformance is kept so RTNLMDB still satisfies
  /// `NLObjectConstructible`.
  public required convenience init(object: NLObject) throws {
    throw NLError.invalidArgument
  }

  convenience init(rawHeader nlh: UnsafeMutablePointer<nlmsghdr>) throws {
    let payload = nlmsg_data(nlh)
    let payloadLen = Int(nlmsg_datalen(nlh))
    guard let payload, payloadLen >= MemoryLayout<br_port_msg>.size else {
      throw NLError.invalidArgument
    }
    let portMsg = payload.assumingMemoryBound(to: br_port_msg.self).pointee
    let entries = RTNLMDB._parseEntries(
      nlh: nlh,
      hdrlen: CInt(MemoryLayout<br_port_msg>.size)
    )
    self.init(bridgeIndex: Int(portMsg.ifindex), entries: entries)
  }

  private static func _parseEntries(
    nlh: UnsafeMutablePointer<nlmsghdr>,
    hdrlen: CInt
  ) -> [RTNLMDBEntry] {
    var entries: [RTNLMDBEntry] = []
    var attrRem = nlmsg_attrlen(nlh, hdrlen)
    var attrPos = nlmsg_attrdata(nlh, hdrlen)
    while nla_ok(attrPos, attrRem) != 0 {
      defer { attrPos = nla_next(attrPos, &attrRem) }
      guard let mdbAttr = attrPos,
            nla_type(mdbAttr) == CInt(MDBA_MDB) else { continue }

      var entryRem = nla_len(mdbAttr)
      var entryPos = nla_data(mdbAttr)?
        .assumingMemoryBound(to: nlattr.self)
      while nla_ok(entryPos, entryRem) != 0 {
        defer { entryPos = nla_next(entryPos, &entryRem) }
        guard let entryAttr = entryPos,
              nla_type(entryAttr) == CInt(MDBA_MDB_ENTRY) else { continue }

        var infoRem = nla_len(entryAttr)
        var infoPos = nla_data(entryAttr)?
          .assumingMemoryBound(to: nlattr.self)
        while nla_ok(infoPos, infoRem) != 0 {
          defer { infoPos = nla_next(infoPos, &infoRem) }
          guard let infoAttr = infoPos,
                nla_type(infoAttr) == CInt(MDBA_MDB_ENTRY_INFO),
                let infoData = nla_data(infoAttr),
                Int(nla_len(infoAttr)) >= MemoryLayout<br_mdb_entry>.size
          else { continue }
          let e = infoData.assumingMemoryBound(to: br_mdb_entry.self).pointee
          entries.append(_makeEntry(from: e))
        }
      }
    }
    return entries
  }

  private static func _makeEntry(from e: br_mdb_entry) -> RTNLMDBEntry {
    // br_mdb_entry.addr.proto is in network byte order; the address union
    // carries IPv4, IPv6, or MAC depending on proto. For non-IP protos we
    // treat the first 6 bytes as a MAC address.
    let proto = UInt16(bigEndian: e.addr.proto)
    let family: sa_family_t
    let address: [UInt8]
    switch CInt(proto) {
    case ETH_P_IP:
      var ip4 = e.addr.u.ip4
      address = withUnsafeBytes(of: &ip4) { Array($0) }
      family = sa_family_t(AF_INET)
    case ETH_P_IPV6:
      var ip6 = e.addr.u.ip6
      address = withUnsafeBytes(of: &ip6) { Array($0) }
      family = sa_family_t(AF_INET6)
    default:
      var mac = e.addr.u.mac_addr
      address = withUnsafeBytes(of: &mac) { Array($0.prefix(6)) }
      family = sa_family_t(AF_UNSPEC)
    }
    return RTNLMDBEntry(
      ifIndex: Int(e.ifindex),
      vid: e.vid,
      state: e.state,
      flags: e.flags,
      proto: proto,
      addressFamily: family,
      address: address
    )
  }

  public var description: String {
    "RTNLMDB(bridge: \(bridgeIndex), entries: \(entries.count))"
  }
}

public enum RTNLMDBMessage: NLObjectConstructible, Sendable {
  case new(RTNLMDB)
  case del(RTNLMDB)

  /// Not used — built directly in `NLSocket_CB_VALID` for `RTM_*MDB`.
  public init(object: NLObject) throws {
    throw NLError.invalidArgument
  }

  public var mdb: RTNLMDB {
    switch self {
    case let .new(m): m
    case let .del(m): m
    }
  }
}

public struct RTNLVLANDBEntry: Sendable, CustomStringConvertible {
  public let vid: UInt16
  public let flags: UInt16

  public var isUntagged: Bool { flags & UInt16(BRIDGE_VLAN_INFO_UNTAGGED) != 0 }
  public var isPVID: Bool { flags & UInt16(BRIDGE_VLAN_INFO_PVID) != 0 }

  public var description: String {
    "RTNLVLANDBEntry(vid: \(vid)\(isPVID ? " pvid" : "")\(isUntagged ? " untagged" : ""))"
  }
}

/// Bridge per-port VLAN database (`RTM_*VLAN`), parsed directly from the raw
/// netlink message — libnl has no object for the VLAN database.
public final class RTNLVLANDB: NLObjectConstructible, @unchecked Sendable,
  CustomStringConvertible, RTNLFactory
{
  public let ifIndex: Int
  public let entries: [RTNLVLANDBEntry]

  init(ifIndex: Int, entries: [RTNLVLANDBEntry]) {
    self.ifIndex = ifIndex
    self.entries = entries
  }

  /// Not used — VLAN messages are parsed directly from the raw nlmsg in
  /// `NLSocket_CB_VALID`.
  public required convenience init(object: NLObject) throws {
    throw NLError.invalidArgument
  }

  convenience init(rawHeader nlh: UnsafeMutablePointer<nlmsghdr>) throws {
    let payload = nlmsg_data(nlh)
    let payloadLen = Int(nlmsg_datalen(nlh))
    guard let payload, payloadLen >= MemoryLayout<br_vlan_msg>.size else {
      throw NLError.invalidArgument
    }
    let vlanMsg = payload.assumingMemoryBound(to: br_vlan_msg.self).pointee
    let entries = RTNLVLANDB._parseEntries(
      nlh: nlh,
      hdrlen: CInt(MemoryLayout<br_vlan_msg>.size)
    )
    self.init(ifIndex: Int(vlanMsg.ifindex), entries: entries)
  }

  private static func _parseEntries(
    nlh: UnsafeMutablePointer<nlmsghdr>,
    hdrlen: CInt
  ) -> [RTNLVLANDBEntry] {
    var entries: [RTNLVLANDBEntry] = []
    var attrRem = nlmsg_attrlen(nlh, hdrlen)
    var attrPos = nlmsg_attrdata(nlh, hdrlen)
    while nla_ok(attrPos, attrRem) != 0 {
      defer { attrPos = nla_next(attrPos, &attrRem) }
      guard let entryAttr = attrPos,
            nla_type(entryAttr) == CInt(BRIDGE_VLANDB_ENTRY) else { continue }

      // A BRIDGE_VLANDB_ENTRY carries a bridge_vlan_info (the VID + flags), and
      // optionally a RANGE attribute giving the last VID of a contiguous range
      // (the INFO's VID being the first, flagged RANGE_BEGIN).
      var info: bridge_vlan_info?
      var rangeEnd: UInt16?
      var infoRem = nla_len(entryAttr)
      var infoPos = nla_data(entryAttr)?.assumingMemoryBound(to: nlattr.self)
      while nla_ok(infoPos, infoRem) != 0 {
        defer { infoPos = nla_next(infoPos, &infoRem) }
        guard let infoAttr = infoPos else { continue }
        switch nla_type(infoAttr) {
        case CInt(BRIDGE_VLANDB_ENTRY_INFO):
          guard let data = nla_data(infoAttr),
                Int(nla_len(infoAttr)) >= MemoryLayout<bridge_vlan_info>.size
          else { continue }
          info = data.assumingMemoryBound(to: bridge_vlan_info.self).pointee
        case CInt(BRIDGE_VLANDB_ENTRY_RANGE):
          guard let data = nla_data(infoAttr),
                Int(nla_len(infoAttr)) >= MemoryLayout<UInt16>.size
          else { continue }
          rangeEnd = data.assumingMemoryBound(to: UInt16.self).pointee
        default:
          break
        }
      }

      guard let info else { continue }
      let last = rangeEnd ?? info.vid
      guard last >= info.vid else { continue }
      for vid in info.vid...last {
        entries.append(RTNLVLANDBEntry(vid: vid, flags: info.flags))
      }
    }
    return entries
  }

  public var description: String {
    "RTNLVLANDB(if: \(ifIndex), entries: \(entries.count))"
  }
}

public enum RTNLVLANDBMessage: NLObjectConstructible, Sendable {
  case new(RTNLVLANDB)
  case del(RTNLVLANDB)

  /// Not used — built directly in `NLSocket_CB_VALID` for `RTM_*VLAN`.
  public init(object: NLObject) throws {
    throw NLError.invalidArgument
  }

  public var vlandb: RTNLVLANDB {
    switch self {
    case let .new(v): v
    case let .del(v): v
    }
  }
}

public extension NLSocket {
  func getNeighbors(family: sa_family_t = sa_family_t(AF_UNSPEC)) async throws
    -> AnyAsyncSequence<RTNLNeighbor>
  {
    let message = try NLMessage(socket: self, type: RTM_GETNEIGH, flags: .dump)
    var hdr = ndmsg()
    hdr.ndm_family = UInt8(family)
    try withUnsafeBytes(of: &hdr) {
      try message.append(Array($0))
    }
    return try streamRequest(message: message)
      .map { ($0 as! RTNLNeighborMessage).neighbor }
      .eraseToAnyAsyncSequence()
  }

  func getMDB() async throws -> AnyAsyncSequence<RTNLMDB> {
    let message = try NLMessage(socket: self, type: RTM_GETMDB, flags: .dump)
    var hdr = br_port_msg(family: UInt8(AF_BRIDGE), ifindex: 0)
    try withUnsafeBytes(of: &hdr) {
      try message.append(Array($0))
    }
    return try streamRequest(message: message)
      .map { ($0 as! RTNLMDBMessage).mdb }
      .eraseToAnyAsyncSequence()
  }
}
