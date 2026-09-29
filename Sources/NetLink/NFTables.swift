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

import CNFTables
import Dispatch
import Glibc
import Synchronization
import SystemPackage

// libmnl's MNL_SOCKET_BUFFER_SIZE macro (min(pagesize, 8192)) is not importable.
private let mnlBufferSize = min(sysconf(Int32(_SC_PAGESIZE)), 8192)

// MARK: - nf_tables objects

// Each object encodes itself as the NFTA_* attributes of an nf_tables message,
// in the order libnftnl emits them. Integer attributes are big-endian.

public struct NFTTable: Sendable {
  public var name: String
  public var flags: UInt32?

  public init(name: String, flags: UInt32? = nil) {
    self.name = name
    self.flags = flags
  }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    mnl_attr_put_strz(nlh, u16(NFTA_TABLE_NAME), name)
    if let flags { mnl_attr_put_u32(nlh, u16(NFTA_TABLE_FLAGS), flags.bigEndian) }
  }
}

public struct NFTChain: Sendable {
  public struct Hook: Sendable {
    public var number: UInt32
    public var priority: Int32

    public init(number: UInt32, priority: Int32) {
      self.number = number
      self.priority = priority
    }
  }

  public var table: String
  public var name: String
  public var type: String?
  public var hook: Hook?
  public var policy: UInt32?

  public init(
    table: String,
    name: String,
    type: String? = nil,
    hook: Hook? = nil,
    policy: UInt32? = nil
  ) {
    self.table = table
    self.name = name
    self.type = type
    self.hook = hook
    self.policy = policy
  }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    mnl_attr_put_strz(nlh, u16(NFTA_CHAIN_TABLE), table)
    mnl_attr_put_strz(nlh, u16(NFTA_CHAIN_NAME), name)
    if let hook {
      let nest = mnl_attr_nest_start(nlh, u16(NFTA_CHAIN_HOOK))
      mnl_attr_put_u32(nlh, u16(NFTA_HOOK_HOOKNUM), hook.number.bigEndian)
      mnl_attr_put_u32(nlh, u16(NFTA_HOOK_PRIORITY), UInt32(bitPattern: hook.priority).bigEndian)
      mnl_attr_nest_end(nlh, nest)
    }
    if let policy { mnl_attr_put_u32(nlh, u16(NFTA_CHAIN_POLICY), policy.bigEndian) }
    if let type { mnl_attr_put_strz(nlh, u16(NFTA_CHAIN_TYPE), type) }
  }
}

public enum NFTExpr: Sendable {
  /// load meta key `key` into register `dreg`
  case meta(key: UInt32, dreg: UInt32)
  /// compare register `sreg` against `data` with operator `op`
  case cmp(sreg: UInt32, op: UInt32, data: [UInt8])
  /// load `length` bytes at `offset` from header `base` into register `dreg`
  case payload(base: UInt32, offset: UInt32, length: UInt32, dreg: UInt32)
  /// set the verdict register to `verdict` (e.g. NF_DROP)
  case verdict(Int32)

  private var name: String {
    switch self {
    case .meta: "meta"
    case .cmp: "cmp"
    case .payload: "payload"
    case .verdict: "immediate"
    }
  }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    mnl_attr_put_strz(nlh, u16(NFTA_EXPR_NAME), name)
    let data = mnl_attr_nest_start(nlh, u16(NFTA_EXPR_DATA))
    switch self {
    case let .meta(key, dreg):
      mnl_attr_put_u32(nlh, u16(NFTA_META_KEY), key.bigEndian)
      mnl_attr_put_u32(nlh, u16(NFTA_META_DREG), dreg.bigEndian)
    case let .cmp(sreg, op, value):
      mnl_attr_put_u32(nlh, u16(NFTA_CMP_SREG), sreg.bigEndian)
      mnl_attr_put_u32(nlh, u16(NFTA_CMP_OP), op.bigEndian)
      let nest = mnl_attr_nest_start(nlh, u16(NFTA_CMP_DATA))
      value.withUnsafeBytes { mnl_attr_put(nlh, u16(NFTA_DATA_VALUE), $0.count, $0.baseAddress) }
      mnl_attr_nest_end(nlh, nest)
    case let .payload(base, offset, length, dreg):
      mnl_attr_put_u32(nlh, u16(NFTA_PAYLOAD_DREG), dreg.bigEndian)
      mnl_attr_put_u32(nlh, u16(NFTA_PAYLOAD_BASE), base.bigEndian)
      mnl_attr_put_u32(nlh, u16(NFTA_PAYLOAD_OFFSET), offset.bigEndian)
      mnl_attr_put_u32(nlh, u16(NFTA_PAYLOAD_LEN), length.bigEndian)
    case let .verdict(code):
      mnl_attr_put_u32(nlh, u16(NFTA_IMMEDIATE_DREG), u32(NFT_REG_VERDICT).bigEndian)
      let immediate = mnl_attr_nest_start(nlh, u16(NFTA_IMMEDIATE_DATA))
      let verdict = mnl_attr_nest_start(nlh, u16(NFTA_DATA_VERDICT))
      mnl_attr_put_u32(nlh, u16(NFTA_VERDICT_CODE), UInt32(bitPattern: code).bigEndian)
      mnl_attr_nest_end(nlh, verdict)
      mnl_attr_nest_end(nlh, immediate)
    }
    mnl_attr_nest_end(nlh, data)
  }
}

public struct NFTRule: Sendable {
  public var table: String
  public var chain: String
  public var expressions: [NFTExpr]

  public init(table: String, chain: String, expressions: [NFTExpr] = []) {
    self.table = table
    self.chain = chain
    self.expressions = expressions
  }

  public mutating func add(_ expr: NFTExpr) { expressions.append(expr) }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    mnl_attr_put_strz(nlh, u16(NFTA_RULE_TABLE), table)
    mnl_attr_put_strz(nlh, u16(NFTA_RULE_CHAIN), chain)
    guard !expressions.isEmpty else { return }
    let list = mnl_attr_nest_start(nlh, u16(NFTA_RULE_EXPRESSIONS))
    for expr in expressions {
      let elem = mnl_attr_nest_start(nlh, u16(NFTA_LIST_ELEM))
      expr.buildPayload(nlh)
      mnl_attr_nest_end(nlh, elem)
    }
    mnl_attr_nest_end(nlh, list)
  }
}

// MARK: - Batch writer

/// Accumulates nf_tables objects into a single netlink transaction. Each object
/// requests NLM_F_ACK; the transport awaits the last object's ACK (or an error
/// for any object, which aborts the whole batch).
public final class NFTBatch {
  private let _buf: UnsafeMutableRawBufferPointer
  private let _batch: OpaquePointer
  private let _nextSeq: () -> UInt32
  // every sequence the batch allocates: the kernel may report a batch error
  // against the BATCH_BEGIN sequence, not an object's, so we track them all
  private(set) var sequences: [UInt32] = []

  init(nextSeq: @escaping () -> UInt32) {
    _nextSeq = nextSeq
    _buf = UnsafeMutableRawBufferPointer.allocate(
      byteCount: 2 * mnlBufferSize, alignment: MemoryLayout<UInt>.alignment
    )
    _batch = mnl_nlmsg_batch_start(_buf.baseAddress, _buf.count)
    _batchHdr(u16(NFNL_MSG_BATCH_BEGIN))
  }

  private func _seq() -> UInt32 {
    let seq = _nextSeq()
    sequences.append(seq)
    return seq
  }

  // nlmsghdr + nfgenmsg at the batch's current position
  @discardableResult
  private func _put(
    type: UInt16,
    family: UInt8,
    flags: UInt16,
    resID: UInt16
  ) -> UnsafeMutablePointer<nlmsghdr> {
    let nlh = mnl_nlmsg_put_header(mnl_nlmsg_batch_current(_batch))!
    nlh.pointee.nlmsg_type = type
    nlh.pointee.nlmsg_flags = u16(NLM_F_REQUEST) | flags
    nlh.pointee.nlmsg_seq = _seq()
    let nfh = mnl_nlmsg_put_extra_header(nlh, MemoryLayout<nfgenmsg>.size)!
      .assumingMemoryBound(to: nfgenmsg.self)
    nfh.pointee.nfgen_family = family
    nfh.pointee.version = UInt8(NFNETLINK_V0)
    nfh.pointee.res_id = resID.bigEndian
    return nlh
  }

  private func _batchHdr(_ type: UInt16) {
    _put(type: type, family: UInt8(NFPROTO_UNSPEC), flags: 0, resID: u16(NFNL_SUBSYS_NFTABLES))
    mnl_nlmsg_batch_next(_batch)
  }

  private func _hdr(_ type: UInt16, _ flags: UInt16) -> UnsafeMutablePointer<nlmsghdr> {
    _put(
      type: u16(NFNL_SUBSYS_NFTABLES) << 8 | type,
      family: UInt8(NFPROTO_BRIDGE),
      flags: flags | u16(NLM_F_ACK),
      resID: 0
    )
  }

  public func newTable(_ table: NFTTable) {
    table.buildPayload(_hdr(u16(NFT_MSG_NEWTABLE), u16(NLM_F_CREATE)))
    mnl_nlmsg_batch_next(_batch)
  }

  public func newChain(_ chain: NFTChain) {
    chain.buildPayload(_hdr(u16(NFT_MSG_NEWCHAIN), u16(NLM_F_CREATE)))
    mnl_nlmsg_batch_next(_batch)
  }

  public func newRule(_ rule: NFTRule) {
    rule.buildPayload(_hdr(u16(NFT_MSG_NEWRULE), u16(NLM_F_CREATE | NLM_F_APPEND)))
    mnl_nlmsg_batch_next(_batch)
  }

  public func deleteTable(_ table: NFTTable) {
    table.buildPayload(_hdr(u16(NFT_MSG_DELTABLE), 0))
    mnl_nlmsg_batch_next(_batch)
  }

  func finish() {
    _batchHdr(u16(NFNL_MSG_BATCH_END))
  }

  var head: UnsafeMutableRawPointer { mnl_nlmsg_batch_head(_batch) }
  var size: Int { mnl_nlmsg_batch_size(_batch) }
  func dispose() {
    mnl_nlmsg_batch_stop(_batch)
    _buf.deallocate()
  }
}

// MARK: - libmnl socket

/// RAII wrapper over a libmnl `mnl_socket`, closed on deinit. Presents the
/// synchronous send/receive primitives; the async layer is built on top.
public final class MNLSocket: @unchecked Sendable {
  let handle: OpaquePointer

  public init(bus: Int32) throws {
    guard let sk = mnl_socket_open(bus) else { throw Errno(rawValue: errno) }
    handle = sk
  }

  deinit { mnl_socket_close(handle) }

  public func bind(groups: UInt32 = 0, pid: pid_t = 0) throws {
    guard mnl_socket_bind(handle, groups, pid) >= 0 else { throw Errno(rawValue: errno) }
  }

  public var fileDescriptor: Int32 { mnl_socket_get_fd(handle) }
  public var portID: UInt32 { mnl_socket_get_portid(handle) }

  public func setNonBlocking() {
    let fd = fileDescriptor
    _ = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK)
  }

  public func send(_ buffer: UnsafeRawBufferPointer) throws {
    guard mnl_socket_sendto(handle, buffer.baseAddress, buffer.count) >= 0 else {
      throw Errno(rawValue: errno)
    }
  }

  /// Receive into `buffer`; returns the byte count, or 0 when the socket would
  /// block (non-blocking) or on error.
  public func receive(into buffer: inout [UInt8]) -> Int {
    let n = buffer.withUnsafeMutableBytes { mnl_socket_recvfrom(handle, $0.baseAddress, $0.count) }
    return n > 0 ? n : 0
  }
}

// MARK: - Async nf_tables transport

/// A non-blocking NETLINK_NETFILTER socket presenting an async API: it sends an
/// nf_tables batch and awaits its ACK, wrapping the socket-readable callback in
/// a continuation (the NetLinkSwift idiom).
public final class NFNLSocket: @unchecked Sendable {
  private final class _AckRequest: @unchecked Sendable {
    let continuation: CheckedContinuation<Void, Error>
    let sequences: [UInt32]
    init(_ continuation: CheckedContinuation<Void, Error>, _ sequences: [UInt32]) {
      self.continuation = continuation
      self.sequences = sequences
    }
  }

  private let _socket: MNLSocket
  private let _queue = DispatchQueue(label: "NFNLSocket")
  private let _readSource: any DispatchSourceRead
  private let _sequence = Mutex<UInt32>(1)
  private let _requests = Mutex<[UInt32: _AckRequest]>([:])

  public init() throws {
    let socket = try MNLSocket(bus: Int32(NETLINK_NETFILTER))
    try socket.bind()
    socket.setNonBlocking()
    _socket = socket

    _readSource = DispatchSource.makeReadSource(
      fileDescriptor: socket.fileDescriptor, queue: _queue
    )
    _readSource.setEventHandler { [weak self] in self?._onReadable() }
    _readSource.resume()
  }

  deinit {
    _readSource.cancel()
    // fail any still-pending requests (each request may appear under several
    // sequence keys; resume its continuation only once)
    _requests.withLock { requests in
      var resumed = Set<ObjectIdentifier>()
      for request in requests.values where resumed.insert(ObjectIdentifier(request)).inserted {
        request.continuation.resume(throwing: Errno(rawValue: ECANCELED))
      }
      requests.removeAll()
    }
  }

  private func _nextSequence() -> UInt32 {
    _sequence.withLock { sequence in
      let value = sequence
      sequence = sequence == UInt32.max ? 1 : sequence + 1
      return value
    }
  }

  /// Assemble a batch via `build`, send it, and await the kernel's ACK.
  public func commit(_ build: (NFTBatch) throws -> Void) async throws {
    let batch = NFTBatch(nextSeq: { [self] in _nextSequence() })
    defer { batch.dispose() }
    try build(batch)
    batch.finish()

    let sequences = batch.sequences
    guard !sequences.isEmpty else { return }

    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let request = _AckRequest(continuation, sequences)
        _requests.withLock { requests in
          for sequence in sequences { requests[sequence] = request }
        }
        do {
          try _socket.send(UnsafeRawBufferPointer(start: batch.head, count: batch.size))
        } catch {
          _resolve(sequences[0], .failure(error))
        }
      }
    } onCancel: {
      _resolve(sequences[0], .failure(Errno(rawValue: ECANCELED)))
    }
  }

  private func _resolve(_ sequence: UInt32, _ result: Result<Void, Error>) {
    var request: _AckRequest?
    _requests.withLock { requests in
      guard let found = requests[sequence] else { return }
      for sequence in found.sequences { requests[sequence] = nil }
      request = found
    }
    request?.continuation.resume(with: result)
  }

  private func _onReadable() {
    var buffer = [UInt8](repeating: 0, count: mnlBufferSize)
    while true {
      let n = _socket.receive(into: &buffer)
      guard n > 0 else { break }
      buffer.withUnsafeBytes { _process($0, count: n) }
    }
  }

  // Walk each nlmsghdr in the response; NLMSG_ERROR with error 0 is an ACK,
  // otherwise it carries -errno. Resolve the request holding that sequence.
  private func _process(_ raw: UnsafeRawBufferPointer, count: Int) {
    guard let base = raw.baseAddress else { return }
    var remaining = Int32(truncatingIfNeeded: count)
    var next: UnsafePointer<nlmsghdr>? = base.assumingMemoryBound(to: nlmsghdr.self)
    while let nlh = next, mnl_nlmsg_ok(nlh, remaining) {
      let sequence = nlh.pointee.nlmsg_seq
      switch nlh.pointee.nlmsg_type {
      case UInt16(NLMSG_ERROR):
        let error = mnl_nlmsg_get_payload(nlh).assumingMemoryBound(to: nlmsgerr.self).pointee.error
        _resolve(sequence, error == 0 ? .success(()) : .failure(Errno(rawValue: -error)))
      case UInt16(NLMSG_DONE):
        _resolve(sequence, .success(()))
      default:
        break
      }
      next = UnsafePointer(mnl_nlmsg_next(nlh, &remaining))
    }
  }
}

// MARK: - Drop table

/// A socket-scoped nf_tables bridge table whose prerouting chain drops frames by
/// destination MAC, so the bridge does not flood them. The table carries
/// NFT_TABLE_F_OWNER: the kernel removes it when this object's socket is
/// released, so a crash cannot leave a stale rule behind. The default table name
/// is generic; a caller should override it to something it owns.
public final class NLNFTablesDropTable: Sendable {
  private let _socket: NFNLSocket
  private let _table: String
  private let _chain: String

  public init(table: String = "filter", chain: String = "prerouting") async throws {
    _socket = try NFNLSocket()
    _table = table
    _chain = chain
    try await _createTableAndChain()
  }

  /// Drop frames received on bridge `bridge` whose Ethernet destination equals
  /// `destinationMAC` (6 bytes).
  public func addDrop(bridge: String, destinationMAC: [UInt8]) async throws {
    let rule = Self.dropRule(
      table: _table,
      chain: _chain,
      bridge: bridge,
      destinationMAC: destinationMAC
    )
    try await _socket.commit { $0.newRule(rule) }
  }

  private func _createTableAndChain() async throws {
    let table = Self.table(_table)
    let chain = Self.chain(_chain, table: _table)
    try await _socket.commit {
      $0.newTable(table)
      $0.newChain(chain)
    }
  }

  static func table(_ name: String) -> NFTTable {
    NFTTable(name: name, flags: u32(NFT_TABLE_F_OWNER))
  }

  static func chain(_ name: String, table: String) -> NFTChain {
    NFTChain(
      table: table,
      name: name,
      type: "filter",
      // bridge prerouting at the "dstnat" priority, matching the previous static rule
      hook: .init(number: u32(NF_BR_PRE_ROUTING), priority: s32(NF_BR_PRI_NAT_DST_BRIDGED)),
      policy: u32(NF_ACCEPT)
    )
  }

  static func dropRule(
    table: String,
    chain: String,
    bridge: String,
    destinationMAC: [UInt8]
  ) -> NFTRule {
    precondition(destinationMAC.count == 6)

    return NFTRule(table: table, chain: chain, expressions: [
      // meta bri iifname => reg1 ; cmp reg1 == bridge
      .meta(key: u32(NFT_META_BRI_IIFNAME), dreg: u32(NFT_REG_1)),
      .cmp(sreg: u32(NFT_REG_1), op: u32(NFT_CMP_EQ), data: Array(bridge.utf8) + [0]),
      // ether daddr (link-layer header, offset 0, 6 bytes) => reg1 ; cmp reg1 == mac
      .payload(base: u32(NFT_PAYLOAD_LL_HEADER), offset: 0, length: 6, dreg: u32(NFT_REG_1)),
      .cmp(sreg: u32(NFT_REG_1), op: u32(NFT_CMP_EQ), data: destinationMAC),
      // immediate verdict: drop
      .verdict(s32(NF_DROP)),
    ])
  }
}

// uapi constants import inconsistently as Swift enums (with .rawValue)
// or as plain integers; these normalise either form to the C argument type.
private func u16<E: RawRepresentable>(_ v: E) -> UInt16 where E.RawValue: FixedWidthInteger {
  UInt16(truncatingIfNeeded: v.rawValue)
}

private func u16(_ v: some FixedWidthInteger) -> UInt16 { UInt16(truncatingIfNeeded: v) }

private func u32<E: RawRepresentable>(_ v: E) -> UInt32 where E.RawValue: FixedWidthInteger {
  UInt32(truncatingIfNeeded: v.rawValue)
}

private func u32(_ v: some FixedWidthInteger) -> UInt32 { UInt32(truncatingIfNeeded: v) }

private func s32<E: RawRepresentable>(_ v: E) -> Int32 where E.RawValue: FixedWidthInteger {
  Int32(truncatingIfNeeded: v.rawValue)
}

private func s32(_ v: some FixedWidthInteger) -> Int32 { Int32(truncatingIfNeeded: v) }
