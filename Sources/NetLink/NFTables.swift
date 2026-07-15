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

// MARK: - RAII wrappers over the libnftnl objects

// Each type owns its C handle and frees it on deinit, so there is no manual
// free/defer bookkeeping. nftnl_rule_add_expr transfers expr ownership to the
// rule, so NFTExpr relinquishes its handle (suppressing its own free) on add.

public struct NFTExpr: ~Copyable {
  let handle: OpaquePointer

  public init(_ name: String) throws {
    guard let e = nftnl_expr_alloc(name) else { throw Errno.noMemory }
    handle = e
  }

  deinit { nftnl_expr_free(handle) }

  public func setU32(_ attr: UInt16, _ value: UInt32) { nftnl_expr_set_u32(handle, attr, value) }

  public func setData(_ attr: UInt16, _ bytes: UnsafeRawBufferPointer) {
    nftnl_expr_set(handle, attr, bytes.baseAddress, UInt32(bytes.count))
  }

  // hand the expr to a rule, which now owns and will free it
  consuming func release() -> OpaquePointer {
    let h = handle
    discard self
    return h
  }
}

public struct NFTRule: ~Copyable {
  let handle: OpaquePointer

  public init() throws {
    guard let r = nftnl_rule_alloc() else { throw Errno.noMemory }
    handle = r
  }

  deinit { nftnl_rule_free(handle) }

  public func setStr(_ attr: UInt16, _ value: String) { nftnl_rule_set_str(handle, attr, value) }
  public func setU32(_ attr: UInt16, _ value: UInt32) { nftnl_rule_set_u32(handle, attr, value) }
  public func add(_ expr: consuming NFTExpr) { nftnl_rule_add_expr(handle, expr.release()) }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    nftnl_rule_nlmsg_build_payload(nlh, handle)
  }
}

public struct NFTTable: ~Copyable {
  let handle: OpaquePointer

  public init() throws {
    guard let t = nftnl_table_alloc() else { throw Errno.noMemory }
    handle = t
  }

  deinit { nftnl_table_free(handle) }

  public func setStr(_ attr: UInt16, _ value: String) { nftnl_table_set_str(handle, attr, value) }
  public func setU32(_ attr: UInt16, _ value: UInt32) { nftnl_table_set_u32(handle, attr, value) }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    nftnl_table_nlmsg_build_payload(nlh, handle)
  }
}

public struct NFTChain: ~Copyable {
  let handle: OpaquePointer

  public init() throws {
    guard let c = nftnl_chain_alloc() else { throw Errno.noMemory }
    handle = c
  }

  deinit { nftnl_chain_free(handle) }

  public func setStr(_ attr: UInt16, _ value: String) { nftnl_chain_set_str(handle, attr, value) }
  public func setU32(_ attr: UInt16, _ value: UInt32) { nftnl_chain_set_u32(handle, attr, value) }
  public func setS32(_ attr: UInt16, _ value: Int32) { nftnl_chain_set_s32(handle, attr, value) }

  func buildPayload(_ nlh: UnsafeMutablePointer<nlmsghdr>) {
    nftnl_chain_nlmsg_build_payload(nlh, handle)
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

  fileprivate init(nextSeq: @escaping () -> UInt32) {
    _nextSeq = nextSeq
    _buf = UnsafeMutableRawBufferPointer.allocate(
      byteCount: 2 * mnlBufferSize, alignment: MemoryLayout<UInt>.alignment
    )
    _batch = mnl_nlmsg_batch_start(_buf.baseAddress, _buf.count)
    nftnl_batch_begin(_current, _seq())
    mnl_nlmsg_batch_next(_batch)
  }

  private var _current: UnsafeMutablePointer<CChar> {
    mnl_nlmsg_batch_current(_batch)!.assumingMemoryBound(to: CChar.self)
  }

  private func _seq() -> UInt32 {
    let seq = _nextSeq()
    sequences.append(seq)
    return seq
  }

  private func _hdr(_ type: UInt16, _ flags: UInt16) -> UnsafeMutablePointer<nlmsghdr> {
    nftnl_nlmsg_build_hdr(_current, type, u16(NFPROTO_BRIDGE), flags | u16(NLM_F_ACK), _seq())!
  }

  public func newTable(_ table: borrowing NFTTable) {
    table.buildPayload(_hdr(u16(NFT_MSG_NEWTABLE), u16(NLM_F_CREATE)))
    mnl_nlmsg_batch_next(_batch)
  }

  public func newChain(_ chain: borrowing NFTChain) {
    chain.buildPayload(_hdr(u16(NFT_MSG_NEWCHAIN), u16(NLM_F_CREATE)))
    mnl_nlmsg_batch_next(_batch)
  }

  public func newRule(_ rule: borrowing NFTRule) {
    rule.buildPayload(_hdr(u16(NFT_MSG_NEWRULE), u16(NLM_F_CREATE | NLM_F_APPEND)))
    mnl_nlmsg_batch_next(_batch)
  }

  public func deleteTable(_ table: borrowing NFTTable) {
    table.buildPayload(_hdr(u16(NFT_MSG_DELTABLE), 0))
    mnl_nlmsg_batch_next(_batch)
  }

  fileprivate func finish() {
    nftnl_batch_end(_current, _seq())
    mnl_nlmsg_batch_next(_batch)
  }

  fileprivate var head: UnsafeMutableRawPointer { mnl_nlmsg_batch_head(_batch) }
  fileprivate var size: Int { mnl_nlmsg_batch_size(_batch) }
  fileprivate func dispose() {
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
    precondition(destinationMAC.count == 6)

    let rule = try NFTRule()
    rule.setStr(u16(NFTNL_RULE_TABLE), _table)
    rule.setStr(u16(NFTNL_RULE_CHAIN), _chain)
    rule.setU32(u16(NFTNL_RULE_FAMILY), u32(NFPROTO_BRIDGE))

    // meta bri iifname => reg1 ; cmp reg1 == bridge
    let meta = try NFTExpr("meta")
    meta.setU32(u16(NFTNL_EXPR_META_KEY), u32(NFT_META_BRI_IIFNAME))
    meta.setU32(u16(NFTNL_EXPR_META_DREG), u32(NFT_REG_1))
    rule.add(meta)

    let iifCmp = try NFTExpr("cmp")
    iifCmp.setU32(u16(NFTNL_EXPR_CMP_SREG), u32(NFT_REG_1))
    iifCmp.setU32(u16(NFTNL_EXPR_CMP_OP), u32(NFT_CMP_EQ))
    bridge.withCString {
      iifCmp.setData(
        u16(NFTNL_EXPR_CMP_DATA),
        UnsafeRawBufferPointer(start: $0, count: bridge.utf8.count + 1)
      )
    }
    rule.add(iifCmp)

    // ether daddr (link-layer header, offset 0, 6 bytes) => reg1 ; cmp reg1 == mac
    let payload = try NFTExpr("payload")
    payload.setU32(u16(NFTNL_EXPR_PAYLOAD_BASE), u32(NFT_PAYLOAD_LL_HEADER))
    payload.setU32(u16(NFTNL_EXPR_PAYLOAD_OFFSET), 0)
    payload.setU32(u16(NFTNL_EXPR_PAYLOAD_LEN), 6)
    payload.setU32(u16(NFTNL_EXPR_PAYLOAD_DREG), u32(NFT_REG_1))
    rule.add(payload)

    let macCmp = try NFTExpr("cmp")
    macCmp.setU32(u16(NFTNL_EXPR_CMP_SREG), u32(NFT_REG_1))
    macCmp.setU32(u16(NFTNL_EXPR_CMP_OP), u32(NFT_CMP_EQ))
    destinationMAC.withUnsafeBytes { macCmp.setData(u16(NFTNL_EXPR_CMP_DATA), $0) }
    rule.add(macCmp)

    // immediate verdict: drop
    let verdict = try NFTExpr("immediate")
    verdict.setU32(u16(NFTNL_EXPR_IMM_DREG), u32(NFT_REG_VERDICT))
    verdict.setU32(u16(NFTNL_EXPR_IMM_VERDICT), u32(NF_DROP))
    rule.add(verdict)

    try await _socket.commit { $0.newRule(rule) }
  }

  private func _createTableAndChain() async throws {
    let table = try NFTTable()
    table.setStr(u16(NFTNL_TABLE_NAME), _table)
    table.setU32(u16(NFTNL_TABLE_FAMILY), u32(NFPROTO_BRIDGE))
    table.setU32(u16(NFTNL_TABLE_FLAGS), u32(NFT_TABLE_F_OWNER))

    let chain = try NFTChain()
    chain.setStr(u16(NFTNL_CHAIN_TABLE), _table)
    chain.setStr(u16(NFTNL_CHAIN_NAME), _chain)
    chain.setStr(u16(NFTNL_CHAIN_TYPE), "filter")
    chain.setU32(u16(NFTNL_CHAIN_HOOKNUM), u32(NF_BR_PRE_ROUTING))
    // bridge prerouting at the "dstnat" priority, matching the previous static rule
    chain.setS32(u16(NFTNL_CHAIN_PRIO), s32(NF_BR_PRI_NAT_DST_BRIDGED))
    chain.setU32(u16(NFTNL_CHAIN_POLICY), u32(NF_ACCEPT))

    try await _socket.commit {
      $0.newTable(table)
      $0.newChain(chain)
    }
  }
}

// libnftnl/uapi constants import inconsistently as Swift enums (with .rawValue)
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
