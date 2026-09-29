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

@testable import NetLink
import XCTest

final class NFTablesTests: XCTestCase {
  // Reference batches captured from libnftnl 1.2.6 building the same objects,
  // with sequence numbers starting at 1.
  private let tableAndChain =
    "140000001000010001000000000000000000000a28000000000a0504020000000000000007000000090001006d7270640000000008000200000000025800000003" +
    "0a0504030000000000000007000000090001006d727064000000000f000300707265726f7574696e67000014000480080001000000000008000200fffffed40800" +
    "0500000000010b00070066696c7465720000140000001100010004000000000000000000000a"

  private let dropRule =
    "140000001000010005000000000000000000000a18010000060a050c060000000000000007000000090001006d727064000000000f000200707265726f7574696e" +
    "670000e800048024000180090001006d6574610000000014000280080002000000001108000100000000012c00018008000100636d7000200002800800010000" +
    "00000108000200000000000c0003800800010062723000340001800c0001007061796c6f6164002400028008000100000000010800020000000000080003000000" +
    "000008000400000000063000018008000100636d70002400028008000100000000010800020000000000100003800a0001000180c2000021000030000180" +
    "0e000100696d6d6564696174650000001c0002800800010000000000100002800c0002800800010000000000140000001100010007000000000000000000000a"

  private func encode(startingAt sequence: UInt32, _ build: (NFTBatch) -> Void) -> String {
    var next = sequence
    let batch = NFTBatch(nextSeq: {
      defer { next += 1 }
      return next
    })
    defer { batch.dispose() }
    build(batch)
    batch.finish()
    return UnsafeRawBufferPointer(start: batch.head, count: batch.size)
      .map { String(format: "%02x", $0) }.joined()
  }

  func testTableAndChainMatchLibnftnl() {
    let encoded = encode(startingAt: 1) {
      $0.newTable(NLNFTablesDropTable.table("mrpd"))
      $0.newChain(NLNFTablesDropTable.chain("prerouting", table: "mrpd"))
    }
    XCTAssertEqual(encoded, tableAndChain)
  }

  func testDropRuleMatchesLibnftnl() {
    let encoded = encode(startingAt: 5) {
      $0.newRule(NLNFTablesDropTable.dropRule(
        table: "mrpd",
        chain: "prerouting",
        bridge: "br0",
        destinationMAC: [0x01, 0x80, 0xC2, 0x00, 0x00, 0x21]
      ))
    }
    XCTAssertEqual(encoded, dropRule)
  }
}
