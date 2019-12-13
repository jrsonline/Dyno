//
//  DynoEndToEndTestSetup.swift
//  DynoTests
//
//  Created by RedPanda on 12-Dec-19.
//

// Run these tests before those in DynoEndToEndTests!


import Foundation
import XCTest
import Foundation
import Combine
import StrictlySwiftLib
@testable import Dyno

// the table name to use for the tests
let TEST_TABLE = "DinoTest"


class DynoBeforeEndToEndTest: XCTestCase {

    @available(OSX 15.0, *)
    func testRunBeforeEndToEndTests() {
        guard let 🦕 = Dyno(options: DynoOptions(log: true)) else {XCTFail("Couldn't create Dyno properly!"); return }

        // start by deleting any table there...
        let resultD1 = XCTWaitForPublisherResult {
            🦕.deleteTableWaitDeleted(name: TEST_TABLE)
        }
        
        XCTAssertEqual(resultD1, true) // able to delete table successfully
        
        let resultC1 = XCTWaitForPublisherResult(timeout: 10) {
            🦕.createTableWaitActive(name: TEST_TABLE, partitionKeyField: ("id",.string) )
        }
        XCTAssertEqual(resultC1, true) // able to create table successfully
        
        
        // check if we can delete it...
        let resultD2 = XCTWaitForPublisherResult {
            🦕.deleteTableWaitDeleted(name: TEST_TABLE)
        }
        XCTAssertEqual(resultD2, true) // able to delete table successfully

        // And re-instantiate it again!
        let resultC2 = XCTWaitForPublisherResult(timeout: 10) {
            🦕.createTableWaitActive(name: TEST_TABLE, partitionKeyField: ("id",.string) )
        }
        XCTAssertEqual(resultC2, true) // able to create table successfully
        
        
        // Then populate the data!
        
        let dinos = [
            Mockosaur(id: "1", name: "Emojisaurus", colours: ["aqua"], teeth: 12),
            Mockosaur(id: "2", name: "Tyrannosaurus", colours: ["green", "black"], teeth: 158),
            Mockosaur(id: "6", name: "Pinkisaur", colours: ["pink"], teeth: 40),
            Mockosaur(id: "7", name: "Dottisaur", colours: ["black","blue"], teeth: 50)
        ]
        
        
        for dino in dinos {
            let result = XCTWaitForPublisherResult() {
                🦕.put(table: TEST_TABLE, item: dino)
            }
            XCTAssertEqual(result?.result.count, 0 )
        }
    }

}
