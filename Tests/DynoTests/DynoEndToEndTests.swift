//
//  DynoEndToEndTests.swift
//  
//
//  Created by RedPanda on 30-Oct-19.
//

import Foundation
import XCTest
import Foundation
import Combine
import StrictlySwiftLib
@testable import Dyno

// note , many tests use the region in ~/.aws/config.   But for those who use the hard-coded region
// this must be set to the database region used.
let TEST_REGION = "us-east-2"

final class DynoEndToEndTests : XCTestCase {
    override func setUp() {
        
    }
    

    
    @available(OSX 15.0, *)
    func testDynoAWSHTTPRequest() {
        let signer = AWSSignatureGenerator(log: true)
        
        let awsHttpRequest = AWSHTTPRequest(
            region:TEST_REGION,
            action: MockAction(),
            signer: signer!,
            log: true)
        
        let publisher = awsHttpRequest.request(forSession: URLSession.shared)
        
        let result = publisher.toBlockingResult(timeout: 5)
        if let success = result.asSuccess(), let returned = String(data:success[0], encoding: .utf8) {
            XCTAssert( returned.contains("Tyrannosaurus"), "Couldn't find Tyrannosaurus in scanned data!")
        } else {
            XCTFail("Failed to retrieve data from scan: \(result)")
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoScanAllOneAtATime() {
        let dynoConnection = DynoHttpConnection(credentialPath: nil, region: TEST_REGION, log: true)
        let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 1, log: true, dummyUrl: false)
        
        let scan = DynoScan(table: "Dinosaurs",
                              options: options,
                              filter: nil,
                              lastEvaluatedKey: nil)
        
        let result = scan
            .sendRequest(forConnection: dynoConnection!, type:Mockosaur.self)
            .toBlockingResult(timeout: 5)
            .map {$0.joined()}
        
        if let success = result.asSuccess() {
            NSLog("\(Array(success))")
            XCTAssertEqual(success.count, 4)
            XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])

        } else {
            XCTFail("Failed: \(result)")
        }
    }

    @available(OSX 15.0, *)
     func testDynoScanAllThreeAtATime() {
         let dynoConnection = DynoHttpConnection(credentialPath: nil, region: TEST_REGION, log: true)
         let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 3, log: true, dummyUrl: false)
         
         let action = DynoScan(table: "Dinosaurs",
                               options: options,
                               filter: nil,
                               lastEvaluatedKey: nil)
         
         let result = action
             .sendRequest(forConnection: dynoConnection!, type:Mockosaur.self)
             .toBlockingResult(timeout: 5)
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 4)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
    
     @available(OSX 15.0, *)
     func testDynoScanThreeAtATimeWithTeethFilter() {
         /*
          {'body': '{"FilterExpression": "#n0 > :v0", "TableName": "Dinosaurs", "ExpressionAttributeValues": {":v0": {"N": "40"}}, "ExpressionAttributeNames": {"#n0": "teeth"}}'
          */
         let dynoConnection = DynoHttpConnection(credentialPath: nil, region: TEST_REGION, log: true)
         let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 3, log: true, dummyUrl: false)
         
         let action = DynoScan(table: "Dinosaurs",
                               options: options,
                               filter: DynoScanFilter.compare("teeth", .gt, 40),
                               lastEvaluatedKey: nil)
         
         let result = action
             .sendRequest(forConnection: dynoConnection!, type:Mockosaur.self)
             .toBlockingResult(timeout: 5)
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 2)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus","Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanOneAtATimeWithInFilter() {
         let dyno = Dyno(options: DynoOptions(pageSize:1, log: true))!
         
         let resultPreJoin = dyno.scan(table: "Dinosaurs",
                                 filter: .in("name",["Pinkisaur", "Dottisaur"]),
                                 type: Mockosaur.self)
             .toBlockingResult(timeout: 5)
         
         XCTAssertEqual(resultPreJoin.asSuccess()?.count, 5)   // 5 as there's always an empty one at the end
         let result = resultPreJoin
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 2)
             XCTAssertEqual(success.map {$0.name}, ["Pinkisaur","Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllWithBetweenFilter() {
         let dyno = Dyno(options: DynoOptions(log: true))!
         
         let resultPreJoin = dyno.scan(table: "Dinosaurs",
                             filter: .betweenValue(of:"teeth", from: 100, to: 200),
                             type: Mockosaur.self)
             .toBlockingResult(timeout: 5)
             
         XCTAssertEqual(resultPreJoin.asSuccess()?.count, 1)
         
         let result = resultPreJoin
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 1)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllWithAttributesFilter() {
         let dyno = Dyno(options: DynoOptions(log: true))!
         
         let result = dyno.scan(table: "Dinosaurs",
                             filter: .attributeExists("teeth"),
                             type: Mockosaur.self)
         .toBlockingResult(timeout: 5)
         .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 4)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllWithNoAttributesFilter() {
         let dyno = Dyno(options: DynoOptions(log: true))!
         
         let result = dyno.scan(table: "Dinosaurs",
                             filter: .attributeNotExists("Dinosaurs.size"),
                             type: Mockosaur.self)
         .toBlockingResult(timeout: 5)
         .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 4)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])
         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     
     @available(OSX 15.0, *)
     func testDynoScanAllComplexFilter() {
         let dyno = Dyno(options: DynoOptions(log: true))!
         
         let result = dyno.scan(table: "Dinosaurs",
                             filter: .and(.or(.in("name",["Pinkisaur"]), .or(.not(.compare("teeth", .gt, 40)), .contains("colours","blue"))), .contains("name", "saur")),
                             type: Mockosaur.self)
         .toBlockingResult(timeout: 5)
         .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 3)
             XCTAssertEqual(success.map {$0.name}, ["Emojisaurus", "Pinkisaur", "Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     
     @available(OSX 15.0, *)
     func testDynoScanAllAttributeTypeFilter() {
         let result = Dyno(options: DynoOptions(log: true))!
             .scan(table: "Dinosaurs",
                   filter: .attributeType("teeth", "N"),
                   type: Mockosaur.self)
             .toBlockingResult(timeout: 5)
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 4)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllBeginsWithFilter() {
         let result = Dyno(options: DynoOptions(log: true))!
             .scan(table: "Dinosaurs",
                   filter: .beginsWith("name", "Pink"),
                   type: Mockosaur.self)
             .toBlockingResult(timeout: 5)
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 1)
             XCTAssertEqual(success.map {$0.name}, ["Pinkisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllSizeFilter() {
         let result = Dyno(options: DynoOptions(log: true))!
             .scan(table: "Dinosaurs",
                   filter: .compareSize("colours", .gt, 1),
                   type: Mockosaur.self)
             .toBlockingResult(timeout: 5)
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 2)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus", "Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllSizeBetweenFilter() {
         let result = Dyno(options: DynoOptions(log: true))!
             .scan(table: "Dinosaurs",
                   filter: .betweenSize(of: "colours", from: 2, to: 5),
                   type: Mockosaur.self)
             .toBlockingResult(timeout: 5)
             .map {$0.joined()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success))")
             XCTAssertEqual(success.count, 2)
             XCTAssertEqual(success.map {$0.name}, ["Tyrannosaurus", "Dottisaur"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }

}
