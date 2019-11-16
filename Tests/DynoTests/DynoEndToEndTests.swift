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


struct Microsaur : Codable {
    let name: String
    let teeth: Int
}


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
            .map {$0.aggregated()}
        
        if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 4)
            XCTAssertEqual(success.result.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])

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
            .map { $0.aggregated() }
        
        if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 4)
            XCTAssertEqual(success.result.map {$0.name}, ["Tyrannosaurus","Emojisaurus","Pinkisaur","Dottisaur"])
            
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
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 2)
            XCTAssertEqual(success.result.map {$0.name}, ["Tyrannosaurus","Dottisaur"])

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
         
         XCTAssertEqual(resultPreJoin.asSuccess()?.count, 1)
         let result = resultPreJoin
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
            NSLog("\(success)")
            XCTAssertEqual(success.consumedCapacity.TotalConsumedCapacity.CapacityUnits, 5)  // tbh, I am not sure if this is right. Should we aggregate here ..?  It is 5 queries so seems logical,  but nothing in detail
            XCTAssertEqual(success.result.count, 2)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur","Pinkisaur"])

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
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 1)
             XCTAssertEqual(success.result.map {$0.name}, ["Tyrannosaurus"])

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
         .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 4)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus", "Tyrannosaurus"])

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
         .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 4)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus", "Tyrannosaurus"])
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
         .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 3)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus"])

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
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 4)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus", "Tyrannosaurus"])

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
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 1)
             XCTAssertEqual(success.result.map {$0.name}, ["Pinkisaur"])

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
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 2)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Tyrannosaurus"])

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
             .map {$0.aggregated()}
         
         if let success = result.asSuccess() {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 2)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Tyrannosaurus"])

         } else {
             XCTFail("Failed: \(result)")
         }
     }

    @available(OSX 15.0, *)
    func testDynoScanProjectionTryMockosaur() {
        let result = Dyno(options: DynoOptions(log: true))!
            .scan(table: "Dinosaurs",
                  projection: ["name", "teeth"],
                  type: Mockosaur.self)
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        if let failure = result.asFailure() {
            XCTAssert(failure.asDynoError().reason.hasPrefix("DynoError(reason: \"Could not construct Mockosaur"),"Failed, but not because we were unable to construct a Mockosaur")

        } else {
            XCTFail("Failed: Expected to not be able to construct Mockosaur, but somehow we could")
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoScanProjectionTryMicrosaur() {
        let result = Dyno(options: DynoOptions(log: true))!
            .scan(table: "Dinosaurs",
                  filter: .compare("teeth", .gt, 40),
                  projection: ["name", "teeth"],
                  type: Microsaur.self)
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 2)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Tyrannosaurus"])
            
        } else {
            XCTFail("Failed: \(result)")
        }
    }

    @available(OSX 15.0, *)
    func testDynoScanSort() {
        let result = Dyno(options: DynoOptions(log: true))!
            .scan(table: "Dinosaurs",
                  projection: ["name", "teeth"],
                  sortedBy: { (a,b) in a.teeth < b.teeth },
                  type: Microsaur.self)
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.map {$0.name}, ["Emojisaurus", "Pinkisaur", "Dottisaur", "Tyrannosaurus"])
            
        } else {
            XCTFail("Failed: \(result)")
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoScanToTypeDescriptors() {
        let result = Dyno(options: DynoOptions(log: true))!
            .scanToTypeDescriptors(
                  table: "Dinosaurs",
                  projection: ["name", "teeth", "colours"])
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        if let success = result.asSuccess() {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result,
            [
                ["name":.S("Tyrannosaurus"), "teeth":.N("158"), "colours":.L([.S("green"), .S("black")])],
                ["name":.S("Emojisaurus"), "teeth":.N("12"), "colours":.L([.S("aqua")])],
                ["name":.S("Pinkisaur"),"teeth":.N("40"),  "colours":.L([.S("pink")])],
                ["name":.S("Dottisaur"), "teeth":.N("50"), "colours":.L([.S("black"), .S("blue")])]
            ])
            
        } else {
            XCTFail("Failed: \(result)")
        }
    }
}
