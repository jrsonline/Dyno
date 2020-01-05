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
import StrictlySwiftTestLib
@testable import Dyno

/* ****************************************************************************
 *      T E S T    S E T U P    C O N F I G U R A T I O N
 *
 */

// note , many tests use the region in ~/.aws/config.   But for those who use the hard-coded region
// this must be set to the database region used.
let TEST_REGION = "us-east-2"

// Table to use for testing
let TEST_TABLE = "DinoTest"

// set to false to avoid recreating the test tables.  You need it 'true' the first time you run.
var RUN_TABLE_SETUP = true

// default timeout for connections. Set to longer for slower connections.
let CONNECTION_TIMEOUT = 5.0


/* ****************************************************************************/



/* Python test setup
 >>> import boto3
 >>> dynamodb = boto3.resource('dynamodb')
 >>> from boto3.dynamodb.conditions import Key, Attr
 >>> boto3.set_stream_logger('botocore')
 >>> table = dynamodb.Table('Dinosaurs')
 
 */

struct MockAction : AWSAction {
    func actionName() -> String {
        return "DynamoDB_20120810.Scan"
    }
    
    func body() -> String {
        return #"""
        {"TableName": "\#(TEST_TABLE)"}
        """#
    }
    
    func decodeResponse(data: Data) -> JSONDecoder {
        return JSONDecoder()
    }
    
    func httpMethod() -> AWSHTTPVerb {
        return .POST
    }
    
    func headers() -> [String : String] {
        return [:]
    }
    
    func service() -> AWSService {
        return .dynamodb
    }
    
    func servicePath() -> String {
        return "/"
    }
    
    func queryParameters() -> [String : String] {
        return [:]
    }
    
}



struct Microsaur : Codable, Equatable {
    let name: String
    let teeth: Int
}

@available(OSX 15.0, *)
final class DynoEndToEndTests : XCTestCase {
    
    let tableSetupQueue = DispatchQueue(label:"TableSetup")
        
    override func setUp() {
        // ensure this piece runs single-threaded, as there may be several tests running in parallel
        tableSetupQueue.sync {
            guard RUN_TABLE_SETUP else { return }

            guard let 🦕 = Dyno(options: DynoOptions(log: true)) else {XCTFail("Couldn't create Dyno properly!"); return }
            
            // start by deleting any table there...
            let resultD1 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT ) {
                🦕.deleteTableWaitDeleted(name: TEST_TABLE)
            }
            
            XCTAssertEqual(resultD1, true) // able to delete table successfully
            
            let resultC1 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT*2 ) {
                🦕.createTableWaitActive(name: TEST_TABLE, partitionKeyField: ("id",.string) )
            }
            XCTAssertEqual(resultC1, true) // able to create table successfully
            
            
            // check if we can delete it...
            let resultD2 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT ) {
                🦕.deleteTableWaitDeleted(name: TEST_TABLE)
            }
            XCTAssertEqual(resultD2, true) // able to delete table successfully
            
            // And re-instantiate it again!
            let resultC2 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT*2) {
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
                let _ = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
                    🦕.put(table: TEST_TABLE, item: dino)
                }
            }
            
            RUN_TABLE_SETUP = false
        }
    }
    
    func testDynoAWSHTTPRequest() {
        let signer = AWSSignatureGenerator(log: true)
        
        let awsHttpRequest = AWSHTTPRequest(
            region:TEST_REGION,
            action: MockAction(),
            signer: signer!,
            log: true)
        
        let result = XCTWaitForPublisherResult {
            awsHttpRequest.request(forSession: URLSession.shared)
        }
        
        if let success = result, let returned = String(data:success, encoding: .utf8) {
            XCTAssert( returned.contains("Tyrannosaurus"), "Couldn't find Tyrannosaurus in scanned data! Do you need to run the test with RUN_TABLE_SETUP set to true?")
        } else {
            XCTFail("Failed to retrieve data from scan")
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoScanAllOneAtATime() {
        let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 1, log: true, dummyUrl: false)

        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: options)!
                .scan(table: TEST_TABLE,
                type: Mockosaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 4)
            XCTAssertEqual(success.result.map {$0.name}.sorted(), ["Dottisaur","Emojisaurus","Pinkisaur","Tyrannosaurus"])

        }
    }

    @available(OSX 15.0, *)
    func testDynoScanAllThreeAtATime() {
        let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 3, log: true, dummyUrl: false)

        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: options)!
                .scan(table: TEST_TABLE,
                      type: Mockosaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 4)
            XCTAssertEqual(success.result.map {$0.name}.sorted(), ["Dottisaur","Emojisaurus","Pinkisaur","Tyrannosaurus"])
            
        }
    }
    
     @available(OSX 15.0, *)
     func testDynoScanThreeAtATimeWithTeethFilter() {
         /*
          {'body': '{"FilterExpression": "#n0 > :v0", "TableName": TEST_TABLE, "ExpressionAttributeValues": {":v0": {"N": "40"}}, "ExpressionAttributeNames": {"#n0": "teeth"}}'
          */
         let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 3, log: true, dummyUrl: false)

        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: options)!
            .scan(table: TEST_TABLE,
                  filter: DynoCondition.compare("teeth", .gt, 40),
                  type: Mockosaur.self)
        }
         
         if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 2)
            XCTAssertEqual(success.result.map {$0.name}.sorted(), ["Dottisaur","Tyrannosaurus"])
         }
     }
     
     @available(OSX 15.0, *)
    func testDynoScanOneAtATimeWithInFilter() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(pageSize:1, log: true))!
                .scan(table: TEST_TABLE,
                      filter: .in("name",["Pinkisaur", "Dottisaur"]),
                      type: Mockosaur.self)
        }
        
        if let success = result {
            NSLog("\(success)")
            XCTAssertEqual(success.consumedCapacity.TotalConsumedCapacity.CapacityUnits, 5)  // tbh, I am not sure if this is right. Should we aggregate here ..?  It is 5 queries so seems logical,  but nothing in detail
            XCTAssertEqual(success.result.count, 2)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur","Pinkisaur"])
            
        }
    }
     
    @available(OSX 15.0, *)
    func testDynoScanAllWithBetweenFilter() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
                .scan(table: TEST_TABLE,
                      filter: .betweenValue(of:"teeth", from: 100, to: 200),
                      type: Mockosaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 1)
            XCTAssertEqual(success.result.map {$0.name}, ["Tyrannosaurus"])
            
        }
    }
     
    @available(OSX 15.0, *)
    func testDynoScanAllWithAttributesFilter() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
                .scan(table: TEST_TABLE,
                      filter: .attributeExists("teeth"),
                      type: Mockosaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 4)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus", "Tyrannosaurus"])
            
        }
    }
     
    @available(OSX 15.0, *)
    func testDynoScanAllWithNoAttributesFilter() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
                .scan(table: TEST_TABLE,
                      filter: .attributeNotExists("Dinosaurs.size"),
                      type: Mockosaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 4)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus", "Tyrannosaurus"])
        }
    }
     
     
     @available(OSX 15.0, *)
    func testDynoScanAllComplexFilter() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
                .scan(table: TEST_TABLE,
                      filter: .and(.or(.in("name",["Pinkisaur"]), .or(.not(.compare("teeth", .gt, 40)), .contains("colours","blue"))), .contains("name", "saur")),
                      type: Mockosaur.self)
        }
        
        if let success = result{
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 3)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus"])
            
        }
    }
     
     @available(OSX 15.0, *)
     func testDynoScanAllAttributeTypeFilter() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
             .scan(table: TEST_TABLE,
                   filter: .attributeType("teeth", "N"),
                   type: Mockosaur.self)
         }
         
         if let success = result {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 4)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Pinkisaur", "Emojisaurus", "Tyrannosaurus"])
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllBeginsWithFilter() {
         let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
             .scan(table: TEST_TABLE,
                   filter: .beginsWith("name", "Pink"),
                   type: Mockosaur.self)
         }
         
         if let success = result {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 1)
             XCTAssertEqual(success.result.map {$0.name}, ["Pinkisaur"])
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllSizeFilter() {
         let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
             .scan(table: TEST_TABLE,
                   filter: .compareSize("colours", .gt, 1),
                   type: Mockosaur.self)
         }
         
         if let success = result {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 2)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Tyrannosaurus"])
         }
     }
     
     @available(OSX 15.0, *)
     func testDynoScanAllSizeBetweenFilter() {
         let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
             .scan(table: TEST_TABLE,
                   filter: .betweenSize(of: "colours", from: 2, to: 5),
                   type: Mockosaur.self)
         }
         
         if let success = result {
             NSLog("\(Array(success.result))")
             XCTAssertEqual(success.result.count, 2)
             XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Tyrannosaurus"])
         }
     }

    @available(OSX 15.0, *)
    func testDynoScanProjectionTryMockosaur() {
        let failure = XCTWaitForPublisherFailure(unexpectedSuccessMessage: "Somehow we could construct a Mockasaur",timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .scan(table: TEST_TABLE,
                  projection: ["name", "teeth"],
                  type: Mockosaur.self)
        }
        
        if let dynoFailure = failure as? DynoError {
            XCTAssert(dynoFailure.reason.hasPrefix("Could not construct Mockosaur"),"Failed, but not because we were unable to construct a Mockosaur")

        } else {
            XCTFail("Failed, but not because we were unable to construct a Mockosaur")
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoScanProjectionTryMicrosaur() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .scan(table: TEST_TABLE,
                  filter: .compare("teeth", .gt, 40),
                  projection: ["name", "teeth"],
                  type: Microsaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.count, 2)
            XCTAssertEqual(success.result.map {$0.name}, ["Dottisaur", "Tyrannosaurus"])
        }
    }

    @available(OSX 15.0, *)
    func testDynoScanSort() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .scan(table: TEST_TABLE,
                  projection: ["name", "teeth"],
                  sortedBy: { (a,b) in a.teeth < b.teeth },
                  type: Microsaur.self)
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result.map {$0.name}, ["Emojisaurus", "Pinkisaur", "Dottisaur", "Tyrannosaurus"])
            
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoScanToTypeDescriptors() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .scanToTypeDescriptors(
                  table: TEST_TABLE,
                  projection: ["name", "teeth", "colours"])
        }
        
        if let success = result {
            NSLog("\(Array(success.result))")
            XCTAssertEqual(success.result,
            [
                ["name":.S("Tyrannosaurus"), "teeth":.N("158"), "colours":.L([.S("green"), .S("black")])],
                ["name":.S("Emojisaurus"), "teeth":.N("12"), "colours":.L([.S("aqua")])],
                ["name":.S("Pinkisaur"),"teeth":.N("40"),  "colours":.L([.S("pink")])],
                ["name":.S("Dottisaur"), "teeth":.N("50"), "colours":.L([.S("black"), .S("blue")])]
            ])
        }
    }
    
    @available(OSX 15.0, *)
    func testDynoPutThenDelete() {
        let newItem = Mockosaur(id: "23456", name: "Hadrosaur", colours: ["brown","green"], teeth: 252)
        
        let _ = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
                .put(table: TEST_TABLE,
                     item: newItem)
        }

        let anotherNewItem = Mockosaur(id: "23456", name: "Pterosaur", colours: ["red"], teeth: 35)
        
        let result2 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
                .put(table: TEST_TABLE,
                     item: anotherNewItem,
                     returnOriginal: true)
        }
        
        if let success = result2?.result {
            XCTAssertEqual(success, newItem)
        } else {
            XCTFail("Failed")
        }
        
        // remove it
        let _ = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .delete(table: TEST_TABLE, keyField: "id", keyValue: "23456")
        }

    }
    
    @available(OSX 15.0, *)
    func testDynoPutPreventOverwrite() {
        let newItem = Mockosaur(id: "150", name: "Velociraptor", colours: ["yellow","pink"], teeth: 453)
        
        let _ = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .put(table: TEST_TABLE,
                 item: newItem)
        }

        let anotherNewItem = Mockosaur(id: "150", name: "Littleraptor", colours: ["white","red"], teeth: 4)

        let _ = XCTWaitForPublisherFailure(unexpectedSuccessMessage: "Unexpectedly overwrote existing item", timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .put(table: TEST_TABLE,
                 item: anotherNewItem,
                 condition: .attributeNotExists("id"))
        }

        // test retrieval !
        let result3 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "150",
                   type: Mockosaur.self)
        }
        
        if let dino = result3?.result {
            XCTAssertEqual( dino, Mockosaur(id: "150", name: "Velociraptor", colours: ["yellow","pink"], teeth: 453)  )
        }
        
        // test retrieval with projection
        let result4 = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "150",
                   projection: ["name", "teeth"],
                   type: Microsaur.self)
            }
        
        if let dino = result4?.result {
            XCTAssertEqual( dino, Microsaur(name: "Velociraptor", teeth: 453)  )
        }
        
        // test deletion
        let _ = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .delete(table: TEST_TABLE, keyField: "id", keyValue: "150")
        }

        // Make sure we can't re-retrieve
        let _ = XCTWaitForPublisherFailure(unexpectedSuccessMessage: "Retrieved deleted object with id=150", timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "150",
                   type: Mockosaur.self)
        }
    }
    
      @available(OSX 15.0, *)
      func testDynoNoItemToGet() {
        let _ = XCTWaitForPublisherFailure(unexpectedSuccessMessage:"Retrieved an object with id = -1", timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "-1",
                   type: Mockosaur.self)
        }
    }
    
    
    @available(OSX 15.0, *)
    func testDynoDescribeTable() {
        let result = XCTWaitForPublisherResult(timeout: CONNECTION_TIMEOUT) {
            Dyno(options: DynoOptions(log: true))!
            .describeTable(name: TEST_TABLE)
        }
        if let tableInfo = result {
            XCTAssertEqual(tableInfo.TableStatus, .active)
        }
    }
    
    

}

