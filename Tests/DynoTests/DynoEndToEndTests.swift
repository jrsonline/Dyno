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
        
        let scan = DynoScan(table: TEST_TABLE,
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
        
        let action = DynoScan(table: TEST_TABLE,
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
          {'body': '{"FilterExpression": "#n0 > :v0", "TableName": TEST_TABLE, "ExpressionAttributeValues": {":v0": {"N": "40"}}, "ExpressionAttributeNames": {"#n0": "teeth"}}'
          */
         let dynoConnection = DynoHttpConnection(credentialPath: nil, region: TEST_REGION, log: true)
         let options = DynoOptions(addVersioning: true, timeout: 30, pageSize: 3, log: true, dummyUrl: false)
         
         let action = DynoScan(table: TEST_TABLE,
                               options: options,
                               filter: DynoCondition.compare("teeth", .gt, 40),
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
         
         let resultPreJoin = dyno.scan(table: TEST_TABLE,
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
         
         let resultPreJoin = dyno.scan(table: TEST_TABLE,
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
         
         let result = dyno.scan(table: TEST_TABLE,
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
         
         let result = dyno.scan(table: TEST_TABLE,
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
         
         let result = dyno.scan(table: TEST_TABLE,
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
             .scan(table: TEST_TABLE,
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
             .scan(table: TEST_TABLE,
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
             .scan(table: TEST_TABLE,
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
             .scan(table: TEST_TABLE,
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
            .scan(table: TEST_TABLE,
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
            .scan(table: TEST_TABLE,
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
            .scan(table: TEST_TABLE,
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
                  table: TEST_TABLE,
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
    
    @available(OSX 15.0, *)
    func testDynoPutThenDelete() {
        let newItem = Mockosaur(id: "23456", name: "Hadrosaur", colours: ["brown","green"], teeth: 252)
        
        let result = Dyno(options: DynoOptions(log: true))!
            .put(table: TEST_TABLE,
                 item: newItem)
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        NSLog("\(result)")
        
        if let success = result.asSuccess() {
            XCTAssert(success.result.count == 0)
            NSLog("\(Array(success.result))")
        } else {
            XCTFail("Failed: \(result)")
        }
        
        let anotherNewItem = Mockosaur(id: "23456", name: "Pterosaur", colours: ["red"], teeth: 35)
        
        let result2 = Dyno(options: DynoOptions(log: true))!
            .put(table: TEST_TABLE,
                 item: anotherNewItem,
                 returnOriginal: true)
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        NSLog("\(result2)")
        
        if let success = result2.asSuccess() {
            XCTAssertEqual(success.result, [newItem])
        } else {
            XCTFail("Failed: \(result2)")
        }
        
        // remove it
        let deleteResult1 = Dyno(options: DynoOptions(log: true))!
            .delete(table: TEST_TABLE, keyField: "id", keyValue: "23456")
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        NSLog("\(deleteResult1)")
        if let success = deleteResult1.asSuccess() {
            XCTAssert(true)
        } else {
            XCTFail("Failed: \(deleteResult1)")
        }

    }
    
    @available(OSX 15.0, *)
    func testDynoPutPreventOverwrite() {
        let newItem = Mockosaur(id: "150", name: "Velociraptor", colours: ["yellow","pink"], teeth: 453)
        
        let result = Dyno(options: DynoOptions(log: true))!
            .put(table: TEST_TABLE,
                 item: newItem)
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        NSLog("\(result)")
        
        if let success = result.asSuccess() {
            XCTAssert(success.result.count == 0)
            NSLog("\(Array(success.result))")
        } else {
            XCTFail("Failed: \(result)")
        }
        
        let anotherNewItem = Mockosaur(id: "150", name: "Littleraptor", colours: ["white","red"], teeth: 4)

        let result2 = Dyno(options: DynoOptions(log: true))!
            .put(table: TEST_TABLE,
                 item: anotherNewItem,
                 condition: .attributeNotExists("id"))
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        NSLog("\(result2)")
        
        if result2.asFailure() != nil {
            XCTAssert(true)
        } else {
            XCTFail("Failed: managed to succeed the condition check, unexpectedly \(result2.asSuccess()!)")
        }
        
        // test retrieval !
        let result3 = Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "150",
                   type: Mockosaur.self)
              .toBlockingResult(timeout: 5)
              .map {$0.aggregated()}
        
        if let dinos = result3.asSuccess() {
            XCTAssertEqual(dinos.result.count, 1)
            XCTAssertEqual( dinos.result[0], Mockosaur(id: "150", name: "Velociraptor", colours: ["yellow","pink"], teeth: 453)  )
        } else {
            XCTFail("Failed: \(result3)")
        }
        
        // test retrieval with projection
        let result4 = Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "150",
                   projection: ["name", "teeth"],
                   type: Microsaur.self)
              .toBlockingResult(timeout: 5)
              .map {$0.aggregated()}
        
        if let dinos = result4.asSuccess() {
            XCTAssertEqual(dinos.result.count, 1)
            XCTAssertEqual( dinos.result[0], Microsaur(name: "Velociraptor", teeth: 453)  )
        } else {
            XCTFail("Failed: \(result4)")
        }
        
        // test deletion
        let deleteResult1 = Dyno(options: DynoOptions(log: true))!
            .delete(table: TEST_TABLE, keyField: "id", keyValue: "150")
            .toBlockingResult(timeout: 5)
            .map {$0.aggregated()}
        
        NSLog("\(deleteResult1)")
        if let success = deleteResult1.asSuccess() {
            XCTAssert(true)
        } else {
            XCTFail("Failed: \(deleteResult1)")
        }

        // Make sure we can't re-retrieve
        let result5 = Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "150",
                   type: Mockosaur.self)
              .toBlockingResult(timeout: 5)
              .map {$0.aggregated()}
        
        if result5.asFailure() != nil {
            XCTAssert(true)
        } else {
            XCTFail("Failed: retrieved an object with id = 150, after deletion \(result5.asSuccess()!)")
        }
    }
    
      @available(OSX 15.0, *)
      func testDynoNoItemToGet() {
        let result = Dyno(options: DynoOptions(log: true))!
              .get(table: TEST_TABLE,
                   keyField: "id",
                   keyValue: "-1",
                   type: Mockosaur.self)
              .toBlockingResult(timeout: 5)
              .map {$0.aggregated()}
        
        
        if result.asFailure() != nil {
            XCTAssert(true)
        } else {
            XCTFail("Failed: retrieved an object with id = -1, unexpectedly \(result.asSuccess()!)")
        }
    }
    
    
    @available(OSX 15.0, *)
    func testDynoDescribeTable() {
        let result = Dyno(options: DynoOptions(log: true))!
            .describeTable(name: TEST_TABLE)
            .toBlockingResult(timeout: 5)
        
        NSLog("\(result)")

        if let result = result.asSuccess() {
     //       XCTAssertEqual(result[0].ItemCount, 4)  // need  to wait quite some time for itemcount to be accurate.
            XCTAssertEqual(result[0].TableStatus, .active)
        } else {
            XCTFail("Failed: \(result)")
        }
    }

}

