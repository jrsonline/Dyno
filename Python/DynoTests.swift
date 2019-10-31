import XCTest
import Foundation
import PythonKit
//import RxSwift
//import RxBlocking
import Combine
import StrictlySwiftLib
import PythonCodable
@testable import Dyno

struct Mockosaur : Codable {
    let id: String
    let name: String
    let colour: String
    let teeth: Int
}

struct MockosaurDiscovery : Codable {
    let id: String
    let dinoId: String
    let when: Date
}

struct MockosaurSize : Codable {
    let id: String
    let dinoId: String
    let size: Double
}

// Helper conversion
extension BlockingError {
    func asDynoError() -> DynoError {
        switch self {
            case .timeoutError(let i): return DynoError("Timed out after \(i) seconds: \(self)")
            case .otherError(let e): return DynoError("\(e)")
        }
    }
}

var testQueue: DispatchQueue! = nil


final class DynoTests: XCTestCase {
    let cleanConnection = DynoMockBoto.MockConnectionQuality(
        waitTime: 0,
        failsAfterWait: false,
        canReconnect: true)

    let badConnection = DynoMockBoto.MockConnectionQuality(
        waitTime: 1,
        failsAfterWait: true,
        canReconnect: false)
    
    let dinos = DynoMockBoto.MockTableInfo(
        tableData: ["Mockosaurs":["1":[
            PythonObject("id"):PythonObject("1"),
            PythonObject("name"):PythonObject("Mockosaurus Mockamusii"),
            PythonObject("colour"):PythonObject("Pink"),
            PythonObject("teeth"):PythonObject(40)]]],
        keyFields: ["Mockosaurs":"id"])
    
    var mockDynoGoodConnection : DynoMockBoto! = nil
    var mockGoodDyno : Dyno! = nil
    var mockDynoBadConnection : DynoMockBoto! = nil
    var mockBadDyno : Dyno! = nil
    
    override func setUp() {
        // set up test connections
        self.mockDynoGoodConnection = DynoMockBoto(connectionQuality: cleanConnection,
                                                   mockTableInfo: dinos,
                                                   isValid: true)
        
        self.mockGoodDyno = Dyno(connection: self.mockDynoGoodConnection, DynoOptions(log:true))
        
        // Bad connection
        self.mockDynoBadConnection = DynoMockBoto(connectionQuality: badConnection,
                                                     mockTableInfo: dinos,
                                                     isValid: true)
        
        self.mockBadDyno = Dyno(connection: self.mockDynoBadConnection, DynoOptions(log:true))
        
        testQueue = DispatchQueue(label: "testDispatchQueue")
    }
    
    
    
    func testScanForOne() throws {
        // We use RxBlocking to convert the Observable into a list of results.  The final result is what we are interested in
        
        if let result = try self.mockGoodDyno
            .scan(inTable: "Mockosaurs", ofType: Mockosaur.self)
            .toBlockingResult(timeout: 2)
            .get()
            .last {
            XCTAssertEqual(1, result.count)
            XCTAssertEqual(result[0].name, "Mockosaurus Mockamusii")
        } else {
            XCTFail("Scan result could not be parsed")
        }

    }
    
    func testTimeout() throws {
        // Use the "Bad" mock which always fails after waiting 1 second.
        // That ensures we'll time out.  So we succeed the test if the last result is a failure
        if let result = self.mockBadDyno
            .scan(inTable: "Mockosaurs", ofType: Mockosaur.self)
            .toBlockingResult(timeout: 2)
            .asFailure()?
            .asOtherError() as? DynoError {
                XCTAssert(result.reason.hasPrefix("Connection failed after waiting"),"Failed but not due to timeout: \(result)")
        } else {
            XCTFail("Scan result could not be parsed")
        }
    }
    
    func testSetItemThenScan() throws {
        // Here we carry out 2 operations, first a set then a scan to check we successfully added something.
        // Note that the 'toBlocking().last()' waits for both operations to complete and gives us the final result.
        
        let newMockasaur = Mockosaur(id: "2", name: "Fakiraptor", colour: "Black", teeth: 40)
        
        if let addScanResult = try self.mockGoodDyno.setItem(inTable: "Mockosaurs",
                                                         value: newMockasaur)
        .map({ [$0] })
        .append( self.mockGoodDyno.scan(inTable: "Mockosaurs", ofType: Mockosaur.self) )
        .toBlockingResult(timeout: 2)
        .get()
        .last {
            XCTAssertEqual(Set(addScanResult.map {$0.name}), Set(["Fakiraptor","Mockosaurus Mockamusii"]))
        } else {
            XCTFail("Scan result could not be parsed")
        }
    }
    
    
    func testSendSetItem() throws {
        try runLocalOnly(
            testing: { dyno in
                dyno.setItem(inTable: "Mockosaurs",
                             value: Mockosaur(id: "3", name: "Fakiraptor", colour: "Black", teeth:35)) },
            checking: { localOnlyBoto3 in
                // Check against reference
                XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                      logKey: "PutItem",
                                      to: ["TableName":"Mockosaurs",
                                           "Item":["colour":["S":"Black"],
                                                   "id":["S":"3"],
                                                   "name":["S":"Fakiraptor"],
                                                   "teeth":["N":"35"]
                                        ]])
        })
    }

    func testSendGetItem() throws {
        try runLocalOnly(
            testing: { dyno in
                dyno.getItem(fromTable: "Mockosaurs", keyField: "id", value: "3", ofType: Mockosaur.self) },
            checking: { localOnlyBoto3 in
                // Check against reference
                XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                      logKey: "GetItem",
                                      to: ["TableName":"Mockosaurs",
                                           "Key":["id":["S":"3"]]])
        })
    }

    func testSendScanAll() throws {
        try runLocalOnly(
            testing: { dyno in
                dyno.scan(inTable: "Mockosaurs", ofType: Mockosaur.self)
        },
            checking: { localOnlyBoto3 in
                // Check against reference
                XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                      logKey: "Scan",
                                      to: [ "TableName":"Mockosaurs",
                                            "Limit":100])
        })
    }

    func testSendScanSimpleFilter() throws {
        try runLocalOnly(
            testing: { dyno in
                dyno.scan(inTable: "Mockosaurs", filter: .between(DynoPathNonKey("teeth"), from: 35, to: 45), ofType: Mockosaur.self)
        },
            checking: { localOnlyBoto3 in
                // Check against reference
                XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                      logKey: "Scan",
                                      to: ["Limit": 100,
                                           "FilterExpression": "#n0 BETWEEN :v0 AND :v1",
                                           "TableName": "Mockosaurs",
                                           "ExpressionAttributeValues":
                                                [":v1": ["N": "45"],
                                                 ":v0": ["N": "35"]],
                                           "ExpressionAttributeNames":
                                                ["#n0":"teeth"]
                                        ])
        })
    }
    
    func testSendScanDoubleFilter() throws {
        try runLocalOnly(
            testing: { dyno in
                dyno.scan(inTable: "Mockosaurs", filter: .and( .between(DynoPathNonKey("teeth"), from: 35, to: 45), .compare(DynoPathKey("id"), .eq , "6")), ofType: Mockosaur.self)
        },
            checking: { localOnlyBoto3 in
                // Check against reference
                XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                      logKey: "Scan",
                                      to: ["FilterExpression": "(#n0 BETWEEN :v0 AND :v1 AND #n1 = :v2)",
                                           "TableName": "Mockosaurs",
                                           "Limit": 100,
                                           "ExpressionAttributeValues":
                                                [":v2": ["S": "6"],
                                                 ":v1": ["N": "45"],
                                                 ":v0": ["N": "35"]],
                                           "ExpressionAttributeNames":
                                                ["#n0": "teeth",
                                                 "#n1": "id"]
                    ])
        })
    }
    
    func testSendScanDoubleFilterWithKey() throws {
        try runLocalOnly(
            testing: { dyno in
                dyno.scan(inTable: "Mockosaurs", filter: .and( .between(DynoPathNonKey("teeth"), from: 35, to: 45), .compare(DynoPathNonKey("teeth"), .eq , 40)), ofType: Mockosaur.self)
        },
            checking: { localOnlyBoto3 in
                // Check against reference
                XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                      logKey: "Scan",
                                      to: ["FilterExpression": "(#n0 BETWEEN :v0 AND :v1 AND #n1 = :v2)",
                                           "TableName": "Mockosaurs",
                                           "Limit": 100,
                                           "ExpressionAttributeValues":
                                            [":v2": ["N": "40"],
                                             ":v1": ["N": "45"],
                                             ":v0": ["N": "35"]],
                                           "ExpressionAttributeNames":
                                            ["#n0": "teeth",
                                             "#n1": "teeth"]
                    ])
        })
    }
    
    /*
     
     Relevant Python for testSettingDate:
     >>> import boto3
     >>> import decimal
     >>> import datetime
     >>> boto3.set_stream_logger("botocore")
     >>> dynamodb = boto3.resource('dynamodb')
     >>> table = dynamodb.Table('Mockosaur_Discovery')
     >>> response = table.put_item(
     Item={
     'id':'1','dinoId':'2', 'when':datetime.datetime(1992,12,30, tzinfo = dateutil.tz.UTC).isoformat()
     })
     
     */
    func testSettingDate() throws {
        try runLocalOnly(testing: { dyno in
                            dyno.setItem(inTable: "Mockosaur_Discovery",
                                         value: MockosaurDiscovery(id: "1", dinoId: "2", when: Date(year: 1992, month: 12, day: 30, isUTC: true)! ))
        },
                         checking: { localOnlyBoto3 in
                            // Check against reference
                            XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                            logKey: "PutItem",
                            to: ["TableName": "Mockosaur_Discovery",
                                 "Item": ["when": ["S": "1992-12-30T00:00:00Z"],
                                          "dinoId": ["S": "2"],
                                          "id": ["S": "1"]
                                         ]
                                ])
        })
    }
    
    /*
     
     Relevant Python for testSettingSize:
     >>> import boto3
     >>> import decimal
     >>> import datetime
     >>> boto3.set_stream_logger("botocore")
     >>> dynamodb = boto3.resource('dynamodb')
     >>> table = dynamodb.Table('Mockosaur_Discovery')
     >>> response = table.put_item(
     Item={
     'id':'1','dinoId':'2', 'when':date(1992,12,30).isoformat()
     })
     
     */
    func testSettingSize() throws {
        try runLocalOnly(testing: { dyno in
            dyno.setItem(inTable: "Mockosaur_Size",
                         value: MockosaurSize(id: "1", dinoId: "2", size: 25.4))
        },
                         checking: { localOnlyBoto3 in
                            // Check against reference
                            XCTAssertEqualBotoLog(localBoto3: localOnlyBoto3,
                                                  logKey: "PutItem",
                                                  to: ["TableName": "Mockosaur_Discovery",
                                                       "Item": ["size": ["N": "25.4"],
                                                                "dinoId": ["S": "2"],
                                                                "id": ["S": "1"]
                                                    ]
                                ])
        })
    }
    
    
    private func runLocalOnly<S>(   expectSuccess: Bool = false,
                                   testing: (Dyno) -> DynoPublisher<S>,
                                   checking: @escaping (DynoLocalOnlyBoto3) -> Void,
                                   timeout: Int = 20,
                                   file: StaticString = #file, line: UInt = #line, description: String = #function) throws {
        // Tests the actual call to dynamoDB by boto3 when putting an item; and compares that to the expected.
        // This therefore uses a different type of "mock" -- that with a fake dynamoDB connection -- and reads the log file
        

        // Python logging REALLY does not deal with multi threading, so we're going to force single threading here
        testQueue.sync {
            let localOnlyBoto3 = DynoLocalOnlyBoto3(source: description)
            let localDyno = Dyno(connection: localOnlyBoto3, DynoOptions(log:true) )

            let outcome = testing(localDyno)
                .toBlockingResult(timeout: timeout)
                .mapError { e in e.asDynoError() }

            let (resultOk, reason) = DynoLocalOnlyBoto3.isExpectedFailure(outcome: outcome, expectSuccess: expectSuccess)
            if !resultOk {
                XCTFail("\(reason ?? "")",file: file, line: line )
            }
        }
    }
}

func XCTAssertEqualBotoLog(localBoto3: DynoLocalOnlyBoto3,
                           logKey: String,
                           to refDict:[String:Any],
                           file: StaticString = #file,
                           line: UInt = #line)
    -> Void {
    // Do we have a valid log?
        do {
            let log = try DynoLocalOnlyBoto3.priorOperationOutput(name: logKey, inLogFile: localBoto3.tempFilename)
            
            guard log.count != 0 else {
                XCTFail("Could not find '\(logKey)' in operation log \(localBoto3.tempFilename)", file: file, line: line)
                return
            }
            
            XCTAssertEqualDictionaries(item: log, refDict: refDict, file: file, line:line)
        } catch let error as DynoError {
            XCTFail("Failed when trying to compare in Boto log : \(error.reason)", file: file, line: line)
        } catch let error {
            XCTFail("Failed when trying to compare in Boto log : \(error.localizedDescription)", file: file, line: line)
        }
        
}
