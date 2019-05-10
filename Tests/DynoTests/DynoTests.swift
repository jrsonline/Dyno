import XCTest
import Foundation
import PythonKit
import RxSwift
import Dyno
import RxBlocking

struct Mockosaur : Codable {
    let id: String
    let name: String
    let colour: String
    let teeth: Int
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
    
    let disposeBag = DisposeBag()

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
        if let scanResult = try self.mockGoodDyno.scan(inTable: "Mockosaurs", ofType: Mockosaur.self)
            .toBlocking(timeout: 2)
            .last()?
            .isFullSuccess() {
                XCTAssertEqual(1, scanResult.count)
                XCTAssertEqual(scanResult[0].name, "Mockosaurus Mockamusii")
        } else {
            XCTFail("Scan result could not be parsed")
        }
    }
    
    func testTimeout() throws {
        // Use the "Bad" mock which always fails after waiting 1 second.
        // That ensures we'll time out.  So we succeed the test if the last result is a failure
        if let scanResult = try self.mockBadDyno.scan(inTable: "Mockosaurs", ofType: Mockosaur.self)
            .toBlocking(timeout: 2)
            .last()?
            .isFailure() {
                XCTAssert(scanResult.reason.hasPrefix("Connection failed after waiting"),"Failed but not due to timeout: \(scanResult)")
        } else {
             XCTFail("Scan result could not be parsed")
        }
    }
    
    func testSetItemThenScan() throws {
        // Here we carry out 2 operations, first a set then a scan to check we successfully added something.
        // Note that the 'toBlocking().last()' waits for both operations to complete and gives us the final result.
        
        if let addScanResult = try self.mockGoodDyno.setItem(inTable: "Mockosaurs",
                                  value: Mockosaur(id: "2", name: "Fakiraptor", colour: "Black", teeth: 40))
            .arrayBox()
            .concat(
                self.mockGoodDyno.scan(inTable: "Mockosaurs", ofType: Mockosaur.self)
            )
            .toBlocking(timeout: 2)
            .last()?
            .isFullSuccess() {
                XCTAssertEqual(Set(addScanResult.map {$0.name}), Set(["Fakiraptor","Mockosaurus Mockamusii"]))
        } else {
            XCTFail("Scan result could not be parsed")
        }
    }
    
    
    func testSendSetItem() throws {
        try runLocalOnly(
            expectSuccess: false,
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
            expectSuccess: false,
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
            expectSuccess: false,
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
            expectSuccess: false,
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
            expectSuccess: false,
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
            expectSuccess: false,
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
    
    private func runLocalOnly<S>(   expectSuccess: Bool,
                                   testing: (Dyno) -> Observable<DynoActivity<S>>,
                                   checking: @escaping (DynoLocalOnlyBoto3) -> Void,
                                   timeout: RxTimeInterval = 20,
                                   file: StaticString = #file, line: UInt = #line, description: String = #function) throws {
        // Tests the actual call to dynamoDB by boto3 when putting an item; and compares that to the expected.
        // This therefore uses a different type of "mock" -- that with a fake dynamoDB connection -- and reads the log file
        

        // Python logging REALLY does not deal with multi threading, so we're going to force single threading here
        try testQueue.sync {
            let localOnlyBoto3 = DynoLocalOnlyBoto3(source: description)
            let localDyno = Dyno(connection: localOnlyBoto3 )

            if let outcome = try testing(localDyno)
                .toBlocking(timeout: timeout)
                .last() {
                switch (outcome, expectSuccess) {
                    case (.fullSuccess(_), true),
                         (.failure(_), false):
                        checking(localOnlyBoto3)
                    
                    default:
                        XCTFail("\(outcome) unexpected with a fake dynamoDB connection, expected \(expectSuccess)",file: file, line: line )
                }
            } else {
                XCTFail("Could not parse testing outcome",file: file, line: line )
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
    guard let log = try? DynoLocalOnlyBoto3.priorOperationOutput(name: logKey, inLogFile: localBoto3.tempFilename), log.count != 0 else {
        XCTFail("Could not find '\(logKey)' in operation log \(localBoto3.tempFilename)", file: file, line: line)
        return
    }
        
    XCTAssertEqualDictionaries(item: log, refDict: refDict)
}

/// Compares two dictionaries of form [String:X]. X can be a further nested dictionary, or a String
/// or an Int.
func XCTAssertEqualDictionaries(item:[String:Any],
                                refDict:[String:Any],
                                file: StaticString = #file,
                                line: UInt = #line) {
    
    // Check if the number of keys in 'item' matches
    XCTAssertEqual(Set(refDict.keys), Set(item.keys), "Fields in item don't match reference", file: file, line: line)

    for field in item.keys {
        switch (item[field], refDict[field]) {
        case let (fieldDict, refSubDict) as ([String:Any], [String:Any]):
            XCTAssertEqualDictionaries(item: fieldDict, refDict: refSubDict)
            
        case let (fieldStr, refStr) as (String, String):
            XCTAssertEqual(fieldStr, refStr, "Item's field '\(field)' has value '\(fieldStr)', but reference '\(field)' has '\(refStr)'" , file: file, line: line)
            
        case let (fieldStr, refStr) as (Int, Int):
            XCTAssertEqual(fieldStr, refStr, "Item's field '\(field)' has value '\(fieldStr)', but reference '\(field)' has '\(refStr)'" , file: file, line: line)
        
        default:
            XCTFail("Could not compare \(item[field] ?? "nil") and \(refDict[field] ?? "nil")", file: file, line: line)
        }
    }
}
