//
//  File.swift
//  
//
//  Created by RedPanda on 25-Sep-19.
//

import XCTest
import Foundation
import Combine
import StrictlySwiftLib
@testable import Dyno

/* Python test setup
 >>> import boto3
 >>> dynamodb = boto3.resource('dynamodb')
 >>> from boto3.dynamodb.conditions import Key, Attr
 >>> boto3.set_stream_logger('botocore')
 >>> table = dynamodb.Table('Dinosaurs')
 
 */

struct Mockosaur : Codable {
    let id: String
    let name: String
    let colours: [String]
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

struct MockAction : AWSAction {
    func actionName() -> String {
        return "DynamoDB_20120810.Scan"
    }
    
    func body() -> String {
        return """
        {"TableName": "Dinosaurs"}
        """
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

// Helper conversion
extension BlockingError {
    func asDynoError() -> DynoError {
        switch self {
            case .timeoutError(let i): return DynoError("Timed out after \(i) seconds: \(self)")
            case .otherError(let e): return DynoError("\(e)")
        }
    }
}

extension DateFormatter {
    static func date(fromAWSStringDate date: String) -> Date {
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyyMMdd'T'HHmmssZZZZZ"
        dateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        
        return dateFormatter.date(from: date)!
    }
}

final class DynoTests: XCTestCase {

    override func setUp() {
    }
    
    // See:  https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
    func testCalculateSigningKeyAndSignature() throws {
        let asofDate = Date(year: 2015, month: 08, day: 30, isUTC: true)!
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL1(), log: true)
        
        let key = signer?.calculateSigningKey(for: asofDate, region: "us-east-1", service: "iam")
        XCTAssertNotNil(key)
        
        let keyData = key!.hexEncodedString
        XCTAssertEqual(keyData, "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9")
        
        let stringToSign =
        """
        AWS4-HMAC-SHA256
        20150830T123600Z
        20150830/us-east-1/iam/aws4_request
        f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59
        """
        let signature = signer?.do_sign(signingKey: key!, stringToSign: stringToSign)
        XCTAssertEqual(signature, "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7")
    }
    
    func testSHA256Hash() throws {
        XCTAssertEqual(AWSSignatureGenerator.sha256Hash(payload: ""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    }
    
    // from test on here:  https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    func testCanonicalRequest() {
        let awsReq = AWSSignableRequest(
            date: DateFormatter.date(fromAWSStringDate: "20130524T000000Z"),
            verb: .GET,
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            region: "examplebucket",
            service: "s3",
            queryParameters: [:],
            headers: ["range":"bytes=0-9"],
            payload: "")
        let req = awsReq.canonicalRequest()
        
        XCTAssertEqual( req,
                        """
                        GET
                        /test.txt

                        host:examplebucket.s3.amazonaws.com
                        range:bytes=0-9
                        x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
                        x-amz-date:20130524T000000Z

                        host;range;x-amz-content-sha256;x-amz-date
                        e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
                        """
                       )
        XCTAssertEqual(AWSSignatureGenerator.sha256Hash(payload: req), "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972")
    }
    
    func testStringToSign() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20130524T000000Z")
        
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true)
        
        let awsReq = AWSSignableRequest(
            date:asofDate,
            verb: .GET,
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            region: "us-east-1",
            service: "s3",
            queryParameters: [:],
            headers: ["range":"bytes=0-9", "x-amz-date":"20130524T000000Z"],
            payload: "")
        
        let req = signer?.createStringToSign(request: awsReq)
        
        XCTAssertEqual(req,
                        """
                        AWS4-HMAC-SHA256
                        20130524T000000Z
                        20130524/us-east-1/s3/aws4_request
                        7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972
                        """
                       )
    }
    
    func test_doSign() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20130524T000000Z")
        
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true)
        
        let awsReq = AWSSignableRequest(
            date: asofDate,
            verb: .GET,
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            region: "us-east-1",
            service: "s3",
            queryParameters: [:],
            headers: ["range":"bytes=0-9"],
            payload: "")

        let req = signer?.createStringToSign(request: awsReq) ?? ""
        
        let signingKey = signer?.calculateSigningKey(for: asofDate, region: "us-east-1", service: "s3")
        
        XCTAssert(signingKey != nil, "Cannot create signing key")
        let nonNilSigningKey = signingKey!
                
        XCTAssertEqual(signer?.do_sign(signingKey: nonNilSigningKey, stringToSign: req),
                       "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
    }
    
    func testNoQueryParameterSignatureCalculation() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20130524T000000Z")
        
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true )
        
        let awsReq = AWSSignableRequest(
            date: asofDate,
            verb: .GET,
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            region: "us-east-1",
            service: "s3",
            queryParameters: [:],
            headers: ["range":"bytes=0-9"],
            payload: "")
        XCTAssertNotNil(signer)
        let nonNilSigner = signer!
        
        XCTAssertEqual(nonNilSigner.sign(request: awsReq),
                       .success("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
        
    }
    
    func testWithQueryParameterSignatureCalculation() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20130524T000000Z")
        
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true )
        
        let awsReq = AWSSignableRequest(
            date: asofDate,
            verb: .GET,
            host: "examplebucket.s3.amazonaws.com",
            path: "/",
            region: "us-east-1",
            service: "s3",
            queryParameters: ["lifecycle":""],
            headers: [:],
            payload: "")
        XCTAssertNotNil(signer)
        let nonNilSigner = signer!
        
        XCTAssertEqual(nonNilSigner.sign(request: awsReq),
                       .success("fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"))
        
    }
    
    func testWithPutSignatureCalculation() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20130524T000000Z")
        
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true)
        
        let awsReq = AWSSignableRequest(
            date: asofDate,
            verb: .PUT,
            host: "examplebucket.s3.amazonaws.com",
            path: "/test$file.text",
            region: "us-east-1",
            service: "s3",
            queryParameters: [:],
            headers: [
                "date":"Fri, 24 May 2013 00:00:00 GMT",
                "x-amz-storage-class":"REDUCED_REDUNDANCY"],
            payload: "Welcome to Amazon S3.")
        XCTAssertNotNil(signer)
        let nonNilSigner = signer!
        
        XCTAssertEqual(nonNilSigner.sign(request: awsReq),
                       .success("98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"))
        
    }
    
    func testNoQueryParameterAuthorization() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20130524T000000Z")
        
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true )
        
        let awsReq = AWSSignableRequest(
            date: asofDate,
            verb: .GET,
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            region: "us-east-1",
            service: "s3",
            queryParameters: [:],
            headers: ["range":"bytes=0-9"],
            payload: "")
        XCTAssertNotNil(signer)
        let nonNilSigner = signer!
        
        XCTAssertEqual(nonNilSigner.authorize(request: awsReq),
                       .success("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
        
    }

    func testDinosaurScanRequest() {
        let asofDate = DateFormatter.date(fromAWSStringDate: "20191015T231704Z")
        
       
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2(), log: true )
       
       let awsReq = AWSSignableRequest(
           date: asofDate,
           verb: .POST,
           host: "dynamodb.us-east-2.amazonaws.com",
           path: "/",
           region: "us-east-2",
           service: "dynamodb",
           queryParameters: [:],
           headers: [
            "content-type":"application/x-amz-json-1.0",
            "x-amz-target":"DynamoDB_20120810.Scan"],
           payload: """
                    {"TableName": "Dinosaurs"}
                    """,
           test_no_content_sha: true)
        XCTAssertEqual(awsReq.canonicalRequest(),
                        """
                        POST
                        /

                        content-type:application/x-amz-json-1.0
                        host:dynamodb.us-east-2.amazonaws.com
                        x-amz-date:20191015T231704Z
                        x-amz-target:DynamoDB_20120810.Scan

                        content-type;host;x-amz-date;x-amz-target
                        2b9107816baf4b22a43cf18a2c3ceb95ac69632991a10efae4af99f87347c68e
                        """)
        
        let s2s = signer!.createStringToSign(request: awsReq)
        XCTAssertEqual(s2s,
                        """
                        AWS4-HMAC-SHA256
                        20191015T231704Z
                        20191015/us-east-2/dynamodb/aws4_request
                        a2cae71621a09375e098f483d435417cca5db737eb755ced1c3d1c3fd315689b
                        """)
        let signature = signer!.sign(request: awsReq)
        XCTAssertEqual(signature, .success("fd6b8a79f737f24b85258be0e71517f192a92b125c4c7993ed35445402dd220f"))
    }
    
    
    func testDecoder() {
        let decoder = JSONDecoder()

        let json1 =  #"{"Count":2,"Items":[],"LastEvaluatedKey":{"id":{"S":"6"}},"ScannedCount":3, "ConsumedCapacity":{}}"#
        
        let decoded1 = try? decoder.decode(DynoScanResponse.self, from: json1.data(using: .utf8)!)
        XCTAssertNotNil(decoded1)
        XCTAssertEqual(decoded1?.Count,2)
        XCTAssertEqual(decoded1?.Items,[])
        XCTAssertEqual(decoded1?.LastEvaluatedKey,["id":.S("6")])
        XCTAssertEqual(decoded1?.ScannedCount,3)
        
        let json2 =  #"{"Count":2,"Items":[{"teeth":{"N":"158"},"id":{"S":"2"},"colours":{"L":[{"S":"green"},{"S":"black"}]},"name":{"S":"Tyrannosaurus"}},{"teeth":{"N":"40"},"id":{"S":"6"},"colours":{"L":[{"S":"pink"}]},"name":{"S":"Pinkisaur"}}],"LastEvaluatedKey":{"id":{"S":"6"}},"ScannedCount":3, "ConsumedCapacity":{}}"#
        
        let decoded2 = try? decoder.decode(DynoScanResponse.self, from: json2.data(using: .utf8)!)
        XCTAssertNotNil(decoded2)
        XCTAssertEqual(decoded2?.Count,2)
        XCTAssertEqual(decoded2?.Items,[["teeth":.N("158"), "id":.S("2"), "colours":.L([.S("green"),.S("black")]), "name":.S("Tyrannosaurus")],
                                        ["teeth":.N("40"), "id":.S("6"), "colours":.L([.S("pink")]), "name":.S("Pinkisaur")]])
        XCTAssertEqual(decoded2?.LastEvaluatedKey,["id":.S("6")])
        XCTAssertEqual(decoded2?.ScannedCount,3)
    }
    
 
    func testFilters() {
        let filter1 = DynoScanFilter.betweenValue(of:"teeth", from: 50, to: 4000)
        let eavFilter1 = filter1.toPayload()
        XCTAssertEqual(filter1.description, #"teeth BETWEEN N("50") AND N("4000")"#)
        XCTAssertEqual(eavFilter1.toDynoFilterExpression(),"#n0 BETWEEN :v0 AND :v1")
        
        NSLog("\(eavFilter1.toDynoExpressionAttributeNames())")
        NSLog("\(eavFilter1.toDynoExpressionAttributeValues())")
        XCTAssertEqual(eavFilter1.toDynoExpressionAttributeNames(), ["#n0":"teeth"])
        XCTAssertEqualDictionaries(item: eavFilter1.toDynoExpressionAttributeValues(), refDict: [":v0":.N("50"),":v1":.N("4000")])
        
        let filter2 = DynoScanFilter.compare("teeth", .ge, 40)
        let eavFilter2 = filter2.toPayload()
        XCTAssertEqual(filter2.description, #"teeth >= N("40")"#)
        XCTAssertEqual(eavFilter2.toDynoFilterExpression(),"#n0 >= :v0")
        XCTAssertEqual(eavFilter2.toDynoExpressionAttributeNames(), ["#n0":"teeth"])
        XCTAssertEqualDictionaries(item: eavFilter2.toDynoExpressionAttributeValues(),  refDict: [":v0":.N("40")])

        let filter3 = DynoScanFilter.in("colour", ["green","aqua"])
        let eavFilter3 = filter3.toPayload()
        XCTAssertEqual(filter3.description, #""colour" IN (S("green"), S("aqua"))"#)
        XCTAssertEqual(eavFilter3.toDynoFilterExpression(), "#n0 IN (:v0,:v1)")
        XCTAssertEqual(eavFilter3.toDynoExpressionAttributeNames(),["#n0":"colour"])
        XCTAssertEqualDictionaries(item: eavFilter3.toDynoExpressionAttributeValues(),refDict: [":v0":.S("green"),":v1":.S("aqua")])
        
        let filterAndOrNot = DynoScanFilter.and(filter1, DynoScanFilter.or(filter2,DynoScanFilter.not(filter3)))
        let eavFilterAndOrNot = filterAndOrNot.toPayload()
        XCTAssertEqual(filterAndOrNot.description, #"(teeth BETWEEN N("50") AND N("4000")) AND ((teeth >= N("40")) OR (NOT ("colour" IN (S("green"), S("aqua")))))"#)
        XCTAssertEqual(eavFilterAndOrNot.toDynoFilterExpression(),"(#n0 BETWEEN :v0 AND :v1 AND (#n2 >= :v2 OR NOT #n3 IN (:v3,:v4)))")
        XCTAssertEqualDictionaries(item: eavFilter3.toDynoExpressionAttributeValues(), refDict: [":v0":.S("green"),":v1":.S("aqua")])
    }
    
    
    @available(OSX 15.0, *)
    func testAWSHTTPRequest() {
        // This makes a call to AWS but with dummy credentials.
        let signer = AWSSignatureGenerator(secretKeyLocation: getTestCredentialsURL2() , log: true)
        
        let awsHttpRequest = AWSHTTPRequest(
            region: "us-east-2",
            action: MockAction(),
            signer: signer!,
            log: true)
        
        let publisher = awsHttpRequest.request(forSession: URLSession.shared)
        
        let result = publisher.toBlockingResult(timeout: 5)
        if let failure = (result.asFailure()?.asOtherError() as? AWSRequestError),
            case AWSRequestError.invalidResponse(400,
                                                 """
    {\"__type\":\"com.amazon.coral.service#UnrecognizedClientException\",\"message\":\"The security token included in the request is invalid.\"}
    """) = failure {
        } else {
            XCTFail("Expected 400, got \(result)")
        }
    }
    
    
     
    
    /* ************************************************************************************************************ */
    

    func getTestCredentialsURL1() -> URL {
        return getTestResourceDirectory().appendingPathComponent("test_credentials1.txt")
    }
    
    func getTestCredentialsURL2() -> URL {
        // argh,  Amazon documentation uses 2 SLIGHTLY different keys for some reason
        return getTestResourceDirectory().appendingPathComponent("test_credentials2.txt")
    }
    
}


public func XCTAssertEqualDictionaries( item:[String:DynoAttributeValue],
                                        refDict:[String:DynoAttributeValue],
                                        file: StaticString = #file,
                                        line: UInt = #line) {
    XCTAssertEqual(item.count, refDict.count, "Different size dictionaries", file: file, line: line)
    
    for key in item.keys {
        let refValue = refDict[key] ?? DynoAttributeValue.NULL(true)
        XCTAssertEqual(item[key], refDict[key], "Dictionaries differ for \(key): Original dictionary has \(refValue), test has \(item[key]!)", file: file, line: line)
    }
}
