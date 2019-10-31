//
//  AWSSignableRequest.swift
//  
//
//  Created by RedPanda on 26-Sep-19.
//

import Foundation
import StrictlySwiftLib

public struct AWSSignableRequest {
    private let verb: AWSHTTPVerb
    private let host: String
    private let path: String
    internal let region: String
    internal let service: String
    internal let date: Date
    private let queryParameters: [String:String]
    private let headers: [String:String]
    internal let extendedHeaders: [String:String]
    internal let payload: String
    private let hexHashedPayload: String
    
    init(date: Date = Date(),
         verb: AWSHTTPVerb,
         host: String,
         path: String,
         region: String,
         service: String,
         queryParameters: [String:String] = [:],
         headers: [String:String] = [:],
         payload: String,
         test_no_content_sha: Bool = false) {
    
        self.date = date
        self.verb = verb
        self.host = host
        self.path = path
        self.region = region
        self.service = service
        self.queryParameters = queryParameters
        self.headers = headers
        self.payload = payload
        
        self.hexHashedPayload = AWSSignatureGenerator.sha256Hash(payload: self.payload)
        
        var requiredHeaders = ["host":self.host,
                               "x-amz-date":self.date.toAWSTimestamp()].append(self.headers)
        if !test_no_content_sha {
            requiredHeaders = requiredHeaders.append(["x-amz-content-sha256":hexHashedPayload])
        }
        self.extendedHeaders = requiredHeaders
    }
    // AWS has a specific URL encoding we must match
    private func awsPercentEncoding(for input: String, encodingSlash: Bool = true) -> String {
        var output = ""
        for ch in input {
            switch ch {
            case "A"..."Z", "a"..."z", "0"..."9", "_", "-", "~", ".":
                output += [ch]
            case "/":
                output += (encodingSlash ? "%2F" : "/")
            default:
                output += "%\(Data(ch.utf8).hexEncodedString)"
            }
        }
        return output
    }
    
    // Create the request in the very specific form AWS requires
    func canonicalRequest() -> String {
        let encodedQueryParameters = queryParameters
            .toSortedArray()
            .map { URLQueryItem(name: self.awsPercentEncoding(for: $0.0), value: self.awsPercentEncoding(for: $0.1)) }
        
        var urlComponents = URLComponents()
        urlComponents.host = self.host
        urlComponents.path = self.path
        urlComponents.queryItems = encodedQueryParameters
        
        let signedHeaders = Array(self.extendedHeaders.keys).sorted()
        
        let formatHeaders : (String,String) -> String = { "\($0.lowercased()):\($1.trimLeadingTrailingWhitespace())" }
        let lowercaseHeaders = self.extendedHeaders.toSortedArray().map( formatHeaders ).joined(separator: "\n")
        let lowercaseSignedHeaders = signedHeaders.map{ $0.lowercased() }.joined(separator: ";")
        
        return  """
        \(self.verb.rawValue)
        \(self.awsPercentEncoding(for: urlComponents.percentEncodedPath, encodingSlash: false))
        \(urlComponents.percentEncodedQuery!)
        \(lowercaseHeaders)
        
        \(lowercaseSignedHeaders)
        \(self.hexHashedPayload)
        """
    }
    
    var allHeaderNames : [String] { get {
        return Array(self.extendedHeaders.keys).sorted()
        }
    }
}
