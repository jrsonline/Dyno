//
//  AWSSignatureGenerator.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation
import StrictlySwiftLib
import CryptoKit

public enum AWSSignatureError : Error {
    case couldntReadSecretKey
    case couldntCalculateSigningKey
}


public struct AWSSignatureGenerator {
    private let requestVersion : String
    private let secretKey: String
    private let secretKeyId: String
    
    /// Creates the signature generator
    /// - Note Performs a lookup against the location of the secret key, ie a file request.
    /// - Parameter date: Pass nil to always use the current date for signing.
    init?(secretKeyLocation: URL? = nil,
          secretKeyData: Data? = nil,
          requestVersion: String = "aws4_request",
          log: Bool = false
    ) {
        guard let (sAc,sId) = (
            secretKeyData.flatMap { AWSSignatureGenerator.readKeys(fromData: $0) } ??
            secretKeyLocation.flatMap { AWSSignatureGenerator.readKeys(from: $0) } ??
            AWSSignatureGenerator.readKeysFromHomeDirectory())
        else { fatalError("Must provide a credential URL or data to Dyno") }
        
        self.requestVersion = requestVersion
        self.secretKey = sAc
        self.secretKeyId = sId
        
        if log {NSLog("Retrieved secret key")}
    }
    
    /// macos only -- try to read config from home directory
    private static func readKeysFromHomeDirectory() -> (secretAccessKey:String, accessKeyId:String)? {
        var credentials: (secretAccessKey:String, accessKeyId:String)? = nil
        #if os(macOS)
        // try to read from default location
            credentials = AWSSignatureGenerator.readKeys(from: URL(fileURLWithPath: ".aws/credentials", relativeTo: FileManager.default.homeDirectoryForCurrentUser))
        #endif
        return credentials
    }
    
    /// Find the last secret access key in the file.
    private static func readKeys(from secretKeyLocation: URL) -> (secretAccessKey:String, accessKeyId:String)? {
        let fileName = secretKeyLocation.standardizedFileURL.path
        guard let lines = FileLinesSequence(fromFile: fileName, encoding: .utf8, delimiter: "\n")  else { return nil }
        
        return AWSSignatureGenerator.readKeysFromSequence(lines: lines)
    }
    
    private static func readKeys(fromData secretKeyData: Data) -> (secretAccessKey:String, accessKeyId:String)? {
        guard let lines = String(data: secretKeyData, encoding: .utf8) else { return nil }
        
        return AWSSignatureGenerator.readKeysFromSequence(lines: lines.split(separator: "\n").map(String.init) )
    }
    
    private static func readKeysFromSequence<S>(lines:S) -> (secretAccessKey:String, accessKeyId:String)?
    where S:Sequence, S.Element == String
    {
        var secretAccessKey : String? = nil
        var accessKeyId : String? = nil
        for line in lines {
            if line.hasPrefix("aws_secret_access_key=") {
                secretAccessKey = String(line.suffix(after: "aws_secret_access_key="))
            }
            if line.hasPrefix("aws_access_key_id=") {
                accessKeyId = String(line.suffix(after: "aws_access_key_id="))
            }
        }
        guard let gotSecretAccessKey = secretAccessKey, let gotAccessKeyId = accessKeyId else { return nil }
        return (secretAccessKey:gotSecretAccessKey, accessKeyId:gotAccessKeyId)
    }
    
    internal func hmacSha256(signingKey: Data, plainText: String) -> Data {
        let hmac = HMAC<SHA256>.authenticationCode(for: plainText.data(using: .utf8)!,
                                                   using: SymmetricKey(data: signingKey))
        return Data(hmac)
    }
    
    public static func sha256Hash(payload: String) -> String {
        let hash = SHA256.hash(data: payload.data(using: .utf8)!)
        return Data(hash).hexEncodedString
    }
    
    private func calculateSigningKey(from secretKey: String) -> String? {
    //    let hmac = hmacSha256(signingKey: "secret", plainText: "Message")
   //     print(hmac)
        return ""
    }
    
    /// Generates a signature for the given request.
    /// - Parameter request: AWS request
    func sign(request: AWSSignableRequest) -> Result<String, AWSSignatureError> {

        let stringToSign = self.createStringToSign(request: request)
        guard let signingKey = self.calculateSigningKey(for: request.date, region: request.region, service: request.service) else { return .failure(.couldntCalculateSigningKey)}
        
        let signed = self.do_sign(signingKey: signingKey, stringToSign: stringToSign)
        return .success(signed)
    }
    
    /// Authorize creates a signed authorization header for the request. More generally useful than `sign` (it calls `sign` internally)
    /// - Parameter request: AWS request
    func authorize(request: AWSSignableRequest) -> Result<String, AWSSignatureError> {
        return self.sign(request: request).flatMap { signature in
            .success(
                """
                AWS4-HMAC-SHA256 Credential=\(self.secretKeyId)/\(request.date.to_yyyyMMdd())/\(request.region)/\(request.service)/aws4_request,SignedHeaders=\(request.allHeaderNames.joined(separator: ";")),Signature=\(signature)
                """
            )
        }
    }

    
    internal func createStringToSign( request: AWSSignableRequest
    ) -> String {
        return """
        AWS4-HMAC-SHA256
        \(request.date.toAWSTimestamp())
        \(request.date.to_yyyyMMdd())/\(request.region)/\(request.service)/aws4_request
        \(AWSSignatureGenerator.sha256Hash(payload: request.canonicalRequest()))
        """
    }
    
    internal func calculateSigningKey(for date: Date, region: String, service: String) -> Data? {
        guard let secretAccessKey = "AWS4\(self.secretKey)".data(using: .utf8) else { return nil }
        let dateKey = hmacSha256(signingKey: secretAccessKey,
                                 plainText: date.to_yyyyMMdd())
        let dateRegionKey = hmacSha256(signingKey: dateKey, plainText: region)
        let dateRegionServiceKey = hmacSha256(signingKey: dateRegionKey, plainText: service)
        
        return hmacSha256(signingKey: dateRegionServiceKey, plainText: "aws4_request")
    }
    
    internal func do_sign( signingKey: Data,
                          stringToSign: String) -> String {
        let hmac = HMAC<SHA256>.authenticationCode(for: stringToSign.data(using: .utf8)!,
                                                   using: SymmetricKey(data: signingKey))
        return Data(hmac).hexEncodedString
    }
}

extension Date {
    func toAWSTimestamp() -> String {
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyyMMdd'T'HHmmssZZZZZ"
        dateFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        
        return dateFormatter.string(from: self)
    }
}




