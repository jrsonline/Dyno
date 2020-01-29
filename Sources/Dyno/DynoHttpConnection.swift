//
//  DynoHttpConnection.swift
//  
//
//  Created by RedPanda on 22-Oct-19.
//

import Foundation
import Combine

/// Represents an authenticated (signed) connection to the remote DynamoDb database. 
public class DynoHttpConnection  {
    let signer: AWSSignatureGenerator
    let session: URLSession = URLSession.shared
    let region: String
    let log: Bool
    
    public init?(credentialPath: URL?, credentialData: Data?, region: String?, log: Bool = true) {
        self.log = log
        guard let signer = AWSSignatureGenerator(secretKeyLocation: credentialPath, secretKeyData: credentialData, log: log) else { return nil }
        self.signer = signer
        self.region = AWSRegionLoader.retrieve(for: region, log: log)
    }
    
    internal func request(for action: AWSAction) -> AnyPublisher<Data, Error> {
        return AWSHTTPRequest(
            region:self.region,
            action: action,
            signer: signer,
            log: self.log).request(forSession: self.session)
    }
    
    public func _reset() {
        session.reset {
            NSLog("Connection reset")
        }
    }
}
