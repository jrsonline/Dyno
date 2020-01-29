//
//  AWSServiceHelper.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation
import Combine

internal enum AWSHTTPVerb : String {
    case GET
    case PUT
    case POST
}

internal enum AWSService : String {
    case s3
    case dynamodb
    case _mock  /// internal testing only
}

internal enum AWSRequestError : Error {
    case signingError(AWSSignatureError)
    case urlError
    case invalidEndpoint
    case invalidResponse(Int,String)
    case noResponse
    case decodeError
    case apiError(Error)
}

extension URLSession {
    func uploadData(request: URLRequest) -> AnyPublisher<Data,Error>  {
        let p = self.dataTaskPublisher(for: request)
            .tryMap { (output:URLSession.DataTaskPublisher.Output) -> Data in
                guard let response = output.response as? HTTPURLResponse else { throw AWSRequestError.noResponse }
                let statusCode = response.statusCode
                guard 200..<299 ~= statusCode else { throw AWSRequestError.invalidResponse(statusCode, String(data: output.data, encoding: .utf8) ?? "<no information available>") }
                
                return output.data
            }
    .handleEvents(receiveCompletion: {_ in NSLog("Received completion") },
                  receiveCancel:  {NSLog("Received cancel")} )
        .eraseToAnyPublisher()
        
        return p
    }
}
