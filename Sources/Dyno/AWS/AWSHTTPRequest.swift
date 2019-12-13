//
//  HTTPRequest.swift
//  
//
//  Created by RedPanda on 16-Oct-19.
//

import Foundation
import Combine


/// Represents a HTTP request being made to AWS
struct AWSHTTPRequest {
    let region: String
    let action: AWSAction
    let signer: AWSSignatureGenerator
    let log: Bool
    
    let id = UUID()
    
    init(region: String,
         action: AWSAction,
         signer: AWSSignatureGenerator,
         log: Bool = false) {
        self.region = region
        self.action = action
        self.signer = signer
        self.log = log
    }
    
    
    /// Logs interesting parts of the AWSHTTPRequest object to NSLog.
    /// Hides the Authorization key.
    func log(urlRequest: URLRequest, urlRequestId : String, amLogging: Bool = true) {
        guard amLogging else { return }
                
        let headers = urlRequest.allHTTPHeaderFields?.map { key,value in
            if key == "Authorization" {
                return #""Authorization":<hidden>"#
            } else {
                return #""\#(key)":"\#(value)""#
            }
        }.joined(separator: ",")
        ?? ""
        
        let data: String
        if let body = urlRequest.httpBody {
            data = String(data: body, encoding: .utf8) ?? "<unreadable>"
        } else {
            data = "<empty>"
        }
        
        let url = (try? urlRequest.url.map (String.init)) ?? ""
        
        NSLog("AWSHTTPRequest[id=\(urlRequestId)][url:\(url), method:\(urlRequest.httpMethod ?? "<none>"), headers:{\(headers)}, body:\"\(data)\"]")
    }
    
    func request(forSession session: URLSession, date: Date = Date() ) -> AnyPublisher<Data, Error> {
        guard let url = URL(string:"https://\(action.service().rawValue).\(self.region).amazonaws.com/") else {
            return Fail(error: AWSRequestError.urlError).eraseToAnyPublisher()
        }
        
        let body = action.body()
        if self.log {
            NSLog("\(action.actionName()) request:")
            NSLog(body)
        }
        
        let request = AWSSignableRequest(
            date: date,
            verb: action.httpMethod(),
            host: url.host ?? "",
            path: action.servicePath(),
            region: self.region,
            service: action.service().rawValue,
            queryParameters: action.queryParameters(),
            headers: action.headers(),
            payload: body)
        
        let authKeyResult = signer.authorize(request: request)
        guard let authKey = authKeyResult.asSuccess() else {
            return Fail(error: AWSRequestError.signingError(authKeyResult.asFailure()! ) ).eraseToAnyPublisher()
        }
            
        // enrich the URL with the query parameters
        guard var urlComponents = URLComponents(url: url, resolvingAgainstBaseURL: true) else {
            return Fail(error: AWSRequestError.urlError).eraseToAnyPublisher()
        }
        if !action.queryParameters().isEmpty {
            urlComponents.queryItems = action.queryParameters().map {URLQueryItem(name: $0, value: $1) }
        }
        
        // Create the Foundation view
        guard var urlRequest = (urlComponents.url.map {URLRequest(url: $0)}) else {
            return Fail(error: AWSRequestError.urlError).eraseToAnyPublisher()
        }
        
        // Set AWS service headers
        urlRequest.addValue(authKey, forHTTPHeaderField: "Authorization")
        urlRequest.addValue("application/x-amz-json-1.0", forHTTPHeaderField: "Content-Type")
        urlRequest.addValue(action.actionName(), forHTTPHeaderField: "X-Amz-Target")
        
        // Set the request type
        urlRequest.httpMethod = action.httpMethod().rawValue
        
        // Add the other headers
        for h in request.extendedHeaders {
            urlRequest.addValue(h.value, forHTTPHeaderField: h.key)
        }
        
        // Add the body
        urlRequest.httpBody = Data(body.utf8)
        
        self.log(urlRequest: urlRequest, urlRequestId: self.id.description, amLogging: self.log)
        
        return session.uploadData(request: urlRequest).map { result in
            if (self.log) {
                NSLog("AWSHTTPRequest Result[id=\(self.id)] \(String(data:result, encoding:.utf8) ?? "<unreadable>")]")
            }
            return result
        }.eraseToAnyPublisher()
    }
}
