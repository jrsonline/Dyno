//
//  File.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation
import Combine

internal protocol DynoAction : AWSAction {
}

extension DynoAction {
    func service() -> AWSService {
        return .dynamodb
    }
    
   func httpMethod() -> AWSHTTPVerb {
       .POST
   }
   
   func headers() -> [String : String] {
       [:]
   }
   
   func servicePath() -> String {
       "/"
   }
   
   func queryParameters() -> [String : String] {
       [:]
   }
   
   internal func encodeProjectionExpression(from:Int, projection: [DynoItemPath]?) -> ([String:String]?,Int) {
       guard let projections = projection else { return (nil, 0) }
       var px = [String:String]()
       var idx = from
       for p in projections {
           px["#n\(idx)"] = p
           idx += 1
       }
       return (px, idx)
   }
    
    internal func decodeResult<Response : Decodable>(connection: DynoHttpConnection, from:Response.Type) ->  AnyPublisher<Response, Error> {
        let awsHttpRequest = connection.request(for: self)
        
        return awsHttpRequest.flatMap{ (data:Data) -> AnyPublisher<Response, Error> in
            guard let returned : Response = try? JSONDecoder().decode(Response.self, from: data) else {
                return Fail(error: DynoError("Could not decode response \(data.debugDescription) as \(Response.self)"))
                    .eraseToAnyPublisher()
            }
            return Result<Response, Error>.Publisher(.success(returned))
                .eraseToAnyPublisher()
        }.eraseToAnyPublisher()
    }
    
    internal func constructItem<T : Decodable>(attributes: [String: DynoAttributeValue]?) throws -> [T] {
        if let attributes = attributes {
            let json = DynoAttributeValue.constructJson(attributes)
            if let constructedItem = try? JSONDecoder().decode(T.self, from: json) {
                return [constructedItem]
            } else {
                throw DynoError("Could not construct \(T.self) from \(attributes)")
            }
        } else {
            return []
        }
    }
    
    internal func decodeResultAndConstructItem<Response : Decodable, T : Decodable>(connection: DynoHttpConnection, from:Response.Type, to:T.Type, attributes: KeyPath<Response,[String:DynoAttributeValue]?>, consumed: KeyPath<Response,DynoConsumedCapacity>) -> AnyPublisher<DynoResult<T>, Error> {
        return decodeResult(connection: connection, from: Response.self)
            .tryMap { response in
                let item : [T] = try self.constructItem(attributes: response[keyPath: attributes])
                return DynoResult<T>(result: item, consumedCapacity: response[keyPath: consumed])
        }
        .eraseToAnyPublisher()
    }
}
