//
//  DynoDeleteTable.swift
//  
//
//  Created by RedPanda on 7-Dec-19.
//

import Foundation
import Combine

public extension Dyno {
    /// Deletes a DynamoDB table
    func deleteTable(name: String ) -> AnyPublisher<DynoTableDescription, Error> {
        
        return DynoDeleteTable( name: name,
                                options: self.options)
            .sendRequest(forConnection: self.connection)
            .eraseToAnyPublisher()
    }
    
    /// Starts the deletion of a DynamoDB table and repeatedly checks until it's actually Deleted (returning True).
    /// If the table doesn't exist, will also return True. The publisher will not return anything until True is returned,
    /// unless an error occurs.
    /// - Parameters:
    ///   - name: Table to delete
    ///   - pollInterval: How frequently to check for the deletion - defaults to 1 second
    @available(OSX 15.0, *)
    func deleteTableWaitDeleted(name: String, pollInterval: Double = 1) -> AnyPublisher<Bool, DynoError> {
        
        // the error handler checks to see if we got a "resource not found" error from AWS, which 'almost certainly'
        // means the table is either nonexistent, or has just been deleted. In those cases, we return true, indicating
        // we can stop polling.
        let errorHandler : (Error) -> AnyPublisher<Bool, DynoError> = { error in
            if let asAWSError = (error as? AWSRequestError),
                case .invalidResponse(_, let err) = asAWSError,
                err.contains("resource not found") {
                return Just<Bool>(true).mapError { _ in DynoError("") }.eraseToAnyPublisher()   // table deleted, or doesn't exist... so we return 'true'
            } else {
                return Fail<Bool,DynoError>(error:DynoError(error)).eraseToAnyPublisher() // any other error, fail!
            }
        }
        
        // timer polls for the table state.  This will error out with 'resource not found' when we have successfully
        // deleted the table, which is handled by the errorHandler.
        let timer = Timer.publish(every: pollInterval, on: .main, in: .common)
            .autoconnect()
            .mapError { _ in DynoError("") }  // we need to promote Timer to a DynoError from Never, as describeTable can fail
            .flatMap { _ in self.describeTable(name: name)  }
            .map { _ in false }  // this means.. if we get a value, then we can't have deleted the table
            .catch(errorHandler)
        
        return DynoDeleteTable( name: name,
                                options: self.options)
            .sendRequest(forConnection: self.connection)
            .map { _ in false }  // assume we don't delete immediately
            .catch(errorHandler)
            .append(timer)
            .first(where: {$0 == true})
            .eraseToAnyPublisher()
    }
}

public struct DynoDeleteTable : DynoAction {
    let name: String
    let options: DynoOptions

    func actionName() -> String {
        "DynamoDB_20120810.DeleteTable"
    }
    
    func body() -> String {
        let delRequest = DynoDeleteTableRequest(TableName: self.name)
         
         return  (try? String(data: JSONEncoder().encode(delRequest), encoding: .utf8)) ?? ""
     }
    
    func sendRequest(forConnection conn: DynoHttpConnection) -> AnyPublisher<DynoTableDescription, Error> {
        return self.decodeResult(connection: conn, from: DynoDeleteTableResponse.self)
            .map {  $0.TableDescription }
        .eraseToAnyPublisher()
    }
}

internal struct DynoDeleteTableRequest : Encodable {
    let TableName: String
}

internal struct DynoDeleteTableResponse : Decodable {
    let TableDescription : DynoTableDescription
}
