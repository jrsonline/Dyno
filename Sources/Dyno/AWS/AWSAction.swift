//
//  File.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation

internal protocol AWSAction {
    func actionName() -> String
    func body() -> String
    func httpMethod() -> AWSHTTPVerb
    func headers() -> [String:String]
    func service() -> AWSService
    func servicePath() -> String
    func queryParameters() -> [String:String]
}
