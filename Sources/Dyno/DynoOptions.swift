//
//  File.swift
//  
//
//  Created by RedPanda on 29-Oct-19.
//

import Foundation

/// Options for the Dyno connection
public struct DynoOptions {
    let addVersioning : Bool
    let timeout : Int
    let pageSize : Int?
    let log : Bool
    let dummyUrl : Bool
    /// Options for the Dyno connection
    ///
    /// - Parameters:
    ///   - addVersioning: **Currently unsupported** Defaults to False
    ///   - timeout: Number of seconds before the connection forcibly times out. Defaults to 5.
    ///   - pageSize: Use this to limit the size of pages (ie number of rows) returned by large queries (eg scan). Dyno will automatically concatenate pages together into a full result set, so this is probably not much use right now. Defaults to `nil`, which means the default of 1MB pages.
    ///   - log: Set to **true** to log onto standard output (NSLog).
    public init(addVersioning : Bool = false,
                timeout: Int = 5,
                pageSize: Int? = nil,
                log: Bool = false,
                dummyUrl : Bool = false) {
        self.addVersioning = addVersioning
        self.timeout = timeout
        self.pageSize = pageSize
        self.log = log
        self.dummyUrl = dummyUrl
    }
}
