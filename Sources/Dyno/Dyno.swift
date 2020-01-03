//
//  Dyno.swift
//  
//
//  Created by RedPanda on 30-Oct-19.
//

import Foundation

public class Dyno {
    internal let region: String?
    internal let options: DynoOptions
    
    internal let connection: DynoHttpConnection

    public init?( region: String? = nil,
          credentialPath: URL? = nil,
          credentialData: Data? = nil,
          options: DynoOptions = DynoOptions() ) {
        self.region = region
        self.options = options
        
        guard let connection = DynoHttpConnection(credentialPath: credentialPath, credentialData: credentialData, region: region, log: options.log) else { return nil}
        self.connection = connection
    }
}
