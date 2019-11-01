//
//  Dyno.swift
//  
//
//  Created by RedPanda on 30-Oct-19.
//

import Foundation

public class Dyno {
    internal let region: String?
    internal let credentialPath: URL?
    internal let options: DynoOptions
    
    internal let connection: DynoHttpConnection

    init?( region: String? = nil,
          credentialPath: URL? = nil,
          options: DynoOptions = DynoOptions() ) {
        self.region = region
        self.credentialPath = credentialPath
        self.options = options
        
        guard let connection = DynoHttpConnection(credentialPath: self.credentialPath, region: region, log: options.log) else { return nil}
        self.connection = connection
    }
}
