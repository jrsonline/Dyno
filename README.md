# Dyno 🦕

Reactive, robust AWS DynamoDb integration with Swift that Just Works.

_"NoPython" version_

This library will
1. Calls AWS DynamoDb
2. Provide Combine wrappers for the DynamoDb calls.
3. Provide robust, asynchronous connectivity to AWS, unlike the official AWS library (!)
4. Provide macOS reactive extensions 
5. Feature Emojisaurus 🦕 


## Getting Started
1. Read the section on *Configuration* at  https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html 
2. Create an AWS credentials file at ~/.aws/config as per the instructions; and a  ~/.aws/config file to specify your region. 
3. Run the DynoTests for unit tests, and the DynoEndToEndTests for tests which call AWS


~~~~
let 🦕 = Dyno()

let items = 🦕.scan(table: "Dinosaurs",
                    filter: .beginsWith("name", "Pink"),
                    type: Dinosaur.self)
                    
                    
~~~~
