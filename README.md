# Dyno 🦕

Reactive, robust AWS DynamoDb integration with Swift that Just Works.

_"Testability" version_

This library will
1. Call python libraries from Swift, for AWS DynamoDb
2. Provide Reactive wrappers for the DynamoDb calls.
3. Provide robust, asynchronous connectivity to AWS, unlike the official AWS library (!)
4. Provide macOS reactive extensions 
5. Feature Emojisaurus 🦕 


## Getting Started
1. Firstly, get hold of Swift 5.  Currently (for macOS at least) that means downloading the XCode 10.2 beta.
2.  We use the Google/Tensorflow Python.swift integration to bridge Python and Swift easily. To make this even easier, we use Pedro Vieto's _PythonKit_ which ensures we have a working/buildable version of the Python.swift file. So, go to https://github.com/pvieito/PythonKit and follow the instructions about adding packages to your SPM file.
3. As per the DynamoDb instructions https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html  _pip install boto3_.  I had to force the right user:  _pip install boto3 --user_.  You may also need to ignore installed packages: _pip install boto3 --user --ignore-installed six_
4. Create an AWS credentials file at l,~/.aws/config as per the instructions
5. I also had to create a ~/.aws/config file to specify the region.  Note that if the credentials and/or region are not correct, you will get errors like 'can't find resource' from Python.


~~~~
let 🦕 = Dyno()

let items = 🦕.selectItems( fromTable: "InstanceTable",
                            matching: "owner",
                            equals: "jrsonline",
                            building: Instance.builder )

items.asDriver() --> rxTableView.rx.rows ..> disposeBag
~~~~
