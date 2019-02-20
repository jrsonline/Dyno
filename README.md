# Dyno 🦕

_Pre-Alpha version_

This library will
1. Call python libraries from Swift, for AWS DynamoDb
2. Provide Reactive wrappers for the DynamoDb calls.
3. Provide robust, asynchronous connectivity to AWS, unlike the official AWS library (!)
4. Provide macOS reactive extensions 


## Getting Started
1. Firstly, get hold of Swift 5.  Currently (for macOS at least) that means downloading the XCode 10.2 beta.
2.  We use the Google/Tensorflow Python.swift integration to bridge Python and Swift easily. To make this even easier, we use Pedro Vieto's _PythonKit_ which ensures we have a working/buildable version of the Python.swift file. So, go to https://github.com/pvieito/PythonKit and follow the instructions about adding packages to your SPM file.
2. As per the DynamoDb instructions https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html  _pip install boto3_.  You might have to force the  user to get it to install:  _pip install boto3 --user_.  You may also need to ignore installed packages: _pip install boto3 --user --ignore-installed six_
3. Create an AWS credentials file at ~/.aws/credentials as per the instructions
4. You may need to create a ~/.aws/config file to specify the region.  Note that if the credentials and/or region are not correct, you will get errors like 'can't find resource' from Python.


## Example
~~~~
let 🦕 = Dyno()

let items = 🦕.selectItems( fromTable: "InstanceTable",
                            matching: "owner",
                            equals: "jrsonline",
                            building: Instance.builder )

items.asDriver() --> rxTableView.rx.rows ..> disposeBag
~~~~
