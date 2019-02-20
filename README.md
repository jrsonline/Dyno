# Dyno

This application demonstrates:
1. How to call python libraries from Swift, for AWS DynamoDb
2. Write simple wrappers for the python calls into Swift.
2. Convert DynamoDb calls into Observables
3. How to display your Observables


## Getting Started
1. Firstly, get hold of Swift 5.  Currently (for macOS at least) that means downloading the XCode 10.2 beta.
2.  We use the Google/Tensorflow Python.swift integration to bridge Python and Swift easily. To make this even easier, we use Pedro Vieto's _PythonKit_ which ensures we have a working/buildable version of the Python.swift file. So, go to https://github.com/pvieito/PythonKit and follow the instructions about adding packages to your SPM file.
2. As per the DynamoDb instructions https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html  _pip install boto3_.  I had to force the right user:  _pip install boto3 --user_.  You may also need to ignore installed packages: _pip install boto3 --user --ignore-installed six_
3. Create an AWS credentials file at l,~/.aws/config as per the instructions
4. I also had to create a ~/.aws/config file to specify the region.  Note that if the credentials and/or region are not correct, you will get errors like 'can't find resource' from Python.



