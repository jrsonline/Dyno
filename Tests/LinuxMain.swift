import XCTest

import DynoTests

var tests = [XCTestCaseEntry]()
tests += DynoTests.allTests()
XCTMain(tests)