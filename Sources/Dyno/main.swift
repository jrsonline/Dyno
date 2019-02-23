import Foundation
import PythonKit

let boto3 = Python.import("boto3")
let dynamodb = boto3.resource("dynamodb")
let table = dynamodb.Table("Dinosaurs")

print( table.scan() )

table.put_item(
    Item:[
        "id" : "1",
        "name" : "Emojisaurus",
        "colour" : "blue"
])

print( table.scan() )
